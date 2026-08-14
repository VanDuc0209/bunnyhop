package bunnyhop

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config cấu hình cho Client
type Config struct {
	URLs                []string      // Danh sách URLs của RabbitMQ (thử lần lượt khi connect)
	ReconnectInterval   time.Duration // Khoảng thời gian cơ sở giữa các lần reconnect (exponential backoff)
	MaxReconnectAttempt int           // Số lần thử reconnect tối đa (0 = unlimited)
	Heartbeat           time.Duration // Heartbeat interval (default 10s)
	ConnectionName      string        // Tên connection hiển thị trên RabbitMQ UI
	DebugLog            bool          // Bật/tắt debug log
	Logger              Logger        // Custom logger interface

	// QoS settings — applied to the shared channel on connect
	PrefetchCount  int  // Max unacknowledged messages. Default: 10
	PrefetchSize   int  // Max unacknowledged bytes. 0 = no limit
	PrefetchGlobal bool // Apply globally to connection vs per-channel
}

// Validate kiểm tra Config hợp lệ trước khi dùng
func (c Config) Validate() error {
	if len(c.URLs) == 0 {
		return fmt.Errorf("config: at least one URL is required")
	}
	if c.MaxReconnectAttempt < 0 {
		return fmt.Errorf("config: MaxReconnectAttempt must be >= 0 (0 = unlimited)")
	}
	if c.PrefetchCount < 0 {
		return fmt.Errorf("config: PrefetchCount must be >= 0")
	}
	return nil
}

// exchangeMeta lưu thông tin exchange để re-declare sau reconnect
type exchangeMeta struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	args       amqp.Table
}

// queueMeta lưu thông tin queue để re-declare sau reconnect
type queueMeta struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	args       amqp.Table
}

// bindMeta lưu thông tin binding để re-bind sau reconnect
type bindMeta struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     amqp.Table
}

// Client quản lý một kết nối AMQP với auto-reconnect.
// Thread-safe cho tất cả public methods.
type Client struct {
	config       Config
	connection   *amqp.Connection
	channel      *amqp.Channel
	mutex        sync.RWMutex
	publishMutex sync.Mutex // serializes all channel writes (amqp.Channel is not thread-safe)
	connected    bool
	reconnecting bool

	// reconnectAttempts được tăng mỗi lần reconnect thất bại, reset khi thành công
	reconnectAttempts int

	ctx          context.Context
	cancel       context.CancelFunc
	workerCancel context.CancelFunc // cancel reconnectWorker goroutine khi reconnect

	// Metadata dùng map để dedup — tránh unbounded growth khi caller declare nhiều lần
	declaredExchanges map[string]exchangeMeta
	declaredQueues    map[string]queueMeta
	boundQueues       map[string]bindMeta // key: "queue|routingKey|exchange"
}

// NewClient tạo Client mới. Chưa connect — cần gọi Connect() sau.
func NewClient(config Config) *Client {
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = 5 * time.Second
	}
	if config.Heartbeat <= 0 {
		config.Heartbeat = 10 * time.Second
	}
	if config.PrefetchCount <= 0 {
		config.PrefetchCount = 10
	}
	if config.Logger == nil {
		config.Logger = NewDefaultLogger(config.DebugLog)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Client{
		config:            config,
		ctx:               ctx,
		cancel:            cancel,
		declaredExchanges: make(map[string]exchangeMeta),
		declaredQueues:    make(map[string]queueMeta),
		boundQueues:       make(map[string]bindMeta),
	}
}

// Connect thiết lập kết nối đến RabbitMQ.
// Thread-safe: safe to call concurrently, idempotent khi đã connected.
func (c *Client) Connect(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connected {
		return nil
	}

	c.logger().Debug("Connecting to RabbitMQ...")

	var lastErr error
	for _, u := range c.config.URLs {
		if err := c.connectToURL(u); err != nil {
			lastErr = err
			// SECURITY: mask password before logging
			c.logger().Warn("Failed to connect to %s: %v", maskAMQPURL(u), err)
			continue
		}
		c.logger().Info("Successfully connected to %s", maskAMQPURL(u))
		return nil
	}

	return fmt.Errorf("failed to connect to any RabbitMQ server: %v", lastErr)
}

// connectToURL tạo connection và channel tới một URL cụ thể.
// Phải được gọi trong khi giữ c.mutex.Lock().
func (c *Client) connectToURL(rawURL string) error {
	props := amqp.NewConnectionProperties()
	if c.config.ConnectionName != "" {
		props.SetClientConnectionName(c.config.ConnectionName)
	}

	amqpCfg := amqp.Config{
		Heartbeat:  c.config.Heartbeat,
		Properties: props,
		Locale:     "en_US",
	}

	conn, err := amqp.DialConfig(rawURL, amqpCfg)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS — configurable, default 10
	if err := ch.Qos(c.config.PrefetchCount, c.config.PrefetchSize, c.config.PrefetchGlobal); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Cancel previous worker goroutine before replacing connection
	if c.workerCancel != nil {
		c.workerCancel()
	}
	workerCtx, workerCancel := context.WithCancel(c.ctx)
	c.workerCancel = workerCancel

	c.connection = conn
	c.channel = ch
	c.connected = true
	c.reconnectAttempts = 0
	c.reconnecting = false

	// Re-declare exchanges, queues, bindings after reconnect
	for _, ex := range c.declaredExchanges {
		if err := ch.ExchangeDeclare(ex.name, ex.kind, ex.durable, ex.autoDelete, ex.internal, false, ex.args); err != nil {
			c.logger().Warn("Failed to re-declare exchange %s: %v", ex.name, err)
		}
	}
	for _, q := range c.declaredQueues {
		if _, err := ch.QueueDeclare(q.name, q.durable, q.autoDelete, q.exclusive, false, q.args); err != nil {
			c.logger().Warn("Failed to re-declare queue %s: %v", q.name, err)
		}
	}
	for _, b := range c.boundQueues {
		if err := ch.QueueBind(b.name, b.key, b.exchange, b.noWait, b.args); err != nil {
			c.logger().Warn("Failed to re-bind queue %s to exchange %s: %v", b.name, b.exchange, err)
		}
	}

	// Subscribe to close notifications — buffered to avoid blocking broker
	connCloseChan := conn.NotifyClose(make(chan *amqp.Error, 1))
	chCloseChan := ch.NotifyClose(make(chan *amqp.Error, 1))

	go c.reconnectWorker(workerCtx, connCloseChan, chCloseChan)

	return nil
}

// reconnectWorker monitors connection and channel close events.
// It exits after the first event (a new worker is spawned after each reconnect).
// FIX #2: handles graceful close (ok=true, err=nil) — previously silent disconnect.
func (c *Client) reconnectWorker(ctx context.Context, connClose, chClose <-chan *amqp.Error) {
	select {
	case <-ctx.Done():
		return

	case err, ok := <-connClose:
		if err != nil {
			c.logger().Error("RabbitMQ connection closed with error: %v", err)
		} else if !ok {
			// Graceful close — still need to reconnect
			c.logger().Warn("RabbitMQ connection closed gracefully")
		} else {
			// ok=true && err==nil: channel open, nothing happened — rare
			return
		}
		c.handleDisconnection()

	case err, ok := <-chClose:
		if err != nil {
			c.logger().Error("RabbitMQ channel closed with error: %v", err)
		} else if !ok {
			c.logger().Warn("RabbitMQ channel closed gracefully")
		} else {
			return
		}
		c.handleDisconnection()
	}
}

// handleDisconnection marks the client as disconnected and starts the reconnect loop.
// Safe to call from multiple goroutines — the reconnecting flag prevents duplicate reconnects.
func (c *Client) handleDisconnection() {
	c.mutex.Lock()
	if c.reconnecting {
		c.mutex.Unlock()
		return
	}
	c.connected = false
	c.reconnecting = true
	c.mutex.Unlock()

	c.logger().Warn("Connection lost, starting reconnect loop...")
	go c.reconnect()
}

// reconnect is the single reconnect loop goroutine.
// FIX #1: iterative loop instead of recursive go c.reconnect() — prevents goroutine explosion.
// Uses exponential backoff with jitter to avoid thundering herd.
func (c *Client) reconnect() {
	for {
		c.mutex.Lock()

		// Check if already reconnected (e.g. by another path)
		if c.connected {
			c.reconnecting = false
			c.mutex.Unlock()
			return
		}

		c.reconnectAttempts++
		attempt := c.reconnectAttempts

		if c.config.MaxReconnectAttempt != 0 && attempt > c.config.MaxReconnectAttempt {
			c.logger().Error("Max reconnect attempts (%d) reached, giving up", c.config.MaxReconnectAttempt)
			c.reconnecting = false
			c.mutex.Unlock()
			return
		}

		c.logger().Info("Reconnect attempt %d (max: %d)", attempt, c.config.MaxReconnectAttempt)
		c.mutex.Unlock()

		// Exponential backoff with jitter — prevents thundering herd
		delay := exponentialBackoff(attempt, c.config.ReconnectInterval)
		timer := time.NewTimer(delay)

		select {
		case <-c.ctx.Done():
			// Client is being closed — stop reconnecting
			timer.Stop()
			c.mutex.Lock()
			c.reconnecting = false
			c.mutex.Unlock()
			return
		case <-timer.C:
		}

		if err := c.Connect(c.ctx); err != nil {
			c.logger().Error("Reconnect attempt %d failed: %v", attempt, err)
			// Continue loop — do NOT spawn a new goroutine
		}
		// If Connect succeeded, next iteration will see c.connected=true and return
	}
}

// IsConnected reports whether the client currently has an open connection.
func (c *Client) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.connected && c.connection != nil && !c.connection.IsClosed()
}

// GetChannel returns the current AMQP channel.
// Warning: amqp.Channel is NOT thread-safe — callers must not use it concurrently.
// For safe concurrent publish, use PublishMessage instead.
func (c *Client) GetChannel() (*amqp.Channel, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if !c.connected || c.channel == nil || c.channel.IsClosed() {
		return nil, fmt.Errorf("client is not connected")
	}

	return c.channel, nil
}

// GetConnection returns the current AMQP connection.
func (c *Client) GetConnection() (*amqp.Connection, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if !c.connected || c.connection == nil || c.connection.IsClosed() {
		return nil, fmt.Errorf("client is not connected")
	}

	return c.connection, nil
}

// Close gracefully shuts down the client:
// cancels reconnect loops, closes channel, closes connection.
func (c *Client) Close() error {
	// Cancel context first — stops reconnect loop and worker goroutine
	c.cancel()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.workerCancel != nil {
		c.workerCancel()
	}

	var errs []error
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close channel: %w", err))
		}
		c.channel = nil
	}
	if c.connection != nil {
		if err := c.connection.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close connection: %w", err))
		}
		c.connection = nil
	}

	c.connected = false
	c.reconnecting = false

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}

// logger is a helper to access the configured logger.
func (c *Client) logger() Logger {
	return c.config.Logger
}

// PublishMessage publishes a message to the given exchange with the given routing key.
// Thread-safe: serialized via publishMutex (amqp.Channel is not thread-safe).
// FIX #3/#9: re-checks connection state under publishMutex to avoid stale channel race.
func (c *Client) PublishMessage(
	exchange, routingKey string,
	mandatory, immediate bool,
	msg amqp.Publishing,
) error {
	c.publishMutex.Lock()
	defer c.publishMutex.Unlock()

	// Re-read state under publishMutex to avoid TOCTOU with reconnect
	c.mutex.RLock()
	connected := c.connected
	ch := c.channel
	c.mutex.RUnlock()

	if !connected || ch == nil {
		return fmt.Errorf("client is not connected")
	}

	return ch.Publish(exchange, routingKey, mandatory, immediate, msg)
}

// PublishWithConfirm publishes a message and waits for broker acknowledgment.
// Returns an error if the broker nacks, the context is cancelled, or timeout elapses.
// Note: enables confirm mode on the shared channel — do not mix with regular Publish in the same goroutine.
func (c *Client) PublishWithConfirm(
	ctx context.Context,
	exchange, routingKey string,
	msg amqp.Publishing,
	timeout time.Duration,
) error {
	c.publishMutex.Lock()
	defer c.publishMutex.Unlock()

	c.mutex.RLock()
	connected := c.connected
	ch := c.channel
	c.mutex.RUnlock()

	if !connected || ch == nil {
		return fmt.Errorf("client is not connected")
	}

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable confirm mode: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := ch.Publish(exchange, routingKey, false, false, msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("publish confirm timeout after %v", timeout)
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("message was nacked by broker")
		}
		return nil
	}
}

// DeclareQueue declares a queue. The declaration is stored and replayed after reconnect.
// FIX #12: uses map dedup — calling DeclareQueue with same name is idempotent.
func (c *Client) DeclareQueue(
	name string,
	durable, autoDelete, exclusive bool,
	args amqp.Table,
) (amqp.Queue, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.connected || c.channel == nil {
		return amqp.Queue{}, fmt.Errorf("client is not connected")
	}

	// Dedup by name — prevents unbounded memory growth
	c.declaredQueues[name] = queueMeta{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		args:       args,
	}

	return c.channel.QueueDeclare(name, durable, autoDelete, exclusive, false, args)
}

// DeclareExchange declares an exchange. The declaration is stored and replayed after reconnect.
func (c *Client) DeclareExchange(
	name, kind string,
	durable, autoDelete, internal bool,
	args amqp.Table,
) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.connected || c.channel == nil {
		return fmt.Errorf("client is not connected")
	}

	// Dedup by name
	c.declaredExchanges[name] = exchangeMeta{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		args:       args,
	}

	return c.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, false, args)
}

// QueueBind binds a queue to an exchange. The binding is stored and replayed after reconnect.
func (c *Client) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.connected || c.channel == nil {
		return fmt.Errorf("client is not connected")
	}

	// Dedup by composite key
	bindKey := fmt.Sprintf("%s|%s|%s", name, key, exchange)
	c.boundQueues[bindKey] = bindMeta{
		name:     name,
		key:      key,
		exchange: exchange,
		noWait:   noWait,
		args:     args,
	}

	return c.channel.QueueBind(name, key, exchange, noWait, args)
}
