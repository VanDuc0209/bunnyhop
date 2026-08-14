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
	URLs                []string      // Danh sách URLs của RabbitMQ
	ReconnectInterval   time.Duration // Thời gian chờ giữa các lần reconnect
	MaxReconnectAttempt int           // Số lần thử reconnect tối đa
	Heartbeat           time.Duration // Heartbeat interval (mặc định 10s)
	ConnectionName      string        // Tên connection hiển thị trên RabbitMQ UI
	DebugLog            bool          // Bật/tắt debug log
	Logger              Logger        // Custom logger interface
}

type exchangeMeta struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	args       amqp.Table
}

type queueMeta struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	args       amqp.Table
}

type bindMeta struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     amqp.Table
}

// Client quản lý kết nối đến RabbitMQ
type Client struct {
	config            Config
	connection        *amqp.Connection
	channel           *amqp.Channel
	mutex             sync.RWMutex
	publishMutex      sync.Mutex // Bảo vệ channel khi publish đồng thời từ nhiều goroutine
	connected         bool
	reconnectAttempts int
	ctx               context.Context
	cancel            context.CancelFunc
	workerCancel      context.CancelFunc // Hủy worker cũ khi reconnect để tránh rò rỉ goroutine
	reconnecting      bool

	// Metadata lưu lại để tự động khai báo lại khi reconnect
	declaredExchanges []exchangeMeta
	declaredQueues    []queueMeta
	boundQueues       []bindMeta
}

// NewClient tạo client mới
func NewClient(config Config) *Client {
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = 5 * time.Second
	}
	if config.Heartbeat <= 0 {
		config.Heartbeat = 10 * time.Second
	}
	if config.Logger == nil {
		config.Logger = NewDefaultLogger(config.DebugLog)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Client{
		config:            config,
		ctx:               ctx,
		cancel:            cancel,
		declaredExchanges: make([]exchangeMeta, 0),
		declaredQueues:    make([]queueMeta, 0),
		boundQueues:       make([]bindMeta, 0),
	}
}

// Connect thiết lập kết nối đến RabbitMQ
func (c *Client) Connect(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connected {
		return nil
	}

	c.logger().Debug("Connecting to RabbitMQ...")

	var lastErr error
	for _, url := range c.config.URLs {
		if err := c.connectToURL(url); err != nil {
			lastErr = err
			c.logger().Warn("Failed to connect to %s: %v", url, err)
			continue
		}
		c.logger().Info("Successfully connected to %s", url)
		return nil
	}

	return fmt.Errorf("failed to connect to any RabbitMQ server: %v", lastErr)
}

// connectToURL kết nối đến một URL cụ thể
func (c *Client) connectToURL(url string) error {
	props := amqp.NewConnectionProperties()
	if c.config.ConnectionName != "" {
		props.SetClientConnectionName(c.config.ConnectionName)
	}

	amqpCfg := amqp.Config{
		Heartbeat:  c.config.Heartbeat,
		Properties: props,
		Locale:     "en_US",
	}

	conn, err := amqp.DialConfig(url, amqpCfg)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Hủy worker cũ (nếu có) để tránh rò rỉ goroutine
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

	// Tự động re-declare lại Exchange / Queue / Binding đã đăng ký trước đó
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
			c.logger().Warn("Failed to re-bind queue %s to %s: %v", b.name, b.exchange, err)
		}
	}

	// Lắng nghe lỗi đóng kết nối
	connCloseChan := conn.NotifyClose(make(chan *amqp.Error, 1))
	chCloseChan := ch.NotifyClose(make(chan *amqp.Error, 1))

	go c.reconnectWorker(workerCtx, connCloseChan, chCloseChan)

	return nil
}

// reconnectWorker xử lý theo dõi lỗi đóng kết nối
func (c *Client) reconnectWorker(ctx context.Context, connClose, chClose chan *amqp.Error) {
	select {
	case <-ctx.Done():
		return
	case err, ok := <-connClose:
		if ok && err != nil {
			c.logger().Error("RabbitMQ Connection error: %v", err)
			c.handleDisconnection()
		}
	case err, ok := <-chClose:
		if ok && err != nil {
			c.logger().Error("RabbitMQ Channel error: %v", err)
			c.handleDisconnection()
		}
	}
}

// handleDisconnection xử lý khi mất kết nối
func (c *Client) handleDisconnection() {
	c.mutex.Lock()
	if c.reconnecting {
		c.mutex.Unlock()
		return
	}
	c.connected = false
	c.reconnecting = true
	c.mutex.Unlock()

	c.logger().Warn("Connection lost, attempting to reconnect...")
	go c.reconnect()
}

// reconnect thực hiện reconnect
func (c *Client) reconnect() {
	c.mutex.Lock()
	if c.connected {
		c.reconnecting = false
		c.mutex.Unlock()
		return
	}

	c.reconnectAttempts++
	if c.config.MaxReconnectAttempt != 0 && c.reconnectAttempts > c.config.MaxReconnectAttempt {
		c.logger().Error("Max reconnection attempts reached")
		c.reconnecting = false
		c.mutex.Unlock()
		return
	}

	c.logger().Info("Reconnection attempt %d/%d", c.reconnectAttempts, c.config.MaxReconnectAttempt)

	if c.channel != nil {
		_ = c.channel.Close()
		c.channel = nil
	}
	if c.connection != nil {
		_ = c.connection.Close()
		c.connection = nil
	}
	c.mutex.Unlock()

	time.Sleep(c.config.ReconnectInterval)

	if err := c.Connect(c.ctx); err != nil {
		c.logger().Error("Reconnection failed: %v", err)
		go c.reconnect()
	}
}

// IsConnected kiểm tra trạng thái kết nối
func (c *Client) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.connected && c.connection != nil && !c.connection.IsClosed()
}

// GetChannel lấy channel hiện tại
func (c *Client) GetChannel() (*amqp.Channel, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if !c.connected || c.channel == nil || c.channel.IsClosed() {
		return nil, fmt.Errorf("client is not connected")
	}

	return c.channel, nil
}

// GetConnection lấy connection hiện tại
func (c *Client) GetConnection() (*amqp.Connection, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if !c.connected || c.connection == nil || c.connection.IsClosed() {
		return nil, fmt.Errorf("client is not connected")
	}

	return c.connection, nil
}

// Close đóng kết nối
func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cancel()
	if c.workerCancel != nil {
		c.workerCancel()
	}

	var errs []error
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close channel: %w", err))
		}
		c.channel = nil
	}
	if c.connection != nil {
		if err := c.connection.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
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

// logger helper để lấy logger
func (c *Client) logger() Logger {
	return c.config.Logger
}

// PublishMessage gửi message
func (c *Client) PublishMessage(
	exchange, routingKey string,
	mandatory, immediate bool,
	msg amqp.Publishing,
) error {
	c.mutex.RLock()
	if !c.connected || c.channel == nil {
		c.mutex.RUnlock()
		return fmt.Errorf("client is not connected")
	}
	ch := c.channel
	c.mutex.RUnlock()

	c.publishMutex.Lock()
	defer c.publishMutex.Unlock()

	return ch.Publish(exchange, routingKey, mandatory, immediate, msg)
}

// DeclareQueue khai báo queue
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

	c.declaredQueues = append(c.declaredQueues, queueMeta{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		args:       args,
	})

	return c.channel.QueueDeclare(name, durable, autoDelete, exclusive, false, args)
}

// DeclareExchange khai báo exchange
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

	c.declaredExchanges = append(c.declaredExchanges, exchangeMeta{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		args:       args,
	})

	return c.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, false, args)
}

// QueueBind bind queue với exchange
func (c *Client) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.connected || c.channel == nil {
		return fmt.Errorf("client is not connected")
	}

	c.boundQueues = append(c.boundQueues, bindMeta{
		name:     name,
		key:      key,
		exchange: exchange,
		noWait:   noWait,
		args:     args,
	})

	return c.channel.QueueBind(name, key, exchange, noWait, args)
}
