package bunnyhop

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Pool quản lý pool các kết nối đến cluster RabbitMQ
type Pool struct {
	config       PoolConfig
	nodes        []*NodeConnection
	mutex        sync.RWMutex
	closed       bool
	roundRobin   int64
	logger       Logger
	ctx          context.Context
	cancel       context.CancelFunc
	healthTicker *time.Ticker

	// Metrics
	totalRequests int64
	totalFailures int64
}

// NewPool tạo pool mới
func NewPool(config PoolConfig) *Pool {
	getDefaultConfig(&config)

	ctx, cancel := context.WithCancel(context.Background())

	pool := &Pool{
		config: config,
		nodes:  make([]*NodeConnection, 0, len(config.URLs)),
		logger: config.Logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// Khởi tạo nodes
	for i, url := range config.URLs {
		node := &NodeConnection{
			URL:      url,
			Client:   nil,
			healthy:  false,
			weight:   1, // Default weight
			lastUsed: time.Now(),
		}
		pool.nodes = append(pool.nodes, node)
		pool.logger.Debug("Initialized node %d: %s", i, url)
	}

	return pool
}

// Start bắt đầu pool
func (p *Pool) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return fmt.Errorf("pool is closed")
	}

	// Tạo connection cho mỗi node
	for _, node := range p.nodes {
		go p.connectToNode(node)
	}

	// Bắt đầu health check goroutine
	p.healthTicker = time.NewTicker(p.config.HealthCheckInterval)
	go p.healthCheckWorker()

	p.logger.Info("Pool started with %d nodes", len(p.nodes))
	return nil
}

// connectToNode tạo connection đến một node
func (p *Pool) connectToNode(node *NodeConnection) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	if node.connecting {
		return
	}
	node.connecting = true
	defer func() { node.connecting = false }()

	p.logger.Debug("Connecting to node %s", node.URL)

	client := NewClient(Config{
		URLs:                []string{node.URL},
		ReconnectInterval:   p.config.ReconnectInterval,
		MaxReconnectAttempt: p.config.MaxReconnectAttempt,
		Heartbeat:           p.config.Heartbeat,
		ConnectionName:      p.config.ConnectionName,
		DebugLog:            p.config.DebugLog,
		Logger:              p.logger,
	})

	err := client.Connect(p.ctx)
	if err != nil {
		p.logger.Error("Failed to connect to node %s: %v", node.URL, err)
		atomic.AddInt64(&node.failures, 1)
		node.healthy = false

		// Thử reconnect sau một khoảng thời gian
		time.AfterFunc(p.config.ReconnectInterval, func() {
			p.connectToNode(node)
		})
		return
	}

	// Nếu đã có client cũ, đóng nó
	if node.Client != nil {
		_ = node.Client.Close()
	}

	node.Client = client
	node.healthy = true
	p.logger.Info("Successfully connected to node %s", node.URL)

	// Theo dõi trạng thái connection
	go p.watchNodeConnection(node)
}

// watchNodeConnection theo dõi trạng thái connection của node
func (p *Pool) watchNodeConnection(node *NodeConnection) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			node.mutex.RLock()
			if node.Client != nil && !node.Client.IsConnected() {
				node.mutex.RUnlock()
				node.mutex.Lock()
				node.healthy = false
				node.mutex.Unlock()
				p.logger.Warn("Node %s connection lost", node.URL)

				// Tự động reconnect
				go p.connectToNode(node)
				return
			}
			node.mutex.RUnlock()
		}
	}
}

// GetClient lấy một client từ pool theo load balancing strategy
func (p *Pool) GetClient() (*Client, error) {
	atomic.AddInt64(&p.totalRequests, 1)

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("pool is closed")
	}

	var selectedNode *NodeConnection
	var err error

	switch p.config.LoadBalanceStrategy {
	case RoundRobin:
		selectedNode, err = p.getClientRoundRobin()
	case Random:
		selectedNode, err = p.getClientRandom()
	case LeastUsed:
		selectedNode, err = p.getClientLeastUsed()
	case WeightedRoundRobin:
		selectedNode, err = p.getClientWeightedRoundRobin()
	default:
		selectedNode, err = p.getClientRoundRobin()
	}

	if err != nil {
		atomic.AddInt64(&p.totalFailures, 1)
		return nil, err
	}

	// Update usage stats
	selectedNode.mutex.Lock()
	atomic.AddInt64(&selectedNode.totalUsed, 1)
	selectedNode.lastUsed = time.Now()
	client := selectedNode.Client
	selectedNode.mutex.Unlock()

	return client, nil
}

// getClientRoundRobin lựa chọn client theo round robin
func (p *Pool) getClientRoundRobin() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	index := int(atomic.AddInt64(&p.roundRobin, 1)) % len(healthyNodes)
	return healthyNodes[index], nil
}

// getClientRandom lựa chọn client ngẫu nhiên
func (p *Pool) getClientRandom() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	index := rand.Intn(len(healthyNodes))
	return healthyNodes[index], nil
}

// getClientLeastUsed lựa chọn node ít được sử dụng nhất
func (p *Pool) getClientLeastUsed() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	var selectedNode *NodeConnection
	minUsed := int64(^uint64(0) >> 1) // Max int64

	for _, node := range healthyNodes {
		used := atomic.LoadInt64(&node.totalUsed)
		if used < minUsed {
			minUsed = used
			selectedNode = node
		}
	}

	return selectedNode, nil
}

// getClientWeightedRoundRobin lựa chọn theo weighted round robin
func (p *Pool) getClientWeightedRoundRobin() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	// Tính tổng weight
	totalWeight := 0
	for _, node := range healthyNodes {
		totalWeight += node.weight
	}

	if totalWeight == 0 {
		// Fallback to round robin
		return p.getClientRoundRobin()
	}

	// Random selection based on weight
	randWeight := rand.Intn(totalWeight)
	currentWeight := 0

	for _, node := range healthyNodes {
		currentWeight += node.weight
		if randWeight < currentWeight {
			return node, nil
		}
	}

	// Fallback to first healthy node
	return healthyNodes[0], nil
}

// getHealthyNodes trả về danh sách nodes đang healthy
func (p *Pool) getHealthyNodes() []*NodeConnection {
	var healthyNodes []*NodeConnection
	for _, node := range p.nodes {
		node.mutex.RLock()
		if node.healthy && node.Client != nil && node.Client.IsConnected() {
			healthyNodes = append(healthyNodes, node)
		}
		node.mutex.RUnlock()
	}
	return healthyNodes
}

// healthCheckWorker worker để thực hiện health check định kỳ
func (p *Pool) healthCheckWorker() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.healthTicker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck thực hiện health check cho tất cả nodes
func (p *Pool) performHealthCheck() {
	p.logger.Debug("Performing health check on all nodes")

	for _, node := range p.nodes {
		go p.checkNodeHealth(node)
	}
}

// checkNodeHealth kiểm tra health của một node
func (p *Pool) checkNodeHealth(node *NodeConnection) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	if node.Client == nil {
		node.healthy = false
		p.logger.Debug("Node %s has no client", node.URL)

		// Thử tạo connection
		go p.connectToNode(node)
		return
	}

	// Kiểm tra connection
	if !node.Client.IsConnected() {
		node.healthy = false
		p.logger.Debug("Node %s connection is not healthy", node.URL)

		// Thử reconnect
		go p.connectToNode(node)
		return
	}

	// Node đang healthy
	if !node.healthy {
		node.healthy = true
		p.logger.Info("Node %s is now healthy", node.URL)
	}
}

// GetStats lấy thống kê của pool
func (p *Pool) GetStats() PoolStats {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	stats := PoolStats{
		TotalNodes:    len(p.nodes),
		TotalRequests: p.totalRequests,
		TotalFailures: p.totalFailures,
		NodesStats:    make([]NodeStats, 0, len(p.nodes)),
	}

	for _, node := range p.nodes {
		node.mutex.RLock()
		nodeStat := NodeStats{
			URL:       node.URL,
			Healthy:   node.healthy,
			Connected: node.Client != nil && node.Client.IsConnected(),
			TotalUsed: node.totalUsed,
			Failures:  node.failures,
			Weight:    node.weight,
			LastUsed:  node.lastUsed.Format(time.RFC3339),
		}
		node.mutex.RUnlock()

		if nodeStat.Healthy {
			stats.HealthyNodes++
		}

		stats.NodesStats = append(stats.NodesStats, nodeStat)
	}

	return stats
}

// Close đóng pool
func (p *Pool) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	// Hủy context
	if p.cancel != nil {
		p.cancel()
	}

	// Dừng health check
	if p.healthTicker != nil {
		p.healthTicker.Stop()
	}

	// Đóng tất cả nodes
	var errs []error
	for _, node := range p.nodes {
		if node.Client != nil {
			if err := node.Client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close node %s: %v", node.URL, err))
			}
		}
	}

	p.logger.Info("Pool closed")

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// SetNodeWeight thiết lập weight cho một node
func (p *Pool) SetNodeWeight(url string, weight int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, node := range p.nodes {
		if node.URL == url {
			node.mutex.Lock()
			node.weight = weight
			node.mutex.Unlock()
			p.logger.Info("Set weight for node %s to %d", url, weight)
			return nil
		}
	}

	return fmt.Errorf("node not found: %s", url)
}

// GetHealthyNodeCount trả về số lượng nodes đang healthy
func (p *Pool) GetHealthyNodeCount() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	count := 0
	for _, node := range p.nodes {
		node.mutex.RLock()
		if node.healthy && node.Client != nil && node.Client.IsConnected() {
			count++
		}
		node.mutex.RUnlock()
	}
	return count
}
