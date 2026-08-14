package bunnyhop

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Pool manages a pool of RabbitMQ connections across multiple nodes.
// It provides load balancing and automatic failover.
// Thread-safe for all public methods.
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

	// Metrics — updated atomically
	totalRequests int64
	totalFailures int64
}

// NewPool creates a new Pool. Call Start() to begin connecting.
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

	for i, url := range config.URLs {
		node := &NodeConnection{
			URL:      url,
			Client:   nil,
			healthy:  false,
			weight:   1,
			lastUsed: time.Now(),
		}
		pool.nodes = append(pool.nodes, node)
		pool.logger.Debug("Initialized node %d: %s", i, maskAMQPURL(url))
	}

	return pool
}

// Start begins connecting to all nodes and starts the health check worker.
func (p *Pool) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return fmt.Errorf("pool is closed")
	}

	for _, node := range p.nodes {
		go p.connectToNode(node)
	}

	p.healthTicker = time.NewTicker(p.config.HealthCheckInterval)
	go p.healthCheckWorker()

	p.logger.Info("Pool started with %d nodes", len(p.nodes))
	return nil
}

// connectToNode establishes a connection to a single node.
// Uses a connecting flag to prevent concurrent duplicate connects to the same node.
func (p *Pool) connectToNode(node *NodeConnection) {
	// FIX #7: guard — don't connect if pool is closed
	p.mutex.RLock()
	if p.closed {
		p.mutex.RUnlock()
		return
	}
	p.mutex.RUnlock()

	node.mutex.Lock()
	if node.connecting {
		node.mutex.Unlock()
		return
	}
	node.connecting = true
	node.mutex.Unlock()

	defer func() {
		node.mutex.Lock()
		node.connecting = false
		node.mutex.Unlock()
	}()

	p.logger.Debug("Connecting to node %s", maskAMQPURL(node.URL))

	client := NewClient(Config{
		URLs:                []string{node.URL},
		ReconnectInterval:   p.config.ReconnectInterval,
		MaxReconnectAttempt: p.config.MaxReconnectAttempt,
		Heartbeat:           p.config.Heartbeat,
		ConnectionName:      p.config.ConnectionName,
		DebugLog:            p.config.DebugLog,
		Logger:              p.logger,
		PrefetchCount:       p.config.PrefetchCount,
		PrefetchSize:        p.config.PrefetchSize,
		PrefetchGlobal:      p.config.PrefetchGlobal,
	})

	err := client.Connect(p.ctx)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	if err != nil {
		p.logger.Error("Failed to connect to node %s: %v", maskAMQPURL(node.URL), err)
		atomic.AddInt64(&node.failures, 1)
		node.healthy = false

		// FIX #11: ctx-aware goroutine instead of time.AfterFunc — prevents leak after Close()
		go func() {
			timer := time.NewTimer(p.config.ReconnectInterval)
			defer timer.Stop()
			select {
			case <-p.ctx.Done():
				return
			case <-timer.C:
				p.connectToNode(node)
			}
		}()
		return
	}

	// Close old client if one exists (e.g. reconnect replacing stale client)
	if node.Client != nil {
		_ = node.Client.Close()
	}

	node.Client = client
	node.healthy = true
	p.logger.Info("Successfully connected to node %s", maskAMQPURL(node.URL))
	// NOTE: watchNodeConnection removed — health is managed solely by healthCheckWorker
}

// healthCheckWorker periodically checks the health of all nodes.
// FIX #5: single authority for health management — watchNodeConnection has been removed.
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

// performHealthCheck checks all nodes concurrently.
func (p *Pool) performHealthCheck() {
	p.logger.Debug("Performing health check on all nodes")
	for _, node := range p.nodes {
		go p.checkNodeHealth(node)
	}
}

// checkNodeHealth checks a single node and triggers reconnect if needed.
// FIX #5: this is the single authority — no duplicate watchNodeConnection goroutine.
func (p *Pool) checkNodeHealth(node *NodeConnection) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	if node.connecting {
		return // already attempting to connect
	}

	isConnected := node.Client != nil && node.Client.IsConnected()

	if !isConnected && node.healthy {
		node.healthy = false
		p.logger.Warn("Node %s is unhealthy", maskAMQPURL(node.URL))
	}

	if !isConnected {
		// Trigger reconnect — connectToNode checks the connecting flag internally
		go p.connectToNode(node)
		return
	}

	if isConnected && !node.healthy {
		node.healthy = true
		p.logger.Info("Node %s is now healthy", maskAMQPURL(node.URL))
	}
}

// GetClient returns a client from the pool using the configured load balancing strategy.
// FIX #4: getHealthyNodes no longer calls IsConnected() (I/O) in the hot path.
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

	selectedNode.mutex.Lock()
	atomic.AddInt64(&selectedNode.totalUsed, 1)
	selectedNode.lastUsed = time.Now()
	client := selectedNode.Client
	selectedNode.mutex.Unlock()

	return client, nil
}

func (p *Pool) getClientRoundRobin() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}
	index := int(atomic.AddInt64(&p.roundRobin, 1)) % len(healthyNodes)
	return healthyNodes[index], nil
}

func (p *Pool) getClientRandom() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}
	return healthyNodes[rand.Intn(len(healthyNodes))], nil
}

func (p *Pool) getClientLeastUsed() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	var selected *NodeConnection
	minUsed := int64(^uint64(0) >> 1)
	for _, node := range healthyNodes {
		used := atomic.LoadInt64(&node.totalUsed)
		if used < minUsed {
			minUsed = used
			selected = node
		}
	}
	return selected, nil
}

func (p *Pool) getClientWeightedRoundRobin() (*NodeConnection, error) {
	healthyNodes := p.getHealthyNodes()
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available")
	}

	totalWeight := 0
	for _, node := range healthyNodes {
		totalWeight += node.weight
	}
	if totalWeight == 0 {
		return p.getClientRoundRobin()
	}

	randWeight := rand.Intn(totalWeight)
	current := 0
	for _, node := range healthyNodes {
		current += node.weight
		if randWeight < current {
			return node, nil
		}
	}
	return healthyNodes[0], nil
}

// getHealthyNodes returns all nodes marked as healthy.
// FIX #4: reads only the cached node.healthy bool — no I/O calls in the hot path.
// node.healthy is updated exclusively by checkNodeHealth() in the background.
func (p *Pool) getHealthyNodes() []*NodeConnection {
	var healthyNodes []*NodeConnection
	for _, node := range p.nodes {
		node.mutex.RLock()
		// Only read the cached bool — do NOT call IsConnected() here (I/O, nested lock)
		if node.healthy && node.Client != nil {
			healthyNodes = append(healthyNodes, node)
		}
		node.mutex.RUnlock()
	}
	return healthyNodes
}

// GetStats returns pool statistics.
// FIX #6: does not call IsConnected() (nested lock → deadlock risk). Uses cached node.healthy.
func (p *Pool) GetStats() PoolStats {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	stats := PoolStats{
		TotalNodes:    len(p.nodes),
		TotalRequests: atomic.LoadInt64(&p.totalRequests),
		TotalFailures: atomic.LoadInt64(&p.totalFailures),
		NodesStats:    make([]NodeStats, 0, len(p.nodes)),
	}

	for _, node := range p.nodes {
		node.mutex.RLock()
		nodeStat := NodeStats{
			URL:       maskAMQPURL(node.URL), // SECURITY: mask credentials
			Healthy:   node.healthy,
			Connected: node.healthy, // cached state — avoids nested lock / I/O
			TotalUsed: atomic.LoadInt64(&node.totalUsed),
			Failures:  atomic.LoadInt64(&node.failures),
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

// Close shuts down the pool: stops health checks, cancels goroutines, closes all clients.
// FIX #7: cancels context first (stops AfterFunc goroutines), then closes clients.
func (p *Pool) Close() error {
	p.mutex.Lock()
	if p.closed {
		p.mutex.Unlock()
		return nil
	}
	p.closed = true
	p.mutex.Unlock()

	// Cancel context first — stops healthCheckWorker and all ctx-aware goroutines
	if p.cancel != nil {
		p.cancel()
	}
	if p.healthTicker != nil {
		p.healthTicker.Stop()
	}

	// Brief wait to allow goroutines to observe ctx.Done()
	time.Sleep(50 * time.Millisecond)

	var errs []error
	for _, node := range p.nodes {
		node.mutex.Lock()
		client := node.Client
		node.Client = nil // prevent double-close by any lingering goroutine
		node.mutex.Unlock()

		if client != nil {
			if err := client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("node %s: %v", maskAMQPURL(node.URL), err))
			}
		}
	}

	p.logger.Info("Pool closed")

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}

// SetNodeWeight sets the weight for a node used in WeightedRoundRobin strategy.
func (p *Pool) SetNodeWeight(url string, weight int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, node := range p.nodes {
		if node.URL == url {
			node.mutex.Lock()
			node.weight = weight
			node.mutex.Unlock()
			p.logger.Info("Set weight for node %s to %d", maskAMQPURL(url), weight)
			return nil
		}
	}
	return fmt.Errorf("node not found: %s", maskAMQPURL(url))
}

// GetHealthyNodeCount returns the number of currently healthy nodes.
func (p *Pool) GetHealthyNodeCount() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	count := 0
	for _, node := range p.nodes {
		node.mutex.RLock()
		if node.healthy && node.Client != nil {
			count++
		}
		node.mutex.RUnlock()
	}
	return count
}
