package bunnyhop

import (
	"sync"
	"time"
)

// PoolConfig cấu hình cho Pool Client
type PoolConfig struct {
	URLs                []string      // Danh sách URLs của các node RabbitMQ
	ReconnectInterval   time.Duration // Thời gian chờ giữa các lần reconnect
	MaxReconnectAttempt int           // Số lần thử reconnect tối đa
	HealthCheckInterval time.Duration // Thời gian giữa các lần health check
	Heartbeat           time.Duration // Heartbeat interval
	ConnectionName      string        // Tên connection hiển thị trên RabbitMQ UI
	LoadBalanceStrategy LoadBalanceStrategy
	DebugLog            bool   // Bật/tắt debug log
	Logger              Logger // Custom logger interface
}

// LoadBalanceStrategy chiến lược load balancing
type LoadBalanceStrategy int

const (
	RoundRobin LoadBalanceStrategy = iota
	Random
	LeastUsed
	WeightedRoundRobin
)

// Logger interface để log các sự kiện
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// NodeConnection thông tin kết nối đến một node (chỉ 1 connection per node)
type NodeConnection struct {
	URL        string
	Client     *Client
	mutex      sync.RWMutex
	healthy    bool
	weight     int
	lastUsed   time.Time
	totalUsed  int64
	failures   int64
	connecting bool
}

// PoolStats thống kê của pool
type PoolStats struct {
	TotalNodes    int         `json:"total_nodes"`
	HealthyNodes  int         `json:"healthy_nodes"`
	TotalRequests int64       `json:"total_requests"`
	TotalFailures int64       `json:"total_failures"`
	NodesStats    []NodeStats `json:"nodes_stats"`
}

// NodeStats thống kê của một node
type NodeStats struct {
	URL       string `json:"url"`
	Healthy   bool   `json:"healthy"`
	Connected bool   `json:"connected"`
	TotalUsed int64  `json:"total_used"`
	Failures  int64  `json:"failures"`
	Weight    int    `json:"weight"`
	LastUsed  string `json:"last_used"`
}
