package bunnyhop

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"time"
)

func getDefaultConfig(config *PoolConfig) {
	if config == nil {
		config = &PoolConfig{}
	}

	if config.ReconnectInterval == 0 {
		config.ReconnectInterval = 30 * time.Second
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 30 * time.Second
	}
	if len(config.URLs) == 0 {
		config.URLs = []string{"amqp://localhost:5672"}
	}
	if config.Logger == nil {
		config.Logger = NewDefaultLogger(config.DebugLog)
	}
}

// maskAMQPURL replaces the password in an AMQP URL with "***" before logging
// to prevent credential leakage in log aggregation systems.
func maskAMQPURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "[invalid-url]"
	}
	if u.User == nil {
		return rawURL
	}
	// Build masked URL manually — url.String() URL-encodes * to %2A
	masked := fmt.Sprintf("%s://%s:***@%s%s", u.Scheme, u.User.Username(), u.Host, u.Path)
	if u.RawQuery != "" {
		masked += "?" + u.RawQuery
	}
	return masked
}

// exponentialBackoff computes a reconnect delay with exponential backoff and ±25% jitter.
// attempt starts at 1. Delay is capped at 5 minutes.
//
//	attempt=1 → ~base
//	attempt=2 → ~2×base
//	attempt=3 → ~4×base (capped at 5min)
func exponentialBackoff(attempt int, base time.Duration) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	multiplier := math.Pow(2, float64(attempt-1))
	backoff := time.Duration(float64(base) * multiplier)

	const maxBackoff = 5 * time.Minute
	if backoff > maxBackoff || backoff < 0 { // guard int64 overflow
		backoff = maxBackoff
	}

	// ±25% jitter — applied BEFORE the final floor so result stays ≤ maxBackoff
	jitterRange := int64(backoff / 4)
	if jitterRange > 0 {
		jitter := time.Duration(rand.Int63n(jitterRange*2) - jitterRange)
		backoff += jitter
		// Re-apply cap after jitter (jitter can push us over)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	// Never go below base
	if backoff < base {
		backoff = base
	}
	return backoff
}
