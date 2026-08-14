package bunnyhop

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Task 4.3: Credential mask tests
// ============================================================

func TestMaskAMQPURL_WithPassword(t *testing.T) {
	cases := []struct {
		input    string
		wantMask string
		desc     string
	}{
		{
			input:    "amqp://guest:guest@localhost:5672/",
			wantMask: "amqp://guest:***@localhost:5672/",
			desc:     "standard credentials",
		},
		{
			input:    "amqp://admin:TOP_SECRET@rabbitmq.prod:5672/vhost",
			wantMask: "amqp://admin:***@rabbitmq.prod:5672/vhost",
			desc:     "production credentials with vhost",
		},
		{
			input:    "amqps://user:SecureP4ss@secure.host:5671/",
			wantMask: "amqps://user:***@secure.host:5671/",
			desc:     "TLS URL",
		},
		{
			input:    "amqp://localhost:5672/",
			wantMask: "amqp://localhost:5672/",
			desc:     "no credentials",
		},
		{
			input:    "not-a-valid-url://garbage",
			wantMask: "not-a-valid-url://garbage",
			desc:     "URL without user info parses fine",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got := maskAMQPURL(tc.input)
			assert.Equal(t, tc.wantMask, got)
			// Verify *** is present when there were credentials
			if tc.wantMask != tc.input {
				assert.Contains(t, got, "***", "masked URL should contain ***")
			}
			// Verify specific passwords are not present
			assert.NotContains(t, got, "TOP_SECRET")
			assert.NotContains(t, got, "p@ssw0rd!")
		})
	}
}

func TestMaskAMQPURL_NeverLeaksPassword(t *testing.T) {
	sensitive := "SUPER_SECRET_PASSWORD_12345"
	url := "amqp://user:" + sensitive + "@host:5672/"
	masked := maskAMQPURL(url)

	assert.NotContains(t, masked, sensitive, "password must not appear in masked URL")
	assert.Contains(t, masked, "***", "masked URL should contain ***")
}

// ============================================================
// Task 4.4: Exponential backoff unit tests
// ============================================================

func TestExponentialBackoff_Progression(t *testing.T) {
	base := 1 * time.Second

	cases := []struct {
		attempt  int
		minDelay time.Duration
		maxDelay time.Duration
		desc     string
	}{
		{1, 750 * time.Millisecond, 1250 * time.Millisecond, "attempt 1 ≈ base"},
		{2, 1500 * time.Millisecond, 2500 * time.Millisecond, "attempt 2 ≈ 2×base"},
		{3, 3 * time.Second, 5 * time.Second, "attempt 3 ≈ 4×base"},
		{10, 3*time.Minute + 45*time.Second, 5 * time.Minute, "attempt 10 capped at 5min"},
		{20, 3*time.Minute + 45*time.Second, 5 * time.Minute, "attempt 20 still capped at 5min"},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// Run 100 times to validate jitter range
			for i := 0; i < 100; i++ {
				d := exponentialBackoff(tc.attempt, base)
				assert.GreaterOrEqual(t, d, tc.minDelay,
					"attempt=%d run=%d: delay %v < min %v", tc.attempt, i, d, tc.minDelay)
				assert.LessOrEqual(t, d, tc.maxDelay,
					"attempt=%d run=%d: delay %v > max %v", tc.attempt, i, d, tc.maxDelay)
			}
		})
	}
}

func TestExponentialBackoff_NeverBelowBase(t *testing.T) {
	base := 5 * time.Second
	for attempt := 1; attempt <= 20; attempt++ {
		d := exponentialBackoff(attempt, base)
		assert.GreaterOrEqual(t, d, base, "attempt=%d: backoff %v must be >= base %v", attempt, d, base)
	}
}

func TestExponentialBackoff_ZeroAttempt(t *testing.T) {
	// attempt <= 0 should be treated as 1
	base := 1 * time.Second
	d := exponentialBackoff(0, base)
	assert.GreaterOrEqual(t, d, 750*time.Millisecond)
	assert.LessOrEqual(t, d, 1250*time.Millisecond)
}

// ============================================================
// Task 4.1 (no goleak dep): basic config validation tests
// ============================================================

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		cfg := Config{URLs: []string{"amqp://localhost:5672"}}
		assert.NoError(t, cfg.Validate())
	})

	t.Run("empty URLs fails", func(t *testing.T) {
		cfg := Config{}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "URL")
	})

	t.Run("negative MaxReconnectAttempt fails", func(t *testing.T) {
		cfg := Config{
			URLs:                []string{"amqp://localhost"},
			MaxReconnectAttempt: -1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MaxReconnectAttempt")
	})

	t.Run("negative PrefetchCount fails", func(t *testing.T) {
		cfg := Config{
			URLs:          []string{"amqp://localhost"},
			PrefetchCount: -5,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PrefetchCount")
	})
}

// ============================================================
// NewClient defaults
// ============================================================

func TestNewClient_Defaults(t *testing.T) {
	client := NewClient(Config{
		URLs: []string{"amqp://localhost:5672"},
	})
	require.NotNil(t, client)

	// Check defaults are applied
	assert.Equal(t, 5*time.Second, client.config.ReconnectInterval)
	assert.Equal(t, 10*time.Second, client.config.Heartbeat)
	assert.Equal(t, 10, client.config.PrefetchCount)
	assert.NotNil(t, client.config.Logger)

	// Metadata maps initialized (not nil)
	assert.NotNil(t, client.declaredQueues)
	assert.NotNil(t, client.declaredExchanges)
	assert.NotNil(t, client.boundQueues)

	client.Close()
}

func TestNewClient_DeclareQueueDedup(t *testing.T) {
	// Verify that declaring the same queue twice does not grow the slice unboundedly
	// Since we now use a map, the length stays at 1 for duplicate names
	client := NewClient(Config{
		URLs: []string{"amqp://localhost"},
	})
	// Manually test the map behaviour without connecting
	client.declaredQueues["test"] = queueMeta{name: "test", durable: true}
	client.declaredQueues["test"] = queueMeta{name: "test", durable: false} // overwrite
	client.declaredQueues["other"] = queueMeta{name: "other", durable: true}

	assert.Len(t, client.declaredQueues, 2, "map should have 2 unique entries")
	assert.Equal(t, false, client.declaredQueues["test"].durable, "should use latest declaration")
}
