package core

import (
	"context"
	"errors"
	"net"
)

// ErrCircuitBreakerOpen signals that a per-endpoint circuit breaker rejected
// the attempt before any network I/O. ExecuteWithRetry treats it as
// failover-eligible but must NOT route it through the connection-error path,
// which would 1-strike-demote the endpoint's persisted health: a breaker-open
// rejection reflects the breaker's accumulated state, not a fresh connectivity
// failure, so demoting here would double-count and corrupt health tracking.
var ErrCircuitBreakerOpen = errors.New("circuit breaker open")

// StreamTimeoutError retains the existing stream error text and proxy behaviour
// while allowing request health to distinguish a stalled read from interruption.
type StreamTimeoutError struct {
	Message string
}

func (e *StreamTimeoutError) Error() string { return e.Message }

// IsTimeoutError preserves timeout classification independently of user-facing
// error formatting, which may intentionally discard the original error chain.
func IsTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var streamTimeout *StreamTimeoutError
	if errors.As(err, &streamTimeout) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
