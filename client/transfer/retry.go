package transfer

import (
	"context"
	"errors"
	"time"

	"github.com/calypr/syfon/client/common"
)

// RetryStrategy defines how to wait between retries.
type RetryStrategy interface {
	WaitTime(retryCount int) time.Duration
}

// ExponentialBackoff implements a standard exponential backoff.
type ExponentialBackoff struct {
	MaxWaitSeconds int64
}

func (e *ExponentialBackoff) WaitTime(retryCount int) time.Duration {
	exp := 1 << retryCount // 2^retryCount
	seconds := int64(exp)
	if seconds > e.MaxWaitSeconds {
		seconds = e.MaxWaitSeconds
	}
	return time.Duration(seconds) * time.Second
}

// DefaultBackoff returns the standard syfon backoff strategy.
func DefaultBackoff() RetryStrategy {
	return &ExponentialBackoff{
		MaxWaitSeconds: common.MaxWaitTime,
	}
}

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	var target nonRetryableError
	if errors.As(err, &target) {
		return err
	}
	return nonRetryableError{err: err}
}

// RetryAction is a helper that executes an action with retries according to a strategy.
func RetryAction(ctx context.Context, logger TransferLogger, strategy RetryStrategy, maxRetries int, action func() error) error {
	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			wait := strategy.WaitTime(i)
			logger.Printf("Retry %d/%d: Waiting %.0f seconds...\n", i, maxRetries, wait.Seconds())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		lastErr = action()
		if lastErr == nil {
			if sb := logger.Scoreboard(); sb != nil {
				sb.IncrementSB(i)
			}
			return nil
		}
		var nonRetryable nonRetryableError
		if errors.As(lastErr, &nonRetryable) {
			return nonRetryable.err
		}

		logger.Error("Action failed", "retry", i, "error", lastErr)
	}

	if sb := logger.Scoreboard(); sb != nil {
		sb.IncrementSB(maxRetries + 1)
	}
	return lastErr
}
