package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fastRetryConfig keeps these tests from actually waiting out
// actionRetryConfig's real 15s/60s delays.
var fastRetryConfig = retry.RetryConfig{
	MaxAttempts:  3,
	InitialDelay: time.Millisecond,
	MaxDelay:     time.Millisecond,
}

func TestWithRetryConfig(t *testing.T) {
	t.Run("succeeds on first try without retrying", func(t *testing.T) {
		calls := 0
		err := withRetryConfig(t.Context(), fastRetryConfig, func() error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("retries codes.Unavailable and eventually succeeds", func(t *testing.T) {
		calls := 0
		err := withRetryConfig(t.Context(), fastRetryConfig, func() error {
			calls++
			if calls < 3 {
				return status.Error(codes.Unavailable, "throttled")
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("retries codes.DeadlineExceeded and eventually succeeds", func(t *testing.T) {
		calls := 0
		err := withRetryConfig(t.Context(), fastRetryConfig, func() error {
			calls++
			if calls < 2 {
				return status.Error(codes.DeadlineExceeded, "timed out")
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})

	t.Run("does not retry a non-retryable code, e.g. PermissionDenied", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.PermissionDenied, "forbidden")
		err := withRetryConfig(t.Context(), fastRetryConfig, func() error {
			calls++
			return wantErr
		})
		require.Equal(t, wantErr, err)
		require.Equal(t, 1, calls)
	})

	t.Run("gives up after MaxAttempts and returns the last error", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Unavailable, "still throttled")
		err := withRetryConfig(t.Context(), fastRetryConfig, func() error {
			calls++
			return wantErr
		})
		require.Equal(t, wantErr, err)
		require.Equal(t, fastRetryConfig.MaxAttempts+1, uint(calls))
	})

	t.Run("a plain (non-gRPC-status) error is not retried", func(t *testing.T) {
		calls := 0
		wantErr := errors.New("boom")
		err := withRetryConfig(t.Context(), fastRetryConfig, func() error {
			calls++
			return wantErr
		})
		require.Equal(t, wantErr, err)
		require.Equal(t, 1, calls)
	})
}

func TestWithRetryConfigValue(t *testing.T) {
	t.Run("retries and returns the eventual successful value", func(t *testing.T) {
		calls := 0
		v, err := withRetryConfigValue(t.Context(), fastRetryConfig, func() (string, error) {
			calls++
			if calls < 2 {
				return "", status.Error(codes.Unavailable, "throttled")
			}
			return "ok", nil
		})
		require.NoError(t, err)
		require.Equal(t, "ok", v)
		require.Equal(t, 2, calls)
	})

	t.Run("does not retry a non-retryable code and returns its zero value", func(t *testing.T) {
		calls := 0
		v, err := withRetryConfigValue(t.Context(), fastRetryConfig, func() (string, error) {
			calls++
			return "", status.Error(codes.InvalidArgument, "bad request")
		})
		require.Error(t, err)
		require.Equal(t, "", v)
		require.Equal(t, 1, calls)
	})
}

func TestNewRetryLoopSharesRetryerAcrossCalls(t *testing.T) {
	t.Run("retries an earlier item needed still count against a later item's budget", func(t *testing.T) {
		loop := newRetryLoop(t.Context(), fastRetryConfig) // MaxAttempts: 3

		// Item 1: throttled twice, then succeeds on the 3rd try - spends 2
		// of the shared retryer's 3 attempts.
		calls := 0
		err := loop(func() error {
			calls++
			if calls < 3 {
				return status.Error(codes.Unavailable, "throttled")
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, calls)

		// Item 2: only 1 attempt of budget remains. A fresh per-item
		// retryer (what withActionRetry would give this item on its own)
		// would allow 3 more retries; the shared retryer allows only 1
		// before giving up, since item 1's spend carried over.
		calls = 0
		err = loop(func() error {
			calls++
			return status.Error(codes.Unavailable, "still throttled")
		})
		require.Error(t, err)
		require.Equal(t, 2, calls, "only 1 more retry should remain from the shared budget, not a fresh 3")
	})

	t.Run("a shared retryer's attempt count carries across items when items fail without succeeding in between", func(t *testing.T) {
		loop := newRetryLoop(t.Context(), fastRetryConfig)

		totalCalls := 0
		// Item 1: always throttled, exhausts the shared budget.
		err := loop(func() error {
			totalCalls++
			return status.Error(codes.Unavailable, "throttled")
		})
		require.Error(t, err)
		require.Equal(t, fastRetryConfig.MaxAttempts+1, uint(totalCalls))

		// Item 2, same loop, same never-reset retryer: with a fresh
		// per-item retryer (the withActionRetry behavior this loop replaces
		// for multi-item calls) this would get its own full budget again.
		// With the shared retryer, the exhausted budget carries over and
		// this call gets zero additional retries.
		beforeItem2 := totalCalls
		err = loop(func() error {
			totalCalls++
			return status.Error(codes.Unavailable, "still throttled")
		})
		require.Error(t, err)
		require.Equal(t, 1, totalCalls-beforeItem2, "a shared retryer with an already-exhausted budget should not retry the next item either")
	})
}

func TestWithActionRetryUsesActionRetryConfig(t *testing.T) {
	// Sanity check that the production-facing helpers are wired to
	// actionRetryConfig (not left pointing at a zero-value RetryConfig,
	// which would retry forever since MaxAttempts: 0 means unlimited).
	require.Equal(t, uint(3), actionRetryConfig.MaxAttempts)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	calls := 0
	err := withActionRetry(ctx, func() error {
		calls++
		return status.Error(codes.Unavailable, "throttled")
	})
	require.Error(t, err)
	// A real retry loop against actionRetryConfig's 15s initial delay would
	// never get past the first attempt inside this test's short deadline.
	require.Equal(t, 1, calls)
}
