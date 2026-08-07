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
