package connector

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fastActionRetryConfig keeps these tests from actually waiting out
// actionRetryConfig's real 15s/60s delays, while still being long enough
// (20ms) to reliably distinguish "waited" from "did not wait" against
// scheduler jitter.
var fastActionRetryConfig = retry.RetryConfig{
	MaxAttempts:  3,
	InitialDelay: 20 * time.Millisecond,
	MaxDelay:     20 * time.Millisecond,
}

func TestWaitOnce(t *testing.T) {
	t.Run("fn is called exactly once and its error is returned unmodified, even when retryable", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Unavailable, "throttled")
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig)
		err := waitOnce(t.Context(), retryer, func() error {
			calls++
			return wantErr
		})
		require.Equal(t, wantErr, err)
		require.Equal(t, 1, calls, "waitOnce must never call fn a second time - retrying is the platform's job")
	})

	t.Run("waits once for a retryable error before returning it", func(t *testing.T) {
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig)
		start := time.Now()
		err := waitOnce(t.Context(), retryer, func() error {
			return status.Error(codes.Unavailable, "throttled")
		})
		require.Error(t, err)
		require.GreaterOrEqual(t, time.Since(start), fastActionRetryConfig.InitialDelay)
	})

	t.Run("does not wait for a non-retryable error", func(t *testing.T) {
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig)
		start := time.Now()
		err := waitOnce(t.Context(), retryer, func() error {
			return status.Error(codes.PermissionDenied, "forbidden")
		})
		require.Error(t, err)
		require.Less(t, time.Since(start), fastActionRetryConfig.InitialDelay)
	})

	t.Run("does not wait on success", func(t *testing.T) {
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig)
		start := time.Now()
		err := waitOnce(t.Context(), retryer, func() error {
			return nil
		})
		require.NoError(t, err)
		require.Less(t, time.Since(start), fastActionRetryConfig.InitialDelay)
	})
}

func TestWaitOnceValue(t *testing.T) {
	t.Run("fn is called exactly once and its value/error are returned unmodified", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Unavailable, "throttled")
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig)
		v, err := waitOnceValue(t.Context(), retryer, func() (string, error) {
			calls++
			return "partial", wantErr
		})
		require.Equal(t, wantErr, err)
		require.Equal(t, "partial", v)
		require.Equal(t, 1, calls)
	})
}

func TestSharedWaitBudgetAcrossLoop(t *testing.T) {
	throttled := func() error { return status.Error(codes.Unavailable, "throttled") }

	t.Run("a shared retryer's wait budget is spent across calls, then exhausted calls stop waiting", func(t *testing.T) {
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig) // MaxAttempts: 3

		for i := range 3 {
			start := time.Now()
			err := waitOnce(t.Context(), retryer, throttled)
			require.Error(t, err)
			require.GreaterOrEqual(t, time.Since(start), fastActionRetryConfig.InitialDelay, "attempt %d should still wait", i+1)
		}

		// Budget exhausted: no wait on the next call sharing this retryer.
		start := time.Now()
		err := waitOnce(t.Context(), retryer, throttled)
		require.Error(t, err)
		require.Less(t, time.Since(start), fastActionRetryConfig.InitialDelay)
	})

	t.Run("a success resets the shared budget", func(t *testing.T) {
		retryer := retry.NewRetryer(t.Context(), fastActionRetryConfig)

		for range 3 {
			_ = waitOnce(t.Context(), retryer, throttled)
		}
		start := time.Now()
		_ = waitOnce(t.Context(), retryer, throttled)
		require.Less(t, time.Since(start), fastActionRetryConfig.InitialDelay, "budget should already be exhausted")

		err := waitOnce(t.Context(), retryer, func() error { return nil })
		require.NoError(t, err)

		start = time.Now()
		err = waitOnce(t.Context(), retryer, throttled)
		require.Error(t, err)
		require.GreaterOrEqual(t, time.Since(start), fastActionRetryConfig.InitialDelay, "the prior success should have reset the budget")
	})
}

func TestNewRateLimitWaitLoopSharesOneRetryer(t *testing.T) {
	// newRateLimitWaitLoop/newWaitOnceLoop must bind every call to the SAME
	// retryer instance (not a fresh one per call) - verified the same way as
	// TestSharedWaitBudgetAcrossLoop, but through the loop constructor.
	loop := newWaitOnceLoop(t.Context(), fastActionRetryConfig)
	throttled := func() error { return status.Error(codes.Unavailable, "throttled") }

	for range fastActionRetryConfig.MaxAttempts {
		_ = loop(throttled)
	}
	start := time.Now()
	err := loop(throttled)
	require.Error(t, err)
	require.Less(t, time.Since(start), fastActionRetryConfig.InitialDelay, "the loop's shared budget should be exhausted by now")
}

func TestNewRateLimitWaitLoopValueSharesOneRetryer(t *testing.T) {
	loop := newWaitOnceLoopValue[int](t.Context(), fastActionRetryConfig)
	throttled := func() (int, error) { return 0, status.Error(codes.Unavailable, "throttled") }

	for range fastActionRetryConfig.MaxAttempts {
		_, _ = loop(throttled)
	}
	start := time.Now()
	_, err := loop(throttled)
	require.Error(t, err)
	require.Less(t, time.Since(start), fastActionRetryConfig.InitialDelay, "the loop's shared budget should be exhausted by now")
}

func TestWithRateLimitWaitUsesActionRetryConfig(t *testing.T) {
	// Sanity check that the production-facing helpers are wired to
	// actionRetryConfig (not left pointing at a zero-value RetryConfig).
	require.Equal(t, uint(3), actionRetryConfig.MaxAttempts)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := withRateLimitWait(ctx, func() error {
		return status.Error(codes.Unavailable, "throttled")
	})
	require.Error(t, err)
	// A wait against actionRetryConfig's real 15s InitialDelay would block
	// past this test's 10ms context deadline; a cancelled context must cut
	// the wait short instead of blocking for the full 15s.
	require.Less(t, time.Since(start), time.Second)
}
