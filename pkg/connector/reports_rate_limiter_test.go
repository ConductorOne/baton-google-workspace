package connector

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"
)

// unlimitedRateLimiter returns a limiter whose Wait() never blocks, so tests exercise only the
// retry/timeout policy in retryListActivities, not the token bucket.
func unlimitedRateLimiter() *reportsRateLimiter {
	return newReportsRateLimiter(1_000_000)
}

// blockingCall returns a listActivitiesFunc that, on each call, either sleeps for delay and
// returns results[callIndex] (nil error means success), or returns ctx.Err() if ctx is done
// first — mirroring how the real Reports API client responds to context cancellation.
func blockingCall(delay time.Duration, results ...error) (listActivitiesFunc, *int32) {
	var calls int32
	fn := func(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken, filters string, maxResults int64) (*reportsAdmin.Activities, error) {
		idx := atomic.AddInt32(&calls, 1) - 1
		select {
		case <-time.After(delay):
			var err error
			if int(idx) < len(results) {
				err = results[idx]
			}
			if err != nil {
				return nil, err
			}
			return &reportsAdmin.Activities{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return fn, &calls
}

// blockingCallWithDelays is blockingCall but each call gets its own delay from delays (the last
// entry repeats if there are more calls than delays), so a test can make the first attempt hang
// and later attempts return quickly.
func blockingCallWithDelays(delays []time.Duration, results ...error) (listActivitiesFunc, *int32) {
	var calls int32
	fn := func(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken, filters string, maxResults int64) (*reportsAdmin.Activities, error) {
		idx := int(atomic.AddInt32(&calls, 1) - 1)
		delay := delays[len(delays)-1]
		if idx < len(delays) {
			delay = delays[idx]
		}
		select {
		case <-time.After(delay):
			var err error
			if idx < len(results) {
				err = results[idx]
			}
			if err != nil {
				return nil, err
			}
			return &reportsAdmin.Activities{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return fn, &calls
}

func rateLimitedGoogleErr() error {
	return &googleapi.Error{Code: http.StatusTooManyRequests}
}

func TestRetryListActivities(t *testing.T) {
	const (
		perAttemptTimeout = 20 * time.Millisecond
		initialBackoff    = 5 * time.Millisecond
		maxBackoff        = 20 * time.Millisecond
	)

	t.Run("succeeds on first attempt without retry", func(t *testing.T) {
		call, calls := blockingCall(0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if got := atomic.LoadInt32(calls); got != 1 {
			t.Fatalf("expected 1 call, got %d", got)
		}
	})

	t.Run("retries a rate-limited error and then succeeds", func(t *testing.T) {
		call, calls := blockingCall(0, rateLimitedGoogleErr())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err != nil {
			t.Fatalf("expected eventual success, got %v", err)
		}
		if got := atomic.LoadInt32(calls); got != 2 {
			t.Fatalf("expected 2 calls, got %d", got)
		}
	})

	t.Run("fails immediately on a non-retryable error", func(t *testing.T) {
		nonRetryable := &googleapi.Error{Code: http.StatusForbidden}
		call, calls := blockingCall(0, nonRetryable, nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if !errors.Is(err, nonRetryable) {
			t.Fatalf("expected the non-retryable error back, got %v", err)
		}
		if got := atomic.LoadInt32(calls); got != 1 {
			t.Fatalf("expected exactly 1 call (no retry), got %d", got)
		}
	})

	t.Run("retries when only attemptCtx's own timeout fires (hung attempt)", func(t *testing.T) {
		// First call sleeps past perAttemptTimeout so attemptCtx fires; the outer ctx has a much
		// longer deadline and is still alive. Second call returns quickly and succeeds.
		call, calls := blockingCallWithDelays([]time.Duration{perAttemptTimeout * 3, 0})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err != nil {
			t.Fatalf("expected the hung attempt to be retried and eventually succeed, got %v", err)
		}
		if got := atomic.LoadInt32(calls); got != 2 {
			t.Fatalf("expected 2 calls (initial hung attempt + retry), got %d", got)
		}
	})

	t.Run("returns immediately when the caller's own ctx is also done", func(t *testing.T) {
		// Outer ctx deadline is shorter than perAttemptTimeout, so when attemptCtx fires it is
		// because the caller's own deadline expired, not attemptCtx's independent timeout.
		outerDeadline := perAttemptTimeout / 4
		call, calls := blockingCall(perAttemptTimeout * 3)
		ctx, cancel := context.WithTimeout(context.Background(), outerDeadline)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if got := atomic.LoadInt32(calls); got != 1 {
			t.Fatalf("expected exactly 1 call (no retry once caller's ctx is done), got %d", got)
		}
	})

	t.Run("imposes no per-attempt timeout when the caller has no deadline", func(t *testing.T) {
		// A call that blocks well past perAttemptTimeout must still be allowed to complete when
		// the caller (e.g. app_login.go) passed a ctx with no deadline of its own.
		var sawDeadline bool
		call := func(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken, filters string, maxResults int64) (*reportsAdmin.Activities, error) {
			_, sawDeadline = ctx.Deadline()
			time.Sleep(perAttemptTimeout * 3)
			return &reportsAdmin.Activities{}, nil
		}

		start := time.Now()
		_, err := retryListActivities(context.Background(), unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if sawDeadline {
			t.Fatalf("expected no deadline to be imposed on attemptCtx for a deadline-less caller")
		}
		if elapsed := time.Since(start); elapsed < perAttemptTimeout*3 {
			t.Fatalf("expected the call to run past perAttemptTimeout uninterrupted, only took %v", elapsed)
		}
	})

	t.Run("stops after maxRetries and returns the last error", func(t *testing.T) {
		retryable := rateLimitedGoogleErr()
		call, calls := blockingCall(0, retryable, retryable, retryable)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 2, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err == nil {
			t.Fatalf("expected an error after exhausting retries")
		}
		if got := atomic.LoadInt32(calls); got != 3 {
			t.Fatalf("expected 3 calls (1 initial + 2 retries), got %d", got)
		}
	})
}
