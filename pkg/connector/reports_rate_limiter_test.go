package connector

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
// and later attempts return quickly. On a ctx timeout, the returned error is ctx.Err() as-is; use
// blockingCallWithDelaysWrapped to pin a production-shaped wrapping of that error instead.
func blockingCallWithDelays(delays []time.Duration, results ...error) (listActivitiesFunc, *int32) {
	return blockingCallWithDelaysWrapped(delays, func(err error) error { return err }, results...)
}

// blockingCallWithDelaysWrapped is blockingCallWithDelays, but passes a ctx timeout's error
// through wrapCtxErr before returning it — e.g. to mirror how net/http's transport wraps a
// context error in a *url.Error, so tests can pin that errors.Is still unwraps it correctly.
func blockingCallWithDelaysWrapped(delays []time.Duration, wrapCtxErr func(error) error, results ...error) (listActivitiesFunc, *int32) {
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
			return nil, wrapCtxErr(ctx.Err())
		}
	}
	return fn, &calls
}

func rateLimitedGoogleErr() error {
	return &googleapi.Error{Code: http.StatusTooManyRequests}
}

// productionWrappedRateLimitErr mirrors what client.ListActivities actually returns for a 429:
// wrapGoogleApiErrorWithContext joins a gRPC status with the original *googleapi.Error, rather
// than returning the bare *googleapi.Error the other tests use.
func productionWrappedRateLimitErr() error {
	return errors.Join(status.Error(codes.Unavailable, "rate limited"), &googleapi.Error{Code: http.StatusTooManyRequests})
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

	t.Run("imposes no per-attempt timeout when perAttemptTimeout is 0, even with a deadline-bearing ctx", func(t *testing.T) {
		// perAttemptTimeout == 0 must disable the sub-timeout on its own terms, regardless of
		// whether ctx happens to carry a deadline (e.g. a future SDK change attaches one to the
		// sync context) — this is what listActivitiesRateLimited (used by app_login.go) relies
		// on to stay unbounded.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		wantDeadline, _ := ctx.Deadline()

		var gotDeadline time.Time
		var gotHasDeadline bool
		call := func(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken, filters string, maxResults int64) (*reportsAdmin.Activities, error) {
			gotDeadline, gotHasDeadline = ctx.Deadline()
			time.Sleep(perAttemptTimeout * 3)
			return &reportsAdmin.Activities{}, nil
		}

		start := time.Now()
		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, 0, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		// attemptCtx must be ctx itself (same deadline as the caller's own, not a new shorter
		// one), proving perAttemptTimeout==0 disabled the sub-timeout rather than the caller
		// simply having no deadline to begin with.
		if !gotHasDeadline || !gotDeadline.Equal(wantDeadline) {
			t.Fatalf("expected attemptCtx to carry the caller's own deadline unchanged, got hasDeadline=%v deadline=%v want=%v", gotHasDeadline, gotDeadline, wantDeadline)
		}
		if elapsed := time.Since(start); elapsed < perAttemptTimeout*3 {
			t.Fatalf("expected the call to run past perAttemptTimeout uninterrupted, only took %v", elapsed)
		}
	})

	t.Run("retries a rate-limited error wrapped the way production code wraps it", func(t *testing.T) {
		// Pins the contract that isRetryableReportsError's errors.As still finds the
		// *googleapi.Error inside wrapGoogleApiErrorWithContext's errors.Join, not just a bare
		// *googleapi.Error.
		call, calls := blockingCall(0, productionWrappedRateLimitErr())
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

	t.Run("retries a hung attempt whose timeout arrives wrapped like production's transport error", func(t *testing.T) {
		// Pins the contract that hungAttempt's errors.Is still finds context.DeadlineExceeded
		// inside a *url.Error, not just a bare context error.
		call, calls := blockingCallWithDelaysWrapped([]time.Duration{perAttemptTimeout * 3, 0}, func(err error) error {
			return &url.Error{Op: "Get", URL: "https://example.com", Err: err}
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := retryListActivities(ctx, unlimitedRateLimiter(), call, perAttemptTimeout, 5, initialBackoff, maxBackoff, "u", "app", "event", "", "", "", 10)
		if err != nil {
			t.Fatalf("expected the wrapped hung attempt to be retried and eventually succeed, got %v", err)
		}
		if got := atomic.LoadInt32(calls); got != 2 {
			t.Fatalf("expected 2 calls (initial hung attempt + retry), got %d", got)
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
