package connector

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/retry"
)

// actionRetryConfig bounds how long, and how many times, this package will
// wait for a rate-limited action-handler call to clear before giving up.
//
// This does NOT loop and retry the call itself: baton-google-workspace's
// actions are invoked by ConductorOne's platform-side BatonActionInvokeFSM,
// which already retries a failed action up to 3 times with no backoff once
// this package's call returns its error. An independent retry loop in here
// too would compound with that - up to attempts² calls against Google's API
// for a single throttled action, in the worst case. Instead, on a retryable
// error this package waits once for the error's rate-limit window (reusing
// retry.Retryer's own wait-time computation) and returns the original error
// unretried, so the platform's next attempt lands after a respectful pause
// instead of immediately.
var actionRetryConfig = retry.RetryConfig{
	MaxAttempts:  3,
	InitialDelay: 15 * time.Second,
	MaxDelay:     60 * time.Second,
}

// withRateLimitWait calls fn once. On a retryable error
// (codes.Unavailable/codes.DeadlineExceeded - see pkg/client/error_helpers.go
// for how this connector's error classification maps throttling to those
// codes), it waits once for that error's rate-limit window before returning
// the original error. Any other error returns immediately without waiting.
func withRateLimitWait(ctx context.Context, fn func() error) error {
	return waitOnce(ctx, retry.NewRetryer(ctx, actionRetryConfig), fn)
}

// withRateLimitWaitValue is withRateLimitWait for calls that also return a value.
func withRateLimitWaitValue[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	return waitOnceValue(ctx, retry.NewRetryer(ctx, actionRetryConfig), fn)
}

// newRateLimitWaitLoop returns a wait function bound to a single shared
// retry.Retryer using actionRetryConfig. Use this instead of
// withRateLimitWait when looping over multiple items (e.g. deleting each of
// a user's OAuth tokens): waiting independently per item would let the
// combined wait time scale with the number of items, so a shared retryer
// caps the total wait across the whole loop at actionRetryConfig.MaxAttempts
// instead.
func newRateLimitWaitLoop(ctx context.Context) func(fn func() error) error {
	return newWaitOnceLoop(ctx, actionRetryConfig)
}

// newRateLimitWaitLoopValue is newRateLimitWaitLoop for calls that also
// return a value (e.g. a paginated list call looped over multiple pages).
func newRateLimitWaitLoopValue[T any](ctx context.Context) func(fn func() (T, error)) (T, error) {
	return newWaitOnceLoopValue[T](ctx, actionRetryConfig)
}

// newWaitOnceLoop and newWaitOnceLoopValue are the above parametrized by
// config, so tests can use a fast config instead of actionRetryConfig's real
// 15s/60s delays.
func newWaitOnceLoop(ctx context.Context, cfg retry.RetryConfig) func(fn func() error) error {
	retryer := retry.NewRetryer(ctx, cfg)
	return func(fn func() error) error {
		return waitOnce(ctx, retryer, fn)
	}
}

func newWaitOnceLoopValue[T any](ctx context.Context, cfg retry.RetryConfig) func(fn func() (T, error)) (T, error) {
	retryer := retry.NewRetryer(ctx, cfg)
	return func(fn func() (T, error)) (T, error) {
		return waitOnceValue(ctx, retryer, fn)
	}
}

// waitOnce and waitOnceValue call fn once against an existing retryer rather
// than constructing one, so newRateLimitWaitLoop/newRateLimitWaitLoopValue
// can share a single retryer's wait budget across many calls. A success
// resets the retryer's attempt count (retryer.ShouldWaitAndRetry(ctx, nil))
// so a shared budget isn't permanently spent by a few transient failures
// earlier in a loop; a retryable failure spends one attempt of the budget
// waiting before the error is returned.
func waitOnce(ctx context.Context, retryer *retry.Retryer, fn func() error) error {
	err := fn()
	if err == nil {
		retryer.ShouldWaitAndRetry(ctx, nil)
		return nil
	}
	retryer.ShouldWaitAndRetry(ctx, err)
	return err
}

func waitOnceValue[T any](ctx context.Context, retryer *retry.Retryer, fn func() (T, error)) (T, error) {
	v, err := fn()
	if err == nil {
		retryer.ShouldWaitAndRetry(ctx, nil)
		return v, nil
	}
	retryer.ShouldWaitAndRetry(ctx, err)
	return v, err
}
