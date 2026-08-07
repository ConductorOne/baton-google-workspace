package connector

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/retry"
)

// actionRetryConfig mirrors the retry policy the SDK itself uses for Grant,
// Revoke, ticket List, and Validate (vendor/.../connectorbuilder/
// resource_provisioner.go, tickets.go, connectorbuilder.go) - unlike those
// paths, InvokeAction (vendor/.../connectorbuilder/actions.go) has no retry
// of its own, so action handlers in this package apply the same policy
// themselves via withActionRetry/withActionRetryValue below.
var actionRetryConfig = retry.RetryConfig{
	MaxAttempts:  3,
	InitialDelay: 15 * time.Second,
	MaxDelay:     60 * time.Second,
}

// withActionRetry retries fn using the SDK's retry.Retryer, which only
// waits-and-retries on codes.Unavailable/codes.DeadlineExceeded (see
// vendor/.../pkg/retry/retry.go) - exactly the codes this connector's error
// classification (pkg/client/error_helpers.go) maps throttling and transient
// failures to. Any other error returns immediately without retrying.
func withActionRetry(ctx context.Context, fn func() error) error {
	return withRetryConfig(ctx, actionRetryConfig, fn)
}

// withActionRetryValue is withActionRetry for calls that also return a value.
func withActionRetryValue[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	return withRetryConfigValue(ctx, actionRetryConfig, fn)
}

// newActionRetryLoop returns a retry function bound to a single shared
// retry.Retryer using actionRetryConfig. Use this instead of withActionRetry
// when looping over multiple items (e.g. deleting each of a user's OAuth
// tokens): a fresh retryer per item would give every item its own
// independent attempt budget, so a sustained throttle could stretch an
// N-item loop to N times the single-call worst case. A shared retryer
// amortizes the budget across the whole loop instead.
func newActionRetryLoop(ctx context.Context) func(fn func() error) error {
	return newRetryLoop(ctx, actionRetryConfig)
}

// newRetryLoop is newActionRetryLoop parametrized by config, so tests can use
// a fast config instead of actionRetryConfig's real 15s/60s delays.
func newRetryLoop(ctx context.Context, cfg retry.RetryConfig) func(fn func() error) error {
	retryer := retry.NewRetryer(ctx, cfg)
	return func(fn func() error) error {
		return withRetryer(ctx, retryer, fn)
	}
}

// withRetryConfig and withRetryConfigValue do the actual retry looping for a
// given retry.RetryConfig. Split out from withActionRetry/withActionRetryValue
// so tests can exercise the retry behavior with a fast config instead of
// actionRetryConfig's real 15s/60s delays.
func withRetryConfig(ctx context.Context, cfg retry.RetryConfig, fn func() error) error {
	return withRetryer(ctx, retry.NewRetryer(ctx, cfg), fn)
}

func withRetryer(ctx context.Context, retryer *retry.Retryer, fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		return err
	}
}

func withRetryConfigValue[T any](ctx context.Context, cfg retry.RetryConfig, fn func() (T, error)) (T, error) {
	retryer := retry.NewRetryer(ctx, cfg)
	for {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		return v, err
	}
}
