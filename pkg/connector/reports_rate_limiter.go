// reports_rate_limiter.go throttles calls to the Admin Reports API activities.list endpoint.
//
// Any activities.list call scoped by userKey, eventName, or filters counts as a "filter query"
// against Google's much stricter per-project quota (250/min, 15,000/hour) rather than the
// general Directory API quota:
// https://developers.google.com/workspace/admin/reports/v1/limits
//
// usage_event_feed, google_login_event_feed, and saml_event_feed all now issue per-user filter
// queries and can run concurrently within the same sync, so they share one limiter instance to
// avoid collectively double-spending the same 250/min budget.
package connector

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

const (
	// reportsFilterQueryQuotaPerMinute mirrors Google's documented 250/min filter-query cap.
	// A small safety margin is left below the hard limit to tolerate clock/measurement jitter.
	reportsFilterQueryQuotaPerMinute = 220
	reportsMaxRetries                = 5
	reportsInitialBackoff            = 500 * time.Millisecond
	reportsMaxBackoff                = 30 * time.Second

	// reportsPerAttemptTimeout bounds a single ListActivities call, not the retry loop as a
	// whole, so a genuinely hung request is retried like any other transient error instead of
	// being confused with the caller's overall lookup deadline expiring.
	reportsPerAttemptTimeout = 25 * time.Second
)

// reportsRateLimiter is a simple token-bucket limiter built on the standard library only
// (no new go.mod dependency). Tokens refill continuously at reportsFilterQueryQuotaPerMinute.
type reportsRateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	perToken   time.Duration
	lastRefill time.Time
	now        func() time.Time
}

func newReportsRateLimiter(quotaPerMinute int) *reportsRateLimiter {
	return &reportsRateLimiter{
		tokens:     float64(quotaPerMinute),
		maxTokens:  float64(quotaPerMinute),
		perToken:   time.Minute / time.Duration(quotaPerMinute),
		lastRefill: time.Now(),
		now:        time.Now,
	}
}

// sharedReportsRateLimiter is spent from by all Reports API filter-query callers in this
// connector (event feeds and app_login OAuth/login discovery) for the lifetime of the process.
var sharedReportsRateLimiter = newReportsRateLimiter(reportsFilterQueryQuotaPerMinute)

// Wait blocks until a token is available or ctx is cancelled.
func (l *reportsRateLimiter) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		l.refillLocked()
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		wait := l.perToken
		l.mu.Unlock()

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *reportsRateLimiter) refillLocked() {
	elapsed := l.now().Sub(l.lastRefill)
	if elapsed <= 0 {
		return
	}
	l.tokens += elapsed.Seconds() * (l.maxTokens / 60)
	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}
	l.lastRefill = l.now()
}

// listActivitiesFunc mirrors GoogleWorkspaceClient.ListActivities so retryListActivities can be
// tested with a fake implementation.
type listActivitiesFunc func(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken, filters string, maxResults int64) (*reportsAdmin.Activities, error)

// listActivitiesRateLimited waits for the shared filter-query budget, then calls
// client.ListActivities, retrying with exponential backoff on 429/503 — both are transient,
// SDK-retryable conditions, not connector bugs (see patterns-error-handling.md).
func listActivitiesRateLimited(
	ctx context.Context,
	client *gwclient.GoogleWorkspaceClient,
	userKey, applicationName, eventName, startTime, pageToken string,
	maxResults int64,
) (*reportsAdmin.Activities, error) {
	return listActivitiesFilteredRateLimited(ctx, client, userKey, applicationName, eventName, startTime, pageToken, "", maxResults)
}

// listActivitiesFilteredRateLimited is listActivitiesRateLimited plus an optional Reports API
// `filters` expression (e.g. "client_id==<id>"), for callers that need to scope a lookup to one
// specific app rather than an entire app-type.
func listActivitiesFilteredRateLimited(
	ctx context.Context,
	client *gwclient.GoogleWorkspaceClient,
	userKey, applicationName, eventName, startTime, pageToken, filters string,
	maxResults int64,
) (*reportsAdmin.Activities, error) {
	return retryListActivities(
		ctx, sharedReportsRateLimiter, client.ListActivities,
		reportsPerAttemptTimeout, reportsMaxRetries, reportsInitialBackoff, reportsMaxBackoff,
		userKey, applicationName, eventName, startTime, pageToken, filters, maxResults,
	)
}

// retryListActivities holds the retry/backoff/per-attempt-timeout policy, parameterized so tests
// can drive it with a fake call and short durations. The per-attempt timeout only applies when
// ctx already has a deadline, so deadline-less callers like app_login.go keep running unbounded.
func retryListActivities(
	ctx context.Context,
	limiter *reportsRateLimiter,
	call listActivitiesFunc,
	perAttemptTimeout time.Duration,
	maxRetries int,
	initialBackoff, maxBackoff time.Duration,
	userKey, applicationName, eventName, startTime, pageToken, filters string,
	maxResults int64,
) (*reportsAdmin.Activities, error) {
	_, callerHasDeadline := ctx.Deadline()
	backoff := initialBackoff
	for attempt := 0; ; attempt++ {
		if err := limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("google-workspace-connector: context cancelled waiting for reports api quota: %w", err)
		}

		attemptCtx := ctx
		cancel := func() {}
		if callerHasDeadline {
			attemptCtx, cancel = context.WithTimeout(ctx, perAttemptTimeout)
		}
		resp, err := call(attemptCtx, userKey, applicationName, eventName, startTime, pageToken, filters, maxResults)
		cancel()
		if err == nil {
			return resp, nil
		}

		// ctx still being live means attemptCtx's own timeout fired, not the caller's deadline —
		// treat that like a retryable 429/503 rather than "out of time."
		hungAttempt := callerHasDeadline && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		if attempt >= maxRetries || (!isRetryableReportsError(err) && !hungAttempt) {
			return nil, err
		}

		sleep := backoff + rand.N(backoff) //nolint:gosec // jitter timing only, not security-sensitive
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func isRetryableReportsError(err error) bool {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code == http.StatusTooManyRequests || gerr.Code == http.StatusServiceUnavailable
	}
	return false
}
