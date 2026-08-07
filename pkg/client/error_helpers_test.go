package client

import (
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWrapGoogleApiErrorWithContext_403Throttle(t *testing.T) {
	t.Run("userRateLimitExceeded reclassifies to Unavailable, not PermissionDenied", func(t *testing.T) {
		e := &googleapi.Error{
			Code:    403,
			Message: "User Rate Limit Exceeded",
			Errors:  []googleapi.ErrorItem{{Reason: errorReasonUserRateLimitExceeded, Message: "User Rate Limit Exceeded"}},
		}
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)
		require.Equal(t, codes.Unavailable, st.Code())
	})

	t.Run("userRateLimitExceeded carries a synthesized RateLimitDescription - ExtractRateLimitData only populates one for 429", func(t *testing.T) {
		e := &googleapi.Error{
			Code:    403,
			Message: "User Rate Limit Exceeded",
			Errors:  []googleapi.ErrorItem{{Reason: errorReasonUserRateLimitExceeded}},
		}
		before := time.Now()
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)

		var found *v2.RateLimitDescription
		for _, d := range st.Details() {
			if rl, ok := d.(*v2.RateLimitDescription); ok {
				found = rl
			}
		}
		require.NotNil(t, found, "expected a RateLimitDescription detail on the reclassified 403")
		require.Equal(t, v2.RateLimitDescription_STATUS_OVERLIMIT, found.GetStatus())
		require.EqualValues(t, 0, found.GetRemaining())
		require.True(t, found.GetResetAt().AsTime().After(before), "ResetAt should be in the future")
	})

	t.Run("quotaExceeded stays PermissionDenied - not reliably transient, unlike userRateLimitExceeded", func(t *testing.T) {
		e := &googleapi.Error{
			Code:   403,
			Errors: []googleapi.ErrorItem{{Reason: errorReasonQuotaExceeded}},
		}
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)
		require.Equal(t, codes.PermissionDenied, st.Code())
	})

	t.Run("structured ErrorInfo RATE_LIMIT_EXCEEDED reclassifies to Unavailable", func(t *testing.T) {
		e := &googleapi.Error{
			Code:    403,
			Details: []interface{}{map[string]interface{}{"reason": errorInfoReasonRateLimitExceeded}},
		}
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)
		require.Equal(t, codes.Unavailable, st.Code())
	})

	t.Run("a genuine 403 with no throttle reason stays PermissionDenied", func(t *testing.T) {
		e := &googleapi.Error{
			Code:    403,
			Message: "Not Authorized to access this resource/api",
			Errors:  []googleapi.ErrorItem{{Reason: "forbidden"}},
		}
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)
		require.Equal(t, codes.PermissionDenied, st.Code())
	})

	t.Run("a 403 with no Errors/Details at all stays PermissionDenied", func(t *testing.T) {
		e := &googleapi.Error{Code: 403, Message: "Forbidden"}
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)
		require.Equal(t, codes.PermissionDenied, st.Code())
	})

	t.Run("429 rateLimitExceeded is still Unavailable, unaffected by this change", func(t *testing.T) {
		e := &googleapi.Error{
			Code:   429,
			Errors: []googleapi.ErrorItem{{Reason: errorReasonRateLimitExceeded}},
		}
		wrapped := wrapGoogleApiErrorWithContext(e, "test")
		st, ok := status.FromError(wrapped)
		require.True(t, ok)
		require.Equal(t, codes.Unavailable, st.Code())
	})
}

func TestIsThrottled(t *testing.T) {
	require.True(t, isThrottled(&googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: errorReasonUserRateLimitExceeded}}}))
	require.False(t, isThrottled(&googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: errorReasonQuotaExceeded}}}),
		"quotaExceeded is deliberately excluded from throttleReasons - not reliably transient")
	require.True(t, isThrottled(&googleapi.Error{Code: 403, Details: []interface{}{map[string]interface{}{"reason": errorInfoReasonRateLimitExceeded}}}))
	require.False(t, isThrottled(&googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "forbidden"}}}))
	require.False(t, isThrottled(&googleapi.Error{Code: 403}))
	// isThrottled now self-scopes to 403 (matching isCloudIdentityAPIDisabledError's
	// pattern) - the same throttle reason on a different status code must not match.
	require.False(t, isThrottled(&googleapi.Error{Code: 429, Errors: []googleapi.ErrorItem{{Reason: errorReasonRateLimitExceeded}}}))
}
