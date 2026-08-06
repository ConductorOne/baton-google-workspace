package client

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wrapGoogleApiErrorWithContext wraps a googleapi.Error with rate limit information and an optional context message.
// The context message is prepended to the error message while preserving the gRPC status code.
// If the error is not a googleapi.Error, it returns the error unchanged.
func wrapGoogleApiErrorWithContext(err error, contextMsg string) error {
	var e *googleapi.Error
	if ok := errors.As(err, &e); !ok {
		return err
	}

	switch e.Code {
	case http.StatusBadRequest:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.InvalidArgument, contextMsg, e, err)
	case http.StatusUnauthorized:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unauthenticated, contextMsg, e, err)
	case http.StatusForbidden:
		if isThrottled(e) {
			// Google documents throttling arriving on 403 with reason
			// userRateLimitExceeded/quotaExceeded, in addition to 429 with
			// rateLimitExceeded (handled below) - see
			// https://developers.google.com/workspace/admin/directory/v1/limits.
			// Reclassify as Unavailable, matching the 429 case, so it rides
			// the same automatic retry-with-backoff the SDK's sync-phase
			// retryer already applies there (it gates strictly on
			// codes.Unavailable/DeadlineExceeded) - a real, genuine 403
			// permissions/delegation failure is left as PermissionDenied.
			return wrapGoogleApiErrorWithRateLimitInfo(codes.Unavailable, contextMsg, e, err)
		}
		return wrapGoogleApiErrorWithRateLimitInfo(codes.PermissionDenied, contextMsg, e, err)
	case http.StatusNotFound:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.NotFound, contextMsg, e, err)
	case http.StatusRequestTimeout:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.DeadlineExceeded, contextMsg, e, err)
	case http.StatusConflict:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Aborted, contextMsg, e, err)
	case http.StatusGone:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.NotFound, contextMsg, e, err)
	case http.StatusPreconditionFailed:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.FailedPrecondition, contextMsg, e, err)
	case http.StatusTooManyRequests:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unavailable, contextMsg, e, err)
	case http.StatusNotImplemented:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unimplemented, contextMsg, e, err)
	case http.StatusServiceUnavailable:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unavailable, contextMsg, e, err)
	}

	if e.Code >= 500 && e.Code <= 599 {
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unavailable, contextMsg, e, err)
	}

	if e.Code < 200 || e.Code >= 300 {
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unknown, contextMsg, e, errors.Join(fmt.Errorf("unexpected status code: %d", e.Code), err))
	}

	contextErr := err
	if contextMsg != "" {
		contextErr = errors.Join(fmt.Errorf("%s", contextMsg), err)
	}
	return errors.Join(
		fmt.Errorf("unexpected status code: %d", e.Code),
		contextErr,
	)
}

// Legacy googleapi.ErrorItem.Reason values and the structured
// google.rpc.ErrorInfo reason that indicate rate limiting rather than a
// genuine permissions failure, on a 403 response
// (https://developers.google.com/workspace/admin/directory/v1/limits).
const (
	errorReasonUserRateLimitExceeded = "userRateLimitExceeded"
	errorReasonQuotaExceeded         = "quotaExceeded"
	errorReasonRateLimitExceeded     = "rateLimitExceeded"
	errorInfoReasonRateLimitExceeded = "RATE_LIMIT_EXCEEDED"
)

var throttleReasons = map[string]bool{
	errorReasonUserRateLimitExceeded: true,
	errorReasonQuotaExceeded:         true,
	errorReasonRateLimitExceeded:     true,
	errorInfoReasonRateLimitExceeded: true,
}

// GoogleAPIErrorReasons returns every reason string carried by e, from both
// the legacy googleapi.ErrorItem list and any structured google.rpc.ErrorInfo
// detail. Exported so pkg/connector (which already imports this package) can
// share it for its own reason-string checks (e.g.
// isCloudIdentityAPIDisabledError in pkg/connector/application.go) instead of
// duplicating this extraction.
func GoogleAPIErrorReasons(e *googleapi.Error) []string {
	reasons := make([]string, 0, len(e.Errors)+len(e.Details))
	for _, item := range e.Errors {
		reasons = append(reasons, item.Reason)
	}
	for _, detail := range e.Details {
		if m, ok := detail.(map[string]interface{}); ok {
			if reason, ok := m["reason"].(string); ok {
				reasons = append(reasons, reason)
			}
		}
	}
	return reasons
}

// isThrottled reports whether e is Google API rate limiting rather than a
// genuine permissions/delegation failure, per the reason strings documented
// at https://developers.google.com/workspace/admin/directory/v1/limits.
// Self-checks e.Code == 403 (rather than relying solely on its caller only
// invoking it from the StatusForbidden branch) to match the same
// self-contained scoping isCloudIdentityAPIDisabledError
// (pkg/connector/application.go) uses for its own 403 check.
func isThrottled(e *googleapi.Error) bool {
	if e.Code != http.StatusForbidden {
		return false
	}
	for _, reason := range GoogleAPIErrorReasons(e) {
		if throttleReasons[reason] {
			return true
		}
	}
	return false
}

// wrapGoogleApiErrorWithRateLimitInfo follows the baton-sdk pattern for WrapErrorsWithRateLimitInfo
// but adapted for googleapi.Error instead of http.Response.
func wrapGoogleApiErrorWithRateLimitInfo(preferredCode codes.Code, contextMsg string, e *googleapi.Error, errs ...error) error {
	msg := e.Message
	if msg == "" {
		msg = fmt.Sprintf("status code: %d", e.Code)
	}

	// Prepend context message to preserve it in the gRPC status
	// This is the ONLY place we should add context, to ensure the gRPC status is preserved
	if contextMsg != "" {
		msg = contextMsg + ": " + msg
	}

	st := status.New(preferredCode, msg)

	description, err := ratelimit.ExtractRateLimitData(e.Code, &e.Header)
	// Ignore any error extracting rate limit data
	if err == nil && description != nil {
		st, _ = st.WithDetails(description)
	}

	if len(errs) == 0 {
		return st.Err()
	}

	allErrs := append([]error{st.Err()}, errs...)
	return errors.Join(allErrs...)
}
