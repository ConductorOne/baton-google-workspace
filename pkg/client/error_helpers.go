package client

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			// 403 can also carry Google's rate-limit reasons, not just 429 -
			// see https://developers.google.com/workspace/admin/directory/v1/limits.
			// Reclassify to Unavailable so it rides the SDK's retry-with-backoff.
			// quotaExceeded is excluded (see throttleReasons): it's long-lived
			// quota exhaustion, not transient, and the retryer has no attempt
			// ceiling. Attach rate-limit detail by hand since
			// ratelimit.ExtractRateLimitData only handles 429.
			return wrapGoogleApiErrorWithRateLimitInfoDetail(codes.Unavailable, contextMsg, e, throttled403RateLimitDescription(), err)
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

// Reason strings (legacy ErrorItem and structured ErrorInfo) indicating rate
// limiting on a 403; errorReasonQuotaExceeded is intentionally excluded from
// throttleReasons below (see the 403 case in wrapGoogleApiErrorWithContext).
const (
	errorReasonUserRateLimitExceeded = "userRateLimitExceeded"
	errorReasonQuotaExceeded         = "quotaExceeded"
	errorReasonRateLimitExceeded     = "rateLimitExceeded"
	errorInfoReasonRateLimitExceeded = "RATE_LIMIT_EXCEEDED"
)

// throttleReasons are the 403 reasons reclassified to codes.Unavailable
// (errorReasonQuotaExceeded deliberately excluded - see the 403 case above).
var throttleReasons = map[string]bool{
	errorReasonUserRateLimitExceeded: true,
	errorReasonRateLimitExceeded:     true,
	errorInfoReasonRateLimitExceeded: true,
}

// GoogleAPIErrorReasons returns every reason string carried by e, from both
// the legacy googleapi.ErrorItem list and any structured google.rpc.ErrorInfo
// detail. Exported so pkg/connector can share it (e.g.
// isCloudIdentityAPIDisabledError) instead of duplicating this extraction.
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
// genuine permissions/delegation failure. Self-checks e.Code == 403 rather
// than trusting the caller to only invoke it from the StatusForbidden branch.
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
	return wrapGoogleApiErrorWithRateLimitInfoDetail(preferredCode, contextMsg, e, nil, errs...)
}

// wrapGoogleApiErrorWithRateLimitInfoDetail is wrapGoogleApiErrorWithRateLimitInfo
// with an optional override for the RateLimitDescription normally derived
// from ratelimit.ExtractRateLimitData, which only populates one for e.Code
// == 429.
func wrapGoogleApiErrorWithRateLimitInfoDetail(preferredCode codes.Code, contextMsg string, e *googleapi.Error, override *v2.RateLimitDescription, errs ...error) error {
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

	description := override
	if description == nil {
		if d, err := ratelimit.ExtractRateLimitData(e.Code, &e.Header); err == nil {
			description = d
		}
	}
	if description != nil {
		st, _ = st.WithDetails(description)
	}

	if len(errs) == 0 {
		return st.Err()
	}

	allErrs := append([]error{st.Err()}, errs...)
	return errors.Join(allErrs...)
}

// throttled403RateLimitDescription synthesizes a RateLimitDescription for a
// reclassified 403 throttle, using userRateLimitExceeded's documented
// per-user, per-100-second window as the wait hint (see
// https://developers.google.com/workspace/admin/directory/v1/limits).
func throttled403RateLimitDescription() *v2.RateLimitDescription {
	return v2.RateLimitDescription_builder{
		Status:    v2.RateLimitDescription_STATUS_OVERLIMIT,
		Limit:     1,
		Remaining: 0,
		ResetAt:   timestamppb.New(time.Now().Add(100 * time.Second)),
	}.Build()
}
