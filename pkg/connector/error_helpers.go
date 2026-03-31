package connector

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
// For non-googleapi errors (e.g., transient network errors like EOF), it wraps them as
// codes.Unavailable so the baton framework treats them as retryable.
func wrapGoogleApiErrorWithContext(err error, contextMsg string) error {
	var e *googleapi.Error
	if ok := errors.As(err, &e); !ok {
		// Handle transient network errors that aren't googleapi.Error.
		// These are typically EOF, connection reset, etc. that occur at the
		// transport layer. Wrap them as Unavailable so the baton framework
		// can retry the sync operation.
		if isTransientError(err) {
			msg := "transient network error"
			if contextMsg != "" {
				msg = contextMsg + ": " + msg
			}
			st := status.New(codes.Unavailable, msg)
			return errors.Join(st.Err(), err)
		}
		return err
	}

	switch e.Code {
	case http.StatusBadRequest:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.InvalidArgument, contextMsg, e, err)
	case http.StatusUnauthorized:
		return wrapGoogleApiErrorWithRateLimitInfo(codes.Unauthenticated, contextMsg, e, err)
	case http.StatusForbidden:
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
