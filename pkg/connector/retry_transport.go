package connector

import (
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// retryTransport wraps an http.RoundTripper and retries requests that fail with
// transient errors (EOF, connection reset, temporary network errors). This is
// necessary because the Google Admin SDK's generated code uses
// gensupport.SendRequest (without retry), so transient network errors propagate
// directly as sync failures.
type retryTransport struct {
	base       http.RoundTripper
	maxRetries int
}

// newRetryTransport wraps the given transport with retry logic for transient errors.
func newRetryTransport(base http.RoundTripper) http.RoundTripper {
	return &retryTransport{
		base:       base,
		maxRetries: 3,
	}
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		resp, err = t.base.RoundTrip(req)
		if err == nil {
			return resp, nil
		}

		if !isTransientError(err) {
			return resp, err
		}

		// Don't retry if the request context is already cancelled.
		if req.Context().Err() != nil {
			return resp, err
		}

		// Don't retry if the request body can't be replayed.
		if req.Body != nil && req.GetBody == nil {
			return resp, err
		}

		if attempt < t.maxRetries {
			l := ctxzap.Extract(req.Context())
			l.Warn("google-workspace: retrying request after transient error",
				zap.Error(err),
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", t.maxRetries),
				zap.String("method", req.Method),
				zap.String("url", req.URL.String()),
			)

			// Close the previous response body if present.
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}

			// Reset the request body for retry.
			if req.GetBody != nil {
				req.Body, err = req.GetBody()
				if err != nil {
					return nil, err
				}
			}

			// Exponential backoff with jitter: base * 2^attempt + random jitter.
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 500 * time.Millisecond
			jitter := time.Duration(rand.Int64N(int64(backoff / 2)))
			sleep := backoff + jitter

			timer := time.NewTimer(sleep)
			select {
			case <-req.Context().Done():
				timer.Stop()
				return resp, req.Context().Err()
			case <-timer.C:
			}
		}
	}

	return resp, err
}

// isTransientError returns true if the error is a transient network error that
// should be retried.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// EOF errors indicate the server closed the connection unexpectedly.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Connection reset by peer.
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	// Connection refused (server not accepting connections).
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	// Broken pipe.
	if errors.Is(err, syscall.EPIPE) {
		return true
	}

	// net.ErrClosed: use of closed network connection.
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	// url.Error wraps transport errors; check if it's a timeout or temporary.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Temporary() {
			return true
		}
		// Check the underlying error message for common transient patterns.
		errMsg := urlErr.Error()
		if strings.Contains(errMsg, "connection refused") ||
			strings.Contains(errMsg, "connection reset") ||
			strings.Contains(errMsg, "broken pipe") ||
			strings.Contains(errMsg, "EOF") {
			return true
		}
	}

	// net.OpError for lower-level network errors.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Temporary() {
			return true
		}
	}

	return false
}
