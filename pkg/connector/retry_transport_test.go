package connector

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "io.EOF",
			err:      io.EOF,
			expected: true,
		},
		{
			name:     "io.ErrUnexpectedEOF",
			err:      io.ErrUnexpectedEOF,
			expected: true,
		},
		{
			name:     "wrapped io.EOF",
			err:      errors.Join(errors.New("something failed"), io.EOF),
			expected: true,
		},
		{
			name:     "ECONNRESET",
			err:      syscall.ECONNRESET,
			expected: true,
		},
		{
			name:     "ECONNREFUSED",
			err:      syscall.ECONNREFUSED,
			expected: true,
		},
		{
			name:     "EPIPE",
			err:      syscall.EPIPE,
			expected: true,
		},
		{
			name:     "net.ErrClosed",
			err:      net.ErrClosed,
			expected: true,
		},
		{
			name: "url.Error with EOF",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: io.EOF,
			},
			expected: true,
		},
		{
			name:     "non-transient error",
			err:      errors.New("permission denied"),
			expected: false,
		},
		{
			name:     "non-transient wrapped error",
			err:      errors.Join(errors.New("something"), errors.New("not transient")),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTransientError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

// mockRoundTripper is a test helper that returns errors for the first N calls,
// then succeeds.
type mockRoundTripper struct {
	errors     []error
	callCount  int
	successful bool
}

func (m *mockRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	defer func() { m.callCount++ }()

	if m.callCount < len(m.errors) {
		return nil, m.errors[m.callCount]
	}

	m.successful = true
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
	}, nil
}

func TestRetryTransport_RetriesOnEOF(t *testing.T) {
	mock := &mockRoundTripper{
		errors: []error{io.EOF, io.ErrUnexpectedEOF},
	}
	transport := newRetryTransport(mock)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, mock.successful)
	require.Equal(t, 3, mock.callCount) // 2 failures + 1 success
}

func TestRetryTransport_DoesNotRetryNonTransient(t *testing.T) {
	mock := &mockRoundTripper{
		errors: []error{errors.New("permission denied")},
	}
	transport := newRetryTransport(mock)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req) //nolint:bodyclose // resp is nil on error
	require.Error(t, err)
	require.Nil(t, resp)
	require.False(t, mock.successful)
	require.Equal(t, 1, mock.callCount) // Only 1 attempt, no retry
}

func TestRetryTransport_ExhaustsRetries(t *testing.T) {
	mock := &mockRoundTripper{
		errors: []error{io.EOF, io.EOF, io.EOF, io.EOF}, // More errors than max retries
	}
	transport := newRetryTransport(mock)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req) //nolint:bodyclose // resp is nil on error
	require.Error(t, err)
	require.Nil(t, resp)
	require.False(t, mock.successful)
	require.Equal(t, 4, mock.callCount) // 1 initial + 3 retries
	require.True(t, strings.Contains(err.Error(), "EOF"))
}

func TestRetryTransport_RetriesOnConnectionReset(t *testing.T) {
	mock := &mockRoundTripper{
		errors: []error{syscall.ECONNRESET},
	}
	transport := newRetryTransport(mock)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, mock.successful)
	require.Equal(t, 2, mock.callCount) // 1 failure + 1 success
}

func TestRetryTransport_SuccessOnFirstAttempt(t *testing.T) {
	mock := &mockRoundTripper{
		errors: []error{}, // No errors
	}
	transport := newRetryTransport(mock)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, mock.successful)
	require.Equal(t, 1, mock.callCount) // Only 1 attempt
}
