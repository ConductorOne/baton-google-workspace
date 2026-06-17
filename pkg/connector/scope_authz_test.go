package connector

import (
	"net/http"
	"testing"

	"golang.org/x/oauth2"
)

// TestIsScopeUnauthorized guards the CXP-661 fix: Google reports an unauthorized
// domain-wide-delegation scope as a 403 with error "access_denied", not a 401. Both 401 and
// that 403 must classify as authorization errors so a missing optional scope (e.g.
// admin.directory.rolemanagement, used only for role provisioning) is treated as a service
// being unavailable rather than aborting the whole sync. Genuine 403s (real permission denials)
// and other statuses must NOT be swallowed.
func TestIsScopeUnauthorized(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		errorCode string
		body      string
		nilResp   bool
		want      bool
	}{
		{name: "401 unauthorized (service account not delegated)", status: http.StatusUnauthorized, want: true},
		{name: "403 access_denied via ErrorCode", status: http.StatusForbidden, errorCode: "access_denied", want: true},
		// JWT two-legged flow: ErrorCode is empty, the code lives only in Body. This is the real
		// shape Google returns for an unauthorized DWD scope.
		{name: "403 access_denied via Body (JWT flow)", status: http.StatusForbidden, body: `{"error":"access_denied","error_description":"Requested client not authorized."}`, want: true},
		{name: "403 without access_denied (genuine permission error)", status: http.StatusForbidden, errorCode: "insufficient_scope", want: false},
		{name: "403 with unrelated Body error", status: http.StatusForbidden, body: `{"error":"insufficient_scope"}`, want: false},
		{name: "403 empty body", status: http.StatusForbidden, want: false},
		{name: "500 server error", status: http.StatusInternalServerError, want: false},
		{name: "nil response", nilResp: true, want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			oe := &oauth2.RetrieveError{ErrorCode: tc.errorCode, Body: []byte(tc.body)}
			if !tc.nilResp {
				oe.Response = &http.Response{StatusCode: tc.status}
			}
			if got := isScopeUnauthorized(oe); got != tc.want {
				t.Fatalf("isScopeUnauthorized = %v, want %v", got, tc.want)
			}
		})
	}
}
