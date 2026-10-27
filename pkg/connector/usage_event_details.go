package connector

import (
	"math"
	"net"
	"regexp"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
)

// These mirror the validate rules on v2.NetworkOrigin and v2.OAuthUsage. Event validation
// rejects the whole event on one failing value, so values outside them are dropped instead.
var (
	countryCodeRegex     = regexp.MustCompile(`^[A-Z]{2}$`)
	subdivisionCodeRegex = regexp.MustCompile(`^[A-Z]{2}-[A-Z0-9]{1,3}$`)
)

const (
	maxNetworkASNs      = 64
	maxOAuthScopes      = 256
	maxOAuthScopeBytes  = 512
	maxOAuthStringBytes = 256
)

var oauthActionsByEventName = map[string]v2.OAuthUsage_Action{
	"authorize": v2.OAuthUsage_ACTION_AUTHORIZE,
	"revoke":    v2.OAuthUsage_ACTION_REVOKE,
	"deny":      v2.OAuthUsage_ACTION_DENY,
	"request":   v2.OAuthUsage_ACTION_REQUEST,
}

// networkOriginFromActivity returns nil when the activity carries no usable network data.
func networkOriginFromActivity(activity *reportsAdmin.Activity) *v2.NetworkOrigin {
	origin := &v2.NetworkOrigin{}
	if net.ParseIP(activity.IpAddress) != nil {
		origin.IpAddress = activity.IpAddress
	}
	if info := activity.NetworkInfo; info != nil {
		if code := strings.ToUpper(info.RegionCode); countryCodeRegex.MatchString(code) {
			origin.CountryCode = code
		}
		if code := strings.ToUpper(info.SubdivisionCode); subdivisionCodeRegex.MatchString(code) {
			origin.SubdivisionCode = code
		}
		for _, asn := range info.IpAsn {
			if len(origin.Asns) == maxNetworkASNs {
				break
			}
			// Stricter than the SDK, which bounds only the count: this stops a negative int64
			// from wrapping into a bogus uint32, and AS 0 is reserved (RFC 7607).
			if asn > 0 && asn <= math.MaxUint32 {
				origin.Asns = append(origin.Asns, uint32(asn))
			}
		}
	}
	if origin.IpAddress == "" && origin.CountryCode == "" && origin.SubdivisionCode == "" && len(origin.Asns) == 0 {
		return nil
	}
	return origin
}

// oauthUsageFromEvent always returns non-nil: the event is OAuth consent activity even when
// every value in it is dropped as unusable.
func oauthUsageFromEvent(event *reportsAdmin.ActivityEvents) *v2.OAuthUsage {
	usage := &v2.OAuthUsage{Action: oauthActionsByEventName[event.Name]}
	if len(event.Name) <= maxOAuthStringBytes {
		usage.SourceEventName = event.Name
	}
	for _, p := range event.Parameters {
		switch p.Name {
		case "scope":
			scopes := p.MultiValue
			if len(scopes) == 0 && p.Value != "" {
				scopes = []string{p.Value}
			}
			for _, scope := range scopes {
				if len(usage.Scopes) == maxOAuthScopes {
					break
				}
				if scope != "" && len(scope) <= maxOAuthScopeBytes {
					usage.Scopes = append(usage.Scopes, scope)
				}
			}
		case "client_type":
			if len(p.Value) <= maxOAuthStringBytes {
				usage.ClientType = p.Value
			}
		}
	}
	return usage
}
