package security

import "testing"

func TestAllowedHost(t *testing.T) {
	patterns := []string{"example.com", "*.trusted.org"}
	for _, tc := range []struct {
		host  string
		allow bool
	}{
		{"example.com", true}, {"EXAMPLE.com.", true}, {"child.trusted.org", true},
		{"trusted.org", false}, {"example.com.evil.com", false}, {"untrusted.org", false},
	} {
		if got := allowedHost(tc.host, patterns); got != tc.allow {
			t.Errorf("host %q allowed=%v expected=%v", tc.host, got, tc.allow)
		}
	}
}
