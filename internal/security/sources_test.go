package security

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestSourceFilePolicy(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "input.csv")
	if err := os.WriteFile(file, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	policy := SourcePolicy{InputDir: root}
	if err := policy.Validate(context.Background(), []domain.SourceConfig{{Type: "csv", Endpoint: file}}); err != nil {
		t.Fatal(err)
	}
	if err := policy.Validate(context.Background(), []domain.SourceConfig{{Type: "csv", Endpoint: filepath.Join(root, "..", "other.csv")}}); err == nil {
		t.Fatal("expected path rejection")
	}
}

func TestPublicIP(t *testing.T) {
	for _, tc := range []struct {
		ip     string
		public bool
	}{
		{"127.0.0.1", false}, {"10.1.2.3", false}, {"169.254.169.254", false},
		{"100.100.100.200", false}, {"8.8.8.8", true}, {"192.0.2.1", false},
		{"::1", false}, {"2001:db8::1", false},
	} {
		if got := isPublicIP(netip.MustParseAddr(tc.ip)); got != tc.public {
			t.Errorf("%s public=%v", tc.ip, got)
		}
	}
}

func TestPrivateURL(t *testing.T) {
	policy := SourcePolicy{AllowPrivateSources: false}
	if err := policy.Validate(context.Background(), []domain.SourceConfig{{Type: "api", Endpoint: "http://127.0.0.1/data"}}); err == nil {
		t.Fatal("expected localhost rejection")
	}
}
