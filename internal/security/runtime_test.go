package security

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestHTTPClientBlocksLoopbackAtConnection(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Error("private target reached") }))
	defer remote.Close()
	policy := SourcePolicy{AllowPrivateSources: false}
	client := policy.HTTPClient(time.Second)
	response, err := client.Get(remote.URL)
	if err == nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("close unexpected response: %v", closeErr)
		}
		t.Fatal("expected private IP denied at connection")
	}
}
func TestHTTPClientRedirectCannotCrossHostname(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, "http://localhost:65532/private", http.StatusFound)
	}))
	defer remote.Close()
	policy := SourcePolicy{AllowPrivateSources: true}
	response, err := policy.HTTPClient(time.Second).Get(remote.URL)
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("cross-host redirect was accepted")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("unexpected redirect error: %v", err)
	}
}
func TestPolicyAttachedToJobContext(t *testing.T) {
	policy := &SourcePolicy{AllowPrivateSources: false}
	ctx := WithPolicy(context.Background(), policy)
	if PolicyFromContext(ctx) != policy {
		t.Fatal("missing execution policy")
	}
	if err := policy.ValidateForExecution(ctx, domain.SourceConfig{Endpoint: "http://127.0.0.1/private"}); err == nil {
		t.Fatal("expected blocked address")
	}
}
