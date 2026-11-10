package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
)

func TestRouterAuthenticationAndDocumentation(t *testing.T) {
	router := NewRouterWithOptions(repository.NewJobRepository(), Options{APIKey: "my-secret"})
	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, PipelinesPath, nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", unauthorized.Code)
	}
	authorized := httptest.NewRequest(http.MethodGet, PipelinesPath, nil)
	authorized.Header.Set("X-API-Key", "my-secret")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, authorized)
	if resp.Code != http.StatusOK || resp.Header().Get("X-Request-ID") == "" {
		t.Fatalf("authorized: status=%d request_id=%q", resp.Code, resp.Header().Get("X-Request-ID"))
	}
	for _, path := range []string{HealthPath, SwaggerPath, OpenAPIPath} {
		check := httptest.NewRecorder()
		router.ServeHTTP(check, httptest.NewRequest(http.MethodGet, path, nil))
		if check.Code != http.StatusOK || check.Body.Len() == 0 {
			t.Fatalf("%s: %d", path, check.Code)
		}
	}
}

func TestRouterRateLimit(t *testing.T) {
	router := NewRouterWithOptions(repository.NewJobRepository(), Options{RateLimitPerMinute: 1})
	for _, want := range []int{200, 429} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, PipelinesPath, nil))
		if response.Code != want {
			t.Fatalf("got status %d, want %d", response.Code, want)
		}
	}
}

func TestRouterRestrictsInputFiles(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "allowed.csv")
	if err := os.WriteFile(file, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	router := NewRouterWithOptions(repository.NewJobRepository(), Options{
		SourcePolicy: &security.SourcePolicy{InputDir: root},
	})
	create := func(path string) int {
		body, err := json.Marshal(map[string]any{"sources": []map[string]string{{"type": "csv", "endpoint": path}}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, PipelinesPath, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response.Code
	}
	if got := create(file); got != http.StatusCreated {
		t.Fatalf("allowed source: %d", got)
	}
	if got := create(filepath.Join(root, "..", "not-allowed.csv")); got != http.StatusBadRequest {
		t.Fatalf("outside source: %d", got)
	}
	if got := create("http://127.0.0.1/private"); got != http.StatusBadRequest {
		t.Fatalf("private remote source: %d", got)
	}
}

func TestSwaggerContainsDocumentedRoutes(t *testing.T) {
	router := NewRouter(repository.NewJobRepository())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, OpenAPIPath, nil))
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), PipelinesPath) {
		t.Fatalf("expected OpenAPI document, got %d", recorder.Code)
	}
}
