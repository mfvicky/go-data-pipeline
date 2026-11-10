package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func fixture(name string) string { return filepath.Join("..", "..", "testdata", "input", name) }

func TestReadCSV(t *testing.T) {
	records, err := Read(context.Background(), domain.SourceConfig{Name: "csv_customers", Type: "csv", Endpoint: fixture("customers.csv")})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || records[0].ID != "csv_customers:1" || records[0].Data["name"] != "Alice" || records[2].Data["amount"] != "210.25" {
		t.Fatalf("unexpected parsed CSV: %+v", records)
	}
}

func TestReadJSONAndNestedPath(t *testing.T) {
	records, err := Read(context.Background(), domain.SourceConfig{Type: "json", Name: "json_customers", Endpoint: fixture("customers.json")})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Data["amount"].(json.Number).String() != "75.5" {
		t.Fatalf("parsed JSON: %+v", records)
	}
	nested, err := Read(context.Background(), domain.SourceConfig{Type: "json", Endpoint: fixture("nested.json"), RecordPath: "data.records"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nested) != 2 || nested[1].Data["id"].(json.Number).String() != "102" {
		t.Fatalf("nested: %+v", nested)
	}
}

func TestReadHTTPAPIWithHeadersAndNestedPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("X-Test-Token") != "secret" || r.Header.Get("Accept") != "application/json" {
			http.Error(w, "incorrect method/headers", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"id":1,"name":"one"},{"id":2,"name":"two"}]}`))
	}))
	defer server.Close()
	records, err := Read(context.Background(), domain.SourceConfig{Type: "api", Endpoint: server.URL, Name: "api_users", Headers: map[string]string{"X-Test-Token": "secret"}, RecordPath: "results"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[1].Source != "api_users" || records[0].Data["name"] != "one" {
		t.Fatalf("parsed API: %+v", records)
	}
}

func TestReadHTTPCSVAndJSONURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/data.csv" {
			_, _ = w.Write([]byte("a,b\n1,hello\n"))
			return
		}
		_, _ = w.Write([]byte(`[{"id":3}]`))
	}))
	defer server.Close()
	for _, tc := range []struct {
		kind, url string
		want      int
	}{
		{"csv", server.URL + "/data.csv", 1},
		{"json", server.URL + "/data.json", 1},
	} {
		got, err := Read(context.Background(), domain.SourceConfig{Type: tc.kind, Endpoint: tc.url})
		if err != nil || len(got) != tc.want {
			t.Fatalf("%s source: %v / %+v", tc.kind, err, got)
		}
	}
}

func TestReadErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "not available", http.StatusBadGateway) }))
	defer server.Close()
	for _, tc := range []struct {
		name   string
		source domain.SourceConfig
		want   string
	}{
		{"empty endpoint", domain.SourceConfig{Type: "csv"}, "endpoint"},
		{"wrong type", domain.SourceConfig{Type: "xml", Endpoint: "x"}, "unsupported"},
		{"missing file", domain.SourceConfig{Type: "csv", Endpoint: "unknown.csv"}, "open source"},
		{"bad URL", domain.SourceConfig{Type: "api", Endpoint: "/path"}, "http(s)"},
		{"bad HTTP status", domain.SourceConfig{Type: "api", Endpoint: server.URL}, "HTTP 502"},
		{"invalid nested field", domain.SourceConfig{Type: "json", Endpoint: fixture("nested.json"), RecordPath: "nope"}, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Read(context.Background(), tc.source)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, expected %q", err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name, data string
		kind       string
		want       string
	}{
		{"csv dup headers", "a,a\n1,2\n", "csv", "duplicated"},
		{"csv malformed row", "a,b\n1\n", "csv", "csv row"},
		{"json invalid", `{"foo":`, "json", "decode JSON"},
		{"json scalar", `123`, "json", "must be an object"},
		{"json mixed", `[{"a":1},42]`, "json", "must be an object"},
		{"json trailing", `{} {}`, "json", "trailing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "data")
			if err := os.WriteFile(file, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Read(context.Background(), domain.SourceConfig{Type: tc.kind, Endpoint: file})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, expected %q", err, tc.want)
			}
		})
	}
}

func TestReadCancelledHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := Read(ctx, domain.SourceConfig{Type: "api", Endpoint: server.URL})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
}

func TestSendToChannelContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan domain.Record)
	result := make(chan error, 1)
	go func() {
		_, err := SendToChannel(ctx, domain.SourceConfig{Type: "csv", Endpoint: fixture("customers.csv")}, out)
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want cancelled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendToChannel did not stop on cancellation")
	}
}
