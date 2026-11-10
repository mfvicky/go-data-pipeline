package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuth(t *testing.T) {
	h := APIKey("secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	bad := httptest.NewRecorder()
	h.ServeHTTP(bad, httptest.NewRequest("GET", "/", nil))
	if bad.Code != 401 {
		t.Fatalf("expected 401, got %d", bad.Code)
	}
	good := httptest.NewRequest("GET", "/", nil)
	good.Header.Set(APIKeyHeader, "secret")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, good)
	if recorder.Code != 200 {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
}

func TestRequestIDAndRecovery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := RequestID(Recovery(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if GetRequestID(r.Context()) == "" {
			t.Error("missing request id")
		}
		panic("example panic")
	})))
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest("GET", "/", nil))
	if recorder.Code != 500 || recorder.Header().Get(RequestIDHeader) == "" {
		t.Fatalf("status=%d", recorder.Code)
	}
}

func TestRateLimit(t *testing.T) {
	limit := NewLimiter(2)
	h := limit.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	for i, want := range []int{200, 200, 429} {
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, httptest.NewRequest("GET", "/", nil))
		if resp.Code != want {
			t.Fatalf("request %d: got %d, want %d", i, resp.Code, want)
		}
	}
}
