package ingestion

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

const maxSourceBytes int64 = 20 << 20 // 20 MiB per source in Day 1

var client = &http.Client{Timeout: 15 * time.Second}

// ReadAPI fetches a JSON HTTP API with a context-bound GET request.
func ReadAPI(ctx context.Context, source domain.SourceConfig) ([]domain.Record, error) {
	if !isWebURL(source.Endpoint) {
		return nil, fmt.Errorf("api endpoint must be an http(s) URL")
	}
	payload, err := loadSource(ctx, source, "application/json", true)
	if err != nil {
		return nil, err
	}
	return parseJSON(ctx, source, payload)
}

// loadSource reads at most maxSourceBytes and closes all opened resources.
// For file-backed CSV/JSON inputs, local paths are relative to the process cwd.
func loadSource(ctx context.Context, source domain.SourceConfig, accept string, apiOnly bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var stream io.ReadCloser
	if isWebURL(source.Endpoint) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.Endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("create HTTP request: %w", err)
		}
		req.Header.Set("Accept", accept)
		for name, value := range source.Headers {
			req.Header.Set(name, value)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch source: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("fetch source: HTTP %d", resp.StatusCode)
		}
		stream = resp.Body
	} else {
		if apiOnly || strings.Contains(source.Endpoint, "://") {
			return nil, fmt.Errorf("source requires an http(s) URL")
		}
		file, err := os.Open(source.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("open source: %w", err)
		}
		stream = file
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, maxSourceBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read source: %w", err)
	}
	if int64(len(data)) > maxSourceBytes {
		return nil, fmt.Errorf("source exceeds maximum size of %d bytes", maxSourceBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func isWebURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
}
