package ingestion

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/security"
)

// Stream decodes top-level CSV/JSON records incrementally and applies
// backpressure through send. Nested record_path uses token traversal without buffering the whole document.
func Stream(ctx context.Context, source domain.SourceConfig, send func(domain.Record) error) (int, error) {
	if source.Adapter != "" {
		return StreamWithErrors(ctx, source, send, nil)
	}
	if source.Type != "csv" && source.Type != "json" && source.Type != "api" {
		return 0, fmt.Errorf("invalid source type %q", source.Type)
	}
	accept := "application/json"
	if source.Type == "csv" {
		accept = "text/csv"
	}
	stream, err := openStream(ctx, source, accept, source.Type == "api")
	if err != nil {
		return 0, err
	}
	defer func() { _ = stream.Close() }()
	reader := &boundedReader{ctx: ctx, source: stream, max: maxSourceBytes}
	if source.Type == "csv" {
		return streamCSV(ctx, source, reader, send)
	}
	return streamJSON(ctx, source, reader, send)
}

type boundedReader struct {
	ctx       context.Context
	source    io.Reader
	used, max int64
}

func (b *boundedReader) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	remaining := b.max - b.used
	if remaining < 0 {
		return 0, fmt.Errorf("source exceeds maximum size of %d bytes", b.max)
	}
	if int64(len(p)) > remaining+1 {
		p = p[:int(remaining+1)]
	}
	n, err := b.source.Read(p)
	b.used += int64(n)
	if b.used > b.max {
		return 0, fmt.Errorf("source exceeds maximum size of %d bytes", b.max)
	}
	return n, err
}

func openStream(ctx context.Context, source domain.SourceConfig, accept string, apiOnly bool) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isWebURL(source.Endpoint) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.Endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", accept)
		for key, value := range source.Headers {
			req.Header.Set(key, value)
		}
		active := client
		if policy := security.PolicyFromContext(ctx); policy != nil {
			if err := policy.ValidateForExecution(ctx, source); err != nil {
				return nil, err
			}
			active = policy.HTTPClient(defaultHTTPTimeout)
		}
		resp, err := active.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch source: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("fetch source: HTTP %d", resp.StatusCode)
		}
		return resp.Body, nil
	}
	if apiOnly || strings.Contains(source.Endpoint, "://") {
		return nil, fmt.Errorf("source requires http(s) URL")
	}
	if policy := security.PolicyFromContext(ctx); policy != nil {
		return policy.OpenFile(source.Endpoint)
	}
	return os.Open(source.Endpoint)
}

func streamCSV(ctx context.Context, source domain.SourceConfig, reader io.Reader, send func(domain.Record) error) (int, error) {
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	headers, err := r.Read()
	if err == io.EOF {
		return 0, fmt.Errorf("csv file is empty")
	}
	if err != nil {
		return 0, fmt.Errorf("csv header: %w", err)
	}
	seen := map[string]bool{}
	for i, header := range headers {
		if i == 0 {
			header = strings.TrimPrefix(header, "\ufeff")
		}
		header = strings.TrimSpace(header)
		if header == "" || seen[header] {
			return 0, fmt.Errorf("csv header %d is empty or duplicate", i+1)
		}
		headers[i] = header
		seen[header] = true
	}
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		row, err := r.Read()
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, fmt.Errorf("csv row %d: %w", count+2, err)
		}
		values := make(map[string]any, len(headers))
		for i, key := range headers {
			values[key] = row[i]
		}
		if err := send(makeRecord(source, count+1, values)); err != nil {
			return count, err
		}
		count++
	}
}
func streamJSON(ctx context.Context, source domain.SourceConfig, reader io.Reader, send func(domain.Record) error) (int, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	count := 0
	var path []string
	if source.RecordPath != "" {
		path = strings.Split(source.RecordPath, ".")
	}
	if err := decodePath(ctx, decoder, path, source, send, &count); err != nil {
		return count, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return count, fmt.Errorf("unexpected trailing JSON content")
	}
	return count, nil
}

func decodePath(ctx context.Context, decoder *json.Decoder, path []string, source domain.SourceConfig, send func(domain.Record) error, count *int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	first, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	delim, ok := first.(json.Delim)
	if !ok {
		return fmt.Errorf("JSON root/record_path must contain objects or arrays")
	}
	if len(path) > 0 {
		if delim != '{' || path[0] == "" {
			return fmt.Errorf("record_path %q: expected an object", source.RecordPath)
		}
		found := false
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			if key.(string) == path[0] {
				if err := decodePath(ctx, decoder, path[1:], source, send, count); err != nil {
					return err
				}
				found = true
			} else if err := skipJSONValue(decoder); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("record_path %q: field %q not found", source.RecordPath, path[0])
		}
		return nil
	}
	switch delim {
	case '[':
		for decoder.More() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var data map[string]any
			if err := decoder.Decode(&data); err != nil {
				return fmt.Errorf("JSON record %d: %w", *count+1, err)
			}
			if data == nil {
				return fmt.Errorf("JSON record %d must be an object", *count+1)
			}
			if err := send(makeRecord(source, *count+1, data)); err != nil {
				return err
			}
			*count++
		}
		_, err = decoder.Token()
		return err
	case '{':
		data := make(map[string]any)
		for decoder.More() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			var value any
			if err := decoder.Decode(&value); err != nil {
				return err
			}
			data[key.(string)] = value
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		if err := send(makeRecord(source, *count+1, data)); err != nil {
			return err
		}
		*count++
		return nil
	default:
		return fmt.Errorf("JSON root/record_path must be an object or array of objects")
	}
}

// skipJSONValue consumes unknown subtrees without buffering them in a large map.
func skipJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return err
			}
			if err := skipJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := skipJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected closing JSON token")
	}
	_, err = decoder.Token()
	return err
}
