// Package ingestion reads local/remote CSV, JSON, and JSON HTTP API sources.
// Day 1 exposes a slice-based Read API; Day 2's runner can fan out one Read
// per source and send returned typed records into the pipeline's channel.
package ingestion

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// Read loads a source into typed domain records. CSV values remain strings;
// JSON numeric values are json.Number for lossless downstream conversion.
func Read(ctx context.Context, source domain.SourceConfig) ([]domain.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.Endpoint == "" {
		return nil, fmt.Errorf("source endpoint is required")
	}
	switch source.Type {
	case "csv":
		return ReadCSV(ctx, source)
	case "json":
		return ReadJSON(ctx, source)
	case "api":
		return ReadAPI(ctx, source)
	default:
		return nil, fmt.Errorf("unsupported source type %q", source.Type)
	}
}

// SendToChannel bridges Day 1 readers into the typed channel interface needed
// by the later pipeline runner. The caller owns/should close out, not this func.
func SendToChannel(ctx context.Context, source domain.SourceConfig, out chan<- domain.Record) (int, error) {
	records, err := Read(ctx, source)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, record := range records {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- record:
			count++
		}
	}
	return count, nil
}

func sourceName(s domain.SourceConfig) string {
	if s.Name != "" {
		return s.Name
	}
	endpoint := s.Endpoint
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Scheme != "" {
		endpoint = parsed.Path
	}
	name := path.Base(strings.ReplaceAll(endpoint, "\\", "/"))
	if name == "." || name == "/" || name == "" {
		name = "source"
	}
	return s.Type + ":" + name
}

func makeRecord(s domain.SourceConfig, number int, data map[string]any) domain.Record {
	name := sourceName(s)
	return domain.Record{
		ID:        fmt.Sprintf("%s:%d", name, number),
		Source:    name,
		Data:      data,
		Timestamp: time.Now().UTC(),
	}
}
