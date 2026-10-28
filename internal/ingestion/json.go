package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// ReadJSON handles local or remote JSON files, with an optional record_path.
func ReadJSON(ctx context.Context, source domain.SourceConfig) ([]domain.Record, error) {
	payload, err := loadSource(ctx, source, "application/json", false)
	if err != nil {
		return nil, err
	}
	return parseJSON(ctx, source, payload)
}

func parseJSON(ctx context.Context, source domain.SourceConfig, payload []byte) ([]domain.Record, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("decode JSON: unexpected trailing JSON content")
	}
	value := root
	if source.RecordPath != "" {
		for _, field := range strings.Split(source.RecordPath, ".") {
			obj, ok := value.(map[string]any)
			if !ok || field == "" {
				return nil, fmt.Errorf("record_path %q: expected an object at %q", source.RecordPath, field)
			}
			var exists bool
			value, exists = obj[field]
			if !exists {
				return nil, fmt.Errorf("record_path %q: field %q not found", source.RecordPath, field)
			}
		}
	}
	var rows []any
	switch v := value.(type) {
	case []any:
		rows = v
	case map[string]any:
		rows = []any{v}
	default:
		return nil, fmt.Errorf("JSON root/record_path must be an object or array of objects")
	}
	records := make([]domain.Record, 0, len(rows))
	for i, item := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("JSON record %d must be an object", i+1)
		}
		records = append(records, makeRecord(source, i+1, data))
	}
	return records, nil
}
