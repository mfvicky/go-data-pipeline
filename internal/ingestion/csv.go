package ingestion

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// ReadCSV parses a CSV file/URL. The first row is a header. It rejects empty
// and duplicate header names, and malformed row lengths.
func ReadCSV(ctx context.Context, source domain.SourceConfig) ([]domain.Record, error) {
	payload, err := loadSource(ctx, source, "text/csv", false)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bytes.NewReader(payload))
	r.TrimLeadingSpace = true
	headers, err := r.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("csv file is empty")
	}
	if err != nil {
		return nil, fmt.Errorf("csv header: %w", err)
	}
	seen := map[string]bool{}
	for i, h := range headers {
		if i == 0 {
			h = strings.TrimPrefix(h, "\ufeff")
		}
		h = strings.TrimSpace(h)
		if h == "" || seen[h] {
			return nil, fmt.Errorf("csv header %d is empty or duplicated: %q", i+1, h)
		}
		headers[i] = h
		seen[h] = true
	}
	records := make([]domain.Record, 0)
	for rowNum := 1; ; rowNum++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv row %d: %w", rowNum+1, err)
		}
		data := make(map[string]any, len(headers))
		for i, header := range headers {
			data[header] = row[i]
		}
		records = append(records, makeRecord(source, rowNum, data))
	}
	return records, nil
}
