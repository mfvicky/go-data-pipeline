package export

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// Report is the JSON export format. CSV exports contain the processed records.
type Report struct {
	JobID         string                     `json:"job_id"`
	Records       []domain.Record            `json:"records"`
	Aggregations  []domain.AggregationResult `json:"aggregations"`
	DailyReport   []domain.DailyReportRow    `json:"daily_report,omitempty"`
	Errors        []domain.PipelineError     `json:"errors"`
	FailedRecords []domain.FailedRecord      `json:"failed_records"`
}

// Write creates an export via a temporary file so failures don't leave a
// partially written target. The caller controls the destination path.
func Write(ctx context.Context, target domain.ExportConfig, report Report) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target.Type == "postgres" {
		return writePostgres(ctx, report)
	}
	if target.Path == "" {
		return fmt.Errorf("export path is required")
	}
	if target.Type != "json" && target.Type != "csv" {
		return fmt.Errorf("unsupported export type %q", target.Type)
	}

	dir := filepath.Dir(target.Path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("create export directory: %w", err)
	}
	file, err := os.CreateTemp(dir, ".pipeline-export-*")
	if err != nil {
		return fmt.Errorf("create export file: %w", err)
	}
	defer func() { _ = os.Remove(file.Name()) }()

	switch target.Type {
	case "json":
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(report)
	case "csv":
		if len(report.DailyReport) > 0 {
			err = writeDailyCSV(ctx, file, report.DailyReport)
		} else {
			err = writeCSV(ctx, file, report.Records)
		}
	}
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s export: %w", target.Type, err)
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close export file: %w", err)
	}
	if err := os.Rename(file.Name(), target.Path); err != nil {
		return fmt.Errorf("publish export file: %w", err)
	}
	return nil
}

func writeCSV(ctx context.Context, file *os.File, records []domain.Record) error {
	fieldsSet := make(map[string]struct{})
	for _, record := range records {
		for field := range record.Data {
			fieldsSet[field] = struct{}{}
		}
	}
	fields := make([]string, 0, len(fieldsSet))
	for field := range fieldsSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	writer := csv.NewWriter(file)
	header := append([]string{"record_id", "source", "timestamp"}, fields...)
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := make([]string, 3, len(header))
		row[0], row[1], row[2] = record.ID, record.Source, record.Timestamp.Format(time.RFC3339Nano)
		for _, field := range fields {
			value, err := valueString(record.Data[field])
			if err != nil {
				return fmt.Errorf("record %q field %q: %w", record.ID, field, err)
			}
			row = append(row, value)
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func valueString(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}

// writeDailyCSV uses stable date/metric columns and one column per crypto asset.
func writeDailyCSV(ctx context.Context, file *os.File, rows []domain.DailyReportRow) error {
	assetsSet := map[string]bool{}
	for _, row := range rows {
		for asset := range row.CryptoPrices {
			assetsSet[asset] = true
		}
	}
	assets := make([]string, 0, len(assetsSet))
	for asset := range assetsSet {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	writer := csv.NewWriter(file)
	header := []string{"date", "cases", "deaths", "temperature_c", "sources"}
	for _, asset := range assets {
		header = append(header, asset+"_usd")
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		values := []string{row.Date, optionalFloat(row.Cases), optionalFloat(row.Deaths), optionalFloat(row.TemperatureC), strings.Join(row.Sources, "|")}
		for _, asset := range assets {
			if price, ok := row.CryptoPrices[asset]; ok {
				values = append(values, strconv.FormatFloat(price, 'f', -1, 64))
			} else {
				values = append(values, "")
			}
		}
		if err := writer.Write(values); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}
func optionalFloat(value *float64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatFloat(*value, 'f', -1, 64)
}
