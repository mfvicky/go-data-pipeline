package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestGlobalDailyThreeConcurrentSourcesAndExports(t *testing.T) {
	coin, err := os.ReadFile("../../testdata/input/global_coingecko_sample.json")
	if err != nil {
		t.Fatal(err)
	}
	meteo, err := os.ReadFile("../../testdata/input/global_openmeteo_sample.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/coins" {
			_, _ = w.Write(coin)
		} else {
			_, _ = w.Write(meteo)
		}
	}))
	defer server.Close()
	jsonPath := filepath.Join(t.TempDir(), "daily.json")
	csvPath := filepath.Join(t.TempDir(), "daily.csv")
	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{
			{Name: "covid", Type: "csv", Endpoint: "../../testdata/input/global_covid_sample.csv", Adapter: "covid_csv", FieldMap: map[string]string{"date": "Date_reported", "cases": "New_cases", "deaths": "New_deaths"}, RequiredFields: []string{"date", "cases", "deaths"}},
			{Name: "coin", Type: "api", Endpoint: server.URL + "/coins", Adapter: "coingecko_market_chart", Asset: "bitcoin"},
			{Name: "meteo", Type: "api", Endpoint: server.URL + "/weather", Adapter: "open_meteo_daily"},
		},
		Report:          &domain.ReportConfig{Type: "global_daily"},
		WorkerPoolSizes: map[string]int{"ingestion": 3, "validation": 5, "transformation": 5},
		Aggregations:    []domain.AggregationConfig{{Name: "cases_by_date", Operation: "sum", Field: "cases", SourceKind: "covid", GroupBy: []string{"date"}}},
		Exports:         []domain.ExportConfig{{Type: "json", Path: jsonPath}, {Type: "csv", Path: csvPath}},
	}
	result, err := Run(context.Background(), "global-report-test", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 11 || len(result.DailyReport) != 3 || len(result.Errors) != 1 || len(result.Aggregations) != 3 {
		t.Fatalf("records=%d daily=%d errors=%v aggregates=%+v", len(result.Records), len(result.DailyReport), result.Errors, result.Aggregations)
	}
	row := result.DailyReport[0]
	if row.Date != "2026-11-03" || row.Cases == nil || *row.Cases != 15 || row.TemperatureC == nil || *row.TemperatureC != 22 || row.CryptoPrices["bitcoin"] != 70000.1 {
		t.Fatalf("row: %+v", row)
	}
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Daily []domain.DailyReportRow `json:"daily_report"`
	}
	if err := json.Unmarshal(raw, &saved); err != nil || len(saved.Daily) != 3 {
		t.Fatalf("json daily rows=%v err=%v", saved, err)
	}
	if bytes, err := os.ReadFile(csvPath); err != nil || len(bytes) == 0 {
		t.Fatalf("csv file: %v", err)
	}
}
