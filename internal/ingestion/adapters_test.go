package ingestion

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestAdapterFixtures(t *testing.T) {
	tests := []struct {
		name, adapter, asset, file, kind string
		want, rejects                    int
	}{
		{"covid", "covid_csv", "", "../../testdata/input/global_covid_sample.csv", "csv", 4, 1},
		{"coin", "coingecko_market_chart", "bitcoin", "../../testdata/input/global_coingecko_sample.json", "json", 4, 0},
		{"meteo", "open_meteo_daily", "", "../../testdata/input/global_openmeteo_sample.json", "json", 3, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			count, rejected := 0, 0
			_, err := StreamWithErrors(context.Background(), domain.SourceConfig{Type: tc.kind, Endpoint: tc.file, Adapter: tc.adapter, Asset: tc.asset}, func(r domain.Record) error {
				count++
				if r.Data["date"] == nil {
					t.Fatalf("missing date: %+v", r)
				}
				return nil
			}, func(_ domain.Record, _ error) { rejected++ })
			if err != nil || count != tc.want || rejected != tc.rejects {
				t.Fatalf("sent=%d rejected=%d err=%v", count, rejected, err)
			}
		})
	}
}
func TestMeteoHourlyAverages(t *testing.T) {
	f := filepath.Join(t.TempDir(), "weather.json")
	if err := os.WriteFile(f, []byte(`{"hourly":{"time":["2026-11-05T00:00","2026-11-05T01:00","2026-11-06T00:00"],"temperature_2m":[18,22,30]}}`), 0600); err != nil {
		t.Fatal(err)
	}
	var temps []float64
	_, err := StreamWithErrors(context.Background(), domain.SourceConfig{Type: "json", Endpoint: f, Adapter: "open_meteo_hourly"}, func(r domain.Record) error { temps = append(temps, r.Data["temperature_c"].(float64)); return nil }, nil)
	if err != nil || len(temps) != 2 {
		t.Fatalf("temps=%v error=%v", temps, err)
	}
	if (temps[0] != 20 || temps[1] != 30) && (temps[0] != 30 || temps[1] != 20) {
		t.Fatalf("averages: %v", temps)
	}
}
func TestCoinGeckoSimple(t *testing.T) {
	f := filepath.Join(t.TempDir(), "coin.json")
	if err := os.WriteFile(f, []byte(`{"bitcoin":{"usd":50000,"last_updated_at":1793836800}}`), 0600); err != nil {
		t.Fatal(err)
	}
	var out []domain.Record
	_, err := StreamWithErrors(context.Background(), domain.SourceConfig{Type: "json", Endpoint: f, Adapter: "coingecko_simple", Asset: "bitcoin"}, func(r domain.Record) error { out = append(out, r); return nil }, nil)
	if err != nil || len(out) != 1 || out[0].Data["date"] != "2026-11-05" {
		t.Fatalf("out=%v err=%v", out, err)
	}
}
func TestSourceSpecificGenericValidation(t *testing.T) {
	f := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(f, []byte(`[{"value":"1"},{"other":"x"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	accepted, rejected := 0, 0
	_, err := StreamWithErrors(context.Background(), domain.SourceConfig{Type: "json", Endpoint: f, FieldMap: map[string]string{"amount": "value"}, RequiredFields: []string{"amount"}}, func(r domain.Record) error { accepted++; return nil }, func(r domain.Record, e error) { rejected++ })
	if err != nil || accepted != 1 || rejected != 1 {
		t.Fatalf("accepted=%d rejected=%d err=%v", accepted, rejected, err)
	}
}

func TestSourceEnrichmentAndTypeConversion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.json")
	if err := os.WriteFile(path, []byte(`[{"gross":"12.50"},{"other":"not-a-number"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	src := domain.SourceConfig{
		Type: "json", Endpoint: path,
		FieldMap:       map[string]string{"amount": "gross"},
		RequiredFields: []string{"amount"},
		EnrichFields:   map[string]any{"region": "asia"},
		TransformRules: map[string]any{"type_conversions": map[string]any{"amount": "number"}},
	}
	var accepted []domain.Record
	var rejected int
	_, err := StreamWithErrors(context.Background(), src, func(r domain.Record) error { accepted = append(accepted, r); return nil }, func(_ domain.Record, _ error) { rejected++ })
	if err != nil || rejected != 1 || len(accepted) != 1 {
		t.Fatalf("accepted=%v rejected=%d err=%v", accepted, rejected, err)
	}
	if accepted[0].Data["amount"] != "12.50" || accepted[0].Data["region"] != "asia" {
		t.Fatalf("bad source-specific transform: %+v", accepted[0].Data)
	}
}
