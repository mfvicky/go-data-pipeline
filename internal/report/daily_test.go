package report

import (
	"context"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"testing"
)

func TestGlobalDailyJoinsAndSorts(t *testing.T) {
	r := []domain.Record{
		{ID: "weather", Source: "meteo", Data: map[string]any{"kind": "weather", "date": "2026-11-05", "temperature_c": 14.0}},
		{ID: "covid1", Source: "covid", Data: map[string]any{"kind": "covid", "date": "2026-11-05", "cases": 10.0, "deaths": 1.0}},
		{ID: "old", Source: "coin", Data: map[string]any{"kind": "crypto", "date": "2026-11-05", "asset": "bitcoin", "price_usd": 400.0, "observed_at_ms": int64(1000)}},
		{ID: "new", Source: "coin", Data: map[string]any{"kind": "crypto", "date": "2026-11-05", "asset": "bitcoin", "price_usd": 500.0, "observed_at_ms": int64(2000)}},
		{ID: "covid2", Source: "covid", Data: map[string]any{"kind": "covid", "date": "2026-11-05", "cases": 5.0, "deaths": 0.0}},
		{ID: "other", Source: "covid", Data: map[string]any{"kind": "covid", "date": "2026-11-04", "cases": 1.0, "deaths": 0.0}},
	}
	out, err := GlobalDaily(context.Background(), r)
	if err != nil || len(out) != 2 {
		t.Fatalf("report: %+v %v", out, err)
	}
	if out[0].Date != "2026-11-04" || out[1].Date != "2026-11-05" {
		t.Fatalf("dates: %+v", out)
	}
	d := out[1]
	if d.Cases == nil || *d.Cases != 15 || d.Deaths == nil || *d.Deaths != 1 || d.TemperatureC == nil || *d.TemperatureC != 14 || d.CryptoPrices["bitcoin"] != 500 || len(d.Sources) != 3 {
		t.Fatalf("join: %+v", d)
	}
	if out[0].TemperatureC != nil {
		t.Fatalf("missing weather should be null: %+v", out[0])
	}
}
