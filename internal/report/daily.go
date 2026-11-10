// Package report builds date-keyed multi-source reports from normalized records.
package report

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type bucket struct {
	row          domain.DailyReportRow
	temperatures float64
	weatherCount int
	sources      map[string]bool
	priceTime    map[string]int64
}

// Builder incrementally joins normalized records by UTC date. It holds
// only one summary bucket per date, never a second copy of all input records.
type Builder struct{ grouped map[string]*bucket }

func NewBuilder() *Builder { return &Builder{grouped: make(map[string]*bucket)} }

func (b *Builder) Add(record domain.Record) error {
	date, ok := record.Data["date"].(string)
	if !ok || len(date) != 10 || date[4] != '-' || date[7] != '-' {
		return fmt.Errorf("record %q: missing normalized date", record.ID)
	}
	kind, ok := record.Data["kind"].(string)
	if !ok {
		return fmt.Errorf("record %q: missing source kind", record.ID)
	}
	item := b.grouped[date]
	if item == nil {
		item = &bucket{row: domain.DailyReportRow{Date: date}, sources: map[string]bool{}, priceTime: map[string]int64{}}
		b.grouped[date] = item
	}
	switch kind {
	case "covid":
		cases, ok := record.Data["cases"].(float64)
		if !ok {
			return fmt.Errorf("COVID cases are not numeric")
		}
		deaths, ok := record.Data["deaths"].(float64)
		if !ok {
			return fmt.Errorf("COVID deaths are not numeric")
		}
		if item.row.Cases == nil {
			a, b := 0.0, 0.0
			item.row.Cases = &a
			item.row.Deaths = &b
		}
		nextCases := *item.row.Cases + cases
		nextDeaths := *item.row.Deaths + deaths
		if math.IsInf(nextCases, 0) || math.IsInf(nextDeaths, 0) || math.IsNaN(nextCases) || math.IsNaN(nextDeaths) {
			return fmt.Errorf("COVID report total overflow")
		}
		*item.row.Cases = nextCases
		*item.row.Deaths = nextDeaths
	case "crypto":
		asset, ok := record.Data["asset"].(string)
		if !ok || strings.TrimSpace(asset) == "" {
			return fmt.Errorf("crypto asset missing")
		}
		price, ok := record.Data["price_usd"].(float64)
		if !ok {
			return fmt.Errorf("crypto price missing")
		}
		ts, ok := record.Data["observed_at_ms"].(int64)
		if !ok {
			return fmt.Errorf("crypto timestamp missing")
		}
		if item.row.CryptoPrices == nil {
			item.row.CryptoPrices = make(map[string]float64)
		}
		if previous, exists := item.priceTime[asset]; !exists || ts >= previous {
			item.row.CryptoPrices[asset] = price
			item.priceTime[asset] = ts
		}
	case "weather":
		temperature, ok := record.Data["temperature_c"].(float64)
		if !ok {
			return fmt.Errorf("temperature missing")
		}
		item.temperatures += temperature
		if math.IsInf(item.temperatures, 0) || math.IsNaN(item.temperatures) {
			return fmt.Errorf("weather mean overflow")
		}
		item.weatherCount++
	default:
		return fmt.Errorf("unsupported report kind %q", kind)
	}
	item.sources[record.Source] = true
	return nil
}

func (b *Builder) Finish(ctx context.Context) ([]domain.DailyReportRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dates := make([]string, 0, len(b.grouped))
	for date := range b.grouped {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	out := make([]domain.DailyReportRow, 0, len(dates))
	for _, date := range dates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := b.grouped[date]
		if item.weatherCount > 0 {
			temp := item.temperatures / float64(item.weatherCount)
			if math.IsNaN(temp) || math.IsInf(temp, 0) {
				return nil, fmt.Errorf("invalid weather mean")
			}
			item.row.TemperatureC = &temp
		}
		for source := range item.sources {
			item.row.Sources = append(item.row.Sources, source)
		}
		sort.Strings(item.row.Sources)
		out = append(out, item.row)
	}
	return out, nil
}

// GlobalDaily retains the original batch API for callers and tests.
func GlobalDaily(ctx context.Context, records []domain.Record) ([]domain.DailyReportRow, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	b := NewBuilder()
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := b.Add(record); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}
