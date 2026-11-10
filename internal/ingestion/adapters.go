package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// StreamWithErrors continues after a source-specific row validation error,
// reporting it via reject instead of failing the entire source.
func StreamWithErrors(ctx context.Context, source domain.SourceConfig, send func(domain.Record) error, reject func(domain.Record, error)) (int, error) {
	// Enrich and transform records at the source boundary, before the
	// shared validation/transformation pools. An invalid source-specific
	// row is rejected without aborting other sources.
	emit := func(r domain.Record) error {
		if len(source.EnrichFields) > 0 {
			data := make(map[string]any, len(r.Data)+len(source.EnrichFields))
			for k, v := range r.Data {
				data[k] = v
			}
			for k, v := range source.EnrichFields {
				if _, exists := data[k]; exists {
					if reject != nil {
						reject(r, fmt.Errorf("enrichment field %q already exists", k))
						return nil
					}
					return fmt.Errorf("enrichment field %q already exists", k)
				}
				data[k] = v
			}
			r.Data = data
		}
		return send(r)
	}
	if source.Adapter == "" {
		if len(source.RequiredFields) == 0 && len(source.FieldMap) == 0 && len(source.EnrichFields) == 0 && len(source.TransformRules) == 0 {
			return Stream(ctx, source, send)
		}
		return Stream(ctx, source, func(record domain.Record) error {
			mapped := make(map[string]any, len(record.Data)+len(source.FieldMap))
			for key, value := range record.Data {
				mapped[key] = value
			}
			for canonical, original := range source.FieldMap {
				if value, ok := record.Data[original]; ok {
					mapped[canonical] = value
				}
			}
			record.Data = mapped
			if err := checkRequired(mapped, source.RequiredFields); err != nil {
				if reject != nil {
					reject(record, err)
					return nil
				}
				return err
			}
			return emit(record)
		})
	}
	if source.Adapter == "covid_csv" {
		stream, err := openStream(ctx, source, "text/csv", false)
		if err != nil {
			return 0, err
		}
		defer func() { _ = stream.Close() }()
		return streamCSV(ctx, source, &boundedReader{ctx: ctx, source: stream, max: maxSourceBytes}, func(r domain.Record) error {
			normalized, err := normalizeCOVID(source, r.Data)
			if err != nil {
				if reject != nil {
					reject(r, err)
					return nil
				}
				return err
			}
			r.Data = normalized
			return emit(r)
		})
	}
	// Columnar provider payloads are bounded to the same maxSourceBytes limit
	// as ordinary ingestion, but their nested arrays require decoding a source.
	stream, err := openStream(ctx, source, "application/json", source.Type == "api")
	if err != nil {
		return 0, err
	}
	defer func() { _ = stream.Close() }()
	decoder := json.NewDecoder(&boundedReader{ctx: ctx, source: stream, max: maxSourceBytes})
	decoder.UseNumber()
	var payload map[string]json.RawMessage
	if err := decoder.Decode(&payload); err != nil {
		return 0, fmt.Errorf("decode adapter payload: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return 0, fmt.Errorf("unexpected trailing JSON content")
	}
	var rows []map[string]any
	switch source.Adapter {
	case "coingecko_market_chart":
		rows, err = decodeCoinGeckoChart(source, payload)
	case "coingecko_simple":
		rows, err = decodeCoinGeckoSimple(source, payload)
	case "open_meteo_daily":
		rows, err = decodeMeteoDaily(payload)
	case "open_meteo_hourly":
		rows, err = decodeMeteoHourly(payload)
	default:
		return 0, fmt.Errorf("unsupported source adapter %q", source.Adapter)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for i, data := range rows {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		record := makeRecord(source, i+1, data)
		if err := checkRequired(data, source.RequiredFields); err != nil {
			if reject != nil {
				reject(record, err)
				count++
				continue
			}
			return count, err
		}
		if err := emit(record); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func lookupField(src domain.SourceConfig, data map[string]any, name string, aliases ...string) (any, bool) {
	if mapped, ok := src.FieldMap[name]; ok {
		aliases = append([]string{mapped}, aliases...)
	}
	aliases = append([]string{name}, aliases...)
	for _, candidate := range aliases {
		if v, ok := data[candidate]; ok {
			return v, true
		}
	}
	for _, candidate := range aliases {
		for k, v := range data {
			if strings.EqualFold(strings.TrimSpace(k), candidate) {
				return v, true
			}
		}
	}
	return nil, false
}
func number(v any) (float64, error) {
	var n float64
	var err error
	switch t := v.(type) {
	case json.Number:
		n, err = t.Float64()
	case float64:
		n = t
	case float32:
		n = float64(t)
	case int:
		n = float64(t)
	case int64:
		n = float64(t)
	case string:
		n, err = strconv.ParseFloat(strings.TrimSpace(strings.ReplaceAll(t, ",", "")), 64)
	default:
		return 0, fmt.Errorf("expected numeric value")
	}
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("expected finite numeric value")
	}
	return n, nil
}
func dateString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("date must be a string")
	}
	s = strings.TrimSpace(s)
	for _, layout := range []string{"2006-01-02", "2006/01/02", "02/01/2006", "01/02/2006", "2006-01-02T15:04:05Z07:00", time.RFC3339, "2006-01-02T15:04"} {
		if d, err := time.Parse(layout, s); err == nil {
			return d.Format("2006-01-02"), nil
		}
	}
	return "", fmt.Errorf("invalid date %q", s)
}
func checkRequired(data map[string]any, fields []string) error {
	for _, field := range fields {
		if v, ok := data[field]; !ok || v == nil || strings.TrimSpace(fmt.Sprint(v)) == "" {
			return fmt.Errorf("required source field %q missing", field)
		}
	}
	return nil
}
func normalizeCOVID(source domain.SourceConfig, raw map[string]any) (map[string]any, error) {
	value, ok := lookupField(source, raw, "date", "Date_reported", "date_reported", "Date")
	if !ok {
		return nil, fmt.Errorf("COVID date missing")
	}
	date, err := dateString(value)
	if err != nil {
		return nil, err
	}
	cases, ok := lookupField(source, raw, "cases", "New_cases", "new_cases")
	if !ok {
		return nil, fmt.Errorf("COVID cases missing")
	}
	deaths, ok := lookupField(source, raw, "deaths", "New_deaths", "new_deaths")
	if !ok {
		return nil, fmt.Errorf("COVID deaths missing")
	}
	nc, err := number(cases)
	if err != nil {
		return nil, fmt.Errorf("COVID cases: %w", err)
	}
	nd, err := number(deaths)
	if err != nil {
		return nil, fmt.Errorf("COVID deaths: %w", err)
	}
	result := map[string]any{"date": date, "cases": nc, "deaths": nd, "kind": "covid"}
	if country, ok := lookupField(source, raw, "country", "Country", "Country_code"); ok {
		result["country"] = country
	}
	if err := checkRequired(result, source.RequiredFields); err != nil {
		return nil, err
	}
	return result, nil
}
func decodeCoinGeckoChart(source domain.SourceConfig, payload map[string]json.RawMessage) ([]map[string]any, error) {
	var data struct {
		Prices [][]json.Number `json:"prices"`
	}
	raw, ok := payload["prices"]
	if !ok {
		return nil, fmt.Errorf("CoinGecko prices missing")
	}
	if err := json.Unmarshal(raw, &data.Prices); err != nil {
		return nil, fmt.Errorf("CoinGecko prices: %w", err)
	}
	if len(data.Prices) == 0 {
		return nil, fmt.Errorf("CoinGecko prices empty")
	}
	records := make([]map[string]any, 0, len(data.Prices))
	for i, pair := range data.Prices {
		if len(pair) != 2 {
			return nil, fmt.Errorf("CoinGecko prices[%d]: expected [timestamp_ms, price]", i)
		}
		stamp, err := pair[0].Int64()
		if err != nil {
			return nil, fmt.Errorf("CoinGecko timestamp: %w", err)
		}
		price, err := number(pair[1])
		if err != nil {
			return nil, fmt.Errorf("CoinGecko price: %w", err)
		}
		if stamp <= 0 || price < 0 {
			return nil, fmt.Errorf("CoinGecko timestamp or price invalid")
		}
		date := time.UnixMilli(stamp).UTC().Format("2006-01-02")
		records = append(records, map[string]any{"date": date, "kind": "crypto", "asset": source.Asset, "price_usd": price, "observed_at_ms": stamp})
	}
	return records, nil
}
func decodeCoinGeckoSimple(source domain.SourceConfig, payload map[string]json.RawMessage) ([]map[string]any, error) {
	raw, ok := payload[source.Asset]
	if !ok {
		return nil, fmt.Errorf("CoinGecko asset %q missing", source.Asset)
	}
	var data struct {
		USD         json.Number `json:"usd"`
		LastUpdated json.Number `json:"last_updated_at"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	price, err := number(data.USD)
	if err != nil || price < 0 {
		return nil, fmt.Errorf("CoinGecko usd price invalid")
	}
	stamp, err := data.LastUpdated.Int64()
	if err != nil || stamp <= 0 {
		return nil, fmt.Errorf("CoinGecko last_updated_at is required (include_last_updated_at=true)")
	}
	ms := stamp * 1000
	return []map[string]any{{"date": time.Unix(stamp, 0).UTC().Format("2006-01-02"), "kind": "crypto", "asset": source.Asset, "price_usd": price, "observed_at_ms": ms}}, nil
}
func decodeColumns(raw json.RawMessage, names ...string) (map[string][]any, error) {
	var obj map[string][]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	dates, ok := obj["time"]
	if !ok || len(dates) == 0 {
		return nil, fmt.Errorf("weather time array missing")
	}
	for _, name := range names {
		if values, ok := obj[name]; ok && len(values) != len(dates) {
			return nil, fmt.Errorf("weather %q and time lengths differ", name)
		}
	}
	return obj, nil
}
func decodeMeteoDaily(payload map[string]json.RawMessage) ([]map[string]any, error) {
	raw, ok := payload["daily"]
	if !ok {
		return nil, fmt.Errorf("Open-Meteo daily object missing")
	}
	obj, err := decodeColumns(raw, "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min")
	if err != nil {
		return nil, err
	}
	dates := obj["time"]
	temp := obj["temperature_2m_mean"]
	highs := obj["temperature_2m_max"]
	lows := obj["temperature_2m_min"]
	if len(temp) == 0 && (len(highs) == 0 || len(lows) == 0) {
		return nil, fmt.Errorf("Open-Meteo requires daily temperature_2m_mean or max/min")
	}
	out := make([]map[string]any, 0, len(dates))
	for i, d := range dates {
		date, err := dateString(d)
		if err != nil {
			return nil, err
		}
		var n float64
		if len(temp) > 0 && temp[i] != nil {
			n, err = number(temp[i])
		} else if len(highs) > 0 && len(lows) > 0 && highs[i] != nil && lows[i] != nil {
			var hi, lo float64
			hi, err = number(highs[i])
			if err == nil {
				lo, err = number(lows[i])
			}
			n = (hi + lo) / 2
		} else {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("Open-Meteo temperature %s: %w", date, err)
		}
		out = append(out, map[string]any{"date": date, "kind": "weather", "temperature_c": n})
	}
	return out, nil
}
func decodeMeteoHourly(payload map[string]json.RawMessage) ([]map[string]any, error) {
	raw, ok := payload["hourly"]
	if !ok {
		return nil, fmt.Errorf("Open-Meteo hourly object missing")
	}
	obj, err := decodeColumns(raw, "temperature_2m")
	if err != nil {
		return nil, err
	}
	dates := obj["time"]
	temps := obj["temperature_2m"]
	if len(temps) == 0 {
		return nil, fmt.Errorf("Open-Meteo hourly temperature_2m missing")
	}
	type values struct {
		sum   float64
		count int
	}
	grouped := map[string]*values{}
	for i, ts := range dates {
		s, ok := ts.(string)
		if !ok || len(s) < 10 {
			return nil, fmt.Errorf("Open-Meteo hourly timestamp invalid")
		}
		date, err := dateString(s[:10])
		if err != nil {
			return nil, err
		}
		if temps[i] == nil {
			continue
		}
		n, err := number(temps[i])
		if err != nil {
			return nil, err
		}
		if grouped[date] == nil {
			grouped[date] = &values{}
		}
		grouped[date].sum += n
		grouped[date].count++
	}
	out := make([]map[string]any, 0, len(grouped))
	for date, val := range grouped {
		out = append(out, map[string]any{"date": date, "kind": "weather", "temperature_c": val.sum / float64(val.count)})
	}
	return out, nil
}
