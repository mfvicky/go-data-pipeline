package domain

import (
	"fmt"
	"net/url"
	"strings"
)

// ValidateJobSpec checks configuration structure, not business/record validity
// (which belongs to the validation stage).
func ValidateJobSpec(spec JobSpec) error {
	if len(spec.Sources) == 0 {
		return fmt.Errorf("sources must contain at least one source")
	}
	if len(spec.Sources) > 32 {
		return fmt.Errorf("sources may contain at most 32 sources")
	}
	names := make(map[string]struct{}, len(spec.Sources))
	for i, source := range spec.Sources {
		if source.Name != "" {
			if _, exists := names[source.Name]; exists {
				return fmt.Errorf("sources[%d].name: duplicate name %q", i, source.Name)
			}
			names[source.Name] = struct{}{}
		}
		if source.Endpoint == "" || strings.TrimSpace(source.Endpoint) != source.Endpoint {
			return fmt.Errorf("sources[%d].endpoint: a nonempty endpoint without surrounding whitespace is required", i)
		}
		switch source.Type {
		case "csv", "json":
			// A local file path or an HTTP(S) URL is accepted.
		case "api":
			if !isHTTPURL(source.Endpoint) {
				return fmt.Errorf("sources[%d].endpoint: api sources require an http(s) URL", i)
			}
		default:
			return fmt.Errorf("sources[%d].type: must be csv, json, or api", i)
		}
		if strings.Contains(source.Endpoint, "://") && !isHTTPURL(source.Endpoint) {
			return fmt.Errorf("sources[%d].endpoint: only http(s) URLs are supported", i)
		}
		if source.Type == "csv" && source.RecordPath != "" {
			return fmt.Errorf("sources[%d].record_path: only valid for json/api", i)
		}
		if source.Type != "api" && len(source.Headers) > 0 {
			return fmt.Errorf("sources[%d].headers: supported only for api sources", i)
		}
		switch source.Adapter {
		case "", "covid_csv", "coingecko_market_chart", "coingecko_simple", "open_meteo_daily", "open_meteo_hourly":
		default:
			return fmt.Errorf("sources[%d].adapter: unsupported adapter %q", i, source.Adapter)
		}
		if source.Adapter == "covid_csv" && source.Type != "csv" {
			return fmt.Errorf("sources[%d].adapter: covid_csv requires csv type", i)
		}
		if source.Adapter != "" && source.Adapter != "covid_csv" && source.Type == "csv" {
			return fmt.Errorf("sources[%d].adapter: adapter requires json or api type", i)
		}
		if strings.HasPrefix(source.Adapter, "coingecko_") && strings.TrimSpace(source.Asset) == "" {
			return fmt.Errorf("sources[%d].asset: required for coingecko adapter", i)
		}
		for canonical, original := range source.FieldMap {
			if strings.TrimSpace(canonical) == "" || strings.TrimSpace(original) == "" {
				return fmt.Errorf("sources[%d].field_map: field names cannot be empty", i)
			}
		}
		if len(source.EnrichFields) > 32 {
			return fmt.Errorf("sources[%d].enrich_fields: at most 32 fields", i)
		}
		for field := range source.EnrichFields {
			if strings.TrimSpace(field) == "" {
				return fmt.Errorf("sources[%d].enrich_fields: empty name", i)
			}
		}
		for _, field := range source.RequiredFields {
			if strings.TrimSpace(field) == "" {
				return fmt.Errorf("sources[%d].required_fields: field cannot be empty", i)
			}
		}
		if source.Adapter != "" && source.RecordPath != "" {
			return fmt.Errorf("sources[%d].record_path: adapters decode provider response directly", i)
		}
		for k, v := range source.Headers {
			if strings.TrimSpace(k) == "" || strings.ContainsAny(k+v, "\r\n") {
				return fmt.Errorf("sources[%d].headers: invalid header", i)
			}
		}
	}
	for stage, size := range spec.WorkerPoolSizes {
		switch stage {
		case "ingestion", "validation", "transformation":
		default:
			return fmt.Errorf("worker_pool_sizes: unknown stage %q", stage)
		}
		if size < 1 || size > 64 {
			return fmt.Errorf("worker_pool_sizes.%s: must be between 1 and 64", stage)
		}
	}
	if spec.TimeoutSeconds < 0 || spec.TimeoutSeconds > 3600 {
		return fmt.Errorf("timeout_seconds: must be between 0 and 3600")
	}
	aggregationNames := make(map[string]struct{}, len(spec.Aggregations))
	for i, a := range spec.Aggregations {
		if strings.TrimSpace(a.Name) == "" {
			return fmt.Errorf("aggregations[%d].name: required", i)
		}
		if _, exists := aggregationNames[a.Name]; exists {
			return fmt.Errorf("aggregations[%d].name: duplicate name", i)
		}
		aggregationNames[a.Name] = struct{}{}
		if a.SourceKind != "" && a.SourceKind != "covid" && a.SourceKind != "crypto" && a.SourceKind != "weather" {
			return fmt.Errorf("aggregations[%d].source_kind: must be covid, crypto or weather", i)
		}
		switch a.Operation {
		case "count":
		case "sum", "avg", "min", "max":
			if strings.TrimSpace(a.Field) == "" {
				return fmt.Errorf("aggregations[%d].field: required for %s", i, a.Operation)
			}
		default:
			return fmt.Errorf("aggregations[%d].operation: must be count, sum, avg, min or max", i)
		}
		for _, field := range a.GroupBy {
			if strings.TrimSpace(field) == "" {
				return fmt.Errorf("aggregations[%d].group_by: field cannot be empty", i)
			}
		}
	}
	if spec.Report != nil {
		if spec.Report.Type != "global_daily" {
			return fmt.Errorf("report.type: must be global_daily")
		}
		for i, source := range spec.Sources {
			if source.Adapter == "" {
				return fmt.Errorf("sources[%d].adapter: required for global_daily report", i)
			}
		}
	}
	for i, kind := range spec.ExportTargets {
		if kind != "csv" && kind != "json" {
			return fmt.Errorf("export_targets[%d]: only csv/json file export is planned", i)
		}
	}
	for i, target := range spec.Exports {
		if target.Type != "csv" && target.Type != "json" && target.Type != "postgres" {
			return fmt.Errorf("exports[%d].type: must be csv, json or postgres", i)
		}
		if target.Type == "postgres" {
			if spec.Report == nil || spec.Report.Type != "global_daily" {
				return fmt.Errorf("exports[%d]: postgres target requires global_daily report", i)
			}
			if target.Path != "" {
				return fmt.Errorf("exports[%d].path: postgres target does not accept a path", i)
			}
		} else if strings.TrimSpace(target.Path) == "" {
			return fmt.Errorf("exports[%d].path: required", i)
		}
	}
	return nil
}

func isHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != "" && u.User == nil
}
