package domain

import (
	"fmt"
	"net/url"
	"strings"
)

// ValidateJobSpec checks configuration structure, not business/record validity
// (which belongs to the Day 2 validation stage).
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
		switch a.Operation {
		case "count":
		case "sum", "avg":
			if strings.TrimSpace(a.Field) == "" {
				return fmt.Errorf("aggregations[%d].field: required for %s", i, a.Operation)
			}
		default:
			return fmt.Errorf("aggregations[%d].operation: must be count, sum, or avg", i)
		}
		for _, field := range a.GroupBy {
			if strings.TrimSpace(field) == "" {
				return fmt.Errorf("aggregations[%d].group_by: field cannot be empty", i)
			}
		}
	}
	for i, kind := range spec.ExportTargets {
		if kind != "csv" && kind != "json" {
			return fmt.Errorf("export_targets[%d]: only csv/json file export is planned", i)
		}
	}
	for i, target := range spec.Exports {
		if target.Type != "csv" && target.Type != "json" {
			return fmt.Errorf("exports[%d].type: must be csv or json", i)
		}
		if strings.TrimSpace(target.Path) == "" {
			return fmt.Errorf("exports[%d].path: required", i)
		}
	}
	return nil
}

func isHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != "" && u.User == nil
}
