package domain

import (
	"strings"
	"testing"
)

func TestValidateJobSpec(t *testing.T) {
	valid := func() JobSpec {
		return JobSpec{
			Sources:         []SourceConfig{{Name: "example", Type: "csv", Endpoint: "testdata/input/customers.csv"}, {Name: "second", Type: "api", Endpoint: "https://example.org/data", RecordPath: "results"}},
			WorkerPoolSizes: map[string]int{"validation": 4, "transformation": 2},
			Aggregations:    []AggregationConfig{{Name: "total", Operation: "sum", Field: "amount", GroupBy: []string{"city"}}},
			Exports:         []ExportConfig{{Type: "json", Path: "output/results.json"}},
			TimeoutSeconds:  60,
		}
	}
	if err := ValidateJobSpec(valid()); err != nil {
		t.Fatalf("valid spec: %v", err)
	}
	cases := []struct {
		name string
		edit func(*JobSpec)
		want string
	}{
		{"no sources", func(s *JobSpec) { s.Sources = nil }, "sources"},
		{"invalid type", func(s *JobSpec) { s.Sources[0].Type = "xml" }, "type"},
		{"bad api URL", func(s *JobSpec) { s.Sources[1].Endpoint = "file.csv" }, "http(s)"},
		{"bad scheme", func(s *JobSpec) { s.Sources[0].Endpoint = "ftp://invalid.local/file" }, "http(s)"},
		{"duplicate name", func(s *JobSpec) { s.Sources[1].Name = "example" }, "duplicate"},
		{"missing endpoint", func(s *JobSpec) { s.Sources[0].Endpoint = "" }, "endpoint"},
		{"bad worker count", func(s *JobSpec) { s.WorkerPoolSizes["validation"] = 0 }, "between 1 and 64"},
		{"unknown worker stage", func(s *JobSpec) { s.WorkerPoolSizes["unknown"] = 2 }, "unknown"},
		{"timeout", func(s *JobSpec) { s.TimeoutSeconds = -1 }, "timeout"},
		{"sum without field", func(s *JobSpec) { s.Aggregations[0].Field = "" }, "field"},
		{"bad aggregate op", func(s *JobSpec) { s.Aggregations[0].Operation = "median" }, "operation"},
		{"no export path", func(s *JobSpec) { s.Exports[0].Path = "" }, "path"},
		{"bad export type", func(s *JobSpec) { s.Exports[0].Type = "sql" }, "type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid()
			tc.edit(&s)
			if err := ValidateJobSpec(s); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}
