package monitor

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// Registry keeps bounded, process-wide counters (not individual job labels).
type Registry struct {
	mu                           sync.Mutex
	active, started, records     int64
	completed, failed, cancelled int64
	durationSum                  float64
	durationCount                int64
	requests                     map[string]int64
}

func NewRegistry() *Registry { return &Registry{requests: make(map[string]int64)} }
func (m *Registry) JobStarted() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active++
	m.started++
}
func (m *Registry) JobFinished(state domain.JobState, processed int64, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active > 0 {
		m.active--
	}
	switch state {
	case domain.StateCompleted:
		m.completed++
	case domain.StateFailed:
		m.failed++
	case domain.StateCancelled:
		m.cancelled++
	}
	m.records += processed
	m.durationSum += elapsed.Seconds()
	m.durationCount++
}
func (m *Registry) Request(method string, status int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if method != http.MethodGet && method != http.MethodPost && method != http.MethodDelete && method != http.MethodPut && method != http.MethodPatch {
		method = "OTHER"
	}
	class := status / 100
	if class < 1 || class > 5 {
		class = 5
	}
	m.requests[fmt.Sprintf("%s:%dxx", method, class)]++
}
func (m *Registry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m.mu.Lock()
	defer m.mu.Unlock()
	write := func(format string, args ...any) bool {
		_, err := fmt.Fprintf(w, format, args...)
		return err == nil
	}
	if !write("# HELP pipeline_jobs_active Currently executing jobs.\n") {
		return
	}
	if !write("# TYPE pipeline_jobs_active gauge\n") {
		return
	}
	if !write("pipeline_jobs_active %d\n", m.active) {
		return
	}
	if !write("# TYPE pipeline_jobs_started_total counter\n") {
		return
	}
	if !write("pipeline_jobs_started_total %d\n", m.started) {
		return
	}
	if !write("# TYPE pipeline_jobs_finished_total counter\n") {
		return
	}
	for _, state := range []struct {
		name string
		n    int64
	}{{"completed", m.completed}, {"failed", m.failed}, {"cancelled", m.cancelled}} {
		if !write("pipeline_jobs_finished_total{state=%q} %d\n", state.name, state.n) {
			return
		}
	}
	if !write("# TYPE pipeline_records_processed_total counter\n") {
		return
	}
	if !write("pipeline_records_processed_total %d\n", m.records) {
		return
	}
	if !write("# TYPE pipeline_job_duration_seconds summary\n") {
		return
	}
	if !write("pipeline_job_duration_seconds_sum %g\n", m.durationSum) {
		return
	}
	if !write("pipeline_job_duration_seconds_count %d\n", m.durationCount) {
		return
	}
	if !write("# TYPE pipeline_http_requests_total counter\n") {
		return
	}
	keys := make([]string, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		// Keys have only controlled characters. Split without user-supplied labels.
		sep := 0
		for i, c := range key {
			if c == ':' {
				sep = i
				break
			}
		}
		if !write("pipeline_http_requests_total{method=%q,status_class=%q} %d\n", key[:sep], key[sep+1:], m.requests[key]) {
			return
		}
	}
}
