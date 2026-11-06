package pipeline

import (
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// Observer receives snapshots and individual failures without blocking the
// error collector. Its callbacks must be safe for concurrent invocation.
type Observer struct {
	OnProgress func(domain.ProgressSnapshot)
	OnError    func(domain.PipelineError)
}

type metricsTracker struct {
	mu                sync.Mutex
	start             time.Time
	ingestionFinished bool
	snapshot          domain.ProgressSnapshot
	observer          Observer
}

func newMetricsTracker(observer Observer) *metricsTracker {
	return &metricsTracker{
		start:    time.Now(),
		snapshot: domain.ProgressSnapshot{StageLatencies: make(map[string]domain.StageLatency)},
		observer: observer,
	}
}

func (m *metricsTracker) recordRead() {
	m.mu.Lock()
	m.snapshot.RecordsRead++
	m.publish(m.snapshotLocked())
	m.mu.Unlock()
}

func (m *metricsTracker) stageComplete(stage string, elapsed time.Duration, failures int64) {
	m.mu.Lock()
	latency := m.snapshot.StageLatencies[stage]
	latency.Count++
	latency.TotalMilliseconds += float64(elapsed) / float64(time.Millisecond)
	latency.AverageMilliseconds = latency.TotalMilliseconds / float64(latency.Count)
	m.snapshot.StageLatencies[stage] = latency
	m.snapshot.ErrorCount += failures
	if (stage == StageValidation || stage == StageTransformation) && failures > 0 {
		m.snapshot.RecordsFailed += failures
	}
	if stage == StageTransformation && failures == 0 {
		m.snapshot.RecordsProcessed++
	}
	m.publish(m.snapshotLocked())
	m.mu.Unlock()
}

func (m *metricsTracker) finishIngestion() {
	m.mu.Lock()
	m.ingestionFinished = true
	m.publish(m.snapshotLocked())
	m.mu.Unlock()
}

func (m *metricsTracker) snapshotLocked() domain.ProgressSnapshot {
	result := m.snapshot
	result.StageLatencies = make(map[string]domain.StageLatency, len(m.snapshot.StageLatencies))
	for key, value := range m.snapshot.StageLatencies {
		result.StageLatencies[key] = value
	}
	result.RecordsPending = result.RecordsRead - result.RecordsProcessed - result.RecordsFailed
	if result.RecordsPending < 0 {
		result.RecordsPending = 0
	}
	elapsed := time.Since(m.start).Seconds()
	if elapsed > 0 {
		result.RatePerSecond = float64(result.RecordsProcessed) / elapsed
	}
	if m.ingestionFinished && result.RecordsRead > 0 {
		result.PercentComplete = 100 * float64(result.RecordsProcessed+result.RecordsFailed) / float64(result.RecordsRead)
		if result.PercentComplete >= 100 {
			result.PercentComplete = 99
		}
	}
	return result
}

func (m *metricsTracker) current() domain.ProgressSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *metricsTracker) publish(progress domain.ProgressSnapshot) {
	if m.observer.OnProgress != nil {
		m.observer.OnProgress(progress)
	}
}

func (m *metricsTracker) reportError(failure domain.PipelineError) {
	if m.observer.OnError != nil {
		m.observer.OnError(failure)
	}
}
