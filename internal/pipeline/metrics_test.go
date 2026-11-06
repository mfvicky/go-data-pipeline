package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestRunObserverCollectsErrorsAndProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	if err := os.WriteFile(path, []byte("id,amount\n1,10\n,20\n2,not-a-number\n3,30\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var snapshots []domain.ProgressSnapshot
	var failures []domain.PipelineError
	result, err := RunWithObserver(context.Background(), "job", domain.JobSpec{
		Sources:         []domain.SourceConfig{{Type: "csv", Endpoint: path}},
		ValidationRules: map[string]any{"required_fields": []string{"id"}},
		TransformRules:  map[string]any{"type_conversions": map[string]any{"amount": "number"}},
		WorkerPoolSizes: map[string]int{"validation": 4, "transformation": 3},
	}, Observer{
		OnProgress: func(p domain.ProgressSnapshot) { mu.Lock(); snapshots = append(snapshots, p); mu.Unlock() },
		OnError:    func(e domain.PipelineError) { mu.Lock(); failures = append(failures, e); mu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 || len(result.Errors) != 2 || len(result.FailedRecords) != 2 || len(failures) != 2 {
		t.Fatalf("unexpected result %+v, callbacks %d", result, len(failures))
	}
	p := result.Progress
	if p.RecordsRead != 4 || p.RecordsProcessed != 2 || p.RecordsFailed != 2 || p.ErrorCount != 2 || p.PercentComplete != 100 || p.RatePerSecond <= 0 {
		t.Fatalf("bad metrics: %+v", p)
	}
	if p.StageLatencies["ingestion"].Count != 1 || p.StageLatencies["validation"].Count != 4 || p.StageLatencies["transformation"].Count != 3 {
		t.Fatalf("bad stage latencies: %+v", p.StageLatencies)
	}
	if len(snapshots) == 0 {
		t.Fatal("progress observer not called")
	}
}
