package repository

import (
	"errors"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"testing"
)

func TestResultsAndErrorsAreDefensivelyCopied(t *testing.T) {
	repo := NewJobRepository()
	if err := repo.Create("job", domain.JobSpec{}, domain.JobStatus{JobID: "job", State: domain.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetResult("job"); !errors.Is(err, ErrResultNotReady) {
		t.Fatalf("expected ErrResultNotReady, got %v", err)
	}
	record := domain.Record{ID: "record", Data: map[string]any{"name": "original"}}
	failure := domain.PipelineError{JobID: "job", RecordID: "record", Record: &record, Stage: "validation", Message: "failed"}
	if err := repo.AppendError("job", failure); err != nil {
		t.Fatal(err)
	}
	failure.Record.Data["name"] = "modified"
	observed, err := repo.GetErrors("job")
	if err != nil || observed[0].Record.Data["name"] != "original" {
		t.Fatalf("error copy failed: %v %+v", err, observed)
	}
	output := domain.JobOutput{JobID: "job", Records: []domain.Record{record}, Errors: observed}
	if err := repo.SaveResult("job", output); err != nil {
		t.Fatal(err)
	}
	first, err := repo.GetResult("job")
	if err != nil {
		t.Fatal(err)
	}
	first.Records[0].Data["name"] = "changed"
	first.Errors[0].Record.Data["name"] = "changed"
	second, err := repo.GetResult("job")
	if err != nil || second.Records[0].Data["name"] != "modified" || second.Errors[0].Record.Data["name"] != "original" {
		t.Fatalf("result was modified by caller: %+v %v", second, err)
	}
}
