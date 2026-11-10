package repository

import (
	"fmt"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"time"
)

func (r *JobRepository) loadPersistent(recoverPending bool) error {
	snapshots, err := r.store.all()
	if err != nil {
		return err
	}
	for id, snapshot := range snapshots {
		r.saveLocked(id, &snapshot.Spec, &snapshot.Status)
		r.lastPersist[id] = time.Now()
		if snapshot.Result != nil {
			value := cloneOutput(*snapshot.Result)
			r.results[id] = &value
		}
		r.errors[id] = snapshot.Errors
		status := r.jobs[id]
		if status.State != domain.StateRunning && status.State != domain.StatePending {
			continue
		}
		if recoverPending {
			status.State = domain.StatePending
			status.StartTime = nil
			status.EndTime = nil
			if err := r.persistLocked(id); err != nil {
				return fmt.Errorf("requeue job %s: %w", id, err)
			}
			continue
		}
		now := time.Now().UTC()
		status.State = domain.StateFailed
		status.EndTime = &now
		failure := domain.PipelineError{JobID: id, Stage: "recovery", Message: "job interrupted by server restart", Timestamp: now}
		r.errors[id] = append(r.errors[id], failure)
		if r.results[id] == nil {
			r.results[id] = &domain.JobOutput{JobID: id}
		}
		r.results[id].Errors = append(r.results[id].Errors, failure)
		status.ErrorCount = int64(len(r.errors[id]))
		if err := r.persistLocked(id); err != nil {
			return fmt.Errorf("recover job %s: %w", id, err)
		}
	}
	return nil
}

// PendingIDs are republished on startup in RabbitMQ mode. Job state is checked
// again by consumers, so replaying an existing queued ID is harmless.
func (r *JobRepository) PendingIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0)
	for id, status := range r.jobs {
		if status.State == domain.StatePending {
			result = append(result, id)
		}
	}
	return result
}
