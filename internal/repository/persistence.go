package repository

import (
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type persistentStore interface {
	put(string, jobSnapshot) error
	remove(string) error
	all() (map[string]jobSnapshot, error)
	close() error
	ping() error
}

type jobSnapshot struct {
	Spec   domain.JobSpec         `json:"spec"`
	Status domain.JobStatus       `json:"status"`
	Result *domain.JobOutput      `json:"result,omitempty"`
	Errors []domain.PipelineError `json:"errors,omitempty"`
}

func (r *JobRepository) persistLocked(id string) error {
	if r.store == nil {
		return nil
	}
	snapshot := jobSnapshot{Spec: cloneSpec(*r.specs[id]), Status: cloneStatus(*r.jobs[id]), Errors: r.errors[id]}
	if result := r.results[id]; result != nil {
		copyResult := cloneOutput(*result)
		snapshot.Result = &copyResult
	}
	return r.store.put(id, snapshot)
}

// Close releases the database connection after jobs have stopped.
func (r *JobRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.store == nil {
		return nil
	}
	err := r.store.close()
	r.store = nil
	return err
}

// DeleteFinishedBefore removes old terminal jobs; pending and active work remains untouched.
func (r *JobRepository) DeleteFinishedBefore(before time.Time) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for id, status := range r.jobs {
		if status.EndTime == nil || !status.EndTime.Before(before) {
			continue
		}
		if r.store != nil {
			if err := r.store.remove(id); err != nil {
				return count, err
			}
		}
		delete(r.jobs, id)
		delete(r.specs, id)
		delete(r.results, id)
		delete(r.errors, id)
		delete(r.lastPersist, id)
		count++
	}
	return count, nil
}

// Ping reports whether the backing store is reachable. In-memory mode is always ready.
func (r *JobRepository) Ping() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.store == nil {
		return nil
	}
	return r.store.ping()
}
