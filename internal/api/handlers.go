package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
)

type handler struct {
	repo         *repository.JobRepository
	manager      *jobs.Manager
	sourcePolicy *security.SourcePolicy
}

type apiError struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, apiError{Error: message})
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mediaType, _, err := mime.ParseMediaType(ct)
		if err != nil || mediaType != "application/json" {
			writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	defer func() { _ = r.Body.Close() }()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var spec domain.JobSpec
	if err := decoder.Decode(&spec); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "job spec exceeds 1 MiB")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON job spec: "+err.Error())
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain exactly one JSON object")
		return
	}
	if err := domain.ValidateJobSpec(spec); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.sourcePolicy != nil {
		if err := h.sourcePolicy.Validate(r.Context(), spec.Sources); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := h.sourcePolicy.ValidateExports(spec.Exports); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	id, err := newJobID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not generate job identifier")
		return
	}
	status := domain.JobStatus{JobID: id, State: domain.StatePending, CreatedAt: time.Now().UTC()}
	if err := h.repo.Create(id, spec, status); err != nil {
		writeError(w, http.StatusInternalServerError, "could not save job")
		return
	}
	h.manager.Start(id, spec)
	w.Header().Set("Location", "/api/v1/pipelines/"+id)
	writeJSON(w, http.StatusCreated, struct {
		JobID   string          `json:"job_id"`
		State   domain.JobState `json:"state"`
		Message string          `json:"message"`
	}{JobID: id, State: domain.StatePending, Message: "Job accepted for asynchronous processing"})
}

func (h *handler) list(w http.ResponseWriter, _ *http.Request) {
	jobs := h.repo.ListJobs()
	writeJSON(w, http.StatusOK, struct {
		Jobs  []*domain.JobStatus `json:"jobs"`
		Count int                 `json:"count"`
	}{Jobs: jobs, Count: len(jobs)})
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	job, err := h.repo.GetJob(r.PathValue("id"))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to retrieve job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.repo.DeleteJob(id)
	if errors.Is(err, repository.ErrRunning) {
		writeError(w, http.StatusConflict, "cancel a pending or running job before deleting it")
		return
	}
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not delete job")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func newJobID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

// results is available once execution has reached a terminal state.
func (h *handler) results(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	status, err := h.repo.GetStatus(id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to retrieve job")
		return
	}
	if status.State == domain.StatePending || status.State == domain.StateRunning {
		writeError(w, http.StatusConflict, "results are not ready")
		return
	}
	result, err := h.repo.GetResult(id)
	if errors.Is(err, repository.ErrResultNotReady) {
		writeError(w, http.StatusConflict, "results are not ready")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to retrieve results")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) errors(w http.ResponseWriter, r *http.Request) {
	failures, err := h.repo.GetErrors(r.PathValue("id"))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to retrieve errors")
		return
	}
	failedRecords := make([]domain.FailedRecord, 0)
	for _, failure := range failures {
		if failure.Record != nil {
			failedRecords = append(failedRecords, domain.FailedRecord{Record: *failure.Record, Error: failure})
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Errors        []domain.PipelineError `json:"errors"`
		FailedRecords []domain.FailedRecord  `json:"failed_records"`
		Count         int                    `json:"count"`
	}{Errors: failures, FailedRecords: failedRecords, Count: len(failures)})
}

func (h *handler) progress(w http.ResponseWriter, r *http.Request) {
	status, err := h.repo.GetStatus(r.PathValue("id"))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to retrieve progress")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	err := h.manager.Cancel(r.PathValue("id"))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, jobs.ErrNotCancellable) {
		writeError(w, http.StatusConflict, "job is not running")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to cancel job")
		return
	}
	writeJSON(w, http.StatusAccepted, struct {
		Message string `json:"message"`
	}{Message: "Cancellation requested"})
}
