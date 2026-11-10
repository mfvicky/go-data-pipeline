package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/middleware"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/services"
)

type handler struct {
	service services.JobService
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
		writeError(w, http.StatusBadRequest, "invalid JSON job specification")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain exactly one JSON object")
		return
	}
	id, err := h.service.Create(r.Context(), spec, middleware.GetRequestID(r.Context()))
	if errors.Is(err, services.ErrInvalidSpec) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if errors.Is(err, services.ErrDispatchPending) {
		w.Header().Set("Location", "/api/v1/pipelines/"+id)
		writeJSON(w, http.StatusServiceUnavailable, struct {
			Error string          `json:"error"`
			JobID string          `json:"job_id"`
			State domain.JobState `json:"state"`
		}{Error: "job saved; RabbitMQ dispatch will be retried", JobID: id, State: domain.StatePending})
		return
	}
	if errors.Is(err, jobs.ErrQueueFull) || errors.Is(err, jobs.ErrShuttingDown) {
		writeError(w, http.StatusServiceUnavailable, "job queue unavailable")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create or enqueue job")
		return
	}
	w.Header().Set("Location", "/api/v1/pipelines/"+id)
	writeJSON(w, http.StatusCreated, struct {
		JobID   string          `json:"job_id"`
		State   domain.JobState `json:"state"`
		Message string          `json:"message"`
	}{JobID: id, State: domain.StatePending, Message: "Job accepted for asynchronous processing"})
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	jobs := h.service.List()
	total := len(jobs)
	offset, limit, err := paging(r, 50)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	jobs = jobs[offset:end]
	writeJSON(w, http.StatusOK, struct {
		Jobs   []*domain.JobStatus `json:"jobs"`
		Count  int                 `json:"count"`
		Total  int                 `json:"total"`
		Offset int                 `json:"offset"`
		Limit  int                 `json:"limit"`
	}{Jobs: jobs, Count: len(jobs), Total: total, Offset: offset, Limit: limit})
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	job, err := h.service.Get(r.PathValue("id"))
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
	err := h.service.Delete(id)
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

// results is available once execution has reached a terminal state.
func (h *handler) results(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.service.Result(id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, repository.ErrResultNotReady) {
		writeError(w, http.StatusConflict, "results are not ready")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to retrieve results")
		return
	}
	if raw := r.URL.Query().Get("offset"); raw != "" || r.URL.Query().Get("limit") != "" {
		offset, limit, parseErr := paging(r, 100)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, parseErr.Error())
			return
		}
		total := len(result.Records)
		if offset > total {
			offset = total
		}
		end := offset + limit
		if end > total {
			end = total
		}
		result.Records = result.Records[offset:end]
		writeJSON(w, http.StatusOK, struct {
			*domain.JobOutput
			TotalRecords int `json:"total_records"`
			Offset       int `json:"offset"`
			Limit        int `json:"limit"`
		}{result, total, offset, limit})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) errors(w http.ResponseWriter, r *http.Request) {
	failures, err := h.service.Errors(r.PathValue("id"))
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
	status, err := h.service.Progress(r.PathValue("id"))
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
	err := h.service.Cancel(r.PathValue("id"))
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

func paging(r *http.Request, fallback int) (int, int, error) {
	offset := 0
	limit := fallback
	if raw := r.URL.Query().Get("offset"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return 0, 0, fmt.Errorf("offset must be nonnegative")
		}
		offset = n
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			return 0, 0, fmt.Errorf("limit must be 1-1000")
		}
		limit = n
	}
	return offset, limit, nil
}
