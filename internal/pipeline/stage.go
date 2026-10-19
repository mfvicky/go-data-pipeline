package pipeline

import "github.com/mfvicky/go-data-pipeline/internal/domain"

// Channels bundles all side-car and forward-flowing channels for a pipeline run
type Channels struct {
	Records     chan domain.Record
	Validated   chan domain.Record
	Transformed chan domain.Record
	Results     chan domain.Record
	Errors      chan domain.PipelineError
	Progress    chan domain.JobStatus
}

// NewChannels initializes and returns a pointer to a Channels struct with buffered channels
func NewChannels(bufferSize int) *Channels {
	return &Channels{
		Records:     make(chan domain.Record, bufferSize),
		Validated:   make(chan domain.Record, bufferSize),
		Transformed: make(chan domain.Record, bufferSize),
		Results:     make(chan domain.Record, bufferSize),
		Errors:      make(chan domain.PipelineError, bufferSize),
		Progress:    make(chan domain.JobStatus, bufferSize),
	}
}