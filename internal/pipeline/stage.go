package pipeline

import "github.com/mfvicky/go-data-pipeline/internal/domain"

// Channels bundles all side-car and forward-flowing channels for a pipeline run
type Channels struct {
	Records chan domain.Record
	Errors  chan domain.PipelineError
}

// NewChannels initializes and returns a pointer to a Channels struct with buffered channels
func NewChannels(bufferSize int) *Channels {
	return &Channels{
		Records: make(chan domain.Record, bufferSize),
		Errors:  make(chan domain.PipelineError, bufferSize),
	}
}
