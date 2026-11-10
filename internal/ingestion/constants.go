package ingestion

import "time"

const (
	maxSourceBytes     int64 = 20 << 20
	defaultHTTPTimeout       = 60 * time.Second
)
