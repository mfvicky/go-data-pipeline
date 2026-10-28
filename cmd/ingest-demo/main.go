// Command ingest-demo exercises the Day 1 readers without starting a job.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/ingestion"
)

func main() {
	kind := flag.String("type", "csv", "source type: csv, json, api")
	endpoint := flag.String("endpoint", "testdata/input/customers.csv", "local file path or HTTP(S) URL")
	name := flag.String("name", "demo", "source name")
	recordPath := flag.String("record-path", "", "optional nested JSON record path, e.g. data.records")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	records, err := ingestion.Read(ctx, domain.SourceConfig{
		Name: *name, Type: *kind, Endpoint: *endpoint, RecordPath: *recordPath,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "read %d records\n", len(records))
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(records); err != nil {
		log.Fatal(err)
	}
}
