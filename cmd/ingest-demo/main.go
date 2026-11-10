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
	timeout := flag.Duration("timeout", 60*time.Second, "max time to load source (e.g. 60s)")
	flag.Parse()
	if *timeout <= 0 {
		log.Fatal("-timeout must be greater than zero")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
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
