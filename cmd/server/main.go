package main

import (
	"fmt"
	"net/http"

	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

func main() {
	// Initialize the job repository (will be used for pipeline jobs later)
	repo := repository.NewJobRepository()
	_ = repo

	fmt.Println("Starting pipeline server on port 8080...")

	// Set up router and HTTP endpoints
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Pipeline API is running successfully!"))
	})

	// Start the HTTP server
	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}
}
