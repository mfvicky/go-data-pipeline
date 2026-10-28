package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/api"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           api.NewRouter(repository.NewJobRepository()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("Starting pipeline API on :%s", port)
	log.Fatal(server.ListenAndServe())
}
