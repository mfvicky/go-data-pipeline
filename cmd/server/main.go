package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/api"
	"github.com/mfvicky/go-data-pipeline/internal/config"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/logger"
	"github.com/mfvicky/go-data-pipeline/internal/monitor"
	"github.com/mfvicky/go-data-pipeline/internal/queue"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
	"github.com/mfvicky/go-data-pipeline/internal/services"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(1)
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get("http://127.0.0.1:" + cfg.Port + "/health")
		if err != nil {
			os.Exit(1)
		}
		statusOK := resp.StatusCode == http.StatusOK
		if err := resp.Body.Close(); err != nil || !statusOK {
			os.Exit(1)
		}
		return
	}

	logger, err := logger.New(os.Stdout, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if cfg.APIKey == "" {
		logger.Warn("API key is disabled; bind only to loopback")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	policy := &security.SourcePolicy{InputDir: cfg.InputDir, OutputDir: "output", AllowPrivateSources: cfg.AllowPrivateSources, AllowedHosts: cfg.SourceHostAllowlist}
	repo := repository.NewJobRepository()
	if cfg.StorageDriver == "postgres" {
		repo, err = repository.OpenPostgresWithRecovery(cfg.DatabaseURL, cfg.QueueDriver == "rabbitmq")
		if err != nil {
			logger.Error("database open failed", "error", err)
			os.Exit(1)
		}
	}
	defer func() {
		if err := repo.Close(); err != nil {
			logger.Error("database close failed", "error", err)
		}
	}()
	metrics := monitor.NewRegistry()
	var broker *queue.Rabbit
	if cfg.QueueDriver == "rabbitmq" {
		broker, err = queue.NewRabbit(cfg.RabbitMQURL, cfg.RabbitMQQueue, logger)
		if err != nil {
			logger.Error("RabbitMQ connection failed", "error", err)
			return
		}
		defer func() { _ = broker.Close() }()
	}
	var publisher jobs.Publisher
	if broker != nil {
		publisher = broker
	}
	manager := jobs.NewManagerWithOptions(repo, ctx, jobs.Options{MaxConcurrent: cfg.MaxConcurrentJobs, QueueCapacity: cfg.JobQueueCapacity, SourcePolicy: policy, Metrics: metrics, Logger: logger, Publisher: publisher})
	var consumerWait interface{ Wait() }
	if broker != nil {
		consumerWait = broker.Consume(ctx, cfg.MaxConcurrentJobs, manager.ProcessExternal)
		// Database rows form a lightweight recovery outbox. A failed publish
		// or API restart cannot permanently strand a PENDING job.
		go replayPending(ctx, repo, broker, logger)
	}
	router := api.NewRouterWithOptions(repo, api.Options{
		JobManager: manager, Metrics: metrics, JobLogger: logger, CORSOrigins: cfg.CORSOrigins,
		APIKey:             cfg.APIKey,
		RateLimitPerMinute: cfg.RateLimitPerMinute,
		Logger:             logger,
		SourcePolicy:       policy,
		JobContext:         ctx,
	})

	if cfg.JobRetention > 0 {
		go func() {
			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					cutoff := time.Now().Add(-cfg.JobRetention)
					cleaner := services.JobService{Store: repo, Policy: policy}
					for _, status := range repo.ListJobs() {
						if status.EndTime == nil || !status.EndTime.Before(cutoff) {
							continue
						}
						if err := cleaner.Delete(status.JobID); err != nil {
							logger.Error("retention cleanup failed", "job_id", status.JobID, "error", err)
						}
					}
				}
			}
		}()
	}
	server := &http.Server{
		Addr:              net.JoinHostPort(cfg.Host, cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown failed", "error", err)
		}
	}()

	logger.Info("server starting", "address", server.Addr, "environment", cfg.Environment)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := manager.Shutdown(shutdownCtx); err != nil {
		logger.Error("job shutdown failed", "error", err)
	}
	if consumerWait != nil {
		consumerWait.Wait()
	}
	logger.Info("server stopped")
}

// replayPending is deliberately single-process: this service runs one API
// replica. Each published ID is still checked against PostgreSQL state by the
// consumer, so duplicates after crashes are safe to skip once terminal.
func replayPending(ctx context.Context, repo *repository.JobRepository, broker *queue.Rabbit, logger *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	lastPublished := make(map[string]time.Time)
	for {
		now := time.Now()
		pending := repo.PendingIDs()
		active := make(map[string]bool, len(pending))
		for _, id := range pending {
			active[id] = true
			if now.Sub(lastPublished[id]) < 2*time.Minute {
				continue
			}
			publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := broker.Publish(publishCtx, id, "recovery")
			cancel()
			if err != nil {
				logger.Error("cannot dispatch pending job", "job_id", id, "error", err)
			} else {
				lastPublished[id] = now
			}
		}
		for id := range lastPublished {
			if !active[id] {
				delete(lastPublished, id)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
