package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Denzyyyy/jobqueue/internal/storage"
	"github.com/Denzyyyy/jobqueue/internal/worker"
	"github.com/Denzyyyy/jobqueue/pkg/models"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgresql://jobqueue:devpass@localhost:5432/jobqueue?sslmode=disable")

	store, err := storage.NewPostgresStore(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer store.Close()

	log.Println("✓ Connected to database")

	w := worker.NewWorker(store, worker.Config{
		ID:            "worker-1",
		Queues:        []string{"default", "email"},
		LeaseDuration: 30 * time.Second,
		PollInterval:  1 * time.Second,
	})

	w.RegisterHandler("send_email", sendEmailHandler)
	w.RegisterHandler("process_data", processDataHandler)
	w.RegisterHandler("flaky_service", flakyServiceHandler)
	w.RegisterHandler("always_fail", alwaysFailHandler)
	w.RegisterHandler("random_fail", randomFailHandler)
	w.RegisterHandler("long_running", longRunningHandler)

	recovery := worker.NewLeaseRecovery(store, 10*time.Second)
	recovery.Start()

	w.Start()

	log.Println("🚀 Worker is running. Press Ctrl+C to stop.")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down worker...")
	w.Stop()
	log.Println("Worker stopped gracefully")
}

func sendEmailHandler(ctx context.Context, job *models.Job) error {
	log.Printf("📧 Sending email for job %s", job.ID)

	var payload struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
		Task    string `json:"task"`
	}

	if err := job.Payload.UnmarshalJSON(job.Payload); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	log.Printf("   To: %s", payload.To)
	log.Printf("   Subject: %s", payload.Subject)

	time.Sleep(3 * time.Second)

	log.Printf("✅ Email sent successfully for job %s", job.ID)
	return nil
}

func processDataHandler(ctx context.Context, job *models.Job) error {
	log.Printf("📊 Processing data for job %s", job.ID)
	time.Sleep(4 * time.Second)
	log.Printf("✅ Data processed successfully for job %s", job.ID)
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// flakyServiceHandler simulates a service that fails sometimes
func flakyServiceHandler(ctx context.Context, job *models.Job) error {
	log.Printf("🔄 Attempting flaky service call for job %s (attempt %d/%d)",
		job.ID, job.Attempts, job.MaxAttempts)

	// Simulate API call delay
	time.Sleep(1 * time.Second)

	// Fails on first 2 attempts, succeeds on 3rd
	if job.Attempts < 3 {
		log.Printf("❌ Flaky service failed for job %s (attempt %d/%d)",
			job.ID, job.Attempts, job.MaxAttempts)
		return fmt.Errorf("service temporarily unavailable (attempt %d)", job.Attempts)
	}

	log.Printf("✅ Flaky service succeeded for job %s on attempt %d!",
		job.ID, job.Attempts)
	return nil
}

// alwaysFailHandler always fails (to test dead letter queue)
func alwaysFailHandler(ctx context.Context, job *models.Job) error {
	log.Printf("💥 Always-fail handler for job %s (attempt %d/%d)",
		job.ID, job.Attempts, job.MaxAttempts)

	time.Sleep(1 * time.Second)

	return fmt.Errorf("this job is designed to always fail")
}

// randomFailHandler fails 60% of the time
func randomFailHandler(ctx context.Context, job *models.Job) error {
	log.Printf("🎲 Random fail handler for job %s (attempt %d/%d)",
		job.ID, job.Attempts, job.MaxAttempts)

	time.Sleep(1 * time.Second)

	// 60% failure rate
	if rand.Float64() < 0.6 {
		log.Printf("❌ Random fail - job %s failed", job.ID)
		return fmt.Errorf("random failure occurred")
	}

	log.Printf("✅ Random fail - job %s succeeded!", job.ID)
	return nil
}

// longRunningHandler simulates a job that takes 60 seconds
func longRunningHandler(ctx context.Context, job *models.Job) error {
	log.Printf("🕐 Long-running job %s started (will take 60 seconds)", job.ID)
	
	// Process in chunks so we can see progress
	for i := 0; i < 60; i++ {
		select {
		case <-ctx.Done():
			// Context cancelled (worker shutdown)
			log.Printf("⚠️  Long-running job %s cancelled", job.ID)
			return fmt.Errorf("job cancelled")
		default:
			time.Sleep(1 * time.Second)
			if i%10 == 0 && i > 0 {
				log.Printf("   Job %s progress: %d/60 seconds", job.ID, i)
			}
		}
	}
	
	log.Printf("✅ Long-running job %s completed after 60 seconds", job.ID)
	return nil
}

