package main

import (
	"context"
	"fmt"
	"log"
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

