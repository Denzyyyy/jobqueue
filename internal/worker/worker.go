package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Denzyyyy/jobqueue/internal/storage"
	"github.com/Denzyyyy/jobqueue/pkg/models"
)

// Handler is a function that processes a job
type Handler func(ctx context.Context, job *models.Job) error

// Worker polls for jobs and processes them
type Worker struct {
	id            string
	store         storage.Store
	queues        []string
	handlers      map[string]Handler
	leaseDuration time.Duration
	pollInterval  time.Duration

	// Concurrency control
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Currently processing jobs (for heartbeats)
	mu         sync.Mutex
	activeJobs map[uuid.UUID]*activeJob
}

type activeJob struct {
	job        *models.Job
	cancelFunc context.CancelFunc
}

// Config holds worker configuration
type Config struct {
	ID            string
	Queues        []string
	LeaseDuration time.Duration
	PollInterval  time.Duration
}

// NewWorker creates a new worker instance
func NewWorker(store storage.Store, config Config) *Worker {
	if config.ID == "" {
		config.ID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}
	if config.LeaseDuration == 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.PollInterval == 0 {
		config.PollInterval = 1 * time.Second
	}
	if len(config.Queues) == 0 {
		config.Queues = []string{"default"}
	}

	return &Worker{
		id:            config.ID,
		store:         store,
		queues:        config.Queues,
		handlers:      make(map[string]Handler),
		leaseDuration: config.LeaseDuration,
		pollInterval:  config.PollInterval,
		stopCh:        make(chan struct{}),
		activeJobs:    make(map[uuid.UUID]*activeJob),
	}
}

// RegisterHandler registers a handler for a specific job type
func (w *Worker) RegisterHandler(jobType string, handler Handler) {
	w.handlers[jobType] = handler
	log.Printf("[%s] Registered handler for job type: %s", w.id, jobType)
}

// Start begins the worker's poll and heartbeat loops
func (w *Worker) Start() {
	log.Printf("[%s] Starting worker for queues: %v", w.id, w.queues)

	// Start polling for jobs
	w.wg.Add(1)
	go w.pollLoop()

	// Start heartbeat loop
	w.wg.Add(1)
	go w.heartbeatLoop()

	log.Printf("[%s] Worker started", w.id)
}

// Stop gracefully stops the worker
func (w *Worker) Stop() {
	log.Printf("[%s] Stopping worker...", w.id)
	close(w.stopCh)

	// Cancel all active jobs
	w.mu.Lock()
	for _, aj := range w.activeJobs {
		aj.cancelFunc()
	}
	w.mu.Unlock()

	// Wait for goroutines to finish
	w.wg.Wait()
	log.Printf("[%s] Worker stopped", w.id)
}

// pollLoop continuously polls for jobs
func (w *Worker) pollLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.pollOnce()
		case <-w.stopCh:
			return
		}
	}
}

// pollOnce attempts to lease and process a job from each queue
func (w *Worker) pollOnce() {
	for _, queueName := range w.queues {
		// Try to lease a job
		job, err := w.store.LeaseJob(context.Background(), queueName, w.leaseDuration)
		if err != nil {
			log.Printf("[%s] Error leasing job from queue %s: %v", w.id, queueName, err)
			continue
		}

		if job == nil {
			// No jobs available in this queue
			continue
		}

		log.Printf("[%s] Leased job %s from queue %s (attempt %d/%d)",
			w.id, job.ID, queueName, job.Attempts, job.MaxAttempts)

		// Process the job asynchronously
		go w.processJob(job)
	}
}

// processJob executes the job handler
func (w *Worker) processJob(job *models.Job) {
	// Create cancellable context for this job
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track active job (for heartbeats)
	w.mu.Lock()
	w.activeJobs[job.ID] = &activeJob{
		job:        job,
		cancelFunc: cancel,
	}
	w.mu.Unlock()

	// Remove from active jobs when done
	defer func() {
		w.mu.Lock()
		delete(w.activeJobs, job.ID)
		w.mu.Unlock()
	}()

	// Extract job type from payload
	var payload struct {
    Task string `json:"task"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
    	log.Printf("[%s] Failed to parse job payload: %v", w.id, err)
    	w.failJob(job, fmt.Sprintf("invalid payload: %v", err))
    return
}

	// Find handler
	handler, exists := w.handlers[payload.Task]
	if !exists {
		// No handler registered - use default handler
		handler = w.defaultHandler
	}

	// Execute handler
	startTime := time.Now()
	err := handler(ctx, job)
	duration := time.Since(startTime)

	if err != nil {
		log.Printf("[%s] Job %s failed after %v: %v", w.id, job.ID, duration, err)
		w.failJob(job, err.Error())
		return
	}

	log.Printf("[%s] Job %s completed successfully in %v", w.id, job.ID, duration)
	w.completeJob(job)
}

// completeJob marks the job as completed
func (w *Worker) completeJob(job *models.Job) {
	if job.LeaseToken == nil {
		log.Printf("[%s] Cannot complete job %s: no lease token", w.id, job.ID)
		return
	}

	err := w.store.CompleteJob(context.Background(), job.ID, *job.LeaseToken)
	if err != nil {
		log.Printf("[%s] Failed to complete job %s: %v", w.id, job.ID, err)
	}
}

// failJob marks the job as failed
func (w *Worker) failJob(job *models.Job, errMsg string) {
	if job.LeaseToken == nil {
		log.Printf("[%s] Cannot fail job %s: no lease token", w.id, job.ID)
		return
	}

	err := w.store.FailJob(context.Background(), job.ID, *job.LeaseToken, errMsg)
	if err != nil {
		log.Printf("[%s] Failed to mark job %s as failed: %v", w.id, job.ID, err)
	}
}

// heartbeatLoop sends heartbeats for active jobs
func (w *Worker) heartbeatLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.sendHeartbeats()
		case <-w.stopCh:
			return
		}
	}
}

func (w *Worker) sendHeartbeats() {
	w.mu.Lock()
	activeJobs := make([]*models.Job, 0, len(w.activeJobs))
	for _, aj := range w.activeJobs {
		activeJobs = append(activeJobs, aj.job)
	}
	w.mu.Unlock()
	
	if len(activeJobs) == 0 {
		return
	}
	
	for _, job := range activeJobs {
		if job.LeaseToken == nil {
			continue
		}
		
		err := w.store.ExtendLease(
			context.Background(),
			job.ID,
			*job.LeaseToken,
			w.leaseDuration,
		)
		
		if err != nil {
			log.Printf("[%s] ⚠️  Failed to extend lease for job %s: %v", w.id, job.ID, err)
		} else {
			log.Printf("[%s] ✅ Extended lease for job %s", w.id, job.ID)
		}
	}
}

// defaultHandler is used when no specific handler is registered
func (w *Worker) defaultHandler(ctx context.Context, job *models.Job) error {
	log.Printf("[%s] Processing job %s with default handler", w.id, job.ID)
	log.Printf("[%s] Payload: %s", w.id, string(job.Payload))

	// Simulate work
	time.Sleep(2 * time.Second)

	return nil
}

