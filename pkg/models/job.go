package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

//enum for job state
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusDead      JobStatus = "dead"
)

//core job entity
type Job struct {
	ID             uuid.UUID       `json:"id"`
	QueueName      string          `json:"queue_name"`
	Payload        json.RawMessage `json:"payload"`
	Status         JobStatus       `json:"status"`
	Priority       int             `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	CreatedAt      time.Time       `json:"created_at"`
	ScheduledAt    time.Time       `json:"scheduled_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	Error          string          `json:"error,omitempty"`
	LeaseToken     *uuid.UUID      `json:"lease_token,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type CreateJobRequest struct {
	QueueName      string          `json:"queue_name"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

func (r *CreateJobRequest) Validate() error {
	if r.QueueName == "" {
		r.QueueName = "default"
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.MaxAttempts < 1 || r.MaxAttempts > 10 {
		return fmt.Errorf("max_attempts must be between 1 and 10")
	}
	if len(r.Payload) == 0 {
		return fmt.Errorf("payload cannot be empty")
	}
	return nil
}

type JobResponse struct {
	Job     *Job   `json:"job"`
	Message string `json:"message,omitempty"`
}

type ListJobsRequest struct {
	QueueName string    `json:"queue_name"`
	Status    JobStatus `json:"status"`
	Limit     int       `json:"limit"`
	Offset    int       `json:"offset"`
}

type ListJobsResponse struct {
	Jobs  []*Job `json:"jobs"`
	Total int    `json:"total"`
}
