package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"

	"github.com/Denzyyyy/jobqueue/pkg/models"
)

type Store interface {
	CreateJob(ctx context.Context, req *models.CreateJobRequest) (*models.Job, error)
	GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error)
	ListJobs(ctx context.Context, req *models.ListJobsRequest) ([]*models.Job, int, error)
	LeaseJob(ctx context.Context, queueName string, leaseDuration time.Duration) (*models.Job, error)
	CompleteJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID) error
	FailJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, errMsg string) error
	ReclaimExpiredLeases(ctx context.Context) (int, error)
	Close() error
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) CreateJob(ctx context.Context, req *models.CreateJobRequest) (*models.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	scheduledAt := time.Now()
	if req.ScheduledAt != nil {
		scheduledAt = *req.ScheduledAt
	}

	job := &models.Job{
		ID:             uuid.New(),
		QueueName:      req.QueueName,
		Payload:        req.Payload,
		Status:         models.JobStatusPending,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		ScheduledAt:    scheduledAt,
		IdempotencyKey: req.IdempotencyKey,
	}

	query := `
		INSERT INTO jobs (id, queue_name, payload, status, priority, max_attempts, scheduled_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''))
		RETURNING created_at
	`

	err := s.db.QueryRowContext(
		ctx, query,
		job.ID, job.QueueName, job.Payload, job.Status,
		job.Priority, job.MaxAttempts, job.ScheduledAt, job.IdempotencyKey,
	).Scan(&job.CreatedAt)

	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" { // unique_violation
			return nil, fmt.Errorf("job with this idempotency key already exists")
		}
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	return job, nil
}

func (s *PostgresStore) GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	job := &models.Job{}

	// Use sql.Null* types for nullable fields
	var startedAt, completedAt, leaseExpiresAt sql.NullTime
	var leaseToken sql.NullString
	var errorMsg sql.NullString
	var idempotencyKey sql.NullString

	query := `
		SELECT id, queue_name, payload, status, priority, attempts, max_attempts,
		       created_at, scheduled_at, started_at, completed_at, error,
		       lease_token, lease_expires_at, idempotency_key
		FROM jobs
		WHERE id = $1
	`

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&job.ID, &job.QueueName, &job.Payload, &job.Status, &job.Priority,
		&job.Attempts, &job.MaxAttempts, &job.CreatedAt, &job.ScheduledAt,
		&startedAt, &completedAt, &errorMsg,
		&leaseToken, &leaseExpiresAt, &idempotencyKey,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Convert sql.Null* types to pointers
	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if leaseToken.Valid {
		token, _ := uuid.Parse(leaseToken.String)
		job.LeaseToken = &token
	}
	if errorMsg.Valid {
		job.Error = errorMsg.String
	}
	if idempotencyKey.Valid {
		job.IdempotencyKey = idempotencyKey.String
	}

	return job, nil
}

func (s *PostgresStore) ListJobs(ctx context.Context, req *models.ListJobsRequest) ([]*models.Job, int, error) {
	if req.Limit == 0 {
		req.Limit = 50
	}

	// Build WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if req.QueueName != "" {
		whereClause += fmt.Sprintf(" AND queue_name = $%d", argIdx)
		args = append(args, req.QueueName)
		argIdx++
	}
	if req.Status != "" {
		whereClause += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, req.Status)
		argIdx++
	}

	// Count total
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", whereClause)
	err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count jobs: %w", err)
	}

	// Get jobs
	query := fmt.Sprintf(`
		SELECT id, queue_name, payload, status, priority, attempts, max_attempts,
		       created_at, scheduled_at, started_at, completed_at, error,
		       lease_token, lease_expires_at, idempotency_key
		FROM jobs
		%s
		ORDER BY priority DESC, created_at ASC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, req.Limit, req.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []*models.Job{}
	for rows.Next() {
		job := &models.Job{}

		var startedAt, completedAt, leaseExpiresAt sql.NullTime
		var leaseToken sql.NullString
		var errorMsg sql.NullString
		var idempotencyKey sql.NullString

		err := rows.Scan(
			&job.ID, &job.QueueName, &job.Payload, &job.Status, &job.Priority,
			&job.Attempts, &job.MaxAttempts, &job.CreatedAt, &job.ScheduledAt,
			&startedAt, &completedAt, &errorMsg,
			&leaseToken, &leaseExpiresAt, &idempotencyKey,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan job: %w", err)
		}

		// Convert sql.Null* types
		if startedAt.Valid {
			job.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			job.CompletedAt = &completedAt.Time
		}
		if leaseExpiresAt.Valid {
			job.LeaseExpiresAt = &leaseExpiresAt.Time
		}
		if leaseToken.Valid {
			token, _ := uuid.Parse(leaseToken.String)
			job.LeaseToken = &token
		}
		if errorMsg.Valid {
			job.Error = errorMsg.String
		}
		if idempotencyKey.Valid {
			job.IdempotencyKey = idempotencyKey.String
		}

		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// LeaseJob atomically leases a job using PostgreSQL's FOR UPDATE SKIP LOCKED
func (s *PostgresStore) LeaseJob(ctx context.Context, queueName string, leaseDuration time.Duration) (*models.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Find and lock a job atomically
	// FOR UPDATE SKIP LOCKED ensures only one worker can grab this job
	query := `
		SELECT id, queue_name, payload, status, priority, attempts, max_attempts,
		       created_at, scheduled_at
		FROM jobs
		WHERE queue_name = $1
		  AND status = 'pending'
		  AND scheduled_at <= NOW()
		ORDER BY priority DESC, scheduled_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`

	job := &models.Job{}
	err = tx.QueryRowContext(ctx, query, queueName).Scan(
		&job.ID, &job.QueueName, &job.Payload, &job.Status, &job.Priority,
		&job.Attempts, &job.MaxAttempts, &job.CreatedAt, &job.ScheduledAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil // No jobs available
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find job: %w", err)
	}

	// Update job with lease
	leaseToken := uuid.New()
	leaseExpiresAt := time.Now().Add(leaseDuration)
	startedAt := time.Now()

	updateQuery := `
		UPDATE jobs
		SET status = 'running',
		    attempts = attempts + 1,
		    started_at = $2,
		    lease_token = $3,
		    lease_expires_at = $4
		WHERE id = $1
	`

	_, err = tx.ExecContext(ctx, updateQuery, job.ID, startedAt, leaseToken, leaseExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to lease job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	job.Status = models.JobStatusRunning
	job.Attempts++
	job.StartedAt = &startedAt
	job.LeaseToken = &leaseToken
	job.LeaseExpiresAt = &leaseExpiresAt

	return job, nil
}

func (s *PostgresStore) CompleteJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID) error {
	completedAt := time.Now()
	query := `
		UPDATE jobs
		SET status = 'completed',
		    completed_at = $2,
		    lease_token = NULL,
		    lease_expires_at = NULL
		WHERE id = $1 AND lease_token = $3 AND status = 'running'
	`

	result, err := s.db.ExecContext(ctx, query, id, completedAt, leaseToken)
	if err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("job not found or lease token mismatch")
	}

	return nil
}

func (s *PostgresStore) FailJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, errMsg string) error {
	// Exponential backoff: 2^attempts seconds
	query := `
		UPDATE jobs
		SET status = CASE
		        WHEN attempts >= max_attempts THEN 'dead'::job_status
		        ELSE 'pending'::job_status
		    END,
		    error = $2,
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    scheduled_at = CASE
		        WHEN attempts >= max_attempts THEN scheduled_at
		        ELSE NOW() + (POWER(2, attempts) || ' seconds')::INTERVAL
		    END
		WHERE id = $1 AND lease_token = $3 AND status = 'running'
	`

	result, err := s.db.ExecContext(ctx, query, id, errMsg, leaseToken)
	if err != nil {
		return fmt.Errorf("failed to fail job: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("job not found or lease token mismatch")
	}

	return nil
}

func (s *PostgresStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	// Return expired jobs to pending queue with exponential backoff
	query := `
		UPDATE jobs
		SET status = 'pending',
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    scheduled_at = NOW() + (POWER(2, attempts) || ' seconds')::INTERVAL
		WHERE status = 'running'
		  AND lease_expires_at < NOW()
	`

	result, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to reclaim expired leases: %w", err)
	}

	rows, _ := result.RowsAffected()
	return int(rows), nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
