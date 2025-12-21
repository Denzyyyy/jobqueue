CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'dead');

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name VARCHAR(255) NOT NULL DEFAULT 'default',
    payload JSONB NOT NULL,
    status job_status NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error TEXT,
    
    lease_token UUID,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    
    idempotency_key VARCHAR(255) UNIQUE
);

CREATE INDEX idx_jobs_status_scheduled ON jobs(status, scheduled_at) 
    WHERE status = 'pending';

CREATE INDEX idx_jobs_queue_status ON jobs(queue_name, status);

CREATE INDEX idx_jobs_lease_expires ON jobs(lease_expires_at) 
    WHERE status = 'running';
