CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    enabled INTEGER NOT NULL,
    schedule_type TEXT NOT NULL,
    schedule_value TEXT NOT NULL,
    fetch_enabled INTEGER NOT NULL,
    fetch_method TEXT,
    fetch_url TEXT,
    fetch_headers TEXT,
    fetch_body TEXT,
    transform_enabled INTEGER NOT NULL,
    transform_script TEXT,
    send_enabled INTEGER NOT NULL,
    send_method TEXT,
    send_url TEXT,
    send_headers TEXT,
    send_body_template TEXT,
    retry_enabled INTEGER NOT NULL,
    max_retries INTEGER,
    retry_interval_seconds INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_run_at TEXT,
    next_run_at TEXT
);

CREATE TABLE IF NOT EXISTS execution_logs (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    fetch_status INTEGER,
    send_status INTEGER,
    duration_ms INTEGER,
    error_message TEXT,
    response_preview TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_jobs_next_run ON jobs(next_run_at);
CREATE INDEX IF NOT EXISTS idx_logs_job_id ON execution_logs(job_id);
