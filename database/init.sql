-- Enable uuid-ossp so uuid_generate_v4() is available as a fallback.
-- NOTE: All application code should prefer gen_random_uuid() (no extension needed, PG13+).
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS documents (
  document_id UUID PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  agency_id TEXT NOT NULL,
  filename TEXT,
  object_key TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  file_size BIGINT NOT NULL,
  status TEXT NOT NULL,
  sequence_number BIGINT NOT NULL,
  kafka_topic TEXT,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  worker_id TEXT,
  processing_started_at TIMESTAMPTZ,
  processing_completed_at TIMESTAMPTZ,
  processing_duration_ms INTEGER,
  audit_type TEXT NOT NULL,
  report_year INTEGER,
  auditor_org TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (agency_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS processing_results (
  result_id UUID PRIMARY KEY,
  document_id UUID NOT NULL UNIQUE REFERENCES documents(document_id),
  classification_status TEXT NOT NULL,
  processing_started_at TIMESTAMPTZ,
  processing_completed_at TIMESTAMPTZ,
  processing_duration_ms INTEGER,
  model_version TEXT,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processing_events (
  event_id UUID PRIMARY KEY,
  document_id UUID REFERENCES documents(document_id),
  agency_id TEXT,
  sequence_number BIGINT,
  event_type TEXT NOT NULL,
  worker_id TEXT,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  severity TEXT NOT NULL DEFAULT 'INFO',
  message TEXT NOT NULL DEFAULT '',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS worker_status (
  worker_id TEXT PRIMARY KEY,
  hostname TEXT,
  process_id INTEGER,
  status TEXT NOT NULL DEFAULT 'REGISTERED',
  current_state TEXT NOT NULL DEFAULT 'IDLE',
  current_document_id UUID,
  cpu_usage_pct FLOAT NOT NULL DEFAULT 0,
  memory_usage_pct FLOAT NOT NULL DEFAULT 0,
  processing_rate FLOAT NOT NULL DEFAULT 0,
  failed_jobs INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS load_test_runs (
  test_run_id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  target_documents INTEGER NOT NULL,
  target_duration_seconds INTEGER NOT NULL,
  requested_documents INTEGER NOT NULL DEFAULT 0,
  accepted_202 INTEGER NOT NULL DEFAULT 0,
  rejected_429 INTEGER NOT NULL DEFAULT 0,
  rejected_4xx INTEGER NOT NULL DEFAULT 0,
  failed_5xx INTEGER NOT NULL DEFAULT 0,
  failed_timeout INTEGER NOT NULL DEFAULT 0,
  p50_latency_ms FLOAT NOT NULL DEFAULT 0,
  p95_latency_ms FLOAT NOT NULL DEFAULT 0,
  p99_latency_ms FLOAT NOT NULL DEFAULT 0,
  test_result TEXT,
  actual_duration_seconds FLOAT,
  throughput_docs_per_sec FLOAT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);
