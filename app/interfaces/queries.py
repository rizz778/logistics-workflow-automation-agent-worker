"""Centralized SQL query constants for PostgreSQL."""

INIT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id TEXT,
    filename TEXT NOT NULL,
    upload_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extracted_data JSONB DEFAULT '{}'::jsonb,
    validation_results JSONB DEFAULT '{}'::jsonb,
    decision TEXT DEFAULT 'pending',
    decision_reason TEXT DEFAULT '',
    amendment_draft TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    edited_data JSONB,
    cross_doc_results JSONB DEFAULT '{"is_consistent": true, "discrepancies": []}'::jsonb,
    source TEXT NOT NULL DEFAULT 'manual_upload',
    email_sender TEXT,
    email_metadata_json JSONB,
    is_cached BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_decision ON pipeline_runs (decision);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_source ON pipeline_runs (source);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_created_at ON pipeline_runs (upload_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_cross_doc ON pipeline_runs USING GIN (cross_doc_results);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_extracted ON pipeline_runs USING GIN (extracted_data);
"""

HEALTH_CHECK_SQL = "SELECT 1;"

INSERT_QUEUED_RUN_SQL = """
INSERT INTO pipeline_runs (
    shipment_id, filename, status, decision, source,
    email_sender, email_metadata_json, is_cached
) VALUES (%s, %s, 'queued', 'pending', %s, %s, %s, FALSE)
RETURNING id;
"""

INSERT_CACHED_RUN_SQL = """
INSERT INTO pipeline_runs (
    shipment_id, filename, extracted_data, validation_results,
    decision, decision_reason, amendment_draft, status,
    cross_doc_results, source, email_sender, email_metadata_json, is_cached
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
RETURNING id;
"""

SELECT_RUN_BY_ID_SQL = """
SELECT * FROM pipeline_runs WHERE id = %s;
"""

SELECT_INCOMING_RUNS_SQL = """
SELECT * FROM pipeline_runs
WHERE source = 'inbox' AND status = 'pending_review'
ORDER BY id DESC;
"""

SELECT_PROCESSED_RUNS_SQL = """
SELECT * FROM pipeline_runs
WHERE source = 'inbox' AND status != 'pending_review'
ORDER BY id DESC;
"""

UPDATE_RUN_STATUS_SQL = """
UPDATE pipeline_runs
SET 
    status = %s,
    edited_data = COALESCE(%s, edited_data),
    amendment_draft = COALESCE(%s, amendment_draft)
WHERE id = %s;
"""

COUNT_TOTAL_RUNS_SQL = """
SELECT COUNT(*) as total FROM pipeline_runs;
"""

COUNT_DECISIONS_GROUPED_SQL = """
SELECT decision, COUNT(*) as count FROM pipeline_runs GROUP BY decision;
"""

COUNT_STATUSES_GROUPED_SQL = """
SELECT status, COUNT(*) as count FROM pipeline_runs GROUP BY status;
"""

