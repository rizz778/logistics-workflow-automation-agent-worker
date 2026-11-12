"""Centralized SQL query constants for PostgreSQL."""

INIT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id TEXT UNIQUE,
    filename TEXT NOT NULL,
    total_files_expected INT NOT NULL DEFAULT 1,
    uploaded_files_count INT NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS shipment_files (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id TEXT NOT NULL,
    run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    s3_key TEXT NOT NULL UNIQUE,
    file_hash TEXT,
    content_type TEXT,
    status TEXT NOT NULL DEFAULT 'pending_upload', -- 'pending_upload' | 'uploaded'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_shipment_id ON pipeline_runs (shipment_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_decision ON pipeline_runs (decision);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_source ON pipeline_runs (source);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_created_at ON pipeline_runs (upload_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_cross_doc ON pipeline_runs USING GIN (cross_doc_results);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_extracted ON pipeline_runs USING GIN (extracted_data);

CREATE INDEX IF NOT EXISTS idx_shipment_files_shipment_id ON shipment_files (shipment_id);
CREATE INDEX IF NOT EXISTS idx_shipment_files_s3_key ON shipment_files (s3_key);
"""

HEALTH_CHECK_SQL = "SELECT 1;"

INSERT_QUEUED_RUN_SQL = """
INSERT INTO pipeline_runs (
    shipment_id, filename, total_files_expected, uploaded_files_count,
    status, decision, source, email_sender, email_metadata_json, is_cached
) VALUES (%s, %s, %s, 0, %s, 'pending', %s, %s, %s, FALSE)
RETURNING id;
"""

INSERT_SHIPMENT_FILE_SQL = """
INSERT INTO shipment_files (
    shipment_id, run_id, filename, s3_key, file_hash, content_type, status
) VALUES (%s, %s, %s, %s, %s, %s, 'pending_upload');
"""

INSERT_CACHED_RUN_SQL = """
INSERT INTO pipeline_runs (
    shipment_id, filename, total_files_expected, uploaded_files_count,
    extracted_data, validation_results, decision, decision_reason,
    amendment_draft, status, cross_doc_results, source, email_sender,
    email_metadata_json, is_cached
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
RETURNING id;
"""

SELECT_RUN_BY_ID_SQL = """
SELECT * FROM pipeline_runs WHERE id = %s;
"""

SELECT_RUN_BY_SHIPMENT_ID_SQL = """
SELECT * FROM pipeline_runs WHERE shipment_id = %s;
"""

SELECT_SHIPMENT_FILES_SQL = """
SELECT * FROM shipment_files WHERE shipment_id = %s ORDER BY id ASC;
"""

MARK_FILE_UPLOADED_SQL = """
UPDATE shipment_files 
SET status = 'uploaded' 
WHERE shipment_id = %s AND s3_key = %s;
"""

CHECK_AND_TRANSITION_SHIPMENT_SQL = """
UPDATE pipeline_runs
SET 
    uploaded_files_count = (SELECT COUNT(*) FROM shipment_files WHERE shipment_id = %s AND status = 'uploaded'),
    status = CASE 
        WHEN (SELECT COUNT(*) FROM shipment_files WHERE shipment_id = %s AND status = 'uploaded') = total_files_expected 
        THEN 'processing' 
        ELSE status 
    END
WHERE shipment_id = %s AND status = 'pending_upload'
RETURNING status, uploaded_files_count, total_files_expected;
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
