-- include/sql/common/audit_tables.sql
-- Tables shared by BOTH DAGs. dag_id tells you which pipeline wrote the row.

-- Part D: quality scorecard. One row per check, per file, per run.
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,     -- Airflow run id, e.g. scheduled__2026-10-13T03:30:00+00:00
    dag_id        VARCHAR,     -- nammamart_etl_sambit or nammamart_elt_sambit
    source_file   VARCHAR,     -- orders / payments / customers / products / stores / orders_increment
    check_name    VARCHAR,     -- <source_file>.<check>, e.g. orders.quantity_positive
    dimension     VARCHAR,     -- Completeness / Uniqueness / Validity / Accuracy / Consistency / Timeliness
    severity      VARCHAR,     -- Critical (quarantine) / Warning (load + log)
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,     -- PASS / FAIL / WARN
    checked_at    TIMESTAMP    -- UTC
);

-- Part F: run audit. One row per DAG run, written even when the run fails.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,   -- UTC
    end_time          TIMESTAMP,   -- UTC
    rows_extracted    BIGINT,      -- rows read from all source files
    rows_loaded       BIGINT,      -- rows that reached the reporting layer
    rows_quarantined  BIGINT,      -- rows rejected by a Critical check
    status            VARCHAR      -- success / failed
);
