-- Tableau reporting replica control plane.
--
-- This schema is downstream from the immutable DuckDB warehouse. Build-specific
-- data tables live in reporting_build_<snapshot> schemas. The `reporting` and
-- `source_detail` schemas contain only stable views switched transactionally
-- after a build passes validation.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS source_detail;

CREATE TABLE IF NOT EXISTS control.reporting_snapshot (
    snapshot_id             TEXT PRIMARY KEY,
    contract_version        INTEGER NOT NULL,
    scope_name              TEXT NOT NULL,
    scope_rule              TEXT NOT NULL,
    warehouse_release_id    TEXT NOT NULL,
    warehouse_sha256        TEXT NOT NULL,
    pipeline_code_commit    TEXT,
    build_schema            TEXT NOT NULL UNIQUE,
    status                  TEXT NOT NULL CHECK (
        status IN ('building', 'validated', 'active', 'superseded', 'failed')
    ),
    started_at              TIMESTAMPTZ NOT NULL,
    completed_at            TIMESTAMPTZ,
    published_at            TIMESTAMPTZ,
    previous_snapshot_id    TEXT REFERENCES control.reporting_snapshot(snapshot_id),
    table_row_counts        JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_results      JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_periods          JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_summary           TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_reporting_snapshot_one_active
    ON control.reporting_snapshot ((status))
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS control.model_catalog (
    snapshot_id             TEXT NOT NULL REFERENCES control.reporting_snapshot(snapshot_id),
    layer                   TEXT NOT NULL CHECK (layer IN ('reporting', 'source_detail')),
    model_name              TEXT NOT NULL,
    source_dataset_id       TEXT,
    source_tables           TEXT[] NOT NULL,
    declared_grain          TEXT NOT NULL,
    scope_rule              TEXT NOT NULL,
    source_period_semantics TEXT NOT NULL,
    attribution             TEXT NOT NULL,
    notes                   TEXT,
    row_count               BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, layer, model_name)
);

CREATE TABLE IF NOT EXISTS control.column_lineage (
    snapshot_id             TEXT NOT NULL REFERENCES control.reporting_snapshot(snapshot_id),
    layer                   TEXT NOT NULL CHECK (layer IN ('reporting', 'source_detail')),
    model_name              TEXT NOT NULL,
    model_column            TEXT NOT NULL,
    ordinal_position        INTEGER NOT NULL,
    source_dataset_id       TEXT NOT NULL,
    source_table            TEXT NOT NULL,
    source_column           TEXT NOT NULL,
    transformation          TEXT NOT NULL,
    declared_grain          TEXT NOT NULL,
    scope_rule              TEXT NOT NULL,
    source_period_semantics TEXT NOT NULL,
    is_derived              BOOLEAN NOT NULL DEFAULT FALSE,
    is_inferred             BOOLEAN NOT NULL DEFAULT FALSE,
    notes                   TEXT,
    PRIMARY KEY (snapshot_id, layer, model_name, model_column)
);

CREATE OR REPLACE VIEW control.active_reporting_snapshot AS
SELECT *
FROM control.reporting_snapshot
WHERE status = 'active';

COMMENT ON SCHEMA reporting IS
    'Certified California analytical models. Respect each view declared grain in control.model_catalog.';
COMMENT ON SCHEMA source_detail IS
    'California-scoped source-faithful evidence. Inspect and validate; do not assume cross-source join safety.';
COMMENT ON SCHEMA control IS
    'Reporting release provenance, model grain, column lineage, validation, and publication state.';
