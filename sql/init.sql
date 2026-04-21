-- ============================================================
-- Geodata Quality Pipeline — PostGIS Schema
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ── INGESTED DATASETS ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.datasets (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(100) NOT NULL,
    source_name     TEXT,
    domain          VARCHAR(10),
    feature_count   INTEGER,
    geometry_type   VARCHAR(50),
    crs             VARCHAR(50),
    date_ingested   TIMESTAMP DEFAULT NOW(),
    pipeline_version VARCHAR(20)
);

-- ── QUALITY REPORTS ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.quality_reports (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(100) NOT NULL,
    check_timestamp TIMESTAMP DEFAULT NOW(),
    overall_score   NUMERIC(5,3),
    status          VARCHAR(10),
    feature_count   INTEGER,
    report_json     JSONB,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_reports_source
    ON pipeline.quality_reports(source_id);
CREATE INDEX IF NOT EXISTS idx_quality_reports_timestamp
    ON pipeline.quality_reports(check_timestamp);

-- ── REFERENCE GEODATA (DD01) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS geodata.admin_boundaries (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(100),
    name            TEXT,
    admin_level     INTEGER,
    country_iso3    CHAR(3),
    geom            GEOMETRY(MultiPolygon, 4326),
    date_ingested   TIMESTAMP DEFAULT NOW(),
    source_name     TEXT,
    pipeline_version VARCHAR(20)
);
CREATE INDEX IF NOT EXISTS idx_admin_boundaries_geom
    ON geodata.admin_boundaries USING GIST(geom);

-- ── HEALTH FACILITIES (DD03) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS geodata.health_facilities (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(100),
    osm_id          BIGINT,
    name            TEXT,
    amenity         VARCHAR(100),
    operator        VARCHAR(200),
    geom            GEOMETRY(Point, 4326),
    date_ingested   TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_health_facilities_geom
    ON geodata.health_facilities USING GIST(geom);

-- ── DISASTER EVENTS (DD05) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS geodata.disaster_events (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(100),
    event_id        VARCHAR(100),
    name            TEXT,
    status          VARCHAR(50),
    disaster_type   VARCHAR(100),
    country         VARCHAR(100),
    date_event      TIMESTAMP,
    geom            GEOMETRY(Point, 4326),
    date_ingested   TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_disaster_events_geom
    ON geodata.disaster_events USING GIST(geom);

-- ── QUALITY CHECK LOG ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.quality_checks_log (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(100),
    check_id        VARCHAR(20),
    check_name      TEXT,
    dimension       VARCHAR(50),
    status          VARCHAR(10),
    score           NUMERIC(5,3),
    affected_count  INTEGER,
    total_count     INTEGER,
    severity        VARCHAR(20),
    details         TEXT,
    check_timestamp TIMESTAMP DEFAULT NOW()
);
