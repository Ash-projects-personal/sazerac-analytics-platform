-- ═══════════════════════════════════════════════════════════════════════════
-- Sazerac Brand & Analytics Intelligence Platform
-- schema.sql — Star Schema DDL
-- ═══════════════════════════════════════════════════════════════════════════
-- Architecture: Bronze → Silver → Gold (Medallion Architecture)
-- Target:       SQLite (local dev) | DuckDB / Snowflake (cloud scale)
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION: dim_brand
-- Conformed dimension for the Sazerac brand portfolio.
-- SCD Type 1 (overwrite on change for this iteration).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_brand (
    brand_sk          INTEGER  PRIMARY KEY AUTOINCREMENT,   -- surrogate key
    brand_id          INTEGER  UNIQUE NOT NULL,             -- natural key from source
    brand_name        TEXT     NOT NULL,
    category          TEXT     NOT NULL,                    -- normalized category
    category_raw      TEXT,                                 -- original scraped category
    description       TEXT,
    description_short TEXT,                                 -- first 300 chars
    url               TEXT,
    is_flagship       INTEGER  DEFAULT 0    CHECK(is_flagship IN (0,1)),
    is_spirits        INTEGER  DEFAULT 1    CHECK(is_spirits IN (0,1)),
    has_description   INTEGER  DEFAULT 0    CHECK(has_description IN (0,1)),
    data_source       TEXT,
    processed_at      TEXT,                                 -- ISO 8601 UTC
    scraped_at        TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION: dim_location
-- Global presence of Sazerac distilleries, offices, and distribution centers.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_sk       INTEGER  PRIMARY KEY AUTOINCREMENT,
    location_id       INTEGER  UNIQUE NOT NULL,
    location_name     TEXT     NOT NULL,
    city              TEXT,
    state_province    TEXT,
    country           TEXT     NOT NULL,
    region            TEXT     NOT NULL,   -- Americas | Europe | APAC | EMEA | Other
    location_type     TEXT     NOT NULL,   -- Distillery | Headquarters | Office | Distribution Center
    latitude          REAL,
    longitude         REAL,
    year_established  INTEGER,
    employee_count    INTEGER  DEFAULT 0,
    processed_at      TEXT,
    scraped_at        TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT: fact_jobs
-- Job postings published by Sazerac. Grain = one row per job posting.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_jobs (
    job_sk            INTEGER  PRIMARY KEY AUTOINCREMENT,
    job_id            INTEGER  UNIQUE NOT NULL,
    job_title         TEXT     NOT NULL,
    department        TEXT,
    location          TEXT,
    state             TEXT,
    is_remote         INTEGER  DEFAULT 0  CHECK(is_remote IN (0,1)),
    is_hybrid         INTEGER  DEFAULT 0  CHECK(is_hybrid IN (0,1)),
    employment_type   TEXT     DEFAULT 'Full-Time',
    posted_date       TEXT,               -- ISO 8601 date
    seniority         TEXT,               -- Junior | Mid-Level | Senior | Manager | Senior Leadership
    skill_count       INTEGER  DEFAULT 0,
    skills_extracted  TEXT,               -- pipe-delimited skill list (ETL artifact)
    description       TEXT,
    processed_at      TEXT,
    scraped_at        TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT: fact_job_skills
-- Aggregated skill frequency across all job postings.
-- Grain = one row per unique skill.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_job_skills (
    skill_sk          INTEGER  PRIMARY KEY AUTOINCREMENT,
    skill             TEXT     NOT NULL UNIQUE,
    frequency         INTEGER  NOT NULL DEFAULT 0,
    pct_of_postings   REAL     DEFAULT 0.0,
    demand_tier       TEXT,   -- Tier 1 Must Have | Tier 2 Preferred | Tier 3 | Tier 4
    skill_category    TEXT,   -- Language | Cloud | BI | Data Engineering | Governance | Analytics
    processed_at      TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- GOVERNANCE: dq_log
-- Data quality check results. Supports data governance reporting.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dq_log (
    log_id        INTEGER  PRIMARY KEY AUTOINCREMENT,
    table_name    TEXT     NOT NULL,
    checked_at    TEXT     NOT NULL,
    total_rows    INTEGER,
    checks_passed INTEGER,
    checks_failed INTEGER,
    issues        TEXT     -- JSON array of failure messages
);

-- ─────────────────────────────────────────────────────────────────────────────
-- INDEXES
-- ─────────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_dim_brand_category     ON dim_brand (category);
CREATE INDEX IF NOT EXISTS idx_dim_brand_flagship     ON dim_brand (is_flagship);
CREATE INDEX IF NOT EXISTS idx_dim_location_region    ON dim_location (region);
CREATE INDEX IF NOT EXISTS idx_dim_location_country   ON dim_location (country);
CREATE INDEX IF NOT EXISTS idx_dim_location_type      ON dim_location (location_type);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_dept         ON fact_jobs (department);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_seniority    ON fact_jobs (seniority);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_remote       ON fact_jobs (is_remote);
CREATE INDEX IF NOT EXISTS idx_fact_skills_tier       ON fact_job_skills (demand_tier);
CREATE INDEX IF NOT EXISTS idx_fact_skills_category   ON fact_job_skills (skill_category);
