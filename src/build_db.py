"""
build_db.py
-----------
Loads processed CSVs into a local SQLite database and creates
analytical views on top. I chose SQLite for portability -- zero config,
works in CI, and the dashboard can query it directly via pandas.
In a real prod setup this would point at Snowflake or Azure SQL.
"""

import logging
import os
import sqlite3
import pandas as pd

# ── logging ────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/build_db.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DB_PATH = "data/sazerac_analytics.db"
PROCESSED_DIR = "data/processed"
MARTS_DIR = "data/marts"

# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

DDL_STATEMENTS = """
-- ─────────────────────────────────────────────────────────────────────────────
-- DIM_BRAND: Sazerac brand portfolio dimension
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_brand (
    brand_sk          INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id          INTEGER UNIQUE NOT NULL,
    brand_name        TEXT    NOT NULL,
    category          TEXT    NOT NULL,         -- normalized category
    category_raw      TEXT,                     -- original category as scraped
    description       TEXT,
    description_short TEXT,
    url               TEXT,
    is_flagship       INTEGER DEFAULT 0,        -- 1=flagship, 0=standard
    is_spirits        INTEGER DEFAULT 1,        -- 1=spirits, 0=non-spirits
    has_description   INTEGER DEFAULT 0,
    data_source       TEXT,
    processed_at      TEXT,
    scraped_at        TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DIM_LOCATION: Global presence dimension
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_sk        INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id        INTEGER UNIQUE NOT NULL,
    location_name      TEXT    NOT NULL,
    city               TEXT,
    state_province     TEXT,
    country            TEXT    NOT NULL,
    region             TEXT    NOT NULL,        -- Americas / Europe / APAC / EMEA
    location_type      TEXT    NOT NULL,        -- Distillery / HQ / Office / Distribution
    latitude           REAL,
    longitude          REAL,
    year_established   INTEGER,
    employee_count     INTEGER DEFAULT 0,
    processed_at       TEXT,
    scraped_at         TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT_JOBS: Job postings fact table
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_jobs (
    job_sk            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id            INTEGER UNIQUE NOT NULL,
    job_title         TEXT    NOT NULL,
    department        TEXT,
    location          TEXT,
    state             TEXT,
    is_remote         INTEGER DEFAULT 0,
    is_hybrid         INTEGER DEFAULT 0,
    employment_type   TEXT    DEFAULT 'Full-Time',
    posted_date       TEXT,
    seniority         TEXT,
    skill_count       INTEGER DEFAULT 0,
    skills_extracted  TEXT,                     -- pipe-delimited skill list
    description       TEXT,
    processed_at      TEXT,
    scraped_at        TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT_JOB_SKILLS: Skill frequency aggregation fact table
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_job_skills (
    skill_sk          INTEGER PRIMARY KEY AUTOINCREMENT,
    skill             TEXT    NOT NULL UNIQUE,
    frequency         INTEGER NOT NULL DEFAULT 0,
    pct_of_postings   REAL    DEFAULT 0.0,
    demand_tier       TEXT,
    skill_category    TEXT,
    processed_at      TEXT
);


-- ─────────────────────────────────────────────────────────────────────────────
-- FACT_DEPLETIONS: Monthly case volume (VIP/iDig-style)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_depletions (
    depletion_sk       INTEGER PRIMARY KEY AUTOINCREMENT,
    year               INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    month_label        TEXT    NOT NULL,
    brand              TEXT    NOT NULL,
    category           TEXT,
    price_tier         TEXT,
    state_code         TEXT    NOT NULL,
    state_name         TEXT    NOT NULL,
    is_control_state   INTEGER DEFAULT 0,
    channel_on_premise  INTEGER DEFAULT 0,
    channel_off_premise INTEGER DEFAULT 0,
    total_cases        INTEGER DEFAULT 0
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT_MARKET_SHARE: Nielsen/IRI-style syndicated metrics
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_market_share (
    ms_sk                INTEGER PRIMARY KEY AUTOINCREMENT,
    brand                TEXT NOT NULL,
    category             TEXT,
    volume_share_pct     REAL DEFAULT 0.0,
    numeric_distribution REAL DEFAULT 0.0,
    velocity_per_point   REAL DEFAULT 0.0,
    price_tier           TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DQ_LOG: Data quality check results (governance)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dq_log (
    log_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name    TEXT,
    checked_at    TEXT,
    total_rows    INTEGER,
    checks_passed INTEGER,
    checks_failed INTEGER,
    issues        TEXT
);
"""

# ══════════════════════════════════════════════════════════════════════════════
# VIEWS
# ══════════════════════════════════════════════════════════════════════════════

VIEW_STATEMENTS = """
-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: brands_by_category — portfolio distribution analytics
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS brands_by_category;
CREATE VIEW brands_by_category AS
SELECT
    category,
    COUNT(*)                                        AS brand_count,
    SUM(is_flagship)                                AS flagship_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_portfolio,
    MAX(is_spirits)                                 AS is_spirits_category,
    GROUP_CONCAT(brand_name, ', ')                  AS brands_in_category
FROM dim_brand
GROUP BY category
ORDER BY brand_count DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: locations_by_region — geographic presence analytics
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS locations_by_region;
CREATE VIEW locations_by_region AS
SELECT
    region,
    location_type,
    COUNT(*)                AS location_count,
    SUM(employee_count)     AS total_employees,
    COUNT(DISTINCT country) AS country_count,
    GROUP_CONCAT(DISTINCT country) AS countries
FROM dim_location
GROUP BY region, location_type
ORDER BY region, location_count DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: job_skill_frequency — top required skills
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS job_skill_frequency;
CREATE VIEW job_skill_frequency AS
SELECT
    skill,
    skill_category,
    frequency,
    pct_of_postings,
    demand_tier,
    RANK() OVER (ORDER BY frequency DESC) AS skill_rank
FROM fact_job_skills
ORDER BY frequency DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: top_requested_tools — Tier 1 & 2 skills only
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS top_requested_tools;
CREATE VIEW top_requested_tools AS
SELECT
    skill,
    skill_category,
    frequency,
    pct_of_postings,
    demand_tier
FROM fact_job_skills
WHERE demand_tier IN ('Tier 1 — Must Have', 'Tier 2 — Strongly Preferred')
ORDER BY frequency DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: jobs_by_department — headcount distribution
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS jobs_by_department;
CREATE VIEW jobs_by_department AS
SELECT
    department,
    COUNT(*)                                         AS job_count,
    SUM(is_remote)                                   AS remote_count,
    SUM(is_hybrid)                                   AS hybrid_count,
    ROUND(AVG(skill_count), 1)                       AS avg_skills_required,
    GROUP_CONCAT(seniority, ' | ')                   AS seniority_mix,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_openings
FROM fact_jobs
GROUP BY department
ORDER BY job_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: depletion_trend — monthly totals per brand (last 12 months)
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS depletion_trend;
CREATE VIEW depletion_trend AS
SELECT
    brand,
    month_label,
    year,
    month,
    price_tier,
    SUM(total_cases)         AS total_cases,
    SUM(channel_on_premise)  AS on_premise_cases,
    SUM(channel_off_premise) AS off_premise_cases
FROM fact_depletions
GROUP BY brand, year, month, month_label, price_tier
ORDER BY brand, year, month;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: control_state_summary — open vs control state performance
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS control_state_summary;
CREATE VIEW control_state_summary AS
SELECT
    brand,
    CASE WHEN is_control_state = 1 THEN 'Control State' ELSE 'Open State' END AS state_type,
    SUM(total_cases)  AS total_cases,
    COUNT(DISTINCT state_code) AS state_count,
    ROUND(AVG(total_cases), 0) AS avg_cases_per_state
FROM fact_depletions
GROUP BY brand, is_control_state
ORDER BY brand, is_control_state;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: portfolio_summary — executive KPI view
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS portfolio_summary;
CREATE VIEW portfolio_summary AS
SELECT 'Total Brands'            AS kpi, COUNT(*)               AS value, 'Portfolio'      AS category FROM dim_brand
UNION ALL
SELECT 'Flagship Brands',              SUM(is_flagship),              'Portfolio'      FROM dim_brand
UNION ALL
SELECT 'Brand Categories',             COUNT(DISTINCT category),      'Portfolio'      FROM dim_brand
UNION ALL
SELECT 'Total Locations',              COUNT(*),                      'Global'         FROM dim_location
UNION ALL
SELECT 'Countries Served',             COUNT(DISTINCT country),       'Global'         FROM dim_location
UNION ALL
SELECT 'Total Employees (Est.)',        SUM(employee_count),           'Global'         FROM dim_location
UNION ALL
SELECT 'Open Job Postings',            COUNT(*),                      'Talent'         FROM fact_jobs
UNION ALL
SELECT 'Remote Positions',             SUM(is_remote),                'Talent'         FROM fact_jobs
UNION ALL
SELECT 'Unique Skills Tracked',        COUNT(*),                      'Talent'         FROM fact_job_skills;
"""

# ══════════════════════════════════════════════════════════════════════════════
# LOAD FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    log.info("Creating schema...")
    for stmt in DDL_STATEMENTS.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()
    log.info("Schema created successfully")


def create_views(conn: sqlite3.Connection) -> None:
    log.info("Creating analytical views...")
    for stmt in VIEW_STATEMENTS.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                conn.execute(stmt)
            except sqlite3.Error as e:
                log.warning("View creation issue: %s", e)
    conn.commit()
    log.info("All views created")


def load_brands(conn: sqlite3.Connection) -> int:
    path = f"{PROCESSED_DIR}/brands_clean.csv"
    df = pd.read_csv(path)
    log.info("Loading %d brands into dim_brand...", len(df))

    conn.execute("DELETE FROM dim_brand")
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT OR REPLACE INTO dim_brand
            (brand_id, brand_name, category, category_raw, description,
             description_short, url, is_flagship, is_spirits, has_description,
             data_source, processed_at, scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            (
                int(row.get("brand_id", 0)),
                str(row.get("brand_name", "")),
                str(row.get("category_clean", row.get("category", ""))),
                str(row.get("category", "")),
                str(row.get("description", "")),
                str(row.get("description_short", "")),
                str(row.get("url", "")),
                int(row.get("is_flagship", 0)),
                int(row.get("is_spirits", 1)),
                int(row.get("has_description", 0)),
                str(row.get("data_source", "")),
                str(row.get("processed_at", "")),
                str(row.get("scraped_at", "")),
            ),
        )
    conn.commit()
    log.info("dim_brand loaded: %d records", len(df))
    return len(df)


def load_locations(conn: sqlite3.Connection) -> int:
    path = f"{PROCESSED_DIR}/locations_clean.csv"
    df = pd.read_csv(path)
    log.info("Loading %d locations into dim_location...", len(df))

    conn.execute("DELETE FROM dim_location")
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT OR REPLACE INTO dim_location
            (location_id, location_name, city, state_province, country, region,
             location_type, latitude, longitude, year_established, employee_count,
             processed_at, scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            (
                int(row.get("location_id", 0)),
                str(row.get("location_name", "")),
                str(row.get("city", "")),
                str(row.get("state_province", "")),
                str(row.get("country", "")),
                str(row.get("region", "")),
                str(row.get("location_type_clean", row.get("location_type", ""))),
                float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
                float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
                int(row["year_established"]) if pd.notna(row.get("year_established")) else None,
                int(row.get("employee_count", 0)),
                str(row.get("processed_at", "")),
                str(row.get("scraped_at", "")),
            ),
        )
    conn.commit()
    log.info("dim_location loaded: %d records", len(df))
    return len(df)


def load_jobs(conn: sqlite3.Connection) -> int:
    path = f"{PROCESSED_DIR}/jobs_clean.csv"
    df = pd.read_csv(path)
    log.info("Loading %d jobs into fact_jobs...", len(df))

    conn.execute("DELETE FROM fact_jobs")
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_jobs
            (job_id, job_title, department, location, state, is_remote, is_hybrid,
             employment_type, posted_date, seniority, skill_count, skills_extracted,
             description, processed_at, scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            (
                int(row.get("job_id", 0)),
                str(row.get("job_title", "")),
                str(row.get("department", "")),
                str(row.get("location", "")),
                str(row.get("state", "")) if pd.notna(row.get("state")) else "",
                int(row.get("is_remote", 0)),
                int(row.get("is_hybrid", 0)),
                str(row.get("employment_type", "Full-Time")),
                str(row.get("posted_date", "")),
                str(row.get("seniority", "")),
                int(row.get("skill_count", 0)),
                str(row.get("skills_extracted", "")),
                str(row.get("description", ""))[:2000],  # cap at 2000 chars
                str(row.get("processed_at", "")),
                str(row.get("scraped_at", "")),
            ),
        )
    conn.commit()
    log.info("fact_jobs loaded: %d records", len(df))
    return len(df)


def load_job_skills(conn: sqlite3.Connection) -> int:
    path = f"{PROCESSED_DIR}/job_skills_clean.csv"
    df = pd.read_csv(path)
    log.info("Loading %d skill records into fact_job_skills...", len(df))

    conn.execute("DELETE FROM fact_job_skills")
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_job_skills
            (skill, frequency, pct_of_postings, demand_tier, skill_category, processed_at)
            VALUES (?,?,?,?,?,?)
        """,
            (
                str(row.get("skill", "")),
                int(row.get("frequency", 0)),
                float(row.get("pct_of_postings", 0.0)),
                str(row.get("demand_tier", "")),
                str(row.get("skill_category", "")),
                str(row.get("processed_at", "")),
            ),
        )
    conn.commit()
    log.info("fact_job_skills loaded: %d records", len(df))
    return len(df)


def validate_db(conn: sqlite3.Connection) -> None:
    """Run post-load validation queries."""
    log.info("Running post-load DB validation...")
    checks = [
        ("dim_brand row count", "SELECT COUNT(*) FROM dim_brand"),
        ("dim_location row count", "SELECT COUNT(*) FROM dim_location"),
        ("fact_jobs row count", "SELECT COUNT(*) FROM fact_jobs"),
        ("fact_job_skills row count", "SELECT COUNT(*) FROM fact_job_skills"),
        (
            "Brands by category (top 3)",
            "SELECT category, brand_count FROM brands_by_category LIMIT 3",
        ),
        ("Locations by region", "SELECT region, location_count FROM locations_by_region LIMIT 5"),
        ("Top 5 skills", "SELECT skill, frequency FROM job_skill_frequency LIMIT 5"),
        ("KPI summary", "SELECT kpi, value FROM portfolio_summary LIMIT 5"),
    ]
    for label, sql in checks:
        try:
            rows = conn.execute(sql).fetchall()
            log.info("[VALIDATE] %s → %s", label, rows)
        except Exception as e:
            log.warning("[VALIDATE FAIL] %s → %s", label, e)


def export_views_to_csv(conn: sqlite3.Connection) -> None:
    """Export key views to marts CSVs for reporting."""
    exports = {
        "brands_by_category": "SELECT * FROM brands_by_category",
        "locations_by_region": "SELECT * FROM locations_by_region",
        "job_skill_frequency": "SELECT * FROM job_skill_frequency",
        "top_requested_tools": "SELECT * FROM top_requested_tools",
        "jobs_by_department": "SELECT * FROM jobs_by_department",
        "portfolio_summary": "SELECT * FROM portfolio_summary",
        "depletion_trend": "SELECT * FROM depletion_trend",
        "control_state_summary": "SELECT * FROM control_state_summary",
        "market_share": "SELECT * FROM fact_market_share",
    }
    os.makedirs(MARTS_DIR, exist_ok=True)
    for name, sql in exports.items():
        df = pd.read_sql_query(sql, conn)
        path = f"{MARTS_DIR}/{name}.csv"
        df.to_csv(path, index=False)
        log.info("Exported view '%s' → %s (%d rows)", name, path, len(df))


def load_depletions(conn: sqlite3.Connection) -> int:
    """Load monthly depletion data from raw CSV into fact_depletions."""
    path = "data/raw/depletions_raw.csv"
    if not os.path.exists(path):
        log.warning("Depletions CSV not found, skipping: %s", path)
        return 0
    df = pd.read_csv(path)
    log.info("Loading %d depletion records into fact_depletions...", len(df))
    conn.execute("DELETE FROM fact_depletions")
    for _, row in df.iterrows():
        conn.execute(
            """INSERT INTO fact_depletions
               (year, month, month_label, brand, category, price_tier,
                state_code, state_name, is_control_state,
                channel_on_premise, channel_off_premise, total_cases)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                int(row["year"]),
                int(row["month"]),
                str(row["month_label"]),
                str(row["brand"]),
                str(row["category"]),
                str(row["price_tier"]),
                str(row["state_code"]),
                str(row["state_name"]),
                int(row["is_control_state"]),
                int(row["channel_on_premise"]),
                int(row["channel_off_premise"]),
                int(row["total_cases"]),
            ),
        )
    conn.commit()
    log.info("fact_depletions loaded: %d records", len(df))
    return len(df)


def load_market_share(conn: sqlite3.Connection) -> int:
    """Load Nielsen/IRI-style market share data into fact_market_share."""
    path = "data/raw/market_share_raw.csv"
    if not os.path.exists(path):
        log.warning("Market share CSV not found, skipping: %s", path)
        return 0
    df = pd.read_csv(path)
    log.info("Loading %d market share records into fact_market_share...", len(df))
    conn.execute("DELETE FROM fact_market_share")
    for _, row in df.iterrows():
        conn.execute(
            """INSERT INTO fact_market_share
               (brand, category, volume_share_pct, numeric_distribution,
                velocity_per_point, price_tier)
               VALUES (?,?,?,?,?,?)""",
            (
                str(row["brand"]),
                str(row["category"]),
                float(row["volume_share_pct"]),
                float(row["numeric_distribution"]),
                float(row["velocity_per_point"]),
                str(row["price_tier"]),
            ),
        )
    conn.commit()
    log.info("fact_market_share loaded: %d records", len(df))
    return len(df)


def main():
    log.info("========== Database Builder Starting ==========")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = get_connection()
    try:
        create_schema(conn)
        load_brands(conn)
        load_locations(conn)
        load_jobs(conn)
        load_job_skills(conn)
        load_depletions(conn)
        load_market_share(conn)
        create_views(conn)
        validate_db(conn)
        export_views_to_csv(conn)
    finally:
        conn.close()

    log.info("========== Database Builder Complete ==========")
    log.info("Database: %s", os.path.abspath(DB_PATH))


if __name__ == "__main__":
    main()
