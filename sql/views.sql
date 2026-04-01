-- ═══════════════════════════════════════════════════════════════════════════
-- Sazerac Brand & Analytics Intelligence Platform
-- views.sql — Analytics Layer (Silver → Gold reporting views)
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 1: brands_by_category
-- Portfolio distribution by brand category — drives pie/bar charts
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS brands_by_category;
CREATE VIEW brands_by_category AS
SELECT
    category,
    COUNT(*)                                                   AS brand_count,
    SUM(is_flagship)                                           AS flagship_count,
    COUNT(*) - SUM(is_flagship)                                AS non_flagship_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)        AS pct_of_portfolio,
    CASE WHEN MAX(is_spirits) = 1 THEN 'Spirits' ELSE 'Non-Spirits' END AS category_type,
    GROUP_CONCAT(
        CASE WHEN is_flagship = 1 THEN brand_name END, ', '
    )                                                          AS flagship_brands
FROM dim_brand
GROUP BY category
ORDER BY brand_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 2: locations_by_region
-- Global footprint aggregated by region and location type — drives map/bar
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS locations_by_region;
CREATE VIEW locations_by_region AS
SELECT
    region,
    location_type,
    COUNT(*)                                                   AS location_count,
    SUM(employee_count)                                        AS total_employees,
    ROUND(AVG(employee_count), 0)                              AS avg_employees_per_site,
    COUNT(DISTINCT country)                                    AS country_count,
    MIN(year_established)                                      AS oldest_site_year,
    MAX(year_established)                                      AS newest_site_year,
    GROUP_CONCAT(DISTINCT country)                             AS countries
FROM dim_location
GROUP BY region, location_type
ORDER BY region, location_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 3: job_skill_frequency
-- Frequency of skills across all job postings — drives bar/ranking charts
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS job_skill_frequency;
CREATE VIEW job_skill_frequency AS
SELECT
    skill,
    skill_category,
    frequency,
    pct_of_postings,
    demand_tier,
    RANK() OVER (ORDER BY frequency DESC)                      AS skill_rank,
    RANK() OVER (PARTITION BY skill_category ORDER BY frequency DESC) AS rank_within_category
FROM fact_job_skills
ORDER BY frequency DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 4: top_requested_tools
-- Must-have and strongly preferred skills (Tier 1 & 2 only)
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS top_requested_tools;
CREATE VIEW top_requested_tools AS
SELECT
    skill,
    skill_category,
    frequency,
    pct_of_postings,
    demand_tier,
    CASE
        WHEN pct_of_postings >= 80 THEN '🔴 Critical'
        WHEN pct_of_postings >= 60 THEN '🟠 High Demand'
        WHEN pct_of_postings >= 40 THEN '🟡 In Demand'
        ELSE                             '🟢 Preferred'
    END                                                        AS urgency_label
FROM fact_job_skills
WHERE demand_tier IN ('Tier 1 — Must Have', 'Tier 2 — Strongly Preferred')
ORDER BY frequency DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 5: jobs_by_department
-- Job openings by department — headcount & composition analytics
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS jobs_by_department;
CREATE VIEW jobs_by_department AS
SELECT
    department,
    COUNT(*)                                                   AS job_count,
    SUM(is_remote)                                             AS remote_count,
    SUM(is_hybrid)                                             AS hybrid_count,
    COUNT(*) - SUM(is_remote) - SUM(is_hybrid)                 AS on_site_count,
    ROUND(AVG(skill_count), 1)                                 AS avg_skills_required,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)        AS pct_of_openings
FROM fact_jobs
GROUP BY department
ORDER BY job_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 6: portfolio_summary (Executive KPI view)
-- Single-row KPIs for the executive overview dashboard page
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS portfolio_summary;
CREATE VIEW portfolio_summary AS
SELECT 'Total Brands'             AS kpi, CAST(COUNT(*) AS TEXT)              AS value, 'Portfolio'  AS category FROM dim_brand
UNION ALL
SELECT 'Flagship Brands',               CAST(SUM(is_flagship) AS TEXT),           'Portfolio'       FROM dim_brand
UNION ALL
SELECT 'Brand Categories',              CAST(COUNT(DISTINCT category) AS TEXT),    'Portfolio'       FROM dim_brand
UNION ALL
SELECT 'Spirits Brands',                CAST(SUM(is_spirits) AS TEXT),             'Portfolio'       FROM dim_brand
UNION ALL
SELECT 'Total Locations',               CAST(COUNT(*) AS TEXT),                    'Global Presence' FROM dim_location
UNION ALL
SELECT 'Countries Served',              CAST(COUNT(DISTINCT country) AS TEXT),     'Global Presence' FROM dim_location
UNION ALL
SELECT 'Global Employees (Est.)',       CAST(SUM(employee_count) AS TEXT),         'Global Presence' FROM dim_location
UNION ALL
SELECT 'US Locations',                  CAST(SUM(CASE WHEN country = "United States" THEN 1 ELSE 0 END) AS TEXT), 'Global Presence' FROM dim_location
UNION ALL
SELECT 'International Locations',       CAST(SUM(CASE WHEN country != "United States" THEN 1 ELSE 0 END) AS TEXT), 'Global Presence' FROM dim_location
UNION ALL
SELECT 'Open Job Postings',             CAST(COUNT(*) AS TEXT),                    'Talent'          FROM fact_jobs
UNION ALL
SELECT 'Remote Positions',              CAST(SUM(is_remote) AS TEXT),              'Talent'          FROM fact_jobs
UNION ALL
SELECT 'Unique Skills Tracked',         CAST(COUNT(*) AS TEXT),                    'Talent'          FROM fact_job_skills
UNION ALL
SELECT 'Tier-1 Must-Have Skills',       CAST(SUM(CASE WHEN demand_tier = "Tier 1 — Must Have" THEN 1 ELSE 0 END) AS TEXT), 'Talent' FROM fact_job_skills;


-- ═══════════════════════════════════════════════════════════════════════════
-- AD HOC ANALYTICAL QUERIES (run manually for exploration)
-- ═══════════════════════════════════════════════════════════════════════════

-- Q1: What percentage of Sazerac brands are flagship products?
-- SELECT
--     ROUND(SUM(is_flagship) * 100.0 / COUNT(*), 1) AS flagship_pct,
--     COUNT(*) AS total_brands
-- FROM dim_brand;

-- Q2: Which region has the most employee headcount?
-- SELECT region, SUM(employee_count) AS total_employees
-- FROM dim_location
-- GROUP BY region
-- ORDER BY total_employees DESC;

-- Q3: What is the most in-demand technical skill across all job postings?
-- SELECT skill, frequency, pct_of_postings
-- FROM fact_job_skills
-- ORDER BY frequency DESC
-- LIMIT 1;

-- Q4: How many jobs require both SQL and Python?
-- SELECT COUNT(*) AS jobs_requiring_sql_and_python
-- FROM fact_jobs
-- WHERE skills_extracted LIKE '%SQL%'
--   AND skills_extracted LIKE '%Python%';

-- Q5: What fraction of Sazerac job postings are fully remote?
-- SELECT
--     ROUND(SUM(is_remote) * 100.0 / COUNT(*), 1) AS pct_remote
-- FROM fact_jobs;

-- Q6: Which brand categories have only one brand? (concentration risk)
-- SELECT category, brand_count
-- FROM brands_by_category
-- WHERE brand_count = 1;

-- Q7: What is the cloud platform adoption in job postings?
-- SELECT skill, frequency, pct_of_postings
-- FROM fact_job_skills
-- WHERE skill_category = 'Cloud / Platform'
-- ORDER BY frequency DESC;
