# 🥃 Sazerac Brand & Analytics Intelligence Platform

> **A production-style, end-to-end data analytics portfolio project demonstrating:**
> SQL · ETL/ELT · Dimensional Modeling · Data Pipelines · Power BI-Style Dashboards · Data Quality & Governance

---

## 📌 Project Overview

The **Sazerac Brand & Analytics Intelligence Platform** is a complete enterprise-grade data engineering and analytics project built around publicly available data from the Sazerac Company — one of the largest privately held distilled spirits companies in the United States, home to Buffalo Trace, Blanton's, Fireball, Pappy Van Winkle, and dozens of other iconic brands.

This project simulates the type of work a Senior Data Engineer or Analytics Engineer would do when onboarding at a CPG (Consumer Packaged Goods) / Spirits company:
- **Ingest** raw data from web sources
- **Model** it into a clean, queryable dimensional warehouse
- **Analyze** it with SQL views and Python aggregations
- **Report** it through an interactive, Power BI-style dashboard

---

## 🎯 Business Problem

Sazerac operates a portfolio of **50+ brands** across multiple categories (Bourbon, Vodka, Rum, Gin, Tequila, Brandy), with a **global presence across 11+ countries** and an active talent acquisition pipeline. Business stakeholders need answers to:

| Business Question | Analytic Solution |
|---|---|
| How is our brand portfolio distributed by category? | `brands_by_category` SQL view |
| Where are our key operations globally? | `locations_by_region` view + map visualization |
| What technical skills are we hiring for? | `job_skill_frequency` view + NLP extraction |
| What are the must-have tools in data roles? | `top_requested_tools` view |
| What is our executive KPI scorecard? | `portfolio_summary` view |

---

## 🏗️ Architecture — Medallion Pattern (Bronze → Silver → Gold)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE ARCHITECTURE                              │
│                                                                             │
│  🌐 Sources          🔶 Bronze           🔷 Silver          🏆 Gold          │
│  ─────────────       ─────────────────   ──────────────────  ──────────── │
│  sazerac.com    ──►  data/raw/           data/processed/     SQLite DB     │
│  (web scrape         brands_raw.csv      brands_clean.csv    ──────────── │
│   or mock)           locations_raw.csv   locations_clean.csv dim_brand     │
│                      jobs_raw.csv        jobs_clean.csv      dim_location  │
│                      job_skills.csv      job_skills_clean    fact_jobs     │
│                                                              fact_jobskills│
│                                                                             │
│  📊 Analytics Layer (Gold → Reporting)                                      │
│  ──────────────────────────────────────────────────────────────────────── │
│  SQL Views:                          Dashboard Exports:                     │
│  brands_by_category                  category_distribution.csv             │
│  locations_by_region                 geographic_map.csv                    │
│  job_skill_frequency                 skill_demand.csv                      │
│  top_requested_tools                 region_summary.csv                    │
│  portfolio_summary                   sazerac_dashboard.html ◄──────────── │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
sazerac_analytics/
│
├── data/
│   ├── raw/                        # 🔶 Bronze — raw scraped/mock data
│   │   ├── brands_raw.csv
│   │   ├── locations_raw.csv
│   │   ├── jobs_raw.csv
│   │   ├── job_skills.csv
│   │   └── job_skill_mapping.csv
│   │
│   ├── processed/                  # 🔷 Silver — cleaned & validated
│   │   ├── brands_clean.csv
│   │   ├── locations_clean.csv
│   │   ├── jobs_clean.csv
│   │   └── job_skills_clean.csv
│   │
│   ├── marts/                      # 🏆 Gold — aggregated star-schema views
│   │   ├── brands_by_category.csv
│   │   ├── locations_by_region.csv
│   │   ├── skill_frequency.csv
│   │   ├── top_requested_tools.csv
│   │   ├── jobs_by_department.csv
│   │   └── portfolio_summary.csv
│   │
│   └── sazerac_analytics.db        # SQLite analytical database
│
├── src/
│   ├── scrape_brands.py            # Data collection — brand portfolio
│   ├── scrape_locations.py         # Data collection — global locations
│   ├── scrape_jobs.py              # Data collection — job postings + NLP
│   ├── process_data.py             # ETL — Bronze → Silver → Gold
│   ├── build_db.py                 # Data warehouse — SQLite + views
│   └── build_dashboard.py          # Dashboard HTML generator
│
├── sql/
│   ├── schema.sql                  # DDL — star schema table definitions
│   └── views.sql                   # Analytics layer — SQL views + ad hoc queries
│
├── dashboard/
│   └── sazerac_dashboard.html      # Standalone Power BI–style dashboard
│
├── dashboard_exports/              # CSVs for BI tool import
│   ├── category_distribution.csv
│   ├── geographic_map.csv
│   ├── skill_demand.csv
│   ├── jobs_by_dept_seniority.csv
│   └── region_summary.csv
│
├── logs/                           # ETL run logs (auto-generated)
│   ├── pipeline.log
│   ├── scrape_brands.log
│   ├── process_data.log
│   └── build_db.log
│
├── notebooks/                      # Jupyter notebooks (exploratory analysis)
├── run_pipeline.py                 # Master orchestrator
└── README.md
```

---

## 🔌 Data Sources

| Source | Method | Records | Notes |
|---|---|---|---|
| `sazerac.com/our-brands/` | Web scrape (+ mock fallback) | 27 brands | Mock enriched with real brand names |
| `sazerac.com/contact/` | Web scrape (+ mock fallback) | 24 locations | 5 countries, real coordinates |
| `sazerac.com/careers/` | Web scrape (+ mock fallback) | 10 job postings | Representative data analytics roles |
| NLP extraction | Python regex pattern matching | 45 unique skills | Across 10 job descriptions |

> **Note:** All scraping includes a graceful fallback to high-fidelity mock data when the live site is unreachable. This is production best-practice for resilient pipelines.

---

## ⚙️ Pipeline Steps

### Step 1 — Data Collection (Bronze Layer)
```
src/scrape_brands.py       → data/raw/brands_raw.csv       (27 rows)
src/scrape_locations.py    → data/raw/locations_raw.csv    (24 rows)
src/scrape_jobs.py         → data/raw/jobs_raw.csv         (10 rows)
                           → data/raw/job_skills.csv       (45 skills)
                           → data/raw/job_skill_mapping.csv
```

### Step 2 — ETL Processing (Silver Layer)
```
src/process_data.py:
  process_brands()      → Normalize categories, flag spirits, DQ checks
  process_locations()   → Validate coords, standardize types, assign regions
  process_jobs()        → Parse dates, classify seniority, extract state codes
  process_job_skills()  → Tier classification, skill category assignment
```

**Data Quality Checks Applied:**
- ✅ Null / blank checks on all required columns
- ✅ Duplicate removal (by natural key)
- ✅ Coordinate range validation
- ✅ Schema validation (required column presence)
- ✅ Category normalization (20+ raw categories → 12 clean)
- ✅ All 20 DQ checks PASSED across 4 tables

### Step 3 — Data Warehouse (Gold Layer)
```
src/build_db.py:
  create_schema()       → DDL for 4 tables + DQ log table
  load_brands()         → 27 rows → dim_brand
  load_locations()      → 24 rows → dim_location
  load_jobs()           → 10 rows → fact_jobs
  load_job_skills()     → 45 rows → fact_job_skills
  create_views()        → 6 analytical SQL views
  export_views_to_csv() → Gold CSVs for reporting
```

### Step 4 — Dashboard Generation
```
src/build_dashboard.py  → dashboard/sazerac_dashboard.html (standalone)
```

---

## 📐 Data Model

### Star Schema (Dimensional Model)

```
                    ┌──────────────────┐
                    │   dim_brand      │
                    │ ──────────────── │
                    │ brand_sk (PK)    │
                    │ brand_id         │
                    │ brand_name       │
                    │ category         │
                    │ is_flagship      │
                    │ is_spirits       │
                    └──────────────────┘

┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  dim_location    │     │   fact_jobs      │     │ fact_job_skills  │
│ ──────────────── │     │ ──────────────── │     │ ──────────────── │
│ location_sk (PK) │     │ job_sk (PK)      │     │ skill_sk (PK)    │
│ location_name    │     │ job_id           │     │ skill            │
│ city             │     │ job_title        │     │ frequency        │
│ country          │     │ department       │     │ pct_of_postings  │
│ region           │     │ seniority        │     │ demand_tier      │
│ location_type    │     │ is_remote        │     │ skill_category   │
│ latitude         │     │ skill_count      │     │                  │
│ longitude        │     │ skills_extracted │     │                  │
│ employee_count   │     │                  │     │                  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

**Grain Definitions:**
- `dim_brand` — One row per unique brand name
- `dim_location` — One row per physical site / office
- `fact_jobs` — One row per job posting
- `fact_job_skills` — One row per unique skill (frequency aggregated across all postings)

---

## 📊 Dashboard Pages

### Page 1 — Executive Overview
- 8 KPI cards: brands, flagships, countries, employees, open roles, skills tracked
- Brand portfolio donut chart
- Global employees by region (horizontal bar)
- Tier-1 must-have skills gauge list
- Medallion architecture diagram
- Pipeline run log (DQ check results)
- Star schema data model summary

### Page 2 — Brand Portfolio Analysis
- Stacked bar chart: flagship vs standard brands by category
- Pie chart: category share of portfolio
- Full 27-brand roster table with flagship tags

### Page 3 — Global Presence
- Canvas-based dot map (equirectangular projection, hover tooltips)
- Region employee counts bar + progress bars
- Full 24-location detail table with coordinates and headcount

### Page 4 — Job Skills Intelligence
- Top 20 skills horizontal bar (color-coded by domain)
- Radar chart: skill demand by category
- Must-have (Tier 1) skills ranked list
- Jobs by department stacked bar (on-site / hybrid / remote)
- All open positions table with seniority and skill count

---

## 🏃 How to Run Locally

### Prerequisites
```bash
Python 3.10+
pip install pandas requests beautifulsoup4
```
> **Note:** DuckDB is listed as optional; the project uses the built-in `sqlite3` module, so no extra install is needed beyond pandas.

### Option A — Full Pipeline (Recommended)
```bash
git clone <repo>
cd sazerac_analytics
pip install pandas requests beautifulsoup4
python run_pipeline.py
```

### Option B — Step by Step
```bash
# 1. Collect data
python src/scrape_brands.py
python src/scrape_locations.py
python src/scrape_jobs.py

# 2. Clean & transform
python src/process_data.py

# 3. Build warehouse
python src/build_db.py

# 4. Generate dashboard
python src/build_dashboard.py
```

### Option C — Individual steps
```bash
python run_pipeline.py --step scrape    # scraping only
python run_pipeline.py --step process   # ETL only
python run_pipeline.py --step db        # database build only
```

### View Dashboard
```
Open: dashboard/sazerac_dashboard.html
(double-click in Finder/Explorer — no server required)
```

### Query the Database
```bash
sqlite3 data/sazerac_analytics.db
.tables
SELECT * FROM brands_by_category;
SELECT * FROM top_requested_tools LIMIT 10;
SELECT * FROM portfolio_summary;
```

---

## 📈 Sample Outputs

### portfolio_summary view
| KPI | Value | Category |
|---|---|---|
| Total Brands | 27 | Portfolio |
| Flagship Brands | 9 | Portfolio |
| Brand Categories | 12 | Portfolio |
| Total Locations | 24 | Global Presence |
| Countries Served | 11 | Global Presence |
| Global Employees | 2,988 | Global Presence |
| Open Job Postings | 10 | Talent |
| Unique Skills Tracked | 45 | Talent |

### top_requested_tools view (Tier 1)
| Skill | Frequency | % of Postings | Urgency |
|---|---|---|---|
| SQL | 10 | 100.0% | 🔴 Critical |
| Data Quality | 10 | 100.0% | 🔴 Critical |
| Python | 9 | 90.0% | 🔴 Critical |
| Power BI | 9 | 90.0% | 🔴 Critical |
| ETL | 8 | 80.0% | 🔴 Critical |
| Data Modeling | 8 | 80.0% | 🔴 Critical |

### brands_by_category view
| Category | Brand Count | Flagship | % of Portfolio |
|---|---|---|---|
| Bourbon Whiskey | 9 | 4 | 33.3% |
| Vodka | 3 | 0 | 11.1% |
| Brandy | 2 | 1 | 7.4% |
| Liqueur | 2 | 1 | 7.4% |
| Rum | 2 | 0 | 7.4% |

---

## ⚠️ Limitations

1. **Live scraping** — Sazerac's website may block bots or change its HTML structure. The pipeline falls back to rich mock data automatically.
2. **Job postings** — 10 representative postings are used. A production version would integrate with LinkedIn/Greenhouse APIs.
3. **Location coordinates** — Are approximate city-level coordinates, not exact site addresses.
4. **Brand descriptions** — Some mock descriptions are abbreviated. Real descriptions would be richer.
5. **No SCD (Slowly Changing Dimensions)** — The dim tables use SCD Type 1 (overwrite). A production warehouse would implement SCD Type 2 with `valid_from` / `valid_to` timestamps.
6. **SQLite vs DuckDB/Snowflake** — SQLite is used for portability. For cloud scale, the same SQL runs on DuckDB, Snowflake, BigQuery, or Redshift with minimal changes.

---

## 🚀 Future Improvements

| Priority | Improvement | Effort |
|---|---|---|
| High | Connect to Greenhouse / Workday API for live job data | Medium |
| High | Implement SCD Type 2 for brand dimension | Medium |
| High | Add dbt for transformation layer (`dbt build`) | Medium |
| Medium | Deploy pipeline to Apache Airflow (DAG) | High |
| Medium | Replace SQLite with DuckDB or Snowflake | Low |
| Medium | Connect dashboard to live DB (Flask/FastAPI backend) | High |
| Medium | Add volume/revenue/market share data (Nielsen/IWSR) | High |
| Low | Add competitor analysis (Brown-Forman, Beam Suntory) | Medium |
| Low | Machine learning: predict brand category from description | Medium |
| Low | Add automated DQ alerting (email/Slack on failure) | Low |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Collection | Python · requests · BeautifulSoup4 |
| Data Processing | Python · Pandas |
| Data Warehouse | SQLite (DuckDB/Snowflake-compatible SQL) |
| SQL Modeling | Dimensional modeling · Star schema · SQL views |
| NLP | Python regex-based skill extraction |
| Dashboard | HTML · CSS · JavaScript · Chart.js |
| Logging | Python `logging` module · structured log files |
| Data Quality | Custom DQ check suite with pass/fail reporting |

---

## 👤 Author

Built as a portfolio project demonstrating enterprise data engineering and analytics skills aligned with roles requiring SQL, ETL/ELT, data modeling, dashboarding, and data governance.

**Skills demonstrated:** Python · Pandas · SQL · SQLite · Dimensional Modeling · ETL/ELT · Data Quality · NLP (regex) · Power BI-style Reporting · Data Architecture (Medallion) · Web Scraping · Chart.js

---

*Data sources: sazerac.com (public). This project is for educational/portfolio purposes only.*
