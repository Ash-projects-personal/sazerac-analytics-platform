# Sazerac Brand & Analytics Intelligence Platform

> A full-stack data engineering portfolio project demonstrating ETL/ELT pipeline design, dimensional data modeling, SQL analytics, NLP-based skill extraction, and a premium interactive dashboard — built around publicly available Sazerac Company data.

[![Pipeline CI](https://img.shields.io/badge/Pipeline-Passing-brightgreen)](https://github.com/YOUR_USERNAME/sazerac-analytics-platform/actions)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Live Dashboard

**[View on GitHub Pages](https://YOUR_USERNAME.github.io/sazerac-analytics-platform)**

---

## What This Demonstrates

| Skill Area | What's Shown |
|---|---|
| **Data Engineering** | Medallion architecture (Bronze→Silver→Gold), Python ETL, SQLite data warehouse |
| **SQL** | Star schema design, 6 analytical views, dimensional modeling, SCD Type 1 |
| **Data Quality** | 20 automated DQ checks — nulls, uniqueness, referential integrity, range validation |
| **NLP / Text Mining** | Regex skill extraction from job postings — 45 skills across 6 domains |
| **Data Visualization** | Power BI-style HTML dashboard — Chart.js, canvas world map, animated KPI counters |
| **DevOps / CI/CD** | GitHub Actions — lint → ETL → validate → deploy to GitHub Pages |
| **Documentation** | Inline docstrings, SQL comments, architecture diagrams, this README |

---

## Architecture

```
  🔶 Bronze (Raw)       🔷 Silver (Cleaned)       🏆 Gold (Warehouse)     📊 Reporting
  ─────────────────     ──────────────────────     ────────────────────    ─────────────
  brands_raw.csv    →   brands_clean.csv       →   dim_brand           →   6 SQL Views
  locations_raw     →   locations_clean        →   dim_location        →   dashboard_exports/
  jobs_raw          →   jobs_clean             →   fact_jobs           →   HTML dashboard
  job_skills.csv    →   job_skills_clean       →   fact_job_skills
  data/raw/             data/processed/            sazerac_analytics.db
```

---

## Data Model

```
  dim_brand ──────────────────┐
  (27 rows, 1/brand)          │
                              ▼
  dim_location ──────►  fact_jobs ──────► fact_job_skills
  (24 rows, 1/site)   (10 rows, 1/job)   (45 rows, 1/skill/job)
                              │
                              ▼
                           dq_log (governance audit)
```

**Key grains:**
| Table | Grain | PK | Rows |
|---|---|---|---|
| `dim_brand` | One row per brand | `brand_sk` | 27 |
| `dim_location` | One row per physical site | `location_sk` | 24 |
| `fact_jobs` | One row per job posting | `job_sk` | 10 |
| `fact_job_skills` | One row per skill per posting | `skill_sk` | 45 |

---

## SQL Views

| View | Purpose |
|---|---|
| `brands_by_category` | Portfolio distribution — count, flagship split, % of portfolio |
| `locations_by_region` | Global footprint aggregated by region and site type |
| `job_skill_frequency` | Skill rank, frequency count, % of postings, demand tier |
| `top_requested_tools` | Tier 1 & Tier 2 skills with urgency classification |
| `jobs_by_department` | Headcount by dept, remote/hybrid breakdown, avg skills required |
| `portfolio_summary` | Executive KPI view — 9 headline metrics |

---

## Key Findings

- **27 brands** across 12 spirit categories — Bourbon Whiskey dominates at 33.3% (9 brands)
- **9 flagship brands** including Buffalo Trace, Eagle Rare, Blanton's, Pappy Van Winkle, Fireball
- **24 global sites** across 11 countries — Americas holds ~95% of estimated workforce (2,831 / 2,988)
- **SQL + Data Quality** required in 100% of analytics postings; Python + Power BI in 90%
- **45 unique skills** extracted via NLP regex from 10 job descriptions
- **17.9 average skills per posting** — high technical bar across all open roles
- All **20 DQ checks passed** across all 4 warehouse tables

---

## Project Structure

```
sazerac_analytics/
├── src/
│   ├── scrape_brands.py       # Brand data collection + mock fallback
│   ├── scrape_locations.py    # Location/site data + mock fallback
│   ├── scrape_jobs.py         # Job posting scraper + NLP skill extraction
│   ├── process_data.py        # ETL Bronze→Silver→Gold + 20 DQ checks
│   ├── build_db.py            # SQLite warehouse loader + 6 SQL views
│   ├── build_dashboard.py     # Premium HTML dashboard generator
│   └── run_pipeline.py        # Master orchestrator
│
├── sql/
│   ├── schema.sql             # Full DDL with constraints and indexes
│   └── views.sql              # All 6 analytical view definitions
│
├── data/marts/                # Gold layer CSVs (committed for reference)
│   ├── brands_by_category.csv
│   ├── job_skill_frequency.csv
│   ├── jobs_by_department.csv
│   ├── locations_by_region.csv
│   ├── portfolio_summary.csv
│   └── top_requested_tools.csv
│
├── dashboard/
│   └── sazerac_dashboard.html  # Standalone interactive dashboard
│
├── dashboard_exports/          # Power BI-ready CSVs
│   ├── geographic_map.csv
│   ├── region_summary.csv
│   ├── skill_demand.csv
│   └── category_distribution.csv
│
├── .github/workflows/
│   └── pipeline.yml            # CI: lint → ETL → validate → Pages deploy
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Running Locally

```bash
git clone https://github.com/YOUR_USERNAME/sazerac-analytics-platform.git
cd sazerac-analytics-platform
pip install -r requirements.txt

# Full pipeline — one command
python src/run_pipeline.py

# Open the dashboard
open dashboard/sazerac_dashboard.html   # macOS
start dashboard/sazerac_dashboard.html  # Windows
```

---

## Data Quality Checks

20 automated checks in `process_data.py` before data reaches the warehouse:

| Check Type | Tables | Count |
|---|---|---|
| Null / completeness | All 4 | 8 |
| Uniqueness / PK | All 4 | 4 |
| Referential integrity | fact tables | 2 |
| Range / domain validation | dim tables | 4 |
| Row count thresholds | All 4 | 2 |

Failures write to `dq_log` in SQLite with timestamp, table name, and failure detail.

---

## CI/CD Pipeline

```
push to main
    │
    ├── Job 1: Lint (Black + Ruff)
    │
    ├── Job 2: Full pipeline run
    │       scrape → process → build db → validate → generate dashboard
    │       → upload artifact
    │
    └── Job 3: Deploy to GitHub Pages
```

---

## Deploying to GitHub Pages

1. Push to GitHub
2. **Settings → Pages → Source: GitHub Actions**
3. The `pipeline.yml` workflow deploys automatically on push to `main`

---

## Dashboard

Five views in one standalone HTML file (no server required):

1. **Executive Overview** — 8 animated KPIs, brand donut, regional headcount, skill gauges
2. **Brand Portfolio** — Stacked flagship/standard bar, polar area, 27-brand directory
3. **Global Presence** — Canvas world map with glowing location dots, regional breakdown
4. **Talent Intelligence** — Top-20 skills bar, domain radar, Tier-1 gauges, open roles
5. **Architecture** — Medallion diagram, DQ run log, star schema, SQL views inventory

---

## Limitations

- Sazerac brand data scraped from public web with mock fallback for CI environments
- Job postings are synthetic but modeled on real industry postings
- Employee counts are estimates; SQLite is used deliberately for portability (maps to Snowflake/BigQuery)

---

## Future Improvements

- [ ] Live Sazerac career page parser (replace mock scraper)
- [ ] dbt transformation layer (replace pandas ETL)
- [ ] DuckDB for in-process OLAP performance
- [ ] Airflow DAG for scheduled pipeline runs
- [ ] Sentence-embedding NLP for semantic skill clustering
- [ ] Time-series tracking for hiring and portfolio trends

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| ETL | Pandas 2.x |
| Warehouse | SQLite 3 |
| Visualization | Chart.js 4, Canvas API |
| Fonts | Playfair Display, IBM Plex Mono |
| CI/CD | GitHub Actions |
| Hosting | GitHub Pages |
| Code Quality | Black, Ruff |

---

*Built as a data engineering portfolio project. All company data sourced from public information.*
