# Sazerac Analytics Intelligence Platform

A self-contained analytics dashboard purpose-built around Sazerac Company's brand portfolio, job market data, and spirits industry KPIs.

**Live demo:** https://ash-projects-personal.github.io/sazerac-analytics-platform/

---

## What it does

**Brand & Portfolio Intelligence** — Structured data on all 27+ Sazerac brands across 12 spirit categories, geographic footprint across 11 countries, and headcount by region.

**Job Market & Skills Intelligence** — Scrapes open roles from Sazerac's careers page and runs NLP extraction on job descriptions to surface in-demand skills (SQL, Python, Power BI, Data Quality) and their frequency.

**Depletion Trends (VIP/iDig-style)** — Monthly case depletion data across 10 US states (4 NABCA control states + 6 open) for Fireball, Buffalo Trace, Pappy Van Winkle, Eagle Rare, Fleischmanns, and Southern Comfort — with real-world seasonality curves and control-state discounts.

**Market Share & Nielsen/IRI-style Analysis** — Volume share, numeric distribution, velocity per point, and price tier segmentation by brand. Fireball at 62.4% volume share in Cinnamon Whiskey; Pappy at 0.3% share but 98.7 velocity.

---

## Tech stack

| Layer | Tools |
|---|---|
| Data pipeline | Python, pandas, SQLite |
| ETL orchestration | GitHub Actions CI/CD |
| Schema | Star schema — `dim_brand`, `dim_location`, `fact_jobs`, `fact_job_skills`, `fact_depletions`, `fact_market_share` |
| SQL views | `depletion_trend`, `control_state_summary`, `market_share` |
| Dashboard | Vanilla JS, Chart.js, CSS Grid |
| Deployment | GitHub Pages (auto-deploy on push) |
| Code quality | Black, Ruff |

---

## Project structure
```
sazerac-analytics-platform/
├── .github/workflows/pipeline.yml   # 3-job CI: lint → ETL → deploy
├── src/
│   ├── scrape_brands.py
│   ├── scrape_locations.py
│   ├── scrape_jobs.py
│   ├── scrape_depletions.py         # VIP/iDig-style depletion simulator
│   ├── scrape_market_share.py       # Nielsen/IRI-style market share simulator
│   ├── build_db.py                  # SQLite star schema + mart exports
│   ├── build_dashboard.py           # Data injection into HTML template
│   └── dashboard_template.html      # Single-file dashboard UI
├── data/
│   ├── raw/                         # Scraper outputs
│   └── marts/                       # Aggregated views for dashboard
└── docs/index.html                  # Built dashboard (GitHub Pages)
```

---

## Pipeline
```
Push to main
    │
    ▼
[Code Quality]   black --check · ruff check
    │
    ▼
[ETL Pipeline]   scrapers → SQLite → mart CSVs → dashboard HTML
    │
    ▼
[Deploy]         GitHub Pages publish
```

20 data quality checks validate row counts, referential integrity, and null rates before every deploy.

---

## Dashboard sections

- **Executive Overview** — KPI scorecard, brand distribution, regional headcount, skills demand
- **Brand Portfolio** — Full brand roster with category and flagship breakdown
- **Global Presence** — Country-level footprint across Americas, Europe, APAC, EMEA
- **Skills Intelligence** — NLP-extracted skill frequencies tiered by requirement level
- **Architecture & DQ** — Live schema, table counts, DQ check results
- **Market Intelligence** — Depletion trends, market share, control vs open state, channel split, price tier matrix

---

## Why Sazerac-specific

Sazerac data analyst roles ask for experience with depletion data (VIP/iDig), syndicated data (Nielsen/IRI), NABCA control state reporting, and on/off-premise channel analysis. This project is built around exactly those workflows. SQL and Data Quality appear in 100% of postings — both are central to how this pipeline is structured.

---

## Running locally
```bash
git clone https://github.com/Ash-projects-personal/sazerac-analytics-platform.git
cd sazerac-analytics-platform
pip install -r requirements.txt

python src/scrape_brands.py
python src/scrape_locations.py
python src/scrape_jobs.py
python src/scrape_depletions.py
python src/scrape_market_share.py
python src/build_db.py
python src/build_dashboard.py

open docs/index.html
```

---

## Data notes

Job postings scraped live from Sazerac's careers site. Depletion and market share figures are simulated using realistic industry parameters — actual depletion data requires a VIP/iDig subscription. Brand and location data from public Sazerac web properties.
