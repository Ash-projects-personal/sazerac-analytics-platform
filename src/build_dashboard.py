"""
build_dashboard.py  (v3 — premium edition)
-------------------------------------------
Generates standalone HTML dashboard using the premium
bourbon-barrel dark aesthetic with real Sazerac brand assets.

Features:
  - Real Sazerac logo from official CDN
  - Cormorant Garamond display + DM Mono data fonts
  - Sidebar navigation (5 pages)
  - Animated KPI counters
  - Chart.js with custom warm palette
  - Canvas equirectangular world map with hover tooltips
  - All data embedded — no server needed

Output: dashboard/sazerac_dashboard.html
"""

import json
import os
import pandas as pd
from datetime import datetime

os.makedirs("dashboard", exist_ok=True)

# ── Load mart data ─────────────────────────────────────────────────────────────
brands    = pd.read_csv("data/marts/brands_by_category.csv").to_dict("records")
skills    = pd.read_csv("data/marts/job_skill_frequency.csv").head(20).to_dict("records")
kpis_df   = pd.read_csv("data/marts/portfolio_summary.csv")
jobs_dept = pd.read_csv("data/marts/jobs_by_department.csv").to_dict("records")
geo       = pd.read_csv("dashboard_exports/geographic_map.csv").to_dict("records")
bfull     = pd.read_csv("data/processed/brands_clean.csv")[
                ["brand_name", "category_clean", "is_flagship", "url", "description_short"]
            ].to_dict("records")
top_tools = pd.read_csv("data/marts/top_requested_tools.csv").to_dict("records")
region_df = pd.read_csv("dashboard_exports/region_summary.csv").to_dict("records")
jobs_full = pd.read_csv("data/processed/jobs_clean.csv")[
                ["job_title", "department", "location", "seniority",
                 "skill_count", "is_remote", "is_hybrid"]
            ].to_dict("records")

as_of = datetime.now().strftime("%B %d, %Y")

DATA_JS = "\n".join([
    "const DATA = {",
    f"  kpis:     {json.dumps(kpis_df.to_dict('records'))},",
    f"  brands:   {json.dumps(brands)},",
    f"  bfull:    {json.dumps(bfull)},",
    f"  skills:   {json.dumps(skills)},",
    f"  topTools: {json.dumps(top_tools)},",
    f"  jobsDept: {json.dumps(jobs_dept)},",
    f"  jobsFull: {json.dumps(jobs_full)},",
    f"  geo:      {json.dumps(geo)},",
    f"  region:   {json.dumps(region_df)},",
    f'  asOf:     "{as_of}"',
    "};"
])

# ── Inject into dashboard template (HTML string below) ─────────────────────────
with open("src/dashboard_template.html", encoding="utf-8") as f:
    html = f.read().replace("/* __DATA_JS__ */", DATA_JS)

out = "dashboard/sazerac_dashboard.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Dashboard → {out}  ({len(html):,} bytes)")
