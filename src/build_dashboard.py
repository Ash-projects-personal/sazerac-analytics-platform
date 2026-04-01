"""
build_dashboard.py  (v2 — premium redesign)
--------------------------------------------
Generates a standalone, zero-dependency HTML dashboard with:
  • Cormorant Garamond display font + DM Mono data font
  • Warm bourbon-barrel dark palette (#0e0b08 base, amber gold accents)
  • SVG grain texture overlay for tactile depth
  • Sidebar navigation with vertical brand stamp
  • Animated KPI counters on page load
  • Liquid-fill progress bars
  • Chart.js with heavily custom-styled datasets
  • Canvas world map with glowing amber location dots
  • Hover micro-interactions throughout
  • Full data embedded — opens with one double-click, no server needed

Output: dashboard/sazerac_dashboard.html
"""

import json, os, pandas as pd
from datetime import datetime

os.makedirs("dashboard", exist_ok=True)

brands    = pd.read_csv("data/marts/brands_by_category.csv").to_dict("records")
skills    = pd.read_csv("data/marts/job_skill_frequency.csv").head(20).to_dict("records")
kpis_df   = pd.read_csv("data/marts/portfolio_summary.csv")
jobs_dept = pd.read_csv("data/marts/jobs_by_department.csv").to_dict("records")
geo       = pd.read_csv("dashboard_exports/geographic_map.csv").to_dict("records")
bfull     = pd.read_csv("data/processed/brands_clean.csv")[
                ["brand_name","category_clean","is_flagship","url","description_short"]
            ].to_dict("records")
top_tools = pd.read_csv("data/marts/top_requested_tools.csv").to_dict("records")
region_df = pd.read_csv("dashboard_exports/region_summary.csv").to_dict("records")
jobs_full = pd.read_csv("data/processed/jobs_clean.csv")[
                ["job_title","department","location","seniority",
                 "skill_count","is_remote","is_hybrid","posted_date"]
            ].to_dict("records")

as_of = datetime.now().strftime("%B %d, %Y")

DATA_JS = f"""const DATA = {{
  kpis:       {json.dumps(kpis_df.to_dict("records"))},
  brands:     {json.dumps(brands)},
  bfull:      {json.dumps(bfull)},
  skills:     {json.dumps(skills)},
  topTools:   {json.dumps(top_tools)},
  jobsDept:   {json.dumps(jobs_dept)},
  jobsFull:   {json.dumps(jobs_full)},
  geo:        {json.dumps(geo)},
  region:     {json.dumps(region_df)},
  asOf:       "{as_of}"
}};"""

HTML = open("src/_dashboard_template.html").read().replace("__DATA__", DATA_JS)

out = "dashboard/sazerac_dashboard.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"Dashboard → {out}  ({len(HTML):,} bytes)")
