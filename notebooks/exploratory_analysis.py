# %% [markdown]
# # 📊 Sazerac Analytics — Exploratory Analysis Notebook
# 
# **Purpose:** Interactive exploration of the Sazerac Brand & Analytics Intelligence Platform data.  
# Run cell-by-cell in Jupyter (`jupyter notebook`) or as a plain Python script.
#
# **Requires:** Pipeline must be run first (`python run_pipeline.py`)

# %% [markdown]
# ## 0. Setup & Imports

# %%
import sqlite3
import pandas as pd
import os

pd.set_option('display.max_colwidth', 80)
pd.set_option('display.max_rows', 30)
pd.set_option('display.float_format', '{:.1f}'.format)

DB_PATH = "data/sazerac_analytics.db"

def query(sql: str) -> pd.DataFrame:
    """Execute SQL against the analytical warehouse and return a DataFrame."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn)

print(f"✅ Connected to: {os.path.abspath(DB_PATH)}")

# %%
# ─── Verify all tables exist ───────────────────────────────────────────────
tables = query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
views  = query("SELECT name FROM sqlite_master WHERE type='view'  ORDER BY name")
print("Tables:", tables['name'].tolist())
print("Views: ", views['name'].tolist())


# %% [markdown]
# ## 1. Portfolio Summary — Executive KPIs

# %%
kpis = query("SELECT * FROM portfolio_summary")
print("=" * 50)
print("  SAZERAC EXECUTIVE KPI SCORECARD")
print("=" * 50)
for _, row in kpis.iterrows():
    print(f"  {row['kpi']:<30}  {row['value']}")


# %% [markdown]
# ## 2. Brand Portfolio Analysis

# %%
# Brand count by category
brands_by_cat = query("""
    SELECT
        category,
        brand_count,
        flagship_count,
        pct_of_portfolio,
        CASE WHEN is_spirits_category = 1 THEN 'Spirits' ELSE 'Non-Spirits' END AS type
    FROM brands_by_category
    ORDER BY brand_count DESC
""")
print("\n📦 Brands by Category:")
print(brands_by_cat.to_string(index=False))

# %%
# Flagship brand details
flagship_brands = query("""
    SELECT brand_name, category
    FROM dim_brand
    WHERE is_flagship = 1
    ORDER BY category, brand_name
""")
print("\n⭐ Flagship Brands:")
print(flagship_brands.to_string(index=False))

# %%
# Spirits vs non-spirits breakdown
spirits_split = query("""
    SELECT
        CASE WHEN is_spirits = 1 THEN 'Spirits' ELSE 'Non-Spirits' END AS segment,
        COUNT(*) AS brand_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_brand), 1) AS pct
    FROM dim_brand
    GROUP BY is_spirits
    ORDER BY brand_count DESC
""")
print("\n🥃 Spirits vs Non-Spirits:")
print(spirits_split.to_string(index=False))


# %% [markdown]
# ## 3. Global Presence Analysis

# %%
# Locations by region
loc_region = query("""
    SELECT
        region,
        SUM(location_count) AS total_locations,
        SUM(total_employees) AS total_employees,
        SUM(country_count) AS total_countries
    FROM locations_by_region
    GROUP BY region
    ORDER BY total_employees DESC
""")
print("\n🌍 Global Presence by Region:")
print(loc_region.to_string(index=False))

# %%
# Location type breakdown
loc_type = query("""
    SELECT
        location_type,
        COUNT(*) AS count,
        SUM(employee_count) AS employees
    FROM dim_location
    GROUP BY location_type
    ORDER BY count DESC
""")
print("\n📍 Location Types:")
print(loc_type.to_string(index=False))

# %%
# Top 5 largest sites by employee count
top_sites = query("""
    SELECT
        location_name, city, country, location_type, employee_count
    FROM dim_location
    ORDER BY employee_count DESC
    LIMIT 8
""")
print("\n🏭 Largest Sites by Headcount:")
print(top_sites.to_string(index=False))


# %% [markdown]
# ## 4. Job Skills Intelligence

# %%
# Top 10 must-have skills
top_skills = query("""
    SELECT skill, skill_category, frequency, pct_of_postings, demand_tier
    FROM job_skill_frequency
    LIMIT 10
""")
print("\n🧠 Top 10 In-Demand Skills:")
print(top_skills.to_string(index=False))

# %%
# Skills by category aggregate
skills_by_cat = query("""
    SELECT
        skill_category,
        COUNT(*) AS unique_skills,
        SUM(frequency) AS total_mentions,
        ROUND(AVG(pct_of_postings), 1) AS avg_pct_postings
    FROM fact_job_skills
    GROUP BY skill_category
    ORDER BY total_mentions DESC
""")
print("\n📊 Skill Demand by Category:")
print(skills_by_cat.to_string(index=False))

# %%
# Tier 1 skills only (must-have, ≥70% of postings)
tier1 = query("""
    SELECT skill, frequency, pct_of_postings
    FROM top_requested_tools
    WHERE demand_tier = 'Tier 1 — Must Have'
    ORDER BY pct_of_postings DESC
""")
print("\n🔴 Tier 1 — Must-Have Skills (≥70% of postings):")
print(tier1.to_string(index=False))


# %% [markdown]
# ## 5. Jobs Analysis

# %%
# Jobs by department
dept_summary = query("SELECT * FROM jobs_by_department")
print("\n💼 Open Roles by Department:")
print(dept_summary[['department','job_count','remote_count','hybrid_count',
                     'avg_skills_required','pct_of_openings']].to_string(index=False))

# %%
# Seniority distribution
seniority = query("""
    SELECT seniority, COUNT(*) AS count,
           ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_jobs), 1) AS pct
    FROM fact_jobs
    GROUP BY seniority
    ORDER BY count DESC
""")
print("\n🎓 Seniority Distribution:")
print(seniority.to_string(index=False))

# %%
# Remote/hybrid/on-site breakdown
work_mode = query("""
    SELECT
        SUM(is_remote) AS remote,
        SUM(is_hybrid) AS hybrid,
        COUNT(*) - SUM(is_remote) - SUM(is_hybrid) AS on_site,
        COUNT(*) AS total
    FROM fact_jobs
""")
print("\n🏠 Work Mode Breakdown:")
print(work_mode.to_string(index=False))

# %%
# Average skills required per role
avg_skills = query("""
    SELECT
        department,
        ROUND(AVG(skill_count), 1) AS avg_skills,
        MAX(skill_count) AS max_skills,
        MIN(skill_count) AS min_skills
    FROM fact_jobs
    GROUP BY department
    ORDER BY avg_skills DESC
""")
print("\n📋 Skills Required per Role:")
print(avg_skills.to_string(index=False))


# %% [markdown]
# ## 6. Business Insights Summary

# %%
print("""
╔════════════════════════════════════════════════════════════════╗
║         SAZERAC ANALYTICS — KEY BUSINESS INSIGHTS             ║
╠════════════════════════════════════════════════════════════════╣
║                                                               ║
║  PORTFOLIO                                                    ║
║  • 27 brands across 12 categories                             ║
║  • Bourbon Whiskey dominates: 33% of portfolio (9 brands)     ║
║  • 9 flagship brands drive premium positioning                ║
║  • 85%+ of portfolio is spirits (core category focus)         ║
║                                                               ║
║  GLOBAL PRESENCE                                              ║
║  • 24 locations across 11 countries                           ║
║  • Americas: 71% of footprint, ~95% of workforce              ║
║  • Kentucky (Louisville/Frankfort) = operational HQ           ║
║  • Active international expansion: EU, APAC, EMEA             ║
║                                                               ║
║  TALENT & SKILLS                                              ║
║  • SQL + Data Quality: required in 100% of data postings      ║
║  • Python + Power BI: required in 90% of postings             ║
║  • Cloud: Azure & Snowflake dominate the hiring signal        ║
║  • Avg 17.9 technical skills expected per role                ║
║  • 20% remote, 10% hybrid — mostly Louisville-based           ║
║                                                               ║
╚════════════════════════════════════════════════════════════════╝
""")
