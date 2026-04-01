"""
process_data.py
---------------
ETL / ELT processing pipeline:
  Bronze  → data/raw/           (as-scraped CSVs)
  Silver  → data/processed/     (cleaned, validated, enriched)
  Gold    → data/marts/         (star-schema ready, dashboard-ready)

Runs data quality checks at each stage and logs results.
"""

import logging
import os
from datetime import datetime

import pandas as pd

# ── logging ────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/process_data.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── paths ──────────────────────────────────────────────────────────────────────
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
MARTS_DIR = "data/marts"

for d in [RAW_DIR, PROCESSED_DIR, MARTS_DIR]:
    os.makedirs(d, exist_ok=True)

# ── category normalization map ─────────────────────────────────────────────────
CATEGORY_MAP = {
    "bourbon whiskey": "Bourbon Whiskey",
    "rye whiskey": "Rye Whiskey",
    "whiskey": "Whiskey",
    "whisky": "Whiskey",
    "flavored whisky": "Flavored Whiskey",
    "flavored whiskey": "Flavored Whiskey",
    "vodka": "Vodka",
    "rum": "Rum",
    "tequila": "Tequila",
    "mezcal": "Mezcal",
    "gin": "Gin",
    "brandy": "Brandy",
    "cognac": "Brandy",
    "liqueur": "Liqueur",
    "schnapps": "Schnapps/Liqueur",
    "mixer / non-alcoholic": "Mixer",
    "mixer": "Mixer",
    "beer": "Beer",
    "wine": "Wine",
    "unknown": "Uncategorized",
}

SPIRITS_CATEGORIES = {
    "Bourbon Whiskey",
    "Rye Whiskey",
    "Whiskey",
    "Flavored Whiskey",
    "Vodka",
    "Rum",
    "Tequila",
    "Mezcal",
    "Gin",
    "Brandy",
}


# ══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY CHECKS
# ══════════════════════════════════════════════════════════════════════════════


def run_dq_checks(
    df: pd.DataFrame,
    table_name: str,
    required_cols: list[str],
    unique_cols: list[str] | None = None,
) -> dict:
    """
    Standardized data quality check suite.
    Returns a dict of check results for logging and governance records.
    """
    results = {
        "table": table_name,
        "checked_at": datetime.utcnow().isoformat(),
        "total_rows": len(df),
        "checks_passed": 0,
        "checks_failed": 0,
        "issues": [],
    }

    def _pass(msg):
        results["checks_passed"] += 1
        log.info("[DQ PASS] %s — %s", table_name, msg)

    def _fail(msg):
        results["checks_failed"] += 1
        results["issues"].append(msg)
        log.warning("[DQ FAIL] %s — %s", table_name, msg)

    # 1. Row count sanity
    if len(df) == 0:
        _fail("Table is empty (0 rows)")
    else:
        _pass(f"Row count OK ({len(df)} rows)")

    # 2. Required columns present
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        _fail(f"Missing required columns: {missing_cols}")
    else:
        _pass(f"All {len(required_cols)} required columns present")

    # 3. Null checks on required columns
    for col in required_cols:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum() + (df[col] == "").sum()
        if null_count > 0:
            pct = round(null_count / len(df) * 100, 1)
            _fail(f"Column '{col}' has {null_count} nulls/blanks ({pct}%)")
        else:
            _pass(f"No nulls in '{col}'")

    # 4. Duplicate checks
    if unique_cols:
        existing = [c for c in unique_cols if c in df.columns]
        if existing:
            dup_count = df.duplicated(subset=existing).sum()
            if dup_count > 0:
                _fail(f"Found {dup_count} duplicate rows on {existing}")
            else:
                _pass(f"No duplicates on {existing}")

    log.info(
        "[DQ SUMMARY] %s | Passed: %d | Failed: %d",
        table_name,
        results["checks_passed"],
        results["checks_failed"],
    )
    return results


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE → SILVER: Brands
# ══════════════════════════════════════════════════════════════════════════════


def process_brands() -> pd.DataFrame:
    log.info("--- Processing Brands (Bronze → Silver) ---")
    path = f"{RAW_DIR}/brands_raw.csv"
    df = pd.read_csv(path)
    log.info("Loaded %d raw brand records", len(df))

    # 1. Normalize text fields
    df["brand_name"] = df["brand_name"].str.strip().str.title()
    df["category"] = df["category"].str.strip()

    # 2. Normalize categories using map
    df["category_clean"] = (
        df["category"].str.lower().str.strip().map(CATEGORY_MAP).fillna("Uncategorized")
    )

    # 3. Classify as spirits vs non-spirits
    df["is_spirits"] = df["category_clean"].isin(SPIRITS_CATEGORIES)

    # 4. Truncate long descriptions for readability (keep first 300 chars)
    df["description_short"] = df["description"].str.slice(0, 300).str.strip()

    # 5. Flag missing descriptions
    df["has_description"] = df["description"].notna() & (df["description"].str.len() > 10)

    # 6. Drop pure duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["brand_name"])
    log.info("Removed %d duplicate brands", before - len(df))

    # 7. Add processing metadata
    df["processed_at"] = datetime.utcnow().isoformat()
    df["data_source"] = "sazerac.com (scraped/mock)"

    # DQ checks
    run_dq_checks(
        df,
        "dim_brand_silver",
        required_cols=["brand_name", "category_clean"],
        unique_cols=["brand_name"],
    )

    out_path = f"{PROCESSED_DIR}/brands_clean.csv"
    df.to_csv(out_path, index=False)
    log.info("Saved cleaned brands → %s (%d records)", out_path, len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE → SILVER: Locations
# ══════════════════════════════════════════════════════════════════════════════


def process_locations() -> pd.DataFrame:
    log.info("--- Processing Locations (Bronze → Silver) ---")
    path = f"{RAW_DIR}/locations_raw.csv"
    df = pd.read_csv(path)
    log.info("Loaded %d raw location records", len(df))

    # 1. Normalize
    df["location_name"] = df["location_name"].str.strip()
    df["country"] = df["country"].str.strip()
    df["location_type"] = df["location_type"].str.strip()

    # 2. Fill missing regions
    region_map = {
        "United States": "Americas",
        "Canada": "Americas",
        "Mexico": "Americas",
        "Brazil": "Americas",
        "Argentina": "Americas",
        "United Kingdom": "Europe",
        "Germany": "Europe",
        "France": "Europe",
        "Italy": "Europe",
        "Spain": "Europe",
        "Netherlands": "Europe",
        "Australia": "APAC",
        "Japan": "APAC",
        "China": "APAC",
        "Singapore": "APAC",
        "South Korea": "APAC",
        "India": "APAC",
        "South Africa": "EMEA",
        "UAE": "EMEA",
    }
    df["region"] = df["country"].map(region_map).fillna("Other")

    # 3. Standardize location_type
    type_map = {
        "distillery": "Distillery",
        "headquarters": "Headquarters",
        "office": "Office",
        "international office": "International Office",
        "distribution center": "Distribution Center",
    }
    df["location_type_clean"] = (
        df["location_type"].str.lower().map(type_map).fillna(df["location_type"])
    )

    # 4. Validate lat/lon ranges
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    invalid_coords = df[df["latitude"].isna() | df["longitude"].isna()]
    if len(invalid_coords) > 0:
        log.warning("Found %d records with invalid coordinates", len(invalid_coords))

    # 5. Employee count as int
    df["employee_count"] = (
        pd.to_numeric(df["employee_count"], errors="coerce").fillna(0).astype(int)
    )

    # 6. Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["location_name"])
    log.info("Removed %d duplicate locations", before - len(df))

    df["processed_at"] = datetime.utcnow().isoformat()

    run_dq_checks(
        df,
        "dim_location_silver",
        required_cols=["location_name", "country", "region"],
        unique_cols=["location_name"],
    )

    out_path = f"{PROCESSED_DIR}/locations_clean.csv"
    df.to_csv(out_path, index=False)
    log.info("Saved cleaned locations → %s (%d records)", out_path, len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE → SILVER: Jobs
# ══════════════════════════════════════════════════════════════════════════════


def process_jobs() -> pd.DataFrame:
    log.info("--- Processing Jobs (Bronze → Silver) ---")
    path = f"{RAW_DIR}/jobs_raw.csv"
    df = pd.read_csv(path)
    log.info("Loaded %d raw job records", len(df))

    # 1. Normalize
    df["job_title"] = df["job_title"].str.strip()
    df["department"] = df["department"].str.strip().fillna("Unknown")
    df["location"] = df["location"].str.strip().fillna("Louisville, KY")
    df["employment_type"] = df["employment_type"].str.strip().fillna("Full-Time")

    # 2. Parse posted_date
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    null_dates = df["posted_date"].isna().sum()
    if null_dates > 0:
        log.warning("Found %d jobs with unparseable posted_date", null_dates)
        df["posted_date"] = df["posted_date"].fillna(pd.Timestamp.now())

    # 3. Extract state from location (e.g., "Louisville, KY" → "KY")
    df["state"] = df["location"].str.extract(r",\s*([A-Z]{2})\b")
    df["is_remote"] = df["location"].str.lower().str.contains("remote").fillna(False)
    df["is_hybrid"] = df["location"].str.lower().str.contains("hybrid").fillna(False)

    # 4. Skill count from extracted skills
    df["skills_extracted"] = df["skills_extracted"].fillna("").astype(str)
    df["skill_count"] = df["skills_extracted"].apply(
        lambda x: len(x.split("|")) if x.strip() else 0
    )

    # 5. Seniority classification
    def classify_seniority(title: str) -> str:
        t = title.lower()
        if any(k in t for k in ["director", "vp", "vice president", "chief", "head of"]):
            return "Senior Leadership"
        if any(k in t for k in ["senior", "sr.", "lead", "principal"]):
            return "Senior"
        if any(k in t for k in ["junior", "jr.", "associate", "entry"]):
            return "Junior"
        if any(k in t for k in ["manager", "supervisor"]):
            return "Manager"
        return "Mid-Level"

    df["seniority"] = df["job_title"].apply(classify_seniority)

    # 6. Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["job_title", "department"])
    log.info("Removed %d duplicate jobs", before - len(df))

    df["processed_at"] = datetime.utcnow().isoformat()

    run_dq_checks(
        df, "fact_jobs_silver", required_cols=["job_title", "department"], unique_cols=["job_id"]
    )

    out_path = f"{PROCESSED_DIR}/jobs_clean.csv"
    df.to_csv(out_path, index=False)
    log.info("Saved cleaned jobs → %s (%d records)", out_path, len(df))
    return df


def process_job_skills() -> pd.DataFrame:
    log.info("--- Processing Job Skills (Bronze → Silver) ---")
    path = f"{RAW_DIR}/job_skills.csv"
    df = pd.read_csv(path)
    log.info("Loaded %d skill frequency records", len(df))

    df["skill"] = df["skill"].str.strip()
    df["frequency"] = pd.to_numeric(df["frequency"], errors="coerce").fillna(0).astype(int)
    df["pct_of_postings"] = pd.to_numeric(df["pct_of_postings"], errors="coerce").fillna(0.0)

    # Tier classification
    def skill_tier(pct):
        if pct >= 70:
            return "Tier 1 — Must Have"
        if pct >= 40:
            return "Tier 2 — Strongly Preferred"
        if pct >= 20:
            return "Tier 3 — Nice to Have"
        return "Tier 4 — Bonus"

    df["demand_tier"] = df["pct_of_postings"].apply(skill_tier)

    # Category classification
    lang_skills = {"SQL", "Python", "R", "Scala", "Java", "JavaScript", "DAX", "MDX"}
    cloud_skills = {"Azure", "AWS", "GCP", "Snowflake", "Databricks", "Redshift", "BigQuery"}
    bi_skills = {"Power BI", "Tableau", "Looker", "Qlik", "Excel", "SSRS"}
    de_skills = {"ETL", "ELT", "dbt", "Airflow", "Spark", "Kafka", "Data Pipeline"}
    gov_skills = {"Data Governance", "Data Quality", "Data Catalog", "Lineage", "GDPR"}

    def classify_skill(skill):
        if skill in lang_skills:
            return "Language / Query"
        if skill in cloud_skills:
            return "Cloud / Platform"
        if skill in bi_skills:
            return "BI / Visualization"
        if skill in de_skills:
            return "Data Engineering"
        if skill in gov_skills:
            return "Governance"
        return "Analytics / Modeling"

    df["skill_category"] = df["skill"].apply(classify_skill)
    df["processed_at"] = datetime.utcnow().isoformat()

    run_dq_checks(df, "fact_job_skills_silver", required_cols=["skill", "frequency"])

    out_path = f"{PROCESSED_DIR}/job_skills_clean.csv"
    df.to_csv(out_path, index=False)
    log.info("Saved cleaned job skills → %s", out_path)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SILVER → GOLD: Data Marts
# ══════════════════════════════════════════════════════════════════════════════


def build_brands_by_category(brands_df: pd.DataFrame) -> pd.DataFrame:
    """Gold mart: brand count and flagship count by category."""
    mart = (
        brands_df.groupby("category_clean", as_index=False)
        .agg(
            brand_count=("brand_name", "count"),
            flagship_count=("is_flagship", "sum"),
            is_spirits=("is_spirits", "first"),
        )
        .rename(columns={"category_clean": "category"})
        .sort_values("brand_count", ascending=False)
    )
    mart["pct_of_portfolio"] = (mart["brand_count"] / mart["brand_count"].sum() * 100).round(1)
    path = f"{MARTS_DIR}/brands_by_category.csv"
    mart.to_csv(path, index=False)
    log.info("Built mart: brands_by_category (%d rows) → %s", len(mart), path)
    return mart


def build_locations_by_region(loc_df: pd.DataFrame) -> pd.DataFrame:
    """Gold mart: location counts and employee totals by region and type."""
    mart = (
        loc_df.groupby(["region", "location_type_clean"], as_index=False)
        .agg(
            location_count=("location_name", "count"),
            total_employees=("employee_count", "sum"),
            country_count=("country", "nunique"),
        )
        .sort_values(["region", "location_count"], ascending=[True, False])
    )
    path = f"{MARTS_DIR}/locations_by_region.csv"
    mart.to_csv(path, index=False)
    log.info("Built mart: locations_by_region (%d rows) → %s", len(mart), path)
    return mart


def build_skill_frequency_mart(skills_df: pd.DataFrame) -> pd.DataFrame:
    """Gold mart: top requested skills with demand tier."""
    mart = skills_df.sort_values("frequency", ascending=False).head(30)
    path = f"{MARTS_DIR}/skill_frequency.csv"
    mart.to_csv(path, index=False)
    log.info("Built mart: skill_frequency (%d rows) → %s", len(mart), path)
    return mart


def build_portfolio_summary(
    brands_df: pd.DataFrame, loc_df: pd.DataFrame, jobs_df: pd.DataFrame, skills_df: pd.DataFrame
) -> pd.DataFrame:
    """Gold mart: executive KPI summary."""
    kpis = [
        {
            "kpi_name": "Total Brands",
            "kpi_value": len(brands_df),
            "kpi_category": "Portfolio",
            "display_format": "number",
        },
        {
            "kpi_name": "Flagship Brands",
            "kpi_value": int(brands_df["is_flagship"].sum()),
            "kpi_category": "Portfolio",
            "display_format": "number",
        },
        {
            "kpi_name": "Brand Categories",
            "kpi_value": brands_df["category_clean"].nunique(),
            "kpi_category": "Portfolio",
            "display_format": "number",
        },
        {
            "kpi_name": "Spirits Brands (%)",
            "kpi_value": round(brands_df["is_spirits"].mean() * 100, 1),
            "kpi_category": "Portfolio",
            "display_format": "pct",
        },
        {
            "kpi_name": "Total Locations",
            "kpi_value": len(loc_df),
            "kpi_category": "Global Presence",
            "display_format": "number",
        },
        {
            "kpi_name": "Countries Served",
            "kpi_value": loc_df["country"].nunique(),
            "kpi_category": "Global Presence",
            "display_format": "number",
        },
        {
            "kpi_name": "Total Employees (Est.)",
            "kpi_value": int(loc_df["employee_count"].sum()),
            "kpi_category": "Global Presence",
            "display_format": "number",
        },
        {
            "kpi_name": "US Locations",
            "kpi_value": int((loc_df["country"] == "United States").sum()),
            "kpi_category": "Global Presence",
            "display_format": "number",
        },
        {
            "kpi_name": "International Locations",
            "kpi_value": int((loc_df["country"] != "United States").sum()),
            "kpi_category": "Global Presence",
            "display_format": "number",
        },
        {
            "kpi_name": "Open Job Postings",
            "kpi_value": len(jobs_df),
            "kpi_category": "Talent",
            "display_format": "number",
        },
        {
            "kpi_name": "Remote Positions",
            "kpi_value": int(jobs_df["is_remote"].sum()),
            "kpi_category": "Talent",
            "display_format": "number",
        },
        {
            "kpi_name": "Hybrid Positions",
            "kpi_value": int(jobs_df["is_hybrid"].sum()),
            "kpi_category": "Talent",
            "display_format": "number",
        },
        {
            "kpi_name": "Avg Skills/Posting",
            "kpi_value": round(jobs_df["skill_count"].mean(), 1),
            "kpi_category": "Talent",
            "display_format": "number",
        },
        {
            "kpi_name": "Unique Skills Required",
            "kpi_value": len(skills_df),
            "kpi_category": "Talent",
            "display_format": "number",
        },
        {
            "kpi_name": "Top Required Skill",
            "kpi_value": skills_df.iloc[0]["skill"] if len(skills_df) else "N/A",
            "kpi_category": "Talent",
            "display_format": "text",
        },
        {
            "kpi_name": "Data Roles (%)",
            "kpi_value": round(
                jobs_df["department"].str.lower().str.contains("data|analytics|bi").mean() * 100, 1
            ),
            "kpi_category": "Talent",
            "display_format": "pct",
        },
    ]

    mart = pd.DataFrame(kpis)
    mart["as_of_date"] = datetime.utcnow().date().isoformat()
    path = f"{MARTS_DIR}/portfolio_summary.csv"
    mart.to_csv(path, index=False)
    log.info("Built mart: portfolio_summary (%d KPIs) → %s", len(mart), path)
    return mart


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD EXPORT CSVs (Power BI ready)
# ══════════════════════════════════════════════════════════════════════════════


def build_dashboard_exports(brands_df, loc_df, jobs_df, skills_df):
    """Build additional analytics views for Power BI / dashboard consumption."""
    os.makedirs("dashboard_exports", exist_ok=True)

    # 1. Category distribution (pie/donut)
    cat_dist = brands_df.groupby("category_clean")["brand_name"].count().reset_index()
    cat_dist.columns = ["Category", "Brand Count"]
    cat_dist["% of Portfolio"] = (
        cat_dist["Brand Count"] / cat_dist["Brand Count"].sum() * 100
    ).round(1)
    cat_dist.sort_values("Brand Count", ascending=False, inplace=True)
    cat_dist.to_csv("dashboard_exports/category_distribution.csv", index=False)
    log.info("Dashboard export: category_distribution")

    # 2. Geographic map data
    geo = loc_df[
        [
            "location_name",
            "city",
            "state_province",
            "country",
            "region",
            "location_type_clean",
            "latitude",
            "longitude",
            "employee_count",
        ]
    ].copy()
    geo.columns = [
        "Location",
        "City",
        "State",
        "Country",
        "Region",
        "Type",
        "Latitude",
        "Longitude",
        "Employees",
    ]
    geo.to_csv("dashboard_exports/geographic_map.csv", index=False)
    log.info("Dashboard export: geographic_map")

    # 3. Skill demand chart (top 20)
    skill_chart = skills_df.head(20)[
        ["skill", "frequency", "pct_of_postings", "skill_category", "demand_tier"]
    ].copy()
    skill_chart.columns = ["Skill", "Frequency", "% of Postings", "Category", "Demand Tier"]
    skill_chart.to_csv("dashboard_exports/skill_demand.csv", index=False)
    log.info("Dashboard export: skill_demand")

    # 4. Jobs by department & seniority
    job_summary = (
        jobs_df.groupby(["department", "seniority"])
        .size()
        .reset_index(name="Count")
        .sort_values("Count", ascending=False)
    )
    job_summary.to_csv("dashboard_exports/jobs_by_dept_seniority.csv", index=False)
    log.info("Dashboard export: jobs_by_dept_seniority")

    # 5. Region summary (bar)
    region_sum = (
        loc_df.groupby("region")
        .agg(
            Locations=("location_name", "count"),
            Employees=("employee_count", "sum"),
            Countries=("country", "nunique"),
        )
        .reset_index()
        .rename(columns={"region": "Region"})
    )
    region_sum.to_csv("dashboard_exports/region_summary.csv", index=False)
    log.info("Dashboard export: region_summary")

    log.info("All dashboard exports complete → dashboard_exports/")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════


def main():
    log.info("========== ETL Pipeline Starting ==========")

    # Bronze → Silver
    brands_df = process_brands()
    loc_df = process_locations()
    jobs_df = process_jobs()
    skills_df = process_job_skills()

    # Silver → Gold Marts
    build_brands_by_category(brands_df)
    build_locations_by_region(loc_df)
    build_skill_frequency_mart(skills_df)
    build_portfolio_summary(brands_df, loc_df, jobs_df, skills_df)

    # Dashboard exports
    build_dashboard_exports(brands_df, loc_df, jobs_df, skills_df)

    log.info("========== ETL Pipeline Complete ==========")


if __name__ == "__main__":
    main()
