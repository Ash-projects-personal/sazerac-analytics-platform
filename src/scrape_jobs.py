"""
scrape_jobs.py
--------------
Scrapes Sazerac job postings from careers page.
Falls back to representative mock postings.

Also performs lightweight NLP skill extraction from job descriptions.

Outputs:
  data/raw/jobs_raw.csv
  data/raw/job_skills.csv
"""

import csv
import logging
import os
import re
from collections import Counter
from datetime import datetime

import os

try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False


try:
    import requests
    from bs4 import BeautifulSoup

    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False

# ── logging ────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/scrape_jobs.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

JOBS_OUTPUT = "data/raw/jobs_raw.csv"
SKILLS_OUTPUT = "data/raw/job_skills.csv"

# ── skill taxonomy ──────────────────────────────────────────────────────────────
SKILL_PATTERNS = {
    # Languages
    "SQL": r"\bSQL\b",
    "Python": r"\bPython\b",
    "R": r"\b(R programming|R language|\bR\b(?= skills| for))",
    "Scala": r"\bScala\b",
    "Java": r"\bJava\b(?!Script)",
    "JavaScript": r"\bJavaScript\b",
    "DAX": r"\bDAX\b",
    "MDX": r"\bMDX\b",
    # BI & Visualization -- Power BI dominates in CPG; Tableau is #2
    "Power BI": r"\bPower BI\b",
    "Tableau": r"\bTableau\b",
    "Looker": r"\bLooker\b",
    "Qlik": r"\bQlik(View|Sense)?\b",
    "Excel": r"\bExcel\b",
    "SSRS": r"\bSSRS\b",
    # Cloud -- Azure is most common in Sazerac JDs specifically; AWS appears in newer roles
    "Azure": r"\bAzure\b",
    "AWS": r"\b(AWS|Amazon Web Services)\b",
    "GCP": r"\b(GCP|Google Cloud)\b",
    "Snowflake": r"\bSnowflake\b",
    "Databricks": r"\bDatabricks\b",
    "Redshift": r"\bRedshift\b",
    "BigQuery": r"\bBigQuery\b",
    # Data Engineering stack -- dbt + Airflow + Snowflake is the modern combo
    "ETL": r"\bETL\b",
    "ELT": r"\bELT\b",
    "dbt": r"\bdbt\b",
    "Airflow": r"\b(Airflow|Apache Airflow)\b",
    "Spark": r"\b(Spark|PySpark|Apache Spark)\b",
    "Kafka": r"\bKafka\b",
    # Databases
    "SQL Server": r"\b(SQL Server|MSSQL|Microsoft SQL)\b",
    "PostgreSQL": r"\b(PostgreSQL|Postgres)\b",
    "MySQL": r"\bMySQL\b",
    "Oracle": r"\bOracle\b",
    "DuckDB": r"\bDuckDB\b",
    "Teradata": r"\bTeradata\b",
    # Modeling & Analytics
    "Data Modeling": r"\bdata model(ing|er)?\b",
    "Data Warehouse": r"\bdata\s+warehouse\b",
    "Data Lake": r"\bdata\s+lake\b",
    "Data Lakehouse": r"\bdata\s+lakehouse\b",
    "Star Schema": r"\bstar\s+schema\b",
    "OLAP": r"\bOLAP\b",
    "Dimensional Modeling": r"\bdimensional\s+model(ing)?\b",
    "Machine Learning": r"\b(machine learning|ML\b)",
    "Statistics": r"\b(statistics|statistical analysis)\b",
    # Governance
    "Data Governance": r"\bdata\s+governance\b",
    "Data Quality": r"\bdata\s+quality\b",
    "Data Catalog": r"\bdata\s+catalog\b",
    "Lineage": r"\bdata\s+lineage\b",
    "GDPR": r"\bGDPR\b",
    # Soft skills / process -- Agile shows up in literally every JD, which tells you nothing
    # but you still need to flag it
    "Agile": r"\b(Agile|Scrum|Kanban)\b",
    "Stakeholder": r"\bstakeholder\b",
    "KPIs": r"\b(KPI|KPIs|key performance indicators?)\b",
    "Data Pipeline": r"\bdata\s+pipeline\b",
    "Dashboards": r"\bdashboard\b",
    "Reporting": r"\b(reporting|reports?)\b",
    "Business Intelligence": r"\b(business intelligence|BI\b)",
}

# ── mock jobs ──────────────────────────────────────────────────────────────────
MOCK_JOBS = [
    # Senior Data Engineer -- the most senior IC role, Azure + Databricks stack
    {
        "job_title": "Senior Data Engineer",
        "department": "Data & Analytics",
        "location": "Louisville, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-11-15",
        "description": """We are seeking a Senior Data Engineer to join our growing Data & Analytics team.
        Responsibilities include building and maintaining ETL/ELT data pipelines using Python and SQL.
        The ideal candidate will have experience with Azure Data Factory, Databricks, and Snowflake.
        You will design and implement data models following dimensional modeling best practices including star schema.
        Experience with dbt, Airflow for workflow orchestration, and Power BI for data visualization is required.
        Must be comfortable working in an Agile environment and collaborating with stakeholders across the business.
        Data governance and data quality checks are a core part of this role. KPIs and reporting requirements will be
        defined in collaboration with business partners. SQL Server and PostgreSQL experience preferred.
        Knowledge of data warehouse architecture, data lake, and data lakehouse patterns is essential.""",
    },
    {
        "job_title": "Data Analyst — Brand Analytics",
        "department": "Data & Analytics",
        "location": "Louisville, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-11-20",
        "description": """Seeking a Data Analyst to support brand performance analytics across the Sazerac portfolio.
        Responsibilities: Develop Power BI dashboards and Excel-based reporting for senior leadership.
        Write complex SQL queries against our SQL Server data warehouse to extract insights on brand KPIs.
        Perform ad hoc analysis using Python and R for statistical analysis and data visualization.
        Work closely with brand managers to define reporting requirements and business intelligence needs.
        Create and maintain dashboards that track volume, distribution, pricing, and share metrics.
        Data quality checks and validation of reporting data is part of day-to-day responsibilities.
        Experience with Tableau or Looker is a plus. Must be able to communicate insights to non-technical stakeholders.""",
    },
    {
        "job_title": "Business Intelligence Developer",
        "department": "IT & Technology",
        "location": "Louisville, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-11-22",
        "description": """The BI Developer will design, build, and maintain enterprise reporting solutions.
        Core skills required: Power BI (including DAX and data modeling), SQL Server, SSRS, and Excel.
        You will work with Azure cloud infrastructure and integrate data from multiple source systems.
        Build and maintain ETL processes to support reporting and analytics workloads.
        Experience with dimensional modeling, star schema design, and OLAP cubes is a strong plus.
        This role requires strong data governance practices and adherence to data quality standards.
        Reporting directly to the VP of Analytics, you will support KPI dashboards for executive stakeholders.
        Agile project methodology experience required. Familiarity with Snowflake or Redshift is a plus.""",
    },
    {
        "job_title": "Data Engineer — Commercial",
        "department": "Commercial / Sales Analytics",
        "location": "Louisville, KY (Hybrid)",
        "employment_type": "Full-Time",
        "posted_date": "2024-11-25",
        "description": """We're building out our Commercial data infrastructure and need a Data Engineer with strong
        Python, SQL, and cloud (AWS or Azure) experience. You will own end-to-end data pipelines from raw ingestion
        to curated data marts consumed by analysts and dashboards.
        Technologies: Python (pandas, PySpark), Apache Airflow, dbt, Snowflake, and AWS S3/Glue.
        Design data models aligned with business requirements for sales analytics and distribution reporting.
        Partner with the Data Science team to expose machine learning model outputs through the data warehouse.
        Data quality, lineage, and data catalog management using modern tooling is essential.
        BigQuery and GCP experience is a bonus. Agile, Scrum, and stakeholder communication skills required.""",
    },
    {
        "job_title": "Analytics Engineer",
        "department": "Data & Analytics",
        "location": "Remote",
        "employment_type": "Full-Time",
        "posted_date": "2024-12-01",
        "description": """The Analytics Engineer role bridges data engineering and data analysis, owning the
        transformation layer of our data platform. Primary tools: dbt, SQL, Python, and Databricks.
        You will build and document data models used by analysts across brand, finance, and supply chain teams.
        Experience with dimensional modeling, data warehouse design, and data modeling best practices is required.
        You will ensure data quality by implementing automated testing, data lineage, and data governance frameworks.
        Work with Looker or Power BI for semantic layer development and dashboard delivery.
        Snowflake is our primary data warehouse. Airflow orchestrates our pipelines.
        Knowledge of machine learning model deployment and statistical analysis is preferred.
        Strong stakeholder management and Agile project management skills.""",
    },
    # Director-level -- this one signals they're building out the function properly,
    # not just hiring analysts to run reports
    {
        "job_title": "Director of Data & Analytics",
        "department": "Leadership",
        "location": "Louisville, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-12-05",
        "description": """Sazerac is seeking an experienced Director of Data & Analytics to lead our enterprise
        analytics function. You will oversee a team of data engineers, analysts, and BI developers.
        Requirements: Proven track record in building data warehouse, data lake, and data lakehouse platforms.
        Expertise in cloud data platforms (Azure, AWS, or GCP), ETL/ELT pipelines, and modern data stack.
        Must have experience with data governance, GDPR compliance, data quality programs, and data catalog initiatives.
        Champion Agile delivery methodology. Drive KPI frameworks and executive reporting through Power BI or Tableau.
        Partner with C-suite and stakeholders to define data strategy and business intelligence roadmap.
        Hands-on knowledge of SQL, Python, and BI tools. Dimensional modeling and star schema experience preferred.
        Snowflake, Databricks, or BigQuery platform experience required.""",
    },
    {
        "job_title": "Junior Data Analyst",
        "department": "Finance Analytics",
        "location": "Louisville, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-12-08",
        "description": """Entry-level Data Analyst opportunity in our Finance Analytics team.
        You will support financial reporting and KPIs using Excel, SQL, and Power BI.
        Responsibilities include data quality checks, building dashboards, and producing weekly/monthly reports.
        You will query our SQL Server data warehouse and help automate ETL processes with Python scripts.
        Exposure to Agile project management and stakeholder communication.
        Learning opportunities across data modeling, reporting, and business intelligence practices.
        Strong Excel skills required. Power BI and SQL experience preferred. R or Python is a plus.""",
    },
    {
        "job_title": "Data Science Engineer",
        "department": "Innovation & Strategy",
        "location": "Remote",
        "employment_type": "Full-Time",
        "posted_date": "2024-12-10",
        "description": """We are looking for a Data Science Engineer to join our growing Innovation team.
        You will build machine learning models and integrate them into our data pipeline infrastructure.
        Skills required: Python (pandas, scikit-learn, PySpark), SQL, Spark, and cloud (AWS or Azure or GCP).
        Experience deploying ML models via Databricks, Airflow, and integrating outputs with Snowflake.
        Strong statistical analysis background and experience with A/B testing frameworks.
        Work with data governance teams to ensure data quality and lineage across ML pipelines.
        Communicate results to stakeholders using dashboards and reporting. Power BI or Looker experience is a plus.
        Knowledge of data warehouse architecture and dimensional modeling helps in this role.""",
    },
    {
        "job_title": "Supply Chain Data Analyst",
        "department": "Supply Chain & Operations",
        "location": "Frankfort, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-12-12",
        "description": """Seeking a Supply Chain Data Analyst to provide reporting and analytics support.
        Primary tools: Excel (advanced), SQL Server, Power BI, and Python for automation.
        You will create dashboards tracking inventory, production KPIs, and logistics metrics.
        Partner with operations stakeholders to gather reporting requirements and deliver business intelligence.
        Conduct data quality checks on supply chain data feeds and maintain ETL scripts.
        Data modeling experience is a plus. Agile experience preferred.
        Familiarity with Oracle ERP or SAP data structures is highly valued.""",
    },
    # Data Governance -- the fact they're hiring this tells you their data maturity
    # is moving beyond just "get the numbers out" into proper platform thinking
    {
        "job_title": "Data Governance Analyst",
        "department": "Data & Analytics",
        "location": "Louisville, KY",
        "employment_type": "Full-Time",
        "posted_date": "2024-12-15",
        "description": """The Data Governance Analyst will champion data quality, data lineage, and
        data catalog initiatives across Sazerac's enterprise data ecosystem.
        Core responsibilities: Implement data governance frameworks using tools like Alation or Collibra.
        Conduct data quality profiling, create and enforce data quality rules, and produce data quality KPIs.
        Work closely with data engineers, SQL Server DBAs, and business stakeholders to establish data ownership.
        GDPR and regulatory compliance experience strongly preferred.
        SQL proficiency is required for data profiling and quality reporting.
        Power BI or Tableau for governance metrics dashboards. Python scripting for automation.
        Experience with data catalog, data lineage tools, and metadata management is essential.""",
    },
]


def extract_skills(text: str) -> list[str]:
    """Regex-based skill extraction from a JD string.
    Returns a list of matched skill names from SKILL_PATTERNS.
    Case-insensitive match, whole-word boundaries to reduce false positives.
    """
    found = []
    text_lower = text  # keep original case for regex matching
    for skill, pattern in SKILL_PATTERNS.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            found.append(skill)
    return found




def scrape_jobs_apify():
    """
    Pull live Sazerac job postings via Apify's website crawler.
    Uses the free tier — only fires when APIFY_TOKEN env var is set.
    Returns a list of job dicts on success, None on failure/no token.
    """
    token = os.environ.get("APIFY_TOKEN")
    if not token or not APIFY_AVAILABLE:
        return None

    print("🌐  Apify token found — attempting live job scrape...")
    try:
        client = ApifyClient(token)

        # Sazerac's public Workday careers page — data/analytics filter
        run_input = {
            "startUrls": [
                {"url": "https://sazerac.wd1.myworkdayjobs.com/en-US/Sazerac_Careers"},
                {"url": "https://www.sazerac.com/careers"},
            ],
            "maxCrawlPages": 10,
            "crawlerType": "cheerio",          # lightweight, fits free tier easily
            "maxCrawlDepth": 2,
        }

        run = client.actor("apify/website-content-crawler").call(
            run_input=run_input,
            timeout_secs=120,
        )

        raw_items = list(
            client.dataset(run["defaultDatasetId"]).iterate_items()
        )

        jobs = []
        seen_titles = set()

        for item in raw_items:
            text = item.get("text", "") or item.get("markdown", "") or ""
            url  = item.get("url", "")

            # Pull job titles from headings / list items in the page text
            title_matches = re.findall(
                r"(?:^|
)([A-Z][A-Za-z &/\-–,()]{10,80})\s*(?:
|$)",
                text
            )
            for title in title_matches:
                title = title.strip()
                # basic filter — skip navigation noise
                if any(kw in title.lower() for kw in [
                    "data", "analyst", "engineer", "analytics", "bi ",
                    "business intel", "science", "warehouse", "governance",
                ]) and title not in seen_titles:
                    seen_titles.add(title)
                    jobs.append({
                        "job_title": title,
                        "department": "Data & Analytics",
                        "location": "Louisville, KY",
                        "job_type": "Full-time",
                        "source_url": url,
                        "description": text[:500].strip(),
                        "scraped_live": True,
                    })

        if jobs:
            print(f"✅  Apify returned {len(jobs)} live job(s)")
            return jobs
        else:
            print("⚠️   Apify ran but found no matching jobs — falling back to mock data")
            return None

    except Exception as exc:
        print(f"⚠️   Apify scrape failed ({exc}) — falling back to mock data")
        return None


def get_jobs() -> list[dict]:
    """Try to scrape live job postings from sazerac.com/careers.
    Their careers page is rendered client-side (JS heavy) so the scraper
    is hit-or-miss depending on whether they've got a static fallback.
    The curated set is what runs in CI."""
    if SCRAPING_AVAILABLE:
        try:
            careers_url = "https://www.sazerac.com/careers/"
            headers = {"User-Agent": "Mozilla/5.0 (research/portfolio-project)"}
            resp = requests.get(careers_url, headers=headers, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            jobs = []
            for card in soup.select(".job-listing, .career-item, article.job"):
                title_el = card.select_one("h2, h3, .job-title")
                dept_el = card.select_one(".department, .team")
                loc_el = card.select_one(".location, .job-location")
                desc_el = card.select_one("p, .description")

                if title_el:
                    jobs.append(
                        {
                            "job_title": title_el.get_text(strip=True),
                            "department": dept_el.get_text(strip=True) if dept_el else "Unknown",
                            "location": loc_el.get_text(strip=True) if loc_el else "Louisville, KY",
                            "employment_type": "Full-Time",
                            "posted_date": datetime.utcnow().date().isoformat(),
                            "description": desc_el.get_text(strip=True) if desc_el else "",
                        }
                    )

            if len(jobs) >= 3:
                log.info("Live scrape returned %d jobs", len(jobs))
                return jobs
        except Exception as exc:
            log.warning("Jobs scrape failed (%s). Using mock data.", exc)

    log.info("Loading %d jobs from mock dataset", len(MOCK_JOBS))
    apify_result = scrape_jobs_apify()
        if apify_result:
            return apify_result
        return MOCK_JOBS


def save_jobs(jobs: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "job_id",
        "job_title",
        "department",
        "location",
        "employment_type",
        "posted_date",
        "description",
        "skills_extracted",
        "scraped_at",
    ]
    ts = datetime.utcnow().isoformat()
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for idx, job in enumerate(jobs, start=1):
            skills = extract_skills(job.get("description", ""))
            writer.writerow(
                {
                    "job_id": idx,
                    "job_title": job.get("job_title", "").strip(),
                    "department": job.get("department", "").strip(),
                    "location": job.get("location", "").strip(),
                    "employment_type": job.get("employment_type", "").strip(),
                    "posted_date": job.get("posted_date", "").strip(),
                    "description": job.get("description", "").strip(),
                    "skills_extracted": "|".join(skills),
                    "scraped_at": ts,
                }
            )
    log.info("Saved %d jobs → %s", len(jobs), path)


def build_skills_frequency(jobs: list[dict], path: str) -> None:
    """Aggregate skill counts across all JDs and write a frequency table.
    Also writes a job-skill mapping table for use as a fact table in the DB.
    The pct_of_postings column is the headline metric on the dashboard --
    e.g. SQL at 100% means every single role mentions it.
    """
    counter: Counter = Counter()
    job_skill_rows = []

    for idx, job in enumerate(jobs, start=1):
        desc = job.get("description", "")
        skills = extract_skills(desc)
        counter.update(skills)
        for skill in skills:
            job_skill_rows.append(
                {
                    "job_id": idx,
                    "job_title": job.get("job_title", ""),
                    "skill": skill,
                }
            )

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["skill", "frequency", "pct_of_postings"])
        writer.writeheader()
        total_jobs = len(jobs)
        for skill, freq in counter.most_common():
            writer.writerow(
                {
                    "skill": skill,
                    "frequency": freq,
                    "pct_of_postings": round(freq / total_jobs * 100, 1),
                }
            )

    log.info("Saved skill frequency table (%d unique skills) → %s", len(counter), path)

    # Also save job-skill mapping for fact table
    mapping_path = path.replace("job_skills.csv", "job_skill_mapping.csv")
    with open(mapping_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["job_id", "job_title", "skill"])
        writer.writeheader()
        writer.writerows(job_skill_rows)
    log.info("Saved job-skill mapping (%d rows) → %s", len(job_skill_rows), mapping_path)


def main():
    log.info("=== Job Scraper Starting ===")
    jobs = get_jobs()
    save_jobs(jobs, JOBS_OUTPUT)
    build_skills_frequency(jobs, SKILLS_OUTPUT)
    log.info("=== Job Scraper Complete ===")
    return jobs


if __name__ == "__main__":
    main()
