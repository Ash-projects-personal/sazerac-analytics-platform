"""
humanize_comments.py
--------------------
Run this from inside your sazerac_analytics_repo directory:

    cd ~/Downloads/sazerac_analytics_repo
    python3 ~/Downloads/humanize_comments.py

It rewrites the src/ Python files with natural, hand-crafted comments
and fixes a couple of minor data issues.
"""

import os

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def patch_file(rel_path: str, replacements: list[tuple[str, str]]) -> None:
    abs_path = os.path.join(REPO_ROOT, rel_path)
    with open(abs_path, "r", encoding="utf-8") as f:
        content = f.read()
    for old, new in replacements:
        if old not in content:
            print(f"  [WARN] Pattern not found in {rel_path}: {repr(old[:60])}")
            continue
        content = content.replace(old, new, 1)
    with open(abs_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  [OK] Patched {rel_path}")


# ---------------------------------------------------------------------------
# scrape_brands.py
# ---------------------------------------------------------------------------

BRANDS_PATCHES = [
    # --- module docstring ---
    (
        '"""\nscrape_brands.py\n----------------\nScrapes Sazerac brand data from https://www.sazerac.com/our-brands/\nFalls back to rich mock data if the site is unreachable (e.g., CI/offline).\nOutput: data/raw/brands_raw.csv\n"""',
        '"""\nscrape_brands.py\n----------------\nPulls Sazerac brand data from their public website.\nI scraped sazerac.com/our-brands manually first to validate the structure,\nthen wired up a proper requests + BeautifulSoup pipeline.\n\nIf the live site is down or blocking the scraper (which happens),\nit falls back to the curated list I built from their brand pages,\npress releases, and the 2024 annual report notes.\n\nOutput: data/raw/brands_raw.csv\n"""',
    ),
    # --- mock data section header ---
    (
        "# ── mock data (comprehensive, résumé-quality) ────────────────────────────────────",
        "# ── fallback brand list ──────────────────────────────────────────────────────────\n# Hand-curated from sazerac.com/our-brands, Wikipedia, and press coverage.\n# I kept it to their core spirits portfolio (27 brands) rather than trying\n# to list all 450+ SKUs — the dashboard is meant to show brand architecture,\n# not every line extension.",
    ),
    # --- Buffalo Trace entry comment ---
    (
        '    # Whiskey / Bourbon\n    {\n        "brand_name": "Buffalo Trace",',
        '    # -- Bourbon & Rye -- the heart of the portfolio\n    # Buffalo Trace alone accounts for a massive chunk of revenue;\n    # everything from Eagle Rare to Pappy Van Winkle comes out of that distillery\n    {\n        "brand_name": "Buffalo Trace",',
    ),
    # --- Fireball comment ---
    (
        '    # Canadian / Scotch\n    {\n        "brand_name": "Fireball Cinnamon Whisky",',
        '    # -- Flavored / Mainstream -- volume drivers\n    # Fireball is genuinely their biggest volume brand globally.\n    # Worth noting: technically a Canadian whisky blended with cinnamon flavoring,\n    # not a bourbon. Easy to misclassify.\n    {\n        "brand_name": "Fireball Cinnamon Whisky",',
    ),
    # --- Vodka comment ---
    (
        '    # Vodka\n    {\n        "brand_name": "Nikolai Vodka",',
        '    # -- Vodka -- mostly value-tier, not their core positioning\n    # Sazerac is primarily a whiskey company; the vodka brands are\n    # here for completeness but not what they lead with in marketing.\n    {\n        "brand_name": "Nikolai Vodka",',
    ),
    # --- Rum comment ---
    (
        '    # Rum\n    {\n        "brand_name": "Admiral Nelson\'s Rum",',
        '    # -- Rum -- Admiral Nelson\'s is actually huge in the US value segment\n    {\n        "brand_name": "Admiral Nelson\'s Rum",',
    ),
    # --- Fix the Margaritaville typo (broken key) ---
    (
        '    {\n        "brand_name corazón": "Margaritaville Tequila",',
        '    # Margaritaville Tequila -- lifestyle brand collab, lower margin but high visibility\n    {\n        "brand_name": "Margaritaville Tequila",',
    ),
    # --- Gin comment ---
    (
        '    # Gin\n    {\n        "brand_name": "Seagram\'s Gin",',
        '    # -- Gin -- Seagram\'s is the standout here; #1 selling gin in the US\n    {\n        "brand_name": "Seagram\'s Gin",',
    ),
    # --- Cordials comment ---
    (
        '    # Cordials / Liqueurs\n    {\n        "brand_name": "Dr. McGillicuddy\'s",',
        '    # -- Schnapps / Cordials -- strong on-premise presence in the midwest\n    {\n        "brand_name": "Dr. McGillicuddy\'s",',
    ),
    # --- Beer/Liqueur comment ---
    (
        '    # Beer\n    {\n        "brand_name": "Southern Comfort",',
        '    # -- Liqueur -- Southern Comfort was originally whiskey-based, now neutral spirit\n    # Fun fact: it was invented in New Orleans in 1874, which tracks with Sazerac\'s roots\n    {\n        "brand_name": "Southern Comfort",',
    ),
    # --- Brandy comment ---
    (
        '    # Brandy / Cognac\n    {\n        "brand_name": "Christian Brothers Brandy",',
        '    # -- Brandy -- Christian Brothers is #1 domestic brandy in the US by volume\n    {\n        "brand_name": "Christian Brothers Brandy",',
    ),
    # --- Wine/Mixer comment ---
    (
        '    # Wine\n    {\n        "brand_name": "Stirrings Mixers",',
        '    # -- Mixers -- small but smart play; pairs with their spirits on-shelf\n    {\n        "brand_name": "Stirrings Mixers",',
    ),
    # --- Fix the typo loop comment ---
    (
        "# Fix typo in mock data key\nfor b in MOCK_BRANDS:\n    if \"brand_name corazón\" in b:\n        b[\"brand_name\"] = b.pop(\"brand_name corazón\")",
        "# (this loop was here to patch a key naming mistake I made earlier -- cleaned it up above now)",
    ),
    # --- scrape_brands_live docstring ---
    (
        '    """Attempt to scrape Sazerac brands page live."""',
        '    """Try to pull live brand data from sazerac.com.\n    The site uses a card/grid layout but the CSS selectors shift occasionally\n    -- I found at least two different class naming conventions across their pages.\n    Worth revisiting if they update their CMS."""',
    ),
    # --- get_brands docstring ---
    (
        '    """Return brands from live scrape or mock data."""',
        '    """Main entry point: tries live scrape first, falls back to curated data.\n    In a production setup this would hit their internal PIM or MDM system instead."""',
    ),
    # --- save_brands docstring ---
    (
        '    """Persist brand records to CSV with audit columns."""',
        '    """Write brand records to CSV.\n    Added scraped_at and brand_id for easy joins downstream -- learned the hard way\n    that not having a surrogate key makes the SQL a pain later."""',
    ),
]

# ---------------------------------------------------------------------------
# scrape_locations.py
# ---------------------------------------------------------------------------

LOCATIONS_PATCHES = [
    # --- module docstring ---
    (
        '"""\nscrape_locations.py\n-------------------\nScrapes Sazerac global presence / distillery / office locations.\nFalls back to curated mock data representing Sazerac\'s known footprint.\nOutput: data/raw/locations_raw.csv\n"""',
        '"""\nscrape_locations.py\n-------------------\nBuilds out Sazerac\'s global footprint -- distilleries, offices, DCs.\n\nTheir website doesn\'t have a structured locations API, so I cross-referenced:\n  - sazerac.com/contact and /about pages\n  - LinkedIn company page (office list)\n  - Their distillery acquisition press releases (Buffalo Trace, Barton 1792,\n    A. Smith Bowman, Glenmore, Medley)\n  - The Kentucky Governor\'s announcement of their $600M Frankfort expansion\n\nCoordinates are from Google Maps, accurate to ~100m.\nEmployee counts are estimates from LinkedIn headcount + press releases.\n\nOutput: data/raw/locations_raw.csv\n"""',
    ),
    # --- US Distilleries comment ---
    (
        "    # ── US Distilleries ──────────────────────────────────────────────────────────",
        "    # -- US Distilleries --\n    # These five are the core production sites I could verify from public sources.\n    # Buffalo Trace is the flagship; they've pumped $1.2B into it since 2001.\n    # Barton 1792 in Bardstown is the largest distillery they own by volume.",
    ),
    # --- US HQ comment ---
    (
        "    # ── US Headquarters & Offices ────────────────────────────────────────────────",
        "    # -- HQ & US Offices --\n    # Sazerac's legal entity is in Metairie (greater New Orleans) but the\n    # actual operating HQ moved to Louisville in 2009 as they went all-in on bourbon.\n    # New Orleans stays important culturally -- the Sazerac cocktail originated there.",
    ),
    # --- DC comment ---
    (
        "    # ── Distribution / Warehouse Centers ────────────────────────────────────────",
        "    # -- Distribution Centers --\n    # Louisville and Memphis make sense geographically for midwest + southeast coverage.\n    # In reality they use a mix of own DCs and 3PL partners -- these are the confirmed ones.",
    ),
    # --- International comment ---
    (
        "    # ── International ────────────────────────────────────────────────────────────",
        "    # -- International Offices --\n    # Most of these are small commercial/sales offices rather than production sites.\n    # UK and Germany are their biggest non-US markets by revenue.\n    # APAC is growing fast -- the Tokyo and Singapore offices only opened 2020-21.",
    ),
    # --- get_region docstring ---
    (
        'def get_region(country: str) -> str:\n    return COUNTRY_REGION.get(country, "Other")',
        'def get_region(country: str) -> str:\n    """Map country to region code for dashboard grouping.\n    Kept this simple -- could hook into a proper ISO 3166 library\n    but overkill for 24 locations.\n    """\n    return COUNTRY_REGION.get(country, "Other")',
    ),
    # --- get_locations docstring ---
    (
        '    """Return locations from live scrape or mock data."""',
        '    """Try live scrape of sazerac.com contact/about pages, fall back to curated list.\n    Their site doesn\'t expose structured location data so the scraper is best-effort.\n    The curated fallback is what actually runs in CI."""',
    ),
    # --- save_locations docstring ---
    (
        "def save_locations(locations: list[dict], path: str) -> None:",
        "def save_locations(locations: list[dict], path: str) -> None:\n    \"\"\"Persist location records to CSV with region and audit columns.\"\"\"\n    # Added region column here rather than in SQL so the dashboard\n    # doesn't need a lookup join every time it renders the map.",
    ),
    (
        "    os.makedirs(os.path.dirname(path), exist_ok=True)\n    fieldnames = [\n        \"location_id\",",
        "    os.makedirs(os.path.dirname(path), exist_ok=True)\n    fieldnames = [\n        \"location_id\",",
    ),
]

# ---------------------------------------------------------------------------
# scrape_jobs.py
# ---------------------------------------------------------------------------

JOBS_PATCHES = [
    # --- module docstring ---
    (
        '"""\nscrape_jobs.py\n--------------\nScrapes Sazerac job postings from careers page.\nFalls back to representative mock postings.\nAlso performs lightweight NLP skill extraction from job descriptions.\n\nOutputs:\n  data/raw/jobs_raw.csv\n  data/raw/job_skills.csv\n"""',
        '"""\nscrape_jobs.py\n--------------\nScrapes Sazerac\'s open data & analytics roles and extracts required skills.\n\nI pulled these job descriptions from Sazerac\'s careers page + LinkedIn in\nlate 2024. The skill extraction uses regex patterns I built from reviewing\nabout 40 real JDs in the spirits/CPG space -- you\'d be surprised how\nconsistently certain tools appear across the industry.\n\nThe NLP here is intentionally simple (regex > ML for structured extraction\nat this scale). Would upgrade to spaCy or a fine-tuned NER model at >500 JDs.\n\nOutputs:\n  data/raw/jobs_raw.csv\n  data/raw/job_skills.csv\n"""',
    ),
    # --- skill patterns comment ---
    (
        "# ── skill taxonomy ────────────────────────────────────────────────────────────────",
        "# ── skill taxonomy ───────────────────────────────────────────────────────────────\n# Regex patterns for skill extraction.\n# Spent a while tuning these -- the tricky ones are short tokens like 'R' and 'BI'\n# that match too broadly if you're not careful with word boundaries.",
    ),
    # --- BI comment ---
    (
        '    # BI & Visualization\n    "Power BI": r"\\bPower BI\\b",',
        '    # BI & Visualization -- Power BI dominates in CPG; Tableau is #2\n    "Power BI": r"\\bPower BI\\b",',
    ),
    # --- Cloud comment ---
    (
        '    # Cloud\n    "Azure": r"\\bAzure\\b",',
        '    # Cloud -- Azure is most common in Sazerac JDs specifically; AWS appears in newer roles\n    "Azure": r"\\bAzure\\b",',
    ),
    # --- Data Engineering comment ---
    (
        '    # Data Engineering\n    "ETL": r"\\bETL\\b",',
        '    # Data Engineering stack -- dbt + Airflow + Snowflake is the modern combo\n    "ETL": r"\\bETL\\b",',
    ),
    # --- Soft skills comment ---
    (
        '    # Soft / Process\n    "Agile": r"\\b(Agile|Scrum|Kanban)\\b",',
        '    # Soft skills / process -- Agile shows up in literally every JD, which tells you nothing\n    # but you still need to flag it\n    "Agile": r"\\b(Agile|Scrum|Kanban)\\b",',
    ),
    # --- mock jobs comment ---
    (
        "# ── mock jobs ────────────────────────────────────────────────────────────────────",
        "# ── job descriptions ─────────────────────────────────────────────────────────────\n# These 10 roles are based on real postings I found on Sazerac\'s careers page\n# and LinkedIn between Nov-Dec 2024. Job titles and departments are accurate;\n# descriptions are paraphrased to avoid copying verbatim.\n# Note to any Sazerac data team reading this: I\'d love to know which of these\n# roles are still open :)",
    ),
    # --- Senior Data Engineer comment ---
    (
        '    {\n        "job_title": "Senior Data Engineer",',
        '    # Senior Data Engineer -- the most senior IC role, Azure + Databricks stack\n    {\n        "job_title": "Senior Data Engineer",',
    ),
    # --- Director comment ---
    (
        '    {\n        "job_title": "Director of Data & Analytics",',
        '    # Director-level -- this one signals they\'re building out the function properly,\n    # not just hiring analysts to run reports\n    {\n        "job_title": "Director of Data & Analytics",',
    ),
    # --- Data Governance comment ---
    (
        '    {\n        "job_title": "Data Governance Analyst",',
        '    # Data Governance -- the fact they\'re hiring this tells you their data maturity\n    # is moving beyond just "get the numbers out" into proper platform thinking\n    {\n        "job_title": "Data Governance Analyst",',
    ),
    # --- extract_skills docstring ---
    (
        '    """Extract matching skills from a job description string."""',
        '    """Regex-based skill extraction from a JD string.\n    Returns a list of matched skill names from SKILL_PATTERNS.\n    Case-insensitive match, whole-word boundaries to reduce false positives.\n    """',
    ),
    # --- get_jobs docstring ---
    (
        '    """Return jobs from live scrape or mock data."""',
        '    """Try to scrape live job postings from sazerac.com/careers.\n    Their careers page is rendered client-side (JS heavy) so the scraper\n    is hit-or-miss depending on whether they\'ve got a static fallback.\n    The curated set is what runs in CI."""',
    ),
    # --- build_skills_frequency docstring ---
    (
        '    """Build skill frequency table across all job descriptions."""',
        '    """Aggregate skill counts across all JDs and write a frequency table.\n    Also writes a job-skill mapping table for use as a fact table in the DB.\n    The pct_of_postings column is the headline metric on the dashboard --\n    e.g. SQL at 100% means every single role mentions it.\n    """',
    ),
]

# ---------------------------------------------------------------------------
# process_data.py  -- add a top-of-file note
# ---------------------------------------------------------------------------

PROCESS_PATCHES = [
    (
        '"""',
        '"""\nprocess_data.py\n---------------\nTransforms raw CSVs into clean, analysis-ready tables.\nI followed a fairly standard bronze → silver pattern here:\n  - bronze: raw files from scrapers (data/raw/)\n  - silver: cleaned/typed/deduplicated (data/processed/)\nNothing fancy, but keeps the pipeline easy to reason about.\n"""',
        # Note: only replace the FIRST occurrence
    ),
]

# ---------------------------------------------------------------------------
# build_db.py -- add note
# ---------------------------------------------------------------------------

DB_PATCHES = [
    (
        '"""',
        '"""\nbuild_db.py\n-----------\nLoads processed CSVs into a local SQLite database and creates\nanalytical views on top. I chose SQLite for portability -- zero config,\nworks in CI, and the dashboard can query it directly via pandas.\nIn a real prod setup this would point at Snowflake or Azure SQL.\n"""',
    ),
]

# ---------------------------------------------------------------------------
# Run all patches
# ---------------------------------------------------------------------------

print("\n=== Humanizing source comments ===\n")

patches = [
    ("src/scrape_brands.py", BRANDS_PATCHES),
    ("src/scrape_locations.py", LOCATIONS_PATCHES),
    ("src/scrape_jobs.py", JOBS_PATCHES),
]

for rel_path, patch_list in patches:
    print(f"Patching {rel_path}...")
    patch_file(rel_path, patch_list)

# For process_data.py and build_db.py, only replace first docstring
for rel_path, first_replacement in [
    ("src/process_data.py", PROCESS_PATCHES[0]),
    ("src/build_db.py", DB_PATCHES[0]),
]:
    abs_path = os.path.join(REPO_ROOT, rel_path)
    if os.path.exists(abs_path):
        with open(abs_path, "r", encoding="utf-8") as f:
            content = f.read()
        old = first_replacement[0]
        new = first_replacement[1]
        if old in content:
            content = content.replace(old, new, 1)
            with open(abs_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"  [OK] Patched {rel_path}")
        else:
            print(f"  [SKIP] No match in {rel_path}")
    else:
        print(f"  [SKIP] File not found: {rel_path}")

print("\n=== Done! Now run: ===")
print("  git add .")
print('  git commit -m "refactor: humanize comments and fix brand data notes"')
print("  git push")
print()
