"""
scrape_locations.py
-------------------
Scrapes Sazerac global presence / distillery / office locations.
Falls back to curated mock data representing Sazerac's known footprint.

Output: data/raw/locations_raw.csv
"""

import csv
import logging
import os
from datetime import datetime

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
        logging.FileHandler("logs/scrape_locations.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

RAW_OUTPUT = "data/raw/locations_raw.csv"

# Region lookup helper
COUNTRY_REGION = {
    "United States": "Americas",
    "Canada": "Americas",
    "Mexico": "Americas",
    "Brazil": "Americas",
    "Argentina": "Americas",
    "Chile": "Americas",
    "Colombia": "Americas",
    "Peru": "Americas",
    "United Kingdom": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Italy": "Europe",
    "Spain": "Europe",
    "Netherlands": "Europe",
    "Sweden": "Europe",
    "Poland": "Europe",
    "Belgium": "Europe",
    "Australia": "APAC",
    "Japan": "APAC",
    "China": "APAC",
    "South Korea": "APAC",
    "India": "APAC",
    "Singapore": "APAC",
    "New Zealand": "APAC",
    "Taiwan": "APAC",
    "South Africa": "EMEA",
    "UAE": "EMEA",
    "Israel": "EMEA",
}

MOCK_LOCATIONS = [
    # ── US Distilleries ────────────────────────────────────────────────────────
    {
        "location_name": "Buffalo Trace Distillery",
        "city": "Frankfort",
        "state_province": "Kentucky",
        "country": "United States",
        "location_type": "Distillery",
        "latitude": 38.2009,
        "longitude": -84.8733,
        "year_established": 1773,
        "employee_count": 450,
    },
    {
        "location_name": "Barton 1792 Distillery",
        "city": "Bardstown",
        "state_province": "Kentucky",
        "country": "United States",
        "location_type": "Distillery",
        "latitude": 37.8090,
        "longitude": -85.4669,
        "year_established": 1879,
        "employee_count": 300,
    },
    {
        "location_name": "Glenmore Distillery",
        "city": "Owensboro",
        "state_province": "Kentucky",
        "country": "United States",
        "location_type": "Distillery",
        "latitude": 37.7719,
        "longitude": -87.1112,
        "year_established": 1901,
        "employee_count": 200,
    },
    {
        "location_name": "Medley Distillery",
        "city": "Owensboro",
        "state_province": "Kentucky",
        "country": "United States",
        "location_type": "Distillery",
        "latitude": 37.7730,
        "longitude": -87.1150,
        "year_established": 1940,
        "employee_count": 120,
    },
    {
        "location_name": "A. Smith Bowman Distillery",
        "city": "Fredericksburg",
        "state_province": "Virginia",
        "country": "United States",
        "location_type": "Distillery",
        "latitude": 38.3032,
        "longitude": -77.4605,
        "year_established": 1934,
        "employee_count": 80,
    },
    # ── US Headquarters & Offices ──────────────────────────────────────────────
    {
        "location_name": "Sazerac Company Headquarters",
        "city": "Louisville",
        "state_province": "Kentucky",
        "country": "United States",
        "location_type": "Headquarters",
        "latitude": 38.2527,
        "longitude": -85.7585,
        "year_established": 1850,
        "employee_count": 600,
    },
    {
        "location_name": "Sazerac New Orleans Office",
        "city": "New Orleans",
        "state_province": "Louisiana",
        "country": "United States",
        "location_type": "Office",
        "latitude": 29.9511,
        "longitude": -90.0715,
        "year_established": 1989,
        "employee_count": 150,
    },
    {
        "location_name": "Sazerac Atlanta Regional Office",
        "city": "Atlanta",
        "state_province": "Georgia",
        "country": "United States",
        "location_type": "Office",
        "latitude": 33.7490,
        "longitude": -84.3880,
        "year_established": 2010,
        "employee_count": 75,
    },
    {
        "location_name": "Sazerac Chicago Regional Office",
        "city": "Chicago",
        "state_province": "Illinois",
        "country": "United States",
        "location_type": "Office",
        "latitude": 41.8781,
        "longitude": -87.6298,
        "year_established": 2008,
        "employee_count": 90,
    },
    {
        "location_name": "Sazerac New York Regional Office",
        "city": "New York City",
        "state_province": "New York",
        "country": "United States",
        "location_type": "Office",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "year_established": 2005,
        "employee_count": 110,
    },
    {
        "location_name": "Sazerac Los Angeles Regional Office",
        "city": "Los Angeles",
        "state_province": "California",
        "country": "United States",
        "location_type": "Office",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "year_established": 2012,
        "employee_count": 85,
    },
    {
        "location_name": "Sazerac Dallas Regional Office",
        "city": "Dallas",
        "state_province": "Texas",
        "country": "United States",
        "location_type": "Office",
        "latitude": 32.7767,
        "longitude": -96.7970,
        "year_established": 2015,
        "employee_count": 60,
    },
    # ── Distribution / Warehouse Centers ──────────────────────────────────────
    {
        "location_name": "Sazerac Distribution Center — Louisville",
        "city": "Louisville",
        "state_province": "Kentucky",
        "country": "United States",
        "location_type": "Distribution Center",
        "latitude": 38.2200,
        "longitude": -85.7500,
        "year_established": 2000,
        "employee_count": 250,
    },
    {
        "location_name": "Sazerac Distribution Center — Memphis",
        "city": "Memphis",
        "state_province": "Tennessee",
        "country": "United States",
        "location_type": "Distribution Center",
        "latitude": 35.1495,
        "longitude": -90.0490,
        "year_established": 2003,
        "employee_count": 180,
    },
    # ── International ──────────────────────────────────────────────────────────
    {
        "location_name": "Sazerac UK Office",
        "city": "London",
        "state_province": "England",
        "country": "United Kingdom",
        "location_type": "International Office",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "year_established": 2016,
        "employee_count": 45,
    },
    {
        "location_name": "Sazerac Germany Office",
        "city": "Hamburg",
        "state_province": "Hamburg",
        "country": "Germany",
        "location_type": "International Office",
        "latitude": 53.5511,
        "longitude": 9.9937,
        "year_established": 2018,
        "employee_count": 30,
    },
    {
        "location_name": "Sazerac France Office",
        "city": "Paris",
        "state_province": "Île-de-France",
        "country": "France",
        "location_type": "International Office",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "year_established": 2017,
        "employee_count": 25,
    },
    {
        "location_name": "Sazerac Australia Office",
        "city": "Sydney",
        "state_province": "New South Wales",
        "country": "Australia",
        "location_type": "International Office",
        "latitude": -33.8688,
        "longitude": 151.2093,
        "year_established": 2019,
        "employee_count": 20,
    },
    {
        "location_name": "Sazerac Japan Office",
        "city": "Tokyo",
        "state_province": "Tokyo",
        "country": "Japan",
        "location_type": "International Office",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "year_established": 2020,
        "employee_count": 15,
    },
    {
        "location_name": "Sazerac Canada Office",
        "city": "Toronto",
        "state_province": "Ontario",
        "country": "Canada",
        "location_type": "International Office",
        "latitude": 43.6532,
        "longitude": -79.3832,
        "year_established": 2014,
        "employee_count": 35,
    },
    {
        "location_name": "Sazerac Mexico Office",
        "city": "Mexico City",
        "state_province": "CDMX",
        "country": "Mexico",
        "location_type": "International Office",
        "latitude": 19.4326,
        "longitude": -99.1332,
        "year_established": 2018,
        "employee_count": 28,
    },
    {
        "location_name": "Sazerac Singapore Office",
        "city": "Singapore",
        "state_province": "Central Region",
        "country": "Singapore",
        "location_type": "International Office",
        "latitude": 1.3521,
        "longitude": 103.8198,
        "year_established": 2021,
        "employee_count": 12,
    },
    {
        "location_name": "Sazerac Brazil Office",
        "city": "São Paulo",
        "state_province": "São Paulo",
        "country": "Brazil",
        "location_type": "International Office",
        "latitude": -23.5505,
        "longitude": -46.6333,
        "year_established": 2022,
        "employee_count": 18,
    },
    {
        "location_name": "Sazerac South Africa Office",
        "city": "Cape Town",
        "state_province": "Western Cape",
        "country": "South Africa",
        "location_type": "International Office",
        "latitude": -33.9249,
        "longitude": 18.4241,
        "year_established": 2023,
        "employee_count": 10,
    },
]


def get_region(country: str) -> str:
    """Map country to region code for dashboard grouping.
    Kept this simple -- could hook into a proper ISO 3166 library
    but overkill for 24 locations.
    """
    return COUNTRY_REGION.get(country, "Other")


def get_locations() -> list[dict]:
    """Try live scrape of sazerac.com contact/about pages, fall back to curated list.
    Their site doesn't expose structured location data so the scraper is best-effort.
    The curated fallback is what actually runs in CI."""
    if SCRAPING_AVAILABLE:
        try:
            urls = [
                "https://www.sazerac.com/contact/",
                "https://www.sazerac.com/about/",
            ]
            headers = {"User-Agent": "Mozilla/5.0 (research/portfolio-project)"}
            locations = []
            for url in urls:
                resp = requests.get(url, headers=headers, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                # Parse address blocks
                for addr in soup.select("address, .location, .office-card"):
                    loc_name = addr.select_one("h3, h4, strong")
                    if loc_name:
                        locations.append(
                            {
                                "location_name": loc_name.get_text(strip=True),
                                "city": "",
                                "state_province": "",
                                "country": "United States",
                                "location_type": "Office",
                                "latitude": None,
                                "longitude": None,
                                "year_established": None,
                                "employee_count": None,
                            }
                        )
            if len(locations) >= 3:
                log.info("Live scrape returned %d locations", len(locations))
                return locations
        except Exception as exc:
            log.warning("Location scrape failed (%s). Using mock data.", exc)

    log.info("Loading %d locations from mock dataset", len(MOCK_LOCATIONS))
    return MOCK_LOCATIONS


def save_locations(locations: list[dict], path: str) -> None:
    """Persist location records to CSV with region and audit columns."""
    # Added region column here rather than in SQL so the dashboard
    # doesn't need a lookup join every time it renders the map.
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "location_id",
        "location_name",
        "city",
        "state_province",
        "country",
        "region",
        "location_type",
        "latitude",
        "longitude",
        "year_established",
        "employee_count",
        "scraped_at",
    ]
    ts = datetime.utcnow().isoformat()
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for idx, loc in enumerate(locations, start=1):
            writer.writerow(
                {
                    "location_id": idx,
                    "location_name": loc.get("location_name", "").strip(),
                    "city": loc.get("city", "").strip(),
                    "state_province": loc.get("state_province", "").strip(),
                    "country": loc.get("country", "").strip(),
                    "region": get_region(loc.get("country", "")),
                    "location_type": loc.get("location_type", "Office").strip(),
                    "latitude": loc.get("latitude", ""),
                    "longitude": loc.get("longitude", ""),
                    "year_established": loc.get("year_established", ""),
                    "employee_count": loc.get("employee_count", ""),
                    "scraped_at": ts,
                }
            )
    log.info("Saved %d locations → %s", len(locations), path)


def main():
    log.info("=== Location Scraper Starting ===")
    locations = get_locations()
    save_locations(locations, RAW_OUTPUT)
    log.info("=== Location Scraper Complete ===")
    return locations


if __name__ == "__main__":
    main()
