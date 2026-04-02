"""
scrape_market_share.py
Simulates Nielsen/IRI syndicated data output — volume share, numeric
distribution, velocity, and price tier. These are the exact metrics
CPG analysts pull from NIQ/IRI reports for Sazerac brand scorecards.
"""

import csv, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# fmt: off
MARKET_SHARE = [
    # brand, category, vol_share_pct, numeric_dist, velocity, price_tier
    ("Fireball",         "Cinnamon Whiskey", 62.4, 94.2, 48.3, "Value"),
    ("Buffalo Trace",    "American Whiskey",  8.1, 78.5, 35.2, "Premium"),
    ("Pappy Van Winkle", "American Whiskey",  0.3, 12.1, 98.7, "Ultra-Premium"),
    ("Fleischmanns",     "Vodka",             3.2, 61.4, 18.9, "Value"),
    ("Southern Comfort", "American Whiskey",  2.8, 55.3, 22.1, "Value"),
    ("Eagle Rare",       "American Whiskey",  1.9, 45.6, 31.4, "Premium"),
]
# fmt: on

FIELDS = [
    "brand",
    "category",
    "volume_share_pct",
    "numeric_distribution",
    "velocity_per_point",
    "price_tier",
]


def get_market_share():
    return [dict(zip(FIELDS, row)) for row in MARKET_SHARE]


def main():
    out = Path("data/raw")
    out.mkdir(parents=True, exist_ok=True)
    records = get_market_share()
    path = out / "market_share_raw.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(records)
    log.info("=== Market Share Scraper Complete: %d records -> %s ===", len(records), path)


if __name__ == "__main__":
    main()
