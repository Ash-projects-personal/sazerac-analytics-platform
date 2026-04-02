"""
scrape_depletions.py
Simulates VIP/iDig-style monthly case depletions — the core data type
Sazerac BI analysts work with daily. 12 months x 6 brands x 10 states.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

BRANDS = [
    ("Fireball", "Cinnamon Whiskey", "Value"),
    ("Buffalo Trace", "American Whiskey", "Premium"),
    ("Pappy Van Winkle", "American Whiskey", "Ultra-Premium"),
    ("Fleischmanns", "Vodka", "Value"),
    ("Southern Comfort", "American Whiskey", "Value"),
    ("Eagle Rare", "American Whiskey", "Premium"),
]

STATES = [
    ("CA", "California", False),
    ("TX", "Texas", False),
    ("FL", "Florida", False),
    ("NY", "New York", False),
    ("PA", "Pennsylvania", True),
    ("OH", "Ohio", True),
    ("IL", "Illinois", False),
    ("NC", "North Carolina", True),
    ("WA", "Washington", True),
    ("TN", "Tennessee", False),
]

BASE_CASES = {
    "Fireball": 45000,
    "Buffalo Trace": 18000,
    "Pappy Van Winkle": 800,
    "Fleischmanns": 22000,
    "Southern Comfort": 12000,
    "Eagle Rare": 5500,
}

SEASONALITY = {
    1: 0.82,
    2: 0.78,
    3: 0.88,
    4: 0.92,
    5: 0.95,
    6: 0.98,
    7: 1.05,
    8: 1.02,
    9: 0.98,
    10: 1.08,
    11: 1.25,
    12: 1.35,
}

TREND = {
    "Fireball": 1.010,
    "Buffalo Trace": 1.025,
    "Pappy Van Winkle": 1.030,
    "Fleischmanns": 0.995,
    "Southern Comfort": 0.980,
    "Eagle Rare": 1.040,
}

FIELDS = [
    "year",
    "month",
    "month_label",
    "brand",
    "category",
    "price_tier",
    "state_code",
    "state_name",
    "is_control_state",
    "channel_on_premise",
    "channel_off_premise",
    "total_cases",
]


def get_depletions():
    records = []
    today = datetime.today()
    for m_back in range(11, -1, -1):
        month_num = (today.month - m_back - 1) % 12 + 1
        yr_offset = (today.month - m_back - 1) // 12
        year = today.year - yr_offset
        label = datetime(year, month_num, 1).strftime("%b %Y")
        for brand, category, tier in BRANDS:
            base = BASE_CASES[brand]
            seasonal = SEASONALITY[month_num]
            trend = TREND[brand] ** (11 - m_back)
            for state_code, state_name, is_control in STATES:
                ctrl = 0.70 if is_control else 1.0
                state_f = (abs(hash(state_code + brand)) % 40) / 100 + 0.80
                total = int(base * seasonal * trend * ctrl * state_f / len(STATES))
                records.append(
                    {
                        "year": year,
                        "month": month_num,
                        "month_label": label,
                        "brand": brand,
                        "category": category,
                        "price_tier": tier,
                        "state_code": state_code,
                        "state_name": state_name,
                        "is_control_state": int(is_control),
                        "channel_on_premise": int(total * 0.22),
                        "channel_off_premise": int(total * 0.78),
                        "total_cases": total,
                    }
                )
    return records


def main():
    out = Path("data/raw")
    out.mkdir(parents=True, exist_ok=True)
    records = get_depletions()
    path = out / "depletions_raw.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(records)
    log.info("=== Depletion Scraper Complete: %d records -> %s ===", len(records), path)


if __name__ == "__main__":
    main()
