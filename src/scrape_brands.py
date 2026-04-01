"""
scrape_brands.py
----------------
Scrapes Sazerac brand data from https://www.sazerac.com/our-brands/
Falls back to rich mock data if the site is unreachable (e.g., CI/offline).

Output: data/raw/brands_raw.csv
"""

import csv
import json
import logging
import os
import time
from datetime import datetime

# ── optional scraping imports ──────────────────────────────────────────────────
try:
    import requests
    from bs4 import BeautifulSoup
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False

# ── logging setup ──────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/scrape_brands.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── constants ──────────────────────────────────────────────────────────────────
BASE_URL = "https://www.sazerac.com/our-brands/"
RAW_OUTPUT = "data/raw/brands_raw.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (research/portfolio-project)"}

# ── mock data (comprehensive, résumé-quality) ──────────────────────────────────
MOCK_BRANDS = [
    # Whiskey / Bourbon
    {"brand_name": "Buffalo Trace", "category": "Bourbon Whiskey",
     "description": "One of the most award-winning distilleries in the world, Buffalo Trace produces "
                    "America's finest bourbon. Located in Frankfort, KY, the distillery has been making "
                    "whiskey continuously since 1773.",
     "url": "https://www.buffalotracedistillery.com", "is_flagship": True},
    {"brand_name": "Eagle Rare", "category": "Bourbon Whiskey",
     "description": "Eagle Rare 10 Year Old Single Barrel Bourbon is an award-winning bourbon that offers "
                    "a complex aroma of dry oak, with notes of toffee and hints of orange peel.",
     "url": "https://www.eaglerarewhiskey.com", "is_flagship": True},
    {"brand_name": "Blanton's", "category": "Bourbon Whiskey",
     "description": "Blanton's is the original single barrel bourbon whiskey, introduced in 1984. "
                    "Blanton's pioneered the single barrel revolution and is recognized worldwide.",
     "url": "https://www.blantonsbourbon.com", "is_flagship": True},
    {"brand_name": "Pappy Van Winkle", "category": "Bourbon Whiskey",
     "description": "Pappy Van Winkle's Family Reserve is one of the most sought-after and critically "
                    "acclaimed bourbon whiskeys in the world, aged 15, 20, and 23 years.",
     "url": "https://www.oldripvanwinkle.com", "is_flagship": True},
    {"brand_name": "W.L. Weller", "category": "Bourbon Whiskey",
     "description": "W.L. Weller is the original wheated bourbon, using wheat as the secondary grain "
                    "instead of rye, producing a softer, more approachable spirit.",
     "url": "https://www.wlwellerbourbon.com", "is_flagship": False},
    {"brand_name": "Sazerac Rye", "category": "Rye Whiskey",
     "description": "Sazerac Rye is the defining ingredient in the classic Sazerac cocktail. "
                    "Produced at Buffalo Trace Distillery, it's America's oldest rye whiskey brand.",
     "url": "https://www.sazeracwhiskey.com", "is_flagship": True},
    {"brand_name": "Thomas H. Handy", "category": "Rye Whiskey",
     "description": "Thomas H. Handy Sazerac is an uncut, unfiltered rye whiskey, part of the "
                    "Buffalo Trace Antique Collection released annually.",
     "url": "https://www.buffalotracedistillery.com/brands/thomas-h-handy", "is_flagship": False},
    {"brand_name": "1792 Bourbon", "category": "Bourbon Whiskey",
     "description": "1792 Bourbon is produced at Barton 1792 Distillery in Bardstown, KY. "
                    "Known for its high rye content and distinctive spicy character.",
     "url": "https://www.1792bourbon.com", "is_flagship": False},
    {"brand_name": "Benchmark", "category": "Bourbon Whiskey",
     "description": "Benchmark Bourbon is an accessible, everyday bourbon produced at Buffalo Trace. "
                    "It delivers quality Kentucky straight bourbon at an outstanding value.",
     "url": "https://www.benchmarkbourbon.com", "is_flagship": False},
    {"brand_name": "Ancient Age", "category": "Bourbon Whiskey",
     "description": "Ancient Age is a value-priced Kentucky straight bourbon whiskey produced at "
                    "Buffalo Trace Distillery, aged a minimum of 36 months.",
     "url": "https://www.sazerac.com/brands/ancient-age", "is_flagship": False},
    {"brand_name": "Elmer T. Lee", "category": "Bourbon Whiskey",
     "description": "Named after the legendary Buffalo Trace master distiller who pioneered single barrel "
                    "bourbon in 1984, Elmer T. Lee is a premium single barrel bourbon.",
     "url": "https://www.elmertlee.com", "is_flagship": False},
    # Canadian / Scotch
    {"brand_name": "Fireball Cinnamon Whisky", "category": "Flavored Whisky",
     "description": "Fireball Cinnamon Whisky is the best-selling flavored whisky in the US. "
                    "Blended Canadian whisky with a cinnamon flavor and a fiery kick.",
     "url": "https://www.fireballwhisky.com", "is_flagship": True},
    {"brand_name": "Fleischmann's", "category": "Whiskey",
     "description": "Fleischmann's is a classic American blended whiskey known for its smooth, "
                    "mellow character and consistent quality at an accessible price point.",
     "url": "https://www.sazerac.com/brands/fleischmanns", "is_flagship": False},
    # Vodka
    {"brand_name": "Nikolai Vodka", "category": "Vodka",
     "description": "Nikolai Vodka is a value vodka produced for the everyday consumer, offering "
                    "consistent quality at an affordable price point across US markets.",
     "url": "https://www.sazerac.com/brands/nikolai", "is_flagship": False},
    {"brand_name": "Platinum 7X Vodka", "category": "Vodka",
     "description": "Platinum 7X is a seven-times distilled vodka that delivers an ultra-smooth "
                    "taste, crafted for consumers seeking premium quality at a competitive price.",
     "url": "https://www.platinum7xvodka.com", "is_flagship": False},
    {"brand_name": "Rain Vodka", "category": "Vodka",
     "description": "Rain Organic Vodka is made with organically grown white corn and distilled "
                    "eight times for exceptional purity and a clean, crisp taste.",
     "url": "https://www.rainvodka.com", "is_flagship": False},
    # Rum
    {"brand_name": "Admiral Nelson's Rum", "category": "Rum",
     "description": "Admiral Nelson's is a premium Caribbean rum known for its affordable price "
                    "and wide variety of flavors. One of the top-selling rums in the United States.",
     "url": "https://www.admiralnelsons.com", "is_flagship": False},
    {"brand_name": "Tortuga Caribbean Rum", "category": "Rum",
     "description": "Tortuga Caribbean Rum is an authentic Caribbean spirit, capturing the essence "
                    "of island life with a smooth, rich flavor profile.",
     "url": "https://www.tortugarums.com", "is_flagship": False},
    # Tequila / Mezcal
    {"brand_name": "Corazón Tequila", "category": "Tequila",
     "description": "Corazón Tequila is crafted at the Casa San Matías distillery in Jalisco, Mexico. "
                    "Available in Blanco, Reposado, and Añejo expressions.",
     "url": "https://www.corazontequila.com", "is_flagship": False},
    {"brand_name corazón": "Margaritaville Tequila", "category": "Tequila",
     "description": "Margaritaville Tequila captures the spirit of the iconic lifestyle brand. "
                    "Crafted for the perfect margarita experience.",
     "url": "https://www.margaritavilletequila.com", "is_flagship": False},
    # Gin
    {"brand_name": "Seagram's Gin", "category": "Gin",
     "description": "Seagram's Extra Dry Gin is one of America's most popular gins, distilled with "
                    "a proprietary blend of botanicals for a balanced, smooth flavor.",
     "url": "https://www.seagramsgin.com", "is_flagship": True},
    # Cordials / Liqueurs
    {"brand_name": "Dr. McGillicuddy's", "category": "Liqueur",
     "description": "Dr. McGillicuddy's is a line of flavored schnapps and liqueurs known for their "
                    "intense, authentic flavors. Available in over 15 varieties.",
     "url": "https://www.drmcgillicuddys.com", "is_flagship": False},
    {"brand_name": "99 Brand Schnapps", "category": "Schnapps",
     "description": "99 Brand is a line of 99-proof mini liqueur shots available in over 30 flavors. "
                    "Extremely popular for shots, cocktails, and mixers at bars and events.",
     "url": "https://www.99schnapps.com", "is_flagship": False},
    # Beer
    {"brand_name": "Southern Comfort", "category": "Liqueur",
     "description": "Southern Comfort is a fruit-and-spice-flavored spirit with whiskey, created in "
                    "New Orleans in 1874 and loved globally for its sweet, smooth flavor.",
     "url": "https://www.southerncomfort.com", "is_flagship": True},
    # Brandy / Cognac
    {"brand_name": "Christian Brothers Brandy", "category": "Brandy",
     "description": "Christian Brothers is the best-selling domestic brandy in the United States, "
                    "crafted from premium California grapes and aged in oak barrels.",
     "url": "https://www.christianbrothersbrandy.com", "is_flagship": True},
    {"brand_name": "Paul Masson Grande Amber Brandy", "category": "Brandy",
     "description": "Paul Masson Grande Amber Brandy is a California brandy aged in American oak, "
                    "known for its smooth flavor and wide recognition in cocktail culture.",
     "url": "https://www.paulmassonbrandy.com", "is_flagship": False},
    # Wine
    {"brand_name": "Stirrings Mixers", "category": "Mixer / Non-Alcoholic",
     "description": "Stirrings premium drink mixes and cocktail ingredients bring professional-quality "
                    "cocktail mixers to home bartenders and hospitality professionals.",
     "url": "https://www.stirrings.com", "is_flagship": False},
]

# Fix typo in mock data key
for b in MOCK_BRANDS:
    if "brand_name corazón" in b:
        b["brand_name"] = b.pop("brand_name corazón")


def scrape_brands_live() -> list[dict]:
    """Attempt to scrape Sazerac brands page live."""
    log.info("Attempting live scrape of %s", BASE_URL)
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    brands = []

    # Sazerac uses a card/grid layout — selectors may shift; this is best-effort
    cards = soup.select(".brand-card, .brand-item, article.brand, .brand-grid-item")
    log.info("Found %d brand card elements", len(cards))

    for card in cards:
        name_el = card.select_one("h2, h3, .brand-name, .card-title")
        cat_el = card.select_one(".category, .brand-category, .tag")
        desc_el = card.select_one("p, .description, .card-text")
        link_el = card.select_one("a[href]")

        brands.append({
            "brand_name": name_el.get_text(strip=True) if name_el else "",
            "category":   cat_el.get_text(strip=True) if cat_el else "Unknown",
            "description": desc_el.get_text(strip=True) if desc_el else "",
            "url": link_el["href"] if link_el else BASE_URL,
            "is_flagship": False,
        })

    return brands


def get_brands() -> list[dict]:
    """Return brands from live scrape or mock data."""
    if SCRAPING_AVAILABLE:
        try:
            brands = scrape_brands_live()
            if len(brands) >= 5:
                log.info("Live scrape succeeded — %d brands collected", len(brands))
                return brands
            log.warning("Live scrape returned too few results (%d). Using mock data.", len(brands))
        except Exception as exc:
            log.warning("Live scrape failed (%s). Falling back to mock data.", exc)

    log.info("Loading %d brands from mock dataset", len(MOCK_BRANDS))
    return MOCK_BRANDS


def save_brands(brands: list[dict], path: str) -> None:
    """Persist brand records to CSV with audit columns."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = ["brand_id", "brand_name", "category", "description", "url",
                  "is_flagship", "scraped_at"]

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        ts = datetime.utcnow().isoformat()
        for idx, brand in enumerate(brands, start=1):
            writer.writerow({
                "brand_id":   idx,
                "brand_name": brand.get("brand_name", "").strip(),
                "category":   brand.get("category", "Unknown").strip(),
                "description": brand.get("description", "").strip(),
                "url":         brand.get("url", "").strip(),
                "is_flagship": brand.get("is_flagship", False),
                "scraped_at":  ts,
            })

    log.info("Saved %d brands → %s", len(brands), path)


def main():
    log.info("=== Brand Scraper Starting ===")
    brands = get_brands()
    save_brands(brands, RAW_OUTPUT)
    log.info("=== Brand Scraper Complete ===")
    return brands


if __name__ == "__main__":
    main()
