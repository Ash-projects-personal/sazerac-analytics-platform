"""
run_pipeline.py
---------------
Master orchestrator — runs the full Sazerac analytics pipeline end-to-end.

Usage:
    python run_pipeline.py                  # full pipeline
    python run_pipeline.py --step scrape    # scraping only
    python run_pipeline.py --step process   # ETL only
    python run_pipeline.py --step db        # database build only
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

# ── logging ────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def run_step(name: str, fn) -> bool:
    """Run a pipeline step and log timing."""
    log.info("━━━━━ STEP: %-25s STARTING ━━━━━", name.upper())
    start = time.time()
    try:
        fn()
        elapsed = time.time() - start
        log.info("━━━━━ STEP: %-25s DONE (%.1fs) ━━━━━", name.upper(), elapsed)
        return True
    except Exception as e:
        log.error("━━━━━ STEP: %-25s FAILED — %s ━━━━━", name.upper(), e)
        return False


def step_scrape():
    import scrape_brands, scrape_locations, scrape_jobs
    scrape_brands.main()
    scrape_locations.main()
    scrape_jobs.main()


def step_process():
    import process_data
    process_data.main()


def step_db():
    import build_db
    build_db.main()


def main():
    parser = argparse.ArgumentParser(description="Sazerac Analytics Pipeline")
    parser.add_argument("--step", choices=["scrape", "process", "db", "all"],
                        default="all", help="Which pipeline step to run")
    args = parser.parse_args()

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  Sazerac Brand & Analytics Intelligence Platform     ║")
    log.info("║  Pipeline Run: %-37s║", datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    log.info("╚══════════════════════════════════════════════════════╝")

    steps = {
        "scrape":  step_scrape,
        "process": step_process,
        "db":      step_db,
    }

    if args.step == "all":
        results = {}
        for name, fn in steps.items():
            results[name] = run_step(name, fn)
    else:
        results = {args.step: run_step(args.step, steps[args.step])}

    passed = sum(results.values())
    total  = len(results)

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  PIPELINE SUMMARY: %d/%d steps passed                  ║", passed, total)
    for step, ok in results.items():
        status = "✓ PASS" if ok else "✗ FAIL"
        log.info("║    %-10s %s                              ║", step, status)
    log.info("╚══════════════════════════════════════════════════════╝")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
