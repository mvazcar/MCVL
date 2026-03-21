"""
Entry point: run the ABDVHW replication pipeline.

Usage:
    python run.py                   # Full run 2005-2024
    python run.py --resume 5        # Resume from step 5 using saved parquets
    python run.py --years 2006 2018 # Run only 2006-2018

Requires: polars (pip install polars)
"""
import sys
import io
import argparse

# Force UTF-8 stdout for polars table printing on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Ensure the package directory is on the path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="ABDVHW MCVL Replication Pipeline")
    parser.add_argument("--years", nargs=2, type=int, default=None,
                        help="Year range: first last (default: 2005 2024)")
    parser.add_argument("--resume", type=int, default=None,
                        help="Resume from step N (1-7) using saved parquets")
    args = parser.parse_args()

    year_first, year_latest = (args.years if args.years
                                else (2005, 2024))

    print(f"ABDVHW Replication Pipeline: {year_first}-{year_latest}")
    if args.resume:
        print(f"Resuming from step {args.resume}")
    print()

    annual = run_pipeline(year_first, year_latest, resume_from=args.resume)
    print(f"\nDone. {len(annual):,} rows in final panel.")


if __name__ == "__main__":
    main()
