#!/usr/bin/env python3
"""
Combine 5-minute chunk files into a single daily CSV.

Usage examples (from the code/ directory):

    # Combine for a specific date (YYYY-MM-DD), keep chunks
    python3 combine_daily.py --date 2025-11-16

    # Combine yesterday's chunks and delete them afterward
    python3 combine_daily.py --yesterday --delete-chunks
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import yaml  # uses same dependency as collect_data.py


def load_config(root: Path):
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Combine 5-minute chunk files into a daily CSV.")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--date", type=str, help="Date to combine (YYYY-MM-DD)")
    group.add_argument("--yesterday", action="store_true", help="Combine chunks for yesterday")

    parser.add_argument(
        "--delete-chunks",
        action="store_true",
        help="Delete 5-minute chunk files after successful combine",
    )
    return parser.parse_args()


def determine_date(args: argparse.Namespace) -> str:
    if args.date:
        # Trust user input; minimal validation
        return args.date
    if args.yesterday:
        d = datetime.now().date() - timedelta(days=1)
        return d.isoformat()
    # Default: today
    return datetime.now().date().isoformat()


def main():
    here = Path(__file__).resolve()
    root = here.parents[1]  # project root (directory containing code/)

    cfg = load_config(root)
    node_id = cfg.get("node_id", "NodeX")

    args = parse_args()
    date_str = determine_date(args)

    chunks_dir = root / "data" / "5minute"
    daily_dir = root / "data" / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    pattern = f"{node_id}_{date_str}_*.csv"
    chunk_files: List[Path] = sorted(chunks_dir.glob(pattern))

    if not chunk_files:
        print(f"No chunk files found for {date_str} (pattern: {pattern})")
        return

    daily_path = daily_dir / f"{node_id}_{date_str}.csv"
    print(f"Combining {len(chunk_files)} files into {daily_path}")

    header_written = False

    with daily_path.open("w", encoding="utf-8", newline="") as out_f:
        for chunk_path in chunk_files:
            with chunk_path.open("r", encoding="utf-8") as in_f:
                lines = in_f.readlines()
                if not lines:
                    continue

                # First line is header
                header = lines[0]
                data_lines = lines[1:]

                if not header_written:
                    out_f.write(header)
                    header_written = True

                for line in data_lines:
                    out_f.write(line)

    print("Done writing daily file.")

    if args.delete_chunks:
        print("Deleting chunk files...")
        for chunk_path in chunk_files:
            chunk_path.unlink()
        print("Chunk files deleted.")


if __name__ == "__main__":
    main()
