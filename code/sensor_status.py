#!/usr/bin/env python3
"""
Print sensor status based on the latest row in today's daily CSV.

Logic:
- If required columns for a sensor are missing from the file header -> "Not connected (no columns)"
- Else if latest row has non-empty values for those columns -> "Connected and recording"
- Else -> "Connected but not recording"

Uses config/node.yaml for node_id and timezone.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import yaml

# --- Sensor -> columns we expect in the DAILY CSV ---
SENSORS: List[Tuple[str, List[str], Optional[str]]] = [
    # name, required value columns, optional status column (if present)
    ("PMS-1", ["pm1_atm_pms1", "pm25_atm_pms1", "pm10_atm_pms1"], "pms1_status"),
    ("PMS-2", ["pm1_atm_pms2", "pm25_atm_pms2", "pm10_atm_pms2"], "pms2_status"),
    ("BME688", ["temp_c", "rh_pct", "pressure_hpa"], None),

    # These two depend on your schema actually including these columns:
    ("OPC-N3", ["pm1_atm_opc", "pm25_atm_opc", "pm10_atm_opc"], "opc_status"),
    ("SPEC SO2", ["so2_ppm"], None),
]

def load_config(root: Path) -> Dict:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def today_local_datestr(tz_name: str) -> str:
    # We’ll mirror your “daily file is named by local date” concept.
    # Your project already has utils.timekeeping.utc_to_local; to keep this
    # script standalone, we’ll assume system time is correct and use local date.
    # If you want it to respect tz_name precisely, we can import your utc_to_local here.
    return datetime.now().date().isoformat()

def newest_daily_file(daily_dir: Path, node_id: str) -> Optional[Path]:
    # Prefer today's file, otherwise fall back to newest matching file
    today = today_local_datestr("local")
    p = daily_dir / f"{node_id}_{today}.csv"
    if p.exists():
        return p

    candidates = sorted(daily_dir.glob(f"{node_id}_*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None

def read_header_and_last_row(path: Path) -> Tuple[List[str], Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        last = None
        for row in reader:
            if row:
                last = row
        if not header or last is None:
            return header, {}
        # Map header -> last row values (trim if needed)
        n = min(len(header), len(last))
        return header[:n], {header[i]: last[i].strip() for i in range(n)}

def nonempty(vals: Dict[str, str], cols: List[str]) -> bool:
    for c in cols:
        v = vals.get(c, "").strip()
        if v != "" and v.lower() != "na" and v.lower() != "nan":
            return True
    return False

def main() -> None:
    root = Path(__file__).resolve().parents[1]
    cfg = load_config(root)
    node_id = cfg.get("node_id", "NodeX")
    tz_name = cfg.get("timezone", "UTC")

    daily_dir = root / "data" / "daily"
    path = newest_daily_file(daily_dir, node_id)

    if path is None:
        print("Sensor Status:")
        print("  No daily CSV found.")
        return

    header, last_vals = read_header_and_last_row(path)

    print(f"Sensor Status (file: {path.name})")
    if not header:
        print("  Daily file has no header/rows yet.")
        return

    header_set = set(header)

    for name, cols, status_col in SENSORS:
        missing = [c for c in cols if c not in header_set]
        if missing:
            print(f"  {name}: Not connected (no columns: {', '.join(missing)})")
            continue

        # If there is a status column and it exists, use it as extra signal
        status_val = None
        if status_col and status_col in header_set:
            status_val = last_vals.get(status_col, "").strip()

        if nonempty(last_vals, cols):
            # If status exists and says error/no_frame, still show that nuance
            if status_val and status_val not in ("", "ok"):
                print(f"  {name}: Connected but status={status_val}")
            else:
                print(f"  {name}: Connected and recording")
        else:
            # Values empty -> could be present but not recording
            if status_val:
                print(f"  {name}: Connected but not recording (status={status_val})")
            else:
                print(f"  {name}: Connected but not recording")

if __name__ == "__main__":
    main()
