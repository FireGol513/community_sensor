#!/usr/bin/env python3
"""
DailyWriter: write samples directly into one rolling daily CSV per node.

- File path: data/daily/<node_id>_YYYY-MM-DD.csv (local date)
- Appends one row per sample in a fixed column order.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from utils.timekeeping import utc_to_local


# Canonical column order for all daily files
COLUMNS = [
    "timestamp_utc",
    "timestamp_local",
    "node_id",

    # --- BME688 ---
    "temp_c",
    "rh_pct",
    "pressure_hpa",
    "voc_ohm",
    "bme_status",          # optional but helpful

    # --- PMS1 ---
    "pm1_atm_pms1",
    "pm25_atm_pms1",
    "pm10_atm_pms1",
    "pms1_status",

    # --- PMS2 ---
    "pm1_atm_pms2",
    "pm25_atm_pms2",
    "pm10_atm_pms2",
    "pms2_status",

    # --- OPC-N3 ---
    "pm1_atm_opc",
    "pm25_atm_opc",
    "pm10_atm_opc",
    "opc_status",

    # --- SO2 (SPEC board raw bytes) ---
    "so2_ppm",             # placeholder if you later compute ppm
    "so2_raw",
    "so2_byte0",
    "so2_byte1",
    "so2_error",
    "so2_status",          # optional but helpful
]


@dataclass
class DailyWriter:
    root_dir: Path
    node_id: str
    tz_name: str

    data_dir: Path = field(init=False)
    _current_date_str: Optional[str] = field(default=None, init=False)
    _file: Optional[object] = field(default=None, init=False)
    _writer: Optional[csv.writer] = field(default=None, init=False)
    _current_path: Optional[Path] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.data_dir = self.root_dir / "data" / "daily"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _open_for_date(self, date_str: str) -> None:
        """Open (or create) the daily CSV for the given YYYY-MM-DD date_str."""
        self.close()

        path = self.data_dir / f"{self.node_id}_{date_str}.csv"
        is_new = not path.exists()

        f = path.open("a", encoding="utf-8", newline="")
        writer = csv.writer(f)

        if is_new:
            writer.writerow(COLUMNS)
            f.flush()

        self._file = f
        self._writer = writer
        self._current_date_str = date_str
        self._current_path = path

    def write_sample(self, row: Dict[str, Any], sample_time_utc: datetime) -> None:
        """
        Append one sample to today's file (local date based on tz_name).
        """
        local_dt = utc_to_local(sample_time_utc, self.tz_name)
        date_str = local_dt.date().isoformat()

        if self._current_date_str != date_str or self._file is None or self._writer is None:
            self._open_for_date(date_str)

        out = []
        for col in COLUMNS:
            val = row.get(col, "")
            if val is None:
                val = ""
            out.append(val)

        self._writer.writerow(out)
        self._file.flush()

    def close(self) -> None:
        """Close the current daily file, if open."""
        if self._file is not None:
            self._file.flush()
            self._file.close()
        self._file = None
        self._writer = None
        self._current_path = None
        self._current_date_str = None