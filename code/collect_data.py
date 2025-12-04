#!/usr/bin/env python3
"""
Main data collection loop.

Now writes directly to one rolling daily CSV per node:

    data/daily/<node_id>_YYYY-MM-DD.csv

Each row is written in the canonical COLUMNS order defined in daily_writer.py.
"""

from __future__ import annotations

import time
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml  # pip install pyyaml

from sensors.pms import PMSReader
from sensors.bme import read_bme
from sensors.so2 import init_so2, read_so2
from sensors.opc_n3 import OPCN3
from utils.timekeeping import (
    now_utc,
    utc_to_local,
    isoformat_utc_z,
    isoformat_local,
)
from daily_writer import DailyWriter


def load_config(root: Path) -> Dict[str, Any]:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(root: Path) -> None:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "emis.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def main() -> None:
    root = Path(__file__).resolve().parents[1]  # project root
    setup_logging(root)
    log = logging.getLogger("emis.collect")

    cfg = load_config(root)
    node_id: str = cfg.get("node_id", "NodeX")
    tz_name: str = cfg.get("timezone", "UTC")
    tick_seconds: float = float(cfg.get("tick_seconds", 1.0))

    # Sensor config
    s_cfg = cfg.get("sensors", {})

    pms1_reader: Optional[PMSReader] = None
    pms2_reader: Optional[PMSReader] = None
    opc_reader: Optional[OPCN3] = None

    # PMS1
    if s_cfg.get("pms1", {}).get("enabled", False):
        p1_port = s_cfg["pms1"]["port"]
        pms1_reader = PMSReader(p1_port)
        log.info(f"PMS1 enabled on {p1_port}")

    # PMS2
    if s_cfg.get("pms2", {}).get("enabled", False):
        p2_port = s_cfg["pms2"]["port"]
        pms2_reader = PMSReader(p2_port)
        log.info(f"PMS2 enabled on {p2_port}")

    # OPC-N3 (SPI-based)
    opc_enabled = s_cfg.get("opc", {}).get("enabled", False)
    opc_bus = int(s_cfg.get("opc", {}).get("spi_bus", 0))
    opc_device = int(s_cfg.get("opc", {}).get("spi_device", 0))
    # opc_speed = int(s_cfg.get("opc", {}).get("spi_max_speed", 5000000))

    if opc_enabled:
        try:
            opc_reader = OPCN3(bus=opc_bus, device=opc_device) #spi_max_speed=opc_speed 
            log.info(
                f"OPC-N3 enabled on SPI bus {opc_bus}, device {opc_device}"
            )
        except Exception as e:
            log.warning(f"Disabling OPC-N3 after init failure: {e}")
            opc_reader = None

    # BME688
    bme_enabled = s_cfg.get("bme", {}).get("enabled", False)
    bme_bus = s_cfg.get("bme", {}).get("i2c_bus", 1)
    bme_addr = s_cfg.get("bme", {}).get("address", 0x76)

    # SO2
    so2_enabled = s_cfg.get("so2", {}).get("enabled", False)
    so2_bus = s_cfg.get("so2", {}).get("i2c_bus", 1)
    so2_addr_raw = s_cfg.get("so2", {}).get("address", 0x74)

    # Normalise SO2 address (can be "0x74" or 116)
    try:
        so2_addr = int(str(so2_addr_raw), 0)
    except Exception:
        so2_addr = 0x74

    # Initialise SO2 once if enabled
    if so2_enabled:
        try:
            init_so2(bus=so2_bus, address=so2_addr)
            log.info(f"SO2 enabled on I2C bus {so2_bus}, addr 0x{so2_addr:02X}")
        except Exception as e:
            log.warning(f"Disabling SO2 after init failure: {e}")
            so2_enabled = False

    # New: daily writer instead of 5-minute chunk manager
    dw = DailyWriter(root_dir=root, node_id=node_id, tz_name=tz_name)

    log.info("Starting main collection loop (daily writer mode)")

    try:
        while True:
            t_utc = now_utc()
            t_local = utc_to_local(t_utc, tz_name)

            row: Dict[str, Any] = {
                "timestamp_utc": isoformat_utc_z(t_utc),
                "timestamp_local": isoformat_local(t_local),
                "node_id": node_id,
            }

            # BME688
            if bme_enabled:
                try:
                    b = read_bme(bus=bme_bus, address=bme_addr)
                    if b:
                        row["temp_c"] = b.get("temp_c")
                        row["rh_pct"] = b.get("rh_pct")
                        row["pressure_hpa"] = b.get("pressure_hpa")
                        row["voc_ohm"] = b.get("voc_ohm")
                except Exception as e:
                    logging.warning(f"BME read error: {e}")

            # PMS1
            if pms1_reader is not None:
                try:
                    s1 = pms1_reader.read()
                    if s1:
                        # Treat pm1/pm25/pm10 from PMSReader as atmospheric mass
                        row["pm1_atm_pms1"] = s1["pm1"]
                        row["pm25_atm_pms1"] = s1["pm25"]
                        row["pm10_atm_pms1"] = s1["pm10"]
                        row["pms1_status"] = "ok"
                    else:
                        row["pms1_status"] = "no_frame"
                except Exception as e:
                    row["pms1_status"] = f"error:{e}"
                    logging.warning(f"PMS1 read error: {e}")

            # PMS2
            if pms2_reader is not None:
                try:
                    s2 = pms2_reader.read()
                    if s2:
                        row["pm1_atm_pms2"] = s2["pm1"]
                        row["pm25_atm_pms2"] = s2["pm25"]
                        row["pm10_atm_pms2"] = s2["pm10"]
                        row["pms2_status"] = "ok"
                    else:
                        row["pms2_status"] = "no_frame"
                except Exception as e:
                    row["pms2_status"] = f"error:{e}"
                    logging.warning(f"PMS2 read error: {e}")

            # OPC-N3
            if opc_reader is not None:
                try:
                    o = opc_reader.read()   # adjust if your method name differs
                    if o:
                        # Assumes dict with pm1/pm25/pm10; tweak keys if needed
                        row["pm1_atm_opc"] = o.get("pm1")
                        row["pm25_atm_opc"] = o.get("pm25")
                        row["pm10_atm_opc"] = o.get("pm10")
                        row["opc_status"] = "ok"
                    else:
                        row["opc_status"] = "no_frame"
                except Exception as e:
                    row["opc_status"] = f"error:{e}"
                    logging.warning(f"OPC-N3 read error: {e}")

            # SO2
            if so2_enabled:
                try:
                    v = read_so2()
                    if v is not None:
                        # v is a dict from sensors.so2.read_so2()
                        row["so2_raw"] = v.get("so2_raw")
                        row["so2_byte0"] = v.get("so2_byte0")
                        row["so2_byte1"] = v.get("so2_byte1")
                        row["so2_error"] = v.get("so2_error")
                except Exception as e:
                    logging.warning(f"SO2 read error: {e}")
                    row["so2_error"] = f"exception:{e}"

            # Write one row into today's daily CSV
            dw.write_sample(row=row, sample_time_utc=t_utc)

            time.sleep(tick_seconds)

    except KeyboardInterrupt:
        log.info("Stopping collection loop (KeyboardInterrupt)")

    finally:
        dw.close()
        if pms1_reader is not None:
            pms1_reader.close()
        if pms2_reader is not None:
            pms2_reader.close()
        if opc_reader is not None:
            opc_reader.close()
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()