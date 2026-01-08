#!/usr/bin/env python3
"""
Main data collection loop (daily CSV only).

Writes directly to:
    data/daily/<node_id>_YYYY-MM-DD.csv

Column order is defined by daily_writer.py (COLUMNS).
"""

from __future__ import annotations

import time
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from sensors.pms import PMSReader
from sensors.bme import read_bme
from sensors.so2 import init_so2, read_so2
from sensors.opc_n3 import OPCN3

from utils.timekeeping import now_utc, utc_to_local, isoformat_utc_z, isoformat_local
from daily_writer import DailyWriter


def load_config(root: Path) -> Dict[str, Any]:
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def setup_logging(root: Path) -> logging.Logger:
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
    return logging.getLogger("emis.collect")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    log = setup_logging(root)

    cfg = load_config(root)
    node_id: str = cfg.get("node_id", "NodeX")
    tz_name: str = cfg.get("timezone", "UTC")
    tick_seconds: float = float(cfg.get("tick_seconds", 1.0))

    s_cfg: Dict[str, Any] = cfg.get("sensors", {}) or {}

    # -----------------------
    # Initialise sensors
    # -----------------------
    pms1_reader: Optional[PMSReader] = None
    pms2_reader: Optional[PMSReader] = None
    opc_reader: Optional[OPCN3] = None

    # PMS1
    p1 = s_cfg.get("pms1", {}) or {}
    if p1.get("enabled", False):
        port = p1.get("port")
        if port:
            pms1_reader = PMSReader(port)
            log.info(f"PMS1 enabled on {port}")
        else:
            log.warning("PMS1 enabled but no port provided; disabling")

    # PMS2
    p2 = s_cfg.get("pms2", {}) or {}
    if p2.get("enabled", False):
        port = p2.get("port")
        if port:
            pms2_reader = PMSReader(port)
            log.info(f"PMS2 enabled on {port}")
        else:
            log.warning("PMS2 enabled but no port provided; disabling")

    # OPC-N3
    o_cfg = s_cfg.get("opc", {}) or {}
    if o_cfg.get("enabled", False):
        try:
            opc_bus = int(o_cfg.get("spi_bus", 0))
            opc_device = int(o_cfg.get("spi_device", 0))
            # Only pass max speed if your class supports it
            # opc_speed = int(o_cfg.get("spi_max_speed", 5000000))

            opc_reader = OPCN3(bus=opc_bus, device=opc_device)
            log.info(f"OPC-N3 enabled on SPI bus {opc_bus}, device {opc_device}")
        except Exception as e:
            log.warning(f"Disabling OPC-N3 after init failure: {e}")
            opc_reader = None

    # BME688
    b_cfg = s_cfg.get("bme", {}) or {}
    bme_enabled = bool(b_cfg.get("enabled", False))
    bme_bus = int(b_cfg.get("i2c_bus", 1))
    bme_addr = b_cfg.get("address", 0x76)  # can be 0x76 or 0x77
    try:
        bme_addr = int(str(bme_addr), 0)
    except Exception:
        bme_addr = 0x76

    # SO2
    so_cfg = s_cfg.get("so2", {}) or {}
    so2_enabled = bool(so_cfg.get("enabled", False))
    so2_bus = int(so_cfg.get("i2c_bus", 1))
    so2_addr = so_cfg.get("address", 0x74)
    try:
        so2_addr = int(str(so2_addr), 0)
    except Exception:
        so2_addr = 0x74

    if so2_enabled:
        try:
            init_so2(bus=so2_bus, address=so2_addr)
            log.info(f"SO2 enabled on I2C bus {so2_bus}, addr 0x{so2_addr:02X}")
        except Exception as e:
            log.warning(f"Disabling SO2 after init failure: {e}")
            so2_enabled = False

    # Daily writer
    dw = DailyWriter(root_dir=root, node_id=node_id, tz_name=tz_name)
    log.info("Starting collection loop (daily CSV only)")

    try:
        while True:
            t_utc = now_utc()
	    t_local = utc_to_local(t_utc, tz_name)

            row: Dict[str, Any] = {
                "timestamp_utc": isoformat_utc_z(t_utc),
                "timestamp_local": isoformat_local(t_local),  # NOTE: timekeeping helper should accept tz_name
                "node_id": node_id,
            }

            # ---- BME ----
            if bme_enabled:
                try:
                    b = read_bme(bus=bme_bus, address=bme_addr)
                    if b:
                        row["temp_c"] = b.get("temp_c")
                        row["rh_pct"] = b.get("rh_pct")
                        row["pressure_hpa"] = b.get("pressure_hpa")
                        row["voc_ohm"] = b.get("voc_ohm")
                        row["bme_status"] = "ok"
                    else:
                        row["bme_status"] = "no_data"
                except Exception as e:
                    row["bme_status"] = f"error:{e}"
                    log.warning(f"BME read error: {e}")

            # ---- PMS1 ----
            if pms1_reader is not None:
                try:
                    s1 = pms1_reader.read()
                    if s1:
                        row["pm1_atm_pms1"] = s1.get("pm1")
                        row["pm25_atm_pms1"] = s1.get("pm25")
                        row["pm10_atm_pms1"] = s1.get("pm10")
                        row["pms1_status"] = "ok"
                    else:
                        row["pms1_status"] = "no_frame"
                except Exception as e:
                    row["pms1_status"] = f"error:{e}"
                    log.warning(f"PMS1 read error: {e}")

            # ---- PMS2 ----
            if pms2_reader is not None:
                try:
                    s2 = pms2_reader.read()
                    if s2:
                        row["pm1_atm_pms2"] = s2.get("pm1")
                        row["pm25_atm_pms2"] = s2.get("pm25")
                        row["pm10_atm_pms2"] = s2.get("pm10")
                        row["pms2_status"] = "ok"
                    else:
                        row["pms2_status"] = "no_frame"
                except Exception as e:
                    row["pms2_status"] = f"error:{e}"
                    log.warning(f"PMS2 read error: {e}")

            # ---- OPC ----
            if opc_reader is not None:
                try:
                    o = opc_reader.read()
                    if o is not None:
                        # allow zeros: 0 is valid data
                        row["pm1_atm_opc"] = o.get("pm1")
                        row["pm25_atm_opc"] = o.get("pm25")
                        row["pm10_atm_opc"] = o.get("pm10")
                        row["opc_status"] = "ok"
                    else:
                        row["opc_status"] = "no_frame"
                except Exception as e:
                    row["opc_status"] = f"error:{e}"
                    log.warning(f"OPC read error: {e}")

            # ---- SO2 ----
            if so2_enabled:
                try:
                    v = read_so2()
                    # v may contain zeros; that still counts as "present"
                    row["so2_raw"] = v.get("so2_raw")
                    row["so2_byte0"] = v.get("so2_byte0")
                    row["so2_byte1"] = v.get("so2_byte1")
                    row["so2_error"] = v.get("so2_error")
                    row["so2_status"] = "ok" if not v.get("so2_error") else "error"
                except Exception as e:
                    row["so2_error"] = f"exception:{e}"
                    row["so2_status"] = "error"
                    log.warning(f"SO2 read error: {e}")

            # Write
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