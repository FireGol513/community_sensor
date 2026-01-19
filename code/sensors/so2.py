#!/usr/bin/env python3
"""
SO2 sensor reader (DFRobot Gravity calibrated SO2, I2C, address 0x74)

SAFETY-FIRST version:
- Does NOT use i2c_rdwr command frames (those can wedge the bus if timing is off)
- Does NOT loop or retry aggressively
- Does ONE quick attempt per call and returns stable columns every time
- Rate limits reads so we don't hammer I2C

Stable output keys (match your daily CSV columns):
  - so2_ppm
  - so2_raw
  - so2_byte0
  - so2_byte1
  - so2_error   ("OK" if no error; otherwise NO_FRAME / exception)
  - so2_status  ("ok" or "error")

Note:
This assumes the device exposes a “latest frame” via register 0x00 (8 bytes),
which matches your earlier working behavior.
If it needs a command frame to update, do that ONLY in a dedicated test script,
not in the always-on collector.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, Any, Optional, List

try:
    import smbus2 as smbus
except ImportError:
    import smbus  # type: ignore

I2C_BUS = 1
DEFAULT_ADDR = 0x74

_bus = None
_addr = DEFAULT_ADDR

# ---- safety knobs ----
MIN_READ_INTERVAL_S = 1.0     # don't read more often than this
_last_read_monotonic = 0.0


def init_so2(bus: int = I2C_BUS, address: int = DEFAULT_ADDR) -> None:
    """Initialize the I2C bus and remember the SO2 address. Safe to call multiple times."""
    global _bus, _addr
    _addr = address
    if _bus is None:
        _bus = smbus.SMBus(bus)


def _read8_from_reg0() -> Optional[List[int]]:
    """Read 8 bytes from register 0x00; return list of ints or None."""
    global _bus, _addr
    if _bus is None:
        init_so2()

    # One fast read, no retries
    data = _bus.read_i2c_block_data(_addr, 0x00, 8)
    if data and len(data) == 8:
        return list(data)
    return None


def _parse_frame(data: List[int]) -> Optional[Dict[str, Any]]:
    """
    Parse FF 86 / FF 78 style frames if present.
    We only use bytes 2-3 as raw and convert to ppm conservatively.
    """
    if len(data) < 6:
        return None
    if data[0] != 0xFF:
        return None
    if data[1] not in (0x86, 0x78):
        return None

    b0 = data[2]
    b1 = data[3]
    raw = (b0 << 8) | b1

    # If decimals exist in byte5 (your tests showed dec=1 often), apply it.
    dec = data[5]
    scale = {0: 1.0, 1: 0.1, 2: 0.01}.get(dec, 1.0)
    ppm = float(raw) * scale

    return {
        "so2_ppm": ppm,
        "so2_raw": raw,
        "so2_byte0": b0,
        "so2_byte1": b1,
    }


def read_so2() -> Dict[str, Any]:
    """
    Safety-first read:
    - rate limited
    - one quick read
    - never blank columns
    """
    global _last_read_monotonic

    result: Dict[str, Any] = {
        "so2_ppm": "NODATA",
        "so2_raw": "NODATA",
        "so2_byte0": "NODATA",
        "so2_byte1": "NODATA",
        "so2_error": "OK",
        "so2_status": "ok",
    }

    # rate limit
    now = time.monotonic()
    if (now - _last_read_monotonic) < MIN_READ_INTERVAL_S:
        # Not an error; we just didn't sample this time
        result["so2_status"] = "error"
        result["so2_error"] = "RATE_LIMIT"
        return result
    _last_read_monotonic = now

    try:
        data = _read8_from_reg0()
        if not data:
            result["so2_status"] = "error"
            result["so2_error"] = "NO_FRAME"
            return result

        parsed = _parse_frame(data)
        if not parsed:
            result["so2_status"] = "error"
            result["so2_error"] = "BAD_FRAME"
            return result

        result.update(parsed)
        result["so2_error"] = "OK"
        result["so2_status"] = "ok"
        return result

    except Exception as e:
        logging.exception("Error reading SO2 sensor (safety-first)")
        result["so2_status"] = "error"
        result["so2_error"] = str(e)
        return result