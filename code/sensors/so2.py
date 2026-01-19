#!/usr/bin/env python3
"""
SO2 sensor reader (DFRobot Gravity calibrated SO2, I2C, address 0x74)

Stable output keys (match your daily CSV columns):
  - so2_ppm
  - so2_raw
  - so2_byte0
  - so2_byte1
  - so2_error   ("OK" if no error; otherwise error code/message)
  - so2_status  ("ok" or "error")

This device is NOT register-based. It uses command/response frames over I2C.

We send:
  FF 01 86 00 00 00 00 00 CS   (read gas in passive mode)
and read 8 bytes back.

Response (observed):
  [0]=0xFF
  [1]=0x86 (echo)   (sometimes you may see other echoes depending on firmware)
  [2]=high
  [3]=low
  [4]=gas_type
  [5]=decimals   (0 => 1, 1 => 0.1, 2 => 0.01)
"""

import logging
from typing import Dict, Any, Optional, List

try:
    import smbus2
    from smbus2 import i2c_msg
except ImportError as e:
    raise SystemExit("smbus2 is required for DFRobot frame-based I2C. Install with: pip install smbus2") from e


I2C_BUS = 1
DEFAULT_ADDR = 0x74

START = 0xFF
DEV_ADDR_BYTE = 0x01

CMD_READ_GAS = 0x86
CMD_SET_MODE = 0x78

MODE_PASSIVE = 0x04


_bus: Optional[smbus2.SMBus] = None
_addr: int = DEFAULT_ADDR


def init_so2(bus: int = I2C_BUS, address: int = DEFAULT_ADDR) -> None:
    """Initialize the I2C bus and remember the SO2 address. Safe to call multiple times."""
    global _bus, _addr
    _addr = address
    if _bus is None:
        _bus = smbus2.SMBus(bus)


def _checksum(frame9: List[int]) -> int:
    """
    Datasheet checksum:
      check = (invert(byte1 + ... + byte7) + 1) & 0xFF
    where byte0 is START and byte8 is checksum.
    """
    s = sum(frame9[1:8]) & 0xFF
    return ((~s + 1) & 0xFF)


def _xfer(out_bytes: List[int], read_len: int) -> List[int]:
    """Raw I2C write then read using i2c_rdwr (correct for this sensor)."""
    global _bus, _addr
    if _bus is None:
        init_so2()

    assert _bus is not None  # for type-checkers
    w = i2c_msg.write(_addr, out_bytes)
    r = i2c_msg.read(_addr, read_len)
    _bus.i2c_rdwr(w, r)
    return list(r)


def _set_passive_mode() -> None:
    """Best-effort: set passive mode (not fatal if it fails)."""
    frame = [START, DEV_ADDR_BYTE, CMD_SET_MODE, MODE_PASSIVE, 0x00, 0x00, 0x00, 0x00, 0x00]
    frame[8] = _checksum(frame)
    try:
        _ = _xfer(frame, 9)
    except Exception:
        pass


def _read_gas_frame() -> Optional[List[int]]:
    """Send read-gas command frame and read 8 bytes response."""
    frame = [START, DEV_ADDR_BYTE, CMD_READ_GAS, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    frame[8] = _checksum(frame)

    try:
        resp = _xfer(frame, 8)
        if resp and len(resp) == 8:
            return resp
        return None
    except Exception:
        return None


def _decode(resp: List[int]) -> Optional[Dict[str, Any]]:
    """
    Decode the 8-byte response.
    We only publish the columns you want.
    """
    if len(resp) < 6:
        return None
    if resp[0] != 0xFF:
        return None

    # Many firmwares echo 0x86 here; if yours sometimes differs,
    # we do NOT hard-fail as long as the frame looks right.
    b0 = resp[2]
    b1 = resp[3]
    raw = (b0 << 8) | b1

    dec = resp[5]
    res = {0: 1.0, 1: 0.1, 2: 0.01}.get(dec, None)
    ppm = float(raw) * res if res is not None else float(raw)

    return {
        "so2_ppm": ppm,     # 0.0 is valid!
        "so2_raw": raw,
        "so2_byte0": b0,
        "so2_byte1": b1,
    }


def read_so2() -> Dict[str, Any]:
    """
    Read SO2 and return stable column dict.
    so2_error is ALWAYS populated:
      - "OK" if fine
      - otherwise error string
    """
    result: Dict[str, Any] = {
        "so2_ppm": "NODATA",
        "so2_raw": "NODATA",
        "so2_byte0": "NODATA",
        "so2_byte1": "NODATA",
        "so2_error": "OK",
        "so2_status": "ok",
    }

    try:
        # Optional but helpful: ensure passive mode
        _set_passive_mode()

        resp = _read_gas_frame()
        if not resp:
            result["so2_status"] = "error"
            result["so2_error"] = "NO_FRAME"
            return result

        decoded = _decode(resp)
        if decoded is None:
            result["so2_status"] = "error"
            result["so2_error"] = "BAD_FRAME"
            return result

        result.update(decoded)
        result["so2_error"] = "OK"
        result["so2_status"] = "ok"
        return result

    except Exception as e:
        logging.exception("Error reading SO2 sensor")
        result["so2_status"] = "error"
        result["so2_error"] = str(e)
        return result


def _pretty_print_reading() -> None:
    from time import sleep

    print(f"Testing SO2 on I2C bus {I2C_BUS}, address 0x{DEFAULT_ADDR:02X}")
    init_so2()

    try:
        while True:
            r = read_so2()
            print(
                f"ppm={r['so2_ppm']} raw={r['so2_raw']} "
                f"b0={r['so2_byte0']} b1={r['so2_byte1']} "
                f"status={r['so2_status']} err={r['so2_error']}"
            )
            sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    _pretty_print_reading()