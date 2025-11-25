#!/usr/bin/env python3
"""
SO2 sensor reader (I2C, address 0x74)

This is a generic reader that:
- opens I2C bus 1
- talks to address 0x74 (the SO2 board you just wired)
- reads a small block of data and returns it as a dict

Right now we treat the reading as "raw" because the exact
register map / scaling depends on the specific board.
Once we know the format we can convert to ppb/ppm.
"""

import logging
from typing import Dict, Any

try:
    import smbus2 as smbus
except ImportError:
    # fallback to smbus if smbus2 isn't available
    import smbus  # type: ignore

I2C_BUS = 1
DEFAULT_ADDR = 0x74

_bus = None
_addr = DEFAULT_ADDR


def init_so2(bus: int = I2C_BUS, address: int = DEFAULT_ADDR) -> None:
    """
    Initialize the I2C bus and remember the SO2 address.
    Safe to call multiple times.
    """
    global _bus, _addr
    _addr = address
    if _bus is None:
        _bus = smbus.SMBus(bus)


def read_so2() -> Dict[str, Any]:
    """
    Read a raw value from the SO2 sensor.

    Returns a dict that you can merge into your row:
      {
        "so2_raw": <int or None>,
        "so2_byte0": <int or None>,
        "so2_byte1": <int or None>,
        "so2_error": <str or None>
      }

    Once we know the exact register map, we can replace this with a
    proper conversion to ppb/ppm, but for now this will tell us
    whether the sensor is alive and changing.
    """
    global _bus, _addr

    if _bus is None:
        init_so2()

    result: Dict[str, Any] = {
        "so2_raw": None,
        "so2_byte0": None,
        "so2_byte1": None,
        "so2_error": None,
    }

    try:
        # Many gas boards store the measurement in 2 bytes starting at 0x00 or 0x01.
        # For now, we just read 2 bytes from 0x00 and treat them as a big-endian value.
        data = _bus.read_i2c_block_data(_addr, 0x00, 2)
        b0, b1 = data[0], data[1]
        raw = (b0 << 8) | b1

        result["so2_raw"] = raw
        result["so2_byte0"] = b0
        result["so2_byte1"] = b1

    except Exception as e:
        logging.exception("Error reading SO2 sensor")
        result["so2_error"] = str(e)

    return result


def _pretty_print_reading() -> None:
    """
    Helper for standalone testing from the command line.
    """
    from time import sleep

    print(f"Testing SO2 sensor on I2C bus {I2C_BUS}, address 0x{DEFAULT_ADDR:02X}")
    init_so2()

    try:
        while True:
            reading = read_so2()
            if reading["so2_error"] is not None:
                print("Error:", reading["so2_error"])
            else:
                print(
                    f"raw={reading['so2_raw']}, "
                    f"byte0={reading['so2_byte0']}, "
                    f"byte1={reading['so2_byte1']}"
                )
            sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    _pretty_print_reading()
