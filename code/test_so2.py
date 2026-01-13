#!/usr/bin/env python3
"""
test_so2_i2c_regs.py

Quick I2C register probe for the SO2 board at address 0x74.

Goal:
- Confirm the device responds on the bus
- Identify registers that return non-zero bytes (possible measurement/status regs)

Usage:
  python3 code/test_so2_i2c_regs.py
  # or make executable:
  chmod +x code/test_so2_i2c_regs.py
  ./code/test_so2_i2c_regs.py

Options:
  python3 code/test_so2_i2c_regs.py --addr 0x74 --bus 1 --start 0x00 --end 0x3F --nbytes 2
"""

from __future__ import annotations

import argparse
import time

try:
    import smbus2 as smbus
except ImportError:
    import smbus  # type: ignore


def parse_int(x: str) -> int:
    """Allow hex like 0x74 or decimal like 116."""
    return int(str(x), 0)


def rb(bus, addr: int, reg: int, n: int):
    """Read n bytes from (addr, reg). Returns list[int] or None on error."""
    try:
        return bus.read_i2c_block_data(addr, reg, n)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Probe I2C registers for SO2 board.")
    ap.add_argument("--bus", default="1", help="I2C bus number (default: 1)")
    ap.add_argument("--addr", default="0x74", help="I2C address (default: 0x74)")
    ap.add_argument("--start", default="0x00", help="Start register (default: 0x00)")
    ap.add_argument("--end", default="0x3F", help="End register inclusive (default: 0x3F)")
    ap.add_argument("--nbytes", default="2", help="Bytes per read (default: 2)")
    ap.add_argument("--all", action="store_true", help="Print all registers, even if all zeros")
    ap.add_argument("--repeat", type=float, default=0.0, help="Repeat scan every N seconds (0 = once)")
    args = ap.parse_args()

    bus_num = parse_int(args.bus)
    addr = parse_int(args.addr)
    start = parse_int(args.start)
    end = parse_int(args.end)
    nbytes = parse_int(args.nbytes)

    bus = smbus.SMBus(bus_num)

    def one_scan():
        print(f"\nI2C probe: bus={bus_num} addr=0x{addr:02X} regs=0x{start:02X}..0x{end:02X} nbytes={nbytes}")
        found = 0
        for reg in range(start, end + 1):
            d = rb(bus, addr, reg, nbytes)
            if d is None:
                # silently skip read errors; you can make this verbose if needed
                continue

            nonzero = any(x != 0 for x in d)
            if args.all or nonzero:
                hex_bytes = " ".join(f"0x{b:02X}" for b in d)
                print(f"  reg 0x{reg:02X}: [{hex_bytes}]")
            if nonzero:
                found += 1

        if found == 0:
            print("  (No non-zero registers found in this range.)")
        else:
            print(f"  Found {found} register(s) with at least one non-zero byte.")

    # Run once or repeat
    if args.repeat and args.repeat > 0:
        try:
            while True:
                one_scan()
                time.sleep(args.repeat)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        one_scan()


if __name__ == "__main__":
    main()
