#!/usr/bin/env python3

"""
Simple BME688 test script.

Reads once from the BME on I2C bus 1, address 0x76, and prints the result.
"""

from .bme import read_bme   # <-- note the dot: relative import


def main():
    sample = read_bme(bus=1, address=0x76)
    print("BME sample:", sample)


if __name__ == "__main__":
    main()
