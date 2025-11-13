#!/usr/bin/env python3
"""
SO₂ sensor reader stub.

For now this just returns None so it doesn't break the rest of the system.
"""

from __future__ import annotations
from typing import Optional


def read_so2(bus: int = 1, address: int = 0x75) -> Optional[float]:
    """
    Read SO₂ concentration from the sensor (stub).

    Args:
        bus: I2C bus number (unused for now).
        address: I2C address (unused for now).

    Returns:
        None – real implementation will go here later.
    """
    return None
