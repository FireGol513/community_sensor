#!/usr/bin/env python3
"""
BME688 reader using the bme680 Python library.

Exposes read_bme(bus, address) -> dict or None.

Returns (if successful):
    {
        "temp_c": float,
        "rh_pct": float,
        "pressure_hpa": float,
        "voc_ohm": float or None,
    }

For more informations about the sensor's specifications, please look at:
https://www.bosch-sensortec.com/media/boschsensortec/downloads/datasheets/bst-bme688-ds000.pdf
"""

import bme680

# We keep a single global sensor instance so we don't re-init on every call.
_sensor: bme680.BME680 | None = None

def _ensure_sensor(address: int = 0x76) -> bme680.BME680:
    """
    Initialize the BME680/BME688 sensor if needed and return it.
    """
    if _sensor is None:
        # i2c_addr uses /dev/i2c-1 by default on a Pi
        sensor = bme680.BME680(i2c_addr=address)

        # Combien de ms le detecteur de gas chauffe avant de faire la detection
        # La temperature que le detecteur doit atteindre en Celsius
        sensor.set_gas_heater_profile(150, 320)

        _sensor = sensor

    return _sensor


def read_bme(bus: int = 1, address: int = 0x76) -> dict[str, float | None] | None:
    """
    Read temperature, relative humidity, pressure, and gas resistance.

    Args:
        bus: I2C bus number (ignored here; we always use /dev/i2c-1).
        address: I2C address (0x76 or 0x77).

    Returns:
        dict with keys: temp_c, rh_pct, pressure_hpa, voc_ohm
        or None if no new data is available.
    """

    # TODO: Enlever le try except et tenter de comprendre quelles erreures arrivent
    try:
        sensor = _ensure_sensor(address=address)

        if sensor.get_sensor_data():
            data = sensor.data
            temp_c = data.temperature
            rh_pct = data.humidity
            pressure_hpa = data.pressure

            # Record VOC resistance even if not heat-stable yet
            # TODO: Verifier data.heat_stable pour voir si la temperature du capteur de gas est arrive a la temperature voulue
            voc_ohm = data.gas_resistance

            return {
                "temp_c": temp_c,
                "rh_pct": rh_pct,
                "pressure_hpa": pressure_hpa,
                "voc_ohm": voc_ohm,
            }

        # No new sample at this instant
        return None

    except Exception as e:
        # In production we'll log this; for now we just print.
        print(f"[BME] Error reading sensor at 0x{address:02x}: {e}")
        return None
