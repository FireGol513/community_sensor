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

For more informations, please check:
https://wiki.dfrobot.com/SKU_SEN0465toSEN0476_Gravity_Gas_Sensor_Calibrated_I2C_UART
https://dfimg.dfrobot.com/nobody/wiki/5953b463b8712f03d0791e98dd592e78.pdf
"""

from __future__ import annotations

import logging
import time

try:
    import smbus2 as smbus
except ImportError:
    import smbus

I2C_BUS = 1
DEFAULT_ADDR = 0x74 # Adresse du sensor dans le i2c bus

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


def _read8_from_reg0() -> list[int] | None:
    """Read 8 bytes from register 0x00; return list of ints or None."""
    global _bus, _addr
    if _bus is None:
        init_so2()

    # One fast read, no retries
    # Lit le 8 bytes du bloc de donnees contenu dans le register 0
    data: list[int] = _bus.read_i2c_block_data(_addr, 0x00, 8)
    if data and len(data) == 8: # Verifie si on a bel et bien recupere 8 bytes
        return data
    
    return None


def _parse_frame(data: list[int]) -> dict[str, float | int] | None:
    """
    Parse FF 86 / FF 78 style frames if present.
    We only use bytes 2-3 as raw and convert to ppm conservatively.
    """
    # FIXME: Verifier 9 bytes, et calculer le checksum
    if len(data) < 6: # Verifie si on a au moins 6 bytes d'informations, en comptant le start byte
        return None
    if data[0] != 0xFF: # Si le premier byte n'est pas 0xFF, qui est le "start byte", donc le byte qui signifie le debut d'un message
        return None
    # FIXME: Only check for 0x86 (gas reading), as 0x78 is unwanted/unneeded, because it doesn't return gas data
    if data[1] not in (0x86, 0x78): # Regarde si c'est une commande recuperation de donnees sur le SO2
        return None

    b0 = data[2] # Recupere le high byte du nombre de la concentration de SO2
    b1 = data[3] # Recupere le low byte du nombre de la concentration de SO2
    raw = (b0 << 8) | b1 # Byte shift le high byte de 8 positions (x256) et y additionne le low byte

    # If decimals exist in byte5 (your tests showed dec=1 often), apply it.
    dec = data[5] # Lit le 5e byte, qui dit ou se trouve la decimale dans le "raw", le chiffre qui represente la concentration en ppm
    scale = {0: 1.0, 1: 0.1, 2: 0.01}.get(dec, 1.0) # En fonction du 5e byte, on choisie un nombre avec lequel multiplier le chiffre qui provient des byte2 et byte3
    ppm = float(raw) * scale # Place la virgule dans le chiffre de concentration

    # TODO: Verifier pourquoi on garde pas juste so2_ppm
    # TODO: Verifier pourquoi on regarde pas le type de gas avec byte4 et la temperature avec byte6 high et byte7 low
    return {
        "so2_ppm": ppm,
        "so2_raw": raw,
        "so2_byte0": b0,
        "so2_byte1": b1,
    }


def read_so2() -> dict[str, float | int | str]:
    """
    Safety-first read:
    - rate limited
    - one quick read
    - never blank columns
    """
    global _last_read_monotonic # Garde le temps de la derniere lecture

    result: dict[str, float | int | str] = {
        "so2_ppm": "NODATA",
        "so2_raw": "NODATA",
        "so2_byte0": "NODATA",
        "so2_byte1": "NODATA",
        "so2_error": "OK",
        "so2_status": "ok",
    }

    # rate limit
    now = time.monotonic() # Utilise cette sorte de timer parce qu'on a pas besoin d'un vrai temps, seulement d'un temps en seconde qui augmente toujours. Cette horloge se fout de l'horloge interne de l'ordi
    if (now - _last_read_monotonic) < MIN_READ_INTERVAL_S: # Eviter de lire trop souvent
        # Not an error; we just didn't sample this time
        result["so2_status"] = "error"
        result["so2_error"] = "RATE_LIMIT"
        return result
    
    # Enregistre le temps de lecture pour faire du rate limit au besoin
    _last_read_monotonic = now

    try:
        data = _read8_from_reg0() # Recupere les 9 bytes d'infos "raw" sur la quantite de SO2
        if not data: # Si on a pas reussi a recuperer les donnees
            result["so2_status"] = "error"
            result["so2_error"] = "NO_FRAME"
            return result

        parsed = _parse_frame(data) # Recupere le ppm apres avoir ete traite
        if not parsed: # Si le ppm n'a pas pu etre extrait
            result["so2_status"] = "error"
            result["so2_error"] = "BAD_FRAME"
            return result

        result.update(parsed) # Update le dict result avec les nouvelles donnees
        result["so2_error"] = "OK"
        result["so2_status"] = "ok"
        return result

    except Exception as e:
        logging.exception("Error reading SO2 sensor (safety-first)")
        result["so2_status"] = "error"
        result["so2_error"] = str(e)
        return result