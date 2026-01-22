#!/usr/bin/env python3
"""
PMS5003 sensor reader.

Provides a PMSReader class that reads PMS5003 frames from a serial port and
returns PM1/PM2.5/PM10 as a dict.

For more informations on bytes order, please check
https://www.aqmd.gov/docs/default-source/aq-spec/resources-page/plantower-pms5003-manual_v2-3.pdf
Appendix 1
"""

from __future__ import annotations

import time
import serial
import struct


class PMSReader:
    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 0.5):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self._ser: serial.Serial | None = None

    def open(self) -> None:
        if self._ser is None or not self._ser.is_open: # Si le stream serial est ferme, ouvre le
            self._ser = serial.Serial(
                self.port,
                self.baudrate,
                timeout=self.timeout
            )

    def close(self) -> None:
        if self._ser is not None and self._ser.is_open: # Si le stream serial est ouvert, ferme le
            self._ser.close()
            self._ser = None

    def _read_frame(self) -> dict[str, int] | None:
        self.open()
        ser = self._ser
        if ser is None:
            return None

        # Sync to header 0x42 0x4D
        while True: # Passe a travers des donnees dans le serial bus
            b = ser.read(1) # Lit la premiere byte dans le stream serial
            if not b:
                return None # Si aucune byte, ca veut dire qu'on a pas recu d'infos depuis la derniere fois qu'on a lu
            # Si on est rendu au chunk d'informations publiees dans le stream serial
            if b == b"\x42": # Regarde si on est rendu au premier byte de header
                if ser.read(1) == b"\x4D": # Regarde si on est rendu au deuxieme byte du header
                    break # Youpi! On a trouve les informations

        rest = ser.read(30) # Lit les 30 bytes d'informations des sensors
        if len(rest) != 30: # Verifie si les donnees ont etes entrees correctement dans le stream serial
            return None

        length = struct.unpack(">H", rest[0:2])[0] # Recupere les 3e et 4e bytes de metadonnees et les parse en big endian les 2 bytes
        if length != 28: # Verifie si elle affirme que la longueur des donnees est ce a quoi on s'attend
            return None

        data_bytes = rest[:-2] # Recupere toutes les donnees sans prendre le checksum a la fin
        checksum_recv = struct.unpack(">H", rest[-2:])[0] # Recupere le checksum et decode les 2 bytes en big endian 

        checksum_calc = (0x42 + 0x4D + sum(data_bytes)) & 0xFFFF # Additionne toutes les bytes des donnees pour faire une sorte de checksum, et reduit le nombre a 2 byte si il est plus grand que 2 byte
        if checksum_calc != checksum_recv: # Verifie si la metadonnee est egale au checksum calcule
            return None

        vals = struct.unpack(">13H", rest[2:2+26]) # Recupere toutes les valeurs interessantes, du data 1 au data 13 en big endian

        return {
            "pm1": vals[3], # Recupere la valeur de la concentration de pm 1 dans data 4
            "pm25": vals[4], # Recupere la valeur de la concentration de pm 2.5 dans data 5
            "pm10": vals[5], # Recupere la valeur de la concentration de pm 10 dans data 6
        }

    def read(self, window_seconds: float = 0.4) -> dict[str, int] | None:
        """
        Try to read a PMS frame for up to window_seconds.
        Returns the first valid frame, or None if none arrive.
        """
        deadline = time.monotonic() + window_seconds # Set le temps limite de la lecture a window_seconds + le temps en ce moment

        while time.monotonic() < deadline: # Tant qu'on a pas atteint le temps limite
            try:
                frame = self._read_frame() # Tente de recuperer les donnees des capteurs dans le serial stream
                if frame is not None:
                    return frame
            except Exception:
                pass
    
            # short pause so we don't spin the CPU
            time.sleep(0.01)

        return None # Pas reussi a recuperer les infos

    def __enter__(self) -> PMSReader:
        self.open()
        return self

    def __exit__(self) -> None:
        self.close()
