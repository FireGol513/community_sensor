#!/usr/bin/env python3
"""
PMS5003 sensor reader.

Provides a PMSReader class that reads PMS5003 frames from a serial port and
returns PM1/PM2.5/PM10 as a dict.
"""

import serial
import struct
from typing import Optional, Dict


class PMSReader:
    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 0.5):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self._ser: Optional[serial.Serial] = None

    def open(self) -> None:
        if self._ser is None or not self._ser.is_open:
            self._ser = serial.Serial(
                self.port,
                self.baudrate,
                timeout=self.timeout
            )

    def close(self) -> None:
        if self._ser is not None and self._ser.is_open:
            self._ser.close()
            self._ser = None

    def _read_frame(self) -> Optional[Dict[str, int]]:
        self.open()
        ser = self._ser

        # Sync to header 0x42 0x4D
        while True:
            b = ser.read(1)
            if not b:
                return None
            if b == b"\x42":
                if ser.read(1) == b"\x4D":
                    break

        rest = ser.read(30)
        if len(rest) != 30:
            return None

        length = struct.unpack(">H", rest[0:2])[0]
        if length != 28:
            return None

        data_bytes = rest[:-2]
        checksum_recv = struct.unpack(">H", rest[-2:])[0]

        checksum_calc = (0x42 + 0x4D + sum(data_bytes)) & 0xFFFF
        if checksum_calc != checksum_recv:
            return None

        vals = struct.unpack(">13H", rest[2:2+26])

        return {
            "pm1": vals[3],
            "pm25": vals[4],
            "pm10": vals[5],
        }

    import time

    def read(self, window_seconds: float = 0.4) -> Optional[Dict[str, int]]:
        """
        Try to read a PMS frame for up to window_seconds.
        Returns the first valid frame, or None if none arrive.
        """
        deadline = time.monotonic() + window_seconds

        while time.monotonic() < deadline:
            try:
                frame = self._read_frame()
                if frame is not None:
                    return frame
            except Exception:
                pass
    
            # short pause so we don't spin the CPU
            time.sleep(0.01)

        return None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
