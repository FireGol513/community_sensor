# ÉMIS Community Sensor Node

This repo contains code for a Raspberry Pi Zero 2W node with:
- Two PMS5003 sensors (PMS1 on /dev/ttyS0, PMS2 on /dev/ttyAMA0)
- One BME688 (I²C)
- Optional SO₂ sensor

Data are written as 5-minute CSV chunks in `data/5minute/`
and combined into daily files in `data/daily/`.
