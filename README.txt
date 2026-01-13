ÉMIS SENSOR NODE – RASPBERRY PI ZERO 2 W

This repository contains the software used to operate an ÉMIS particulate matter and SO2
monitoring node. The system is designed to run continuously on a Raspberry Pi
Zero 2 W and record air-quality measurements to daily CSV files.

The node is modular, resilient to sensor failure, and intended for long-term,
unattended deployment in field or community monitoring settings.

WHAT THIS SENSOR DOES

At a fixed sampling interval (every 2 seconds), the node:
	1.	Reads data from enabled sensors:
	•	Two PMS5003 particulate sensors (PM1, PM2.5, PM10)
	•	One BME688 (temperature, relative humidity, pressure, VOC resistance)
	•	One DFRobot Gravity SO2 sensor (I2C)
	2.	Computes PMS sensor agreement diagnostics
	3.	Writes one row per sample to a rolling daily CSV file
	4.	Automatically creates a new CSV file at local midnight
	5.	Logs runtime activity and errors to a log file

The system does not require a database. CSV files are the authoritative data
output.

DIRECTORY STRUCTURE

emis/
│
├── code/
│   ├── collect_data.py        Main data collection loop
│   ├── daily_writer.py        Handles daily CSV creation and column order
│   ├── sensor_status.py       Optional CLI sensor health checker
│   │
│   ├── sensors/
│   │   ├── pms.py             PMS5003 serial reader
│   │   ├── bme.py             BME688 temperature/RH/pressure/VOC reader
│   │   └── so2.py             DFRobot Gravity SO2 reader (I2C)
│   │
│   └── utils/
│       └── timekeeping.py     UTC/local time utilities
│
├── config/
│   └── node.yaml              Node configuration file
│
├── data/
│   └── daily/
│       └── NodeX_YYYY-MM-DD.csv   Daily measurement files
│
├── logs/
│   └── emis.log               Runtime and error logs
│
└── README.txt

CONFIGURATION (node.yaml)

All node-specific configuration is defined in config/node.yaml.

Key settings include:
	•	node_id:        Unique identifier for the node
	•	timezone:       Local timezone used for daily file rollover
	•	tick_seconds:   Sampling interval in seconds

Each sensor has an enable flag and hardware-specific settings (port, I2C
address, etc.). Disabled sensors remain in the CSV schema but produce blank
values.

DAILY CSV OUTPUT

CSV files are written to:

data/daily/<node_id>_YYYY-MM-DD.csv

A new file is created automatically at local midnight.

COLUMN GROUPS

Timestamps
	•	timestamp_utc
	•	timestamp_local
	•	node_id

BME688
	•	temp_c
	•	rh_pct
	•	pressure_hpa
	•	voc_ohm
	•	bme_status

PMS5003 – sensor 1
	•	pm1_atm_pms1
	•	pm25_atm_pms1
	•	pm10_atm_pms1
	•	pms1_status

PMS5003 – sensor 2
	•	pm1_atm_pms2
	•	pm25_atm_pms2
	•	pm10_atm_pms2
	•	pms2_status

PMS agreement diagnostics (PM2.5)
	•	pm25_pms_mean
	•	pm25_pms_rpd
	•	pm25_pair_flag
	•	pm25_suspect_sensor

SO2
	•	so2_ppm
	•	so2_raw
	•	so2_byte0
	•	so2_byte1
	•	so2_error
	•	so2_status

All columns are always present. Missing data is represented as blank values
or explicit strings such as NODATA.

PMS AGREEMENT LOGIC

The node continuously evaluates agreement between PMS1 and PMS2 using PM2.5:
	•	Computes relative percent difference (RPD)
	•	Maintains rolling baselines (last 30 valid samples)
	•	Flags sensor disagreement and likely sensor drift

Possible values for pm25_pair_flag include:
	•	OK
	•	LOW_PM_OK
	•	MISMATCH
	•	PMS1_BAD
	•	PMS2_BAD
	•	BOTH_BAD
	•	INCOMPLETE

pm25_suspect_sensor is explicitly set to OK when neither sensor is suspect.

Raw sensor values are always preserved; diagnostics are additive.

LOGGING

Runtime logs are written to:

logs/emis.log

Logs include:
	•	Sensor initialization messages
	•	Read and communication errors
	•	SO2 frame parsing issues
	•	Startup and shutdown events

If no log output appears, the program likely did not start.

RUNNING THE NODE

From the project root directory:

cd ~/emis/code
python3 collect_data.py

collect_data.py and daily_writer.py start on boot, so no action is needed
to initialize data rathering or the creation of daily data files

POSSIBLE EXTENSIONS
	•	Calibration layers applied in post-processing
	•	Automated upload or synchronization to github or other server
	•	Node heartbeat or health summary file

END OF FILE
