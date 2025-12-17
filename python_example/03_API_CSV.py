import requests
import csv
import os
import time
from datetime import datetime, timezone


CITY = "Bangkok"
URL = f"https://goweather.xyz/weather/{CITY}"
CSV_FILE = "goweather_bangkok.csv"
INTERVAL_SEC = 300  # 5 นาที


def to_float_temp(text):
    return float(text.replace("+", "").replace("°C", "").strip())

def to_float_wind(text):
    return float(text.replace("km/h", "").strip())

def fetch_weather():
    r = requests.get(URL, timeout=10)
    r.raise_for_status()
    return r.json()


file_exists = os.path.isfile(CSV_FILE)

with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    if not file_exists:
        writer.writerow([
            "timestamp",
            "city",
            "temp_c",
            "wind_kmh",
            "description"
        ])


while True:
    try:
        data = fetch_weather()

        timestamp = datetime.now(timezone.utc).isoformat()
        temp = to_float_temp(data["temperature"])
        wind = to_float_wind(data["wind"])
        desc = data["description"]

        row = [
            timestamp,
            CITY,
            temp,
            wind,
            desc
        ]

        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)

        print(f"[OK] {row}")

    except Exception as e:
        print("[ERROR]", e)

    time.sleep(INTERVAL_SEC)
