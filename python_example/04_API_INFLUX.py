import time
import requests
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WriteOptions


CITY = "Bangkok"
API_URL = f"https://goweather.xyz/weather/{CITY}"
INTERVAL_SEC = 300  # ดึงทุก 5 นาที

# --- InfluxDB v2 ---
INFLUX_URL = "http://localhost:8086"     # ถ้า python อยู่นอก docker
# INFLUX_URL = "http://influxdb:8086"    # ถ้า python อยู่ใน docker เดียวกัน
INFLUX_TOKEN = "YOUR_INFLUX_TOKEN"
INFLUX_ORG = "YOUR_ORG"
INFLUX_BUCKET = "YOUR_BUCKETr"

MEASUREMENT = "weather_api"

def to_float_temp(text: str) -> float:

    return float(text.replace("+", "").replace("°C", "").strip())

def to_float_wind(text: str) -> float:

    return float(text.replace("km/h", "").strip())

def fetch_weather() -> dict:
    r = requests.get(API_URL, timeout=10)
    r.raise_for_status()
    return r.json()

# =====================================================
# INFLUXDB CLIENT
# =====================================================

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(
    write_options=WriteOptions(
        batch_size=1,
        flush_interval=1_000
    )
)

# =====================================================
# MAIN LOOP
# =====================================================

print("Weather API → InfluxDB started")
print(f"City: {CITY}")
print(f"Interval: {INTERVAL_SEC} sec")
print("-" * 50)

while True:
    try:
        data = fetch_weather()

        temp = to_float_temp(data["temperature"])
        wind = to_float_wind(data["wind"])
        description = data.get("description", "")

        now = datetime.now(timezone.utc)

        point = (
            Point(MEASUREMENT)
            .tag("city", CITY)
            .tag("source", "goweather")
            .field("temp", temp)
            .field("wind", wind)
            .field("description", description)
            .time(now)
        )

        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=point
        )

        print(f"[OK] {now.isoformat()} | temp={temp}°C wind={wind}km/h {description}")

    except Exception as e:
        print("[ERROR]", e)

    time.sleep(INTERVAL_SEC)
