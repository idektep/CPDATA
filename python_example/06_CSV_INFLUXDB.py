import csv
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WriteOptions


CSV_FILE = "weather_ocean_2024.csv"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "YOUR_INFLUX_TOKEN"
INFLUX_ORG = "YOUR_ORG"
INFLUX_BUCKET = "YOUR_BUCKET"

MEASUREMENT = "weather_ocean"
BATCH_SIZE = 500


def parse_timestamp(s: str) -> datetime:
    if not s:
        raise ValueError("Empty timestamp")

    s = s.strip()

    #nanosecond
    if "." in s:
        s = s.split(".")[0]

    # format: yyyy-mm-dd HH:MM:SS
    return datetime.strptime(
        s, "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)


client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(
    write_options=WriteOptions(batch_size=BATCH_SIZE)
)

points = []
written = 0
skipped = 0

with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        try:
            dt = parse_timestamp(row["date"])

            point = (
                Point(MEASUREMENT)
                .tag("country", row["country"])
                .field("temperature_C", float(row["temperature_C"]))
                .field("humidity_percent", float(row["humidity_percent"]))
                .field("wind_speed_mps", float(row["wind_speed_mps"]))
                .field("wave_height_m", float(row["wave_height_m"]))
                .field("pressure_hPa", float(row["pressure_hPa"]))
                .time(dt)
            )

            points.append(point)
            written += 1

            if len(points) >= BATCH_SIZE:
                write_api.write(
                    bucket=INFLUX_BUCKET,
                    org=INFLUX_ORG,
                    record=points
                )
                points.clear()

        except Exception as e:
            skipped += 1
            print("[SKIP]", row.get("date"), e)

if points:
    write_api.write(
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG,
        record=points
    )

write_api.flush()
write_api.close()
client.close()

print("===================================")
print("IMPORT FINISHED")
print("Written :", written)
print("Skipped :", skipped)
print("Measurement :", MEASUREMENT)
print("Bucket :", INFLUX_BUCKET)
print("===================================")
