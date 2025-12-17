import requests
from datetime import datetime, timezone

def to_float(text):
    # "+31 °C" -> 31.0
    return float(text.replace("+", "").replace("°C", "").strip())

def wind_to_float(text):
    # "7 km/h" -> 7.0
    return float(text.replace("km/h", "").strip())


url = "https://goweather.xyz/weather/Bangkok"

r = requests.get(url, timeout=10)
r.raise_for_status()

data = r.json()
# print(data)


record = {
    "measurement": "weather_api",
    "tags": {
        "city": "Bangkok",
        "source": "goweather"
    },
    "fields": {
        "temp": to_float(data["temperature"]),
        "wind": wind_to_float(data["wind"]),
    },
    "timestamp": datetime.now(timezone.utc)
}

print(record)


