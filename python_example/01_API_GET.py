import requests

url = "https://goweather.xyz/weather/Bangkok"

r = requests.get(url, timeout=10)
r.raise_for_status()

data = r.json()
print(data["wind"])
