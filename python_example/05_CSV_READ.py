import csv

CSV_FILE = "goweather_bangkok.csv"

with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        print(row)
        if i >= 4:   # ดูแค่ 5 แถวแรก
            break
