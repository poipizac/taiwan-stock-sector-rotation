import json
import os
import pandas as pd

# Load stock info
with open("data_cache/stock_info.json", "r", encoding="utf-8") as f:
    stock_info = json.load(f)

# Load base date ratios
with open("data_cache/whale_history_20260515.json", "r", encoding="utf-8") as f:
    base_ratios = json.load(f)

# Fetch latest CSV
import requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
r = requests.get(url, headers=HEADERS, verify=False, timeout=15)
lines = r.text.split("\n")

latest_whale = {}
for line in lines[1:]:
    parts = line.strip().split(",")
    if len(parts) > 5:
        stock_id = parts[1].replace('"', '').strip()
        try:
            level = int(parts[2].replace('"', '').strip())
            if level == 15:
                pct = float(parts[5].replace('"', '').strip())
                latest_whale[stock_id] = pct
        except ValueError:
            continue

# Calculate changes
changes = []
for sid, latest in latest_whale.items():
    if sid in base_ratios:
        base = base_ratios[sid]
        diff = latest - base
        name = stock_info.get(sid, {}).get("name", "未知")
        changes.append((sid, name, base, latest, diff))

# Sort by diff
changes.sort(key=lambda x: x[4], reverse=True)

print("大戶比例增加前 20 名：")
for i, (sid, name, base, latest, diff) in enumerate(changes[:20]):
    print(f"{i+1}. {name}({sid}): 基期 {base:.2f}% -> 最新 {latest:.2f}% | 變動: +{diff:.2f}%")
