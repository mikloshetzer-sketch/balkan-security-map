
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import csv
import re
from pathlib import Path
from datetime import datetime

# =============================
# CONFIG
# =============================

COUNTRIES = [
    "Serbia",
    "Romania",
    "Bulgaria",
    "Croatia",
    "Albania",
    "Kosovo",
    "North Macedonia",
    "Bosnia and Herzegovina"
]

BASE_WIKI = "https://en.wikipedia.org/wiki/Opinion_polling_for_the_next_{}_parliamentary_election"

OUT_DIR = Path("docs/data/manual_polls")
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "balkan-monitor"
}

# =============================
# HELPERS
# =============================

def slugify(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code != 200:
        return None
    return r.text


def parse_percent(text):
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if m:
        return float(m.group(1))
    return None


# =============================
# WIKIPEDIA PARSER
# =============================

def parse_wikipedia(country):
    url = BASE_WIKI.format(country.replace(" ", "_"))

    print(f"[{country}] Trying Wikipedia...")

    html = fetch(url)
    if not html:
        print("  → no page")
        return []

    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table", {"class": "wikitable"})
    if not tables:
        print("  → no tables")
        return []

    records = []

    for table in tables:
        rows = table.find_all("tr")

        headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

        if len(headers) < 4:
            continue

        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            pollster = cols[0].get_text(strip=True)
            date = cols[1].get_text(strip=True)

            for i in range(3, len(cols)):
                party = headers[i]
                value = parse_percent(cols[i].get_text())

                if value is None:
                    continue

                records.append([
                    country,
                    date,
                    pollster,
                    party,
                    value
                ])

    print(f"  → {len(records)} records")

    return records


# =============================
# NEWS FALLBACK (egyszerű)
# =============================

def parse_news_fallback(country):
    print(f"[{country}] Trying news fallback...")

    # egyszerű Google News RSS
    url = f"https://news.google.com/rss/search?q={country}+poll"

    try:
        html = fetch(url)
        if not html:
            return []
    except:
        return []

    # itt csak logolunk, nincs komoly parsing
    print("  → no structured poll data")

    return []


# =============================
# MAIN
# =============================

def main():
    all_count = 0

    for country in COUNTRIES:
        records = []

        # 1. Wikipedia
        records = parse_wikipedia(country)

        # 2. fallback
        if not records:
            records = parse_news_fallback(country)

        # 3. save
        if records:
            out = OUT_DIR / f"{slugify(country)}_auto.csv"

            with open(out, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["country", "date", "source", "party", "value"])
                writer.writerows(records)

            print(f"  → saved {out}")
            all_count += len(records)
        else:
            print(f"[{country}] ❌ no data")

    print("\n=== DONE ===")
    print(f"Total records: {all_count}")


if __name__ == "__main__":
    main()
