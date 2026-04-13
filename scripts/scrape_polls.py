#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrape_polls.py

KÖZÖS POLL SCRAPER RENDSZER

Cél:
- központi, országfüggetlen scraper-vezérlő
- parser-regiszter alapú működés
- ország + forrás konfiguráció egy helyen
- országonként automatikus CSV export a meglévő pipeline számára

Kimenetek:
- docs/data/manual_polls/<country_slug>_auto.csv
- docs/data/processed/polls/scrape_status.json

Javasolt futási sorrend:
1) python scripts/scrape_polls.py
2) python scripts/fetch_and_aggregate_party_polls.py

Telepítés:
pip install requests beautifulsoup4

Fontos:
- Ez az első közös rendszer.
- Jelenleg egy stabil parser van benne: wikipedia_table
- A többi parser típushoz később bővítjük a regisztert.
- Az inaktív országok/források nem hibák, hanem tudatos placeholder-ek.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
    from bs4 import BeautifulSoup
except Exception as exc:
    print(f"[FATAL] Missing dependency: {exc}")
    print("Install with: pip install requests beautifulsoup4")
    sys.exit(1)


# ============================================================
# PATHS
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

DOCS_DIR = REPO_ROOT / "docs"
MANUAL_POLLS_DIR = DOCS_DIR / "data" / "manual_polls"
PROCESSED_POLLS_DIR = DOCS_DIR / "data" / "processed" / "polls"
RAW_POLLS_DIR = DOCS_DIR / "data" / "raw" / "polls"

SCRAPE_STATUS_OUT = PROCESSED_POLLS_DIR / "scrape_status.json"


# ============================================================
# CONFIG
# ============================================================

USER_AGENT = "balkan-security-map/common-poll-scraper"
REQUEST_TIMEOUT = 30

# FONTOS:
# - Az active=False sorok nem futnak.
# - Először egy stabil forrással indulunk (Szerbia).
# - A rendszer közös, nem az országok számától függ.
# - Később csak új source blokkot kell hozzáadni, nem új scriptet.

COUNTRY_SOURCES: List[Dict[str, Any]] = [
    {
        "country": "Serbia",
        "active": True,
        "sources": [
            {
                "source_id": "serbia_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Opinion polling for the next Serbian parliamentary election",
                "parser": "wikipedia_table",
                "url": "https://en.wikipedia.org/wiki/Opinion_polling_for_the_next_Serbian_parliamentary_election",
                "section_id": "Poll_results",
                "notes": "Első stabil automata forrás a közös rendszerhez."
            }
        ]
    },

    # Ezek most tudatosan inaktív minták.
    # Ha később pontos forrásoldalt adsz, csak active=True kell.
    {
        "country": "Romania",
        "active": False,
        "sources": [
            {
                "source_id": "romania_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Romania parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    },
    {
        "country": "Bulgaria",
        "active": False,
        "sources": [
            {
                "source_id": "bulgaria_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Bulgaria parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    },
    {
        "country": "Croatia",
        "active": False,
        "sources": [
            {
                "source_id": "croatia_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Croatia parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    },
    {
        "country": "Albania",
        "active": False,
        "sources": [
            {
                "source_id": "albania_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Albania parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    },
    {
        "country": "Kosovo",
        "active": False,
        "sources": [
            {
                "source_id": "kosovo_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Kosovo parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    },
    {
        "country": "North Macedonia",
        "active": False,
        "sources": [
            {
                "source_id": "north_macedonia_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – North Macedonia parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    },
    {
        "country": "Bosnia and Herzegovina",
        "active": False,
        "sources": [
            {
                "source_id": "bih_wikipedia_parliamentary_polling",
                "source_name": "Wikipedia – Bosnia and Herzegovina parliamentary polling",
                "parser": "wikipedia_table",
                "url": "",
                "section_id": "Poll_results",
                "notes": "Későbbi aktiválásra."
            }
        ]
    }
]


# ============================================================
# HEADER ALIASES
# ============================================================

HEADER_ALIASES = {
    "sns-led coalition": "SNS-led coalition",
    "sns-led": "SNS-led coalition",
    "sns led coalition": "SNS-led coalition",
    "sps–js": "SPS–JS",
    "sps-js": "SPS–JS",
    "sps": "SPS–JS",
    "ssp": "SSP",
    "nps": "NPS",
    "zlf": "ZLF",
    "srce": "SRCE",
    "ds": "DS",
    "nada": "NADA",
    "ndss": "NADA",
    "poks": "NADA",
    "kp": "KP",
    "mi-sn": "MI-SN",
    "others": "Others",
    "student list": "Student list",
}

KNOWN_POLLSTERS = {
    "ipsos",
    "faktor plus",
    "sprint insight",
    "nspm",
    "nova srpska politička misao",
    "cesid",
}


# ============================================================
# MODELS
# ============================================================

@dataclass
class PollRecord:
    country: str
    date: str
    source: str
    party: str
    value: float
    sample_size: Optional[int]
    fieldwork_start: Optional[str]
    fieldwork_end: Optional[str]
    notes: str


@dataclass
class ScrapeStatus:
    country: str
    source_id: str
    source_name: str
    parser: str
    url: str
    active: bool
    fetched: bool
    ok: bool
    saved_raw_path: Optional[str]
    saved_csv_path: Optional[str]
    record_count: int
    error: Optional[str]


# ============================================================
# HELPERS
# ============================================================

def ensure_dirs() -> None:
    MANUAL_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_POLLS_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def clean_text(text: str) -> str:
    text = unescape(text or "")
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_header(text: str) -> str:
    t = clean_text(text).lower()
    t = t.replace("–", "-").replace("—", "-")
    t = re.sub(r"\(.*?\)", "", t).strip()
    t = re.sub(r"\s+", " ", t)
    return HEADER_ALIASES.get(t, clean_text(text))


def parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    t = clean_text(value)
    if not t:
        return None
    m = re.search(r"-?\d+(?:[.,]\d+)?", t)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except Exception:
        return None


def parse_int(value: str) -> Optional[int]:
    if value is None:
        return None
    t = clean_text(value)
    if not t:
        return None
    m = re.search(r"\d[\d,.\s]*", t)
    if not m:
        return None
    raw = re.sub(r"[^\d]", "", m.group(0))
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def normalize_date_cell(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    raw = clean_text(text)
    if not raw:
        return None, None, None

    year_match = re.search(r"\b(20\d{2})\b", raw)
    year = year_match.group(1) if year_match else None

    months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12"
    }

    m1 = re.search(
        r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b",
        raw,
        re.I
    )
    if m1:
        day, mon_txt, yr = m1.group(1), m1.group(2).lower(), m1.group(3)
        return f"{yr}-{months[mon_txt]}-{int(day):02d}", None, None

    m2 = re.search(
        r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\b",
        raw,
        re.I
    )
    if m2 and year:
        day, mon_txt = m2.group(1), m2.group(2).lower()
        return f"{year}-{months[mon_txt]}-{int(day):02d}", None, None

    m3 = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b",
        raw,
        re.I
    )
    if m3:
        mon_txt, yr = m3.group(1).lower(), m3.group(2)
        return f"{yr}-{months[mon_txt]}", None, None

    if year:
        return year, None, None

    return None, None, None


def is_probably_pollster(text: str) -> bool:
    t = clean_text(text).lower()
    if not t:
        return False
    if t in KNOWN_POLLSTERS:
        return True
    return any(name in t for name in KNOWN_POLLSTERS)


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def save_raw_html(country: str, source_id: str, html: str) -> str:
    country_dir = RAW_POLLS_DIR / slugify(country)
    country_dir.mkdir(parents=True, exist_ok=True)
    path = country_dir / f"{slugify(source_id)}.html"
    path.write_text(html, encoding="utf-8")
    return str(path.relative_to(REPO_ROOT))


def write_country_csv(country: str, records: List[PollRecord]) -> str:
    path = MANUAL_POLLS_DIR / f"{slugify(country)}_auto.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "country",
            "date",
            "source",
            "party",
            "value",
            "sample_size",
            "fieldwork_start",
            "fieldwork_end",
            "notes",
        ])
        for r in records:
            writer.writerow([
                r.country,
                r.date,
                r.source,
                r.party,
                r.value,
                r.sample_size if r.sample_size is not None else "",
                r.fieldwork_start or "",
                r.fieldwork_end or "",
                r.notes,
            ])
    return str(path.relative_to(REPO_ROOT))


def write_status_json(statuses: List[ScrapeStatus]) -> None:
    payload = {
        "generated_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "source_count": len(statuses),
        "sources": [asdict(x) for x in statuses],
    }
    SCRAPE_STATUS_OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ============================================================
# PARSER: WIKIPEDIA TABLE
# ============================================================

def wikipedia_find_poll_results_table(soup: BeautifulSoup, section_id: str = "Poll_results"):
    headline = soup.find(id=section_id)
    if not headline:
        return None

    heading = headline.find_parent(["h2", "h3", "h4"])
    if not heading:
        return None

    node = heading
    while node:
        node = node.find_next_sibling()
        if node is None:
            break
        if getattr(node, "name", None) in {"h2", "h3"}:
            break
        if getattr(node, "name", None) == "table" and "wikitable" in (node.get("class") or []):
            return node

    return None


def wikipedia_extract_headers(table) -> List[str]:
    rows = table.find_all("tr")
    if not rows:
        return []

    header_cells = []
    for tr in rows[:4]:
        ths = tr.find_all("th")
        if not ths:
            continue
        current = [normalize_header(th.get_text(" ", strip=True)) for th in ths]
        if len(current) >= 4:
            header_cells = current

    return header_cells


def wikipedia_parse_table(country: str, source_name: str, table) -> List[PollRecord]:
    headers = wikipedia_extract_headers(table)
    if not headers:
        raise RuntimeError("Nem sikerült fejlécet kiolvasni a Wikipédia táblából.")

    rows = table.find_all("tr")
    records: List[PollRecord] = []

    for tr in rows:
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [clean_text(td.get_text(" ", strip=True)) for td in tds]
        if len(cells) < 4:
            continue

        pollster = cells[0]
        if not is_probably_pollster(pollster):
            continue

        date_cell = cells[1] if len(cells) > 1 else ""
        sample_size_cell = cells[2] if len(cells) > 2 else ""

        publication_date, fieldwork_start, fieldwork_end = normalize_date_cell(date_cell)
        sample_size = parse_int(sample_size_cell)

        if not publication_date:
            continue

        party_headers = headers[3:]
        party_values = cells[3:]
        max_len = min(len(party_headers), len(party_values))

        for i in range(max_len):
            party = normalize_header(party_headers[i])
            raw_value = party_values[i]

            if not party:
                continue
            if party.lower() in {"lead"}:
                continue

            value = parse_float(raw_value)
            if value is None:
                continue

            records.append(
                PollRecord(
                    country=country,
                    date=publication_date,
                    source=pollster or source_name,
                    party=party,
                    value=value,
                    sample_size=sample_size,
                    fieldwork_start=fieldwork_start,
                    fieldwork_end=fieldwork_end,
                    notes="auto_scraped_from_wikipedia_table",
                )
            )

    return records


def scrape_with_wikipedia_table_parser(
    session: requests.Session,
    country: str,
    source_id: str,
    source_name: str,
    url: str,
    section_id: str,
) -> Tuple[List[PollRecord], str]:
    if not url:
        raise RuntimeError("A forrás URL üres.")

    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    html = resp.text
    raw_path = save_raw_html(country, source_id, html)

    soup = BeautifulSoup(html, "html.parser")
    table = wikipedia_find_poll_results_table(soup, section_id=section_id)
    if table is None:
        raise RuntimeError(f"Nem találtam meg a '{section_id}' szekció tábláját.")

    records = wikipedia_parse_table(country, source_name, table)
    if not records:
        raise RuntimeError("A táblából nem sikerült használható rekordokat kinyerni.")

    return records, raw_path


# ============================================================
# PARSER REGISTRY
# ============================================================

def scrape_source(session: requests.Session, country: str, source_cfg: Dict[str, Any]) -> Tuple[List[PollRecord], Optional[str]]:
    parser = source_cfg.get("parser")

    if parser == "wikipedia_table":
        return scrape_with_wikipedia_table_parser(
            session=session,
            country=country,
            source_id=str(source_cfg.get("source_id", "")),
            source_name=str(source_cfg.get("source_name", "")),
            url=str(source_cfg.get("url", "")),
            section_id=str(source_cfg.get("section_id", "Poll_results")),
        )

    raise RuntimeError(f"Ismeretlen parser: {parser}")


# ============================================================
# MAIN LOGIC
# ============================================================

def merge_country_records(records: List[PollRecord]) -> List[PollRecord]:
    """
    Később itt lehet deduplikálni.
    Most csak visszaadjuk a rendezett listát.
    """
    return sorted(
        records,
        key=lambda x: (x.country, x.date, x.source, x.party)
    )


def run() -> None:
    ensure_dirs()
    session = build_session()

    statuses: List[ScrapeStatus] = []

    print("=== Common Poll Scraper ===")

    for country_cfg in COUNTRY_SOURCES:
        country = str(country_cfg.get("country", "")).strip()
        country_active = bool(country_cfg.get("active", False))
        sources = country_cfg.get("sources", []) or []

        if not country:
            continue

        print(f"\n[COUNTRY] {country} | active={country_active}")

        country_records: List[PollRecord] = []

        if not country_active:
            for source_cfg in sources:
                statuses.append(
                    ScrapeStatus(
                        country=country,
                        source_id=str(source_cfg.get("source_id", "unknown")),
                        source_name=str(source_cfg.get("source_name", "unknown")),
                        parser=str(source_cfg.get("parser", "")),
                        url=str(source_cfg.get("url", "")),
                        active=False,
                        fetched=False,
                        ok=False,
                        saved_raw_path=None,
                        saved_csv_path=None,
                        record_count=0,
                        error="country_inactive",
                    )
                )
            print("  - skipped (country inactive)")
            continue

        for source_cfg in sources:
            source_id = str(source_cfg.get("source_id", "unknown"))
            source_name = str(source_cfg.get("source_name", "unknown"))
            parser = str(source_cfg.get("parser", ""))
            url = str(source_cfg.get("url", ""))
            active = bool(source_cfg.get("active", True))

            status = ScrapeStatus(
                country=country,
                source_id=source_id,
                source_name=source_name,
                parser=parser,
                url=url,
                active=active,
                fetched=False,
                ok=False,
                saved_raw_path=None,
                saved_csv_path=None,
                record_count=0,
                error=None,
            )

            if not active:
                status.error = "source_inactive"
                statuses.append(status)
                print(f"  - {source_id}: skipped (source inactive)")
                continue

            try:
                records, raw_path = scrape_source(session, country, source_cfg)
                status.fetched = True
                status.ok = True
                status.saved_raw_path = raw_path
                status.record_count = len(records)

                country_records.extend(records)
                print(f"  - {source_id}: ok ({len(records)} rekord)")
            except Exception as exc:
                status.fetched = True
                status.ok = False
                status.error = str(exc)
                print(f"  - {source_id}: FAIL ({exc})")

            statuses.append(status)

        if country_records:
            merged = merge_country_records(country_records)
            csv_path = write_country_csv(country, merged)

            # az adott országhoz tartozó sikeres status sorokhoz hozzáírjuk a csv path-t
            for st in statuses:
                if st.country == country and st.ok:
                    st.saved_csv_path = csv_path

            print(f"  => wrote CSV: {csv_path} ({len(merged)} rekord)")
        else:
            print("  => no usable records")

    write_status_json(statuses)

    ok_count = sum(1 for s in statuses if s.ok)
    fail_count = sum(1 for s in statuses if s.fetched and not s.ok)
    skipped_count = sum(1 for s in statuses if not s.active)

    print("\n=== Summary ===")
    print(f"sources total: {len(statuses)}")
    print(f"sources ok: {ok_count}")
    print(f"sources failed: {fail_count}")
    print(f"sources skipped: {skipped_count}")
    print(f"status written: {SCRAPE_STATUS_OUT.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    run()
