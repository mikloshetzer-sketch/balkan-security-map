#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrape_polls.py

KÖZÖS POLL SCRAPER RENDSZER – ORSZÁG-HINTEKKEL, RUGALMAS WIKIPÉDIA PARSERREL

Mit csinál:
- végigmegy az összes figyelt országon
- országonként több ismert / valószínű Wikipédia oldalcímet próbál
- ha talál használható wikitable poll táblát, rekordokat képez
- fejléc alapján próbálja felismerni:
    - pollster oszlop
    - dátum oszlop
    - mintanagyság oszlop
    - pártoszlopok
- országonként automatikus CSV-t ír:
    docs/data/manual_polls/<country_slug>_auto.csv
- scrape státuszt ír:
    docs/data/processed/polls/scrape_status.json

Használat:
1) pip install requests beautifulsoup4
2) python scripts/scrape_polls.py
3) python scripts/fetch_and_aggregate_party_polls.py
"""

from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote

try:
    import requests
    from bs4 import BeautifulSoup
except Exception as exc:
    print(f"[FATAL] Missing dependency: {exc}")
    print("Install with: pip install requests beautifulsoup4")
    sys.exit(1)


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def slugify_seed(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def slugify(value: str) -> str:
    return slugify_seed(value)


def clean_text(text: str) -> str:
    text = unescape(text or "")
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    t = clean_text(value)
    if not t or t in {"—", "-", "–", "N/A", "n/a"}:
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

COUNTRIES: List[str] = [
    "Serbia",
    "Romania",
    "Bulgaria",
    "Croatia",
    "Albania",
    "Kosovo",
    "North Macedonia",
    "Bosnia and Herzegovina",
    "Montenegro",
]

COUNTRY_WIKIPEDIA_HINTS: Dict[str, List[str]] = {
    "Serbia": [
        "Opinion polling for the next Serbian parliamentary election",
        "2027 Serbian parliamentary election",
        "Next Serbian parliamentary election",
    ],
    "Romania": [
        "Opinion polling for the next Romanian legislative election",
        "Opinion polling for the next Romanian parliamentary election",
        "2028 Romanian legislative election",
        "Next Romanian legislative election",
    ],
    "Bulgaria": [
        "Opinion polling for the next Bulgarian parliamentary election",
        "2027 Bulgarian parliamentary election",
        "Next Bulgarian parliamentary election",
        "Next Bulgarian parliamentary election opinion polls",
    ],
    "Croatia": [
        "Opinion polling for the next Croatian parliamentary election",
        "2028 Croatian parliamentary election",
        "Next Croatian parliamentary election",
    ],
    "Albania": [
        "Opinion polling for the next Albanian parliamentary election",
        "2029 Albanian parliamentary election",
        "2025 Albanian parliamentary election",
        "Next Albanian parliamentary election",
    ],
    "Kosovo": [
        "Opinion polling for the next Kosovan parliamentary election",
        "Opinion polling for the next Kosovo parliamentary election",
        "December 2025 Kosovan parliamentary election",
        "2025 Kosovan parliamentary election",
        "2025 Kosovo parliamentary election",
        "2029 Kosovan parliamentary election",
        "Next Kosovan parliamentary election",
        "Next Kosovo parliamentary election",
    ],
    "North Macedonia": [
        "Opinion polling for the next Macedonian parliamentary election",
        "Opinion polling for the next North Macedonian parliamentary election",
        "2028 Macedonian parliamentary election",
        "2028 North Macedonian parliamentary election",
        "Next Macedonian parliamentary election",
        "Next North Macedonian parliamentary election",
    ],
    "Bosnia and Herzegovina": [
        "Opinion polling for the next Bosnia and Herzegovina general election",
        "Opinion polling for the next Bosnia and Herzegovina parliamentary election",
        "2026 Bosnia and Herzegovina general election",
        "Next Bosnia and Herzegovina general election",
    ],
    "Montenegro": [
        "Next Montenegrin parliamentary election",
        "Opinion polling for the next Montenegrin parliamentary election",
        "2027 Montenegrin parliamentary election",
        "2027 Montenegro parliamentary election",
        "Next Montenegro parliamentary election",
    ],
}

COUNTRY_PARSER_PLAN: Dict[str, List[Dict[str, str]]] = {
    country: [
        {
            "source_id": f"{slugify_seed(country)}_wikipedia_hinted_polling",
            "source_name": f"Wikipedia hinted polling resolver – {country}",
            "parser": "wikipedia_hinted_polling",
        }
    ]
    for country in COUNTRIES
}


# ============================================================
# NORMALIZATION / ALIASES
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
    "serbia against violence": "Serbia Against Violence",
    "we - voice from the people": "MI-SN",
    "we – voice from the people": "MI-SN",
    "nestorović": "MI-SN",
}

KNOWN_POLLSTERS = {
    "ipsos",
    "faktor plus",
    "sprint insight",
    "nspm",
    "nova srpska politička misao",
    "cesid",
    "avangarde",
    "atlasintel",
    "iras",
    "imas",
    "gallup",
    "yougov",
    "sova harris",
    "alpha research",
    "market links",
    "marketlinks",
    "trend",
    "sociological agency",
    "median",
    "inscop",
    "curs",
    "flashdata",
    "barometar",
    "barometer",
    "prism research",
    "idra",
    "cedem",
    "ubo consulting",
    "m-prospect",
    "m prospect",
    "spektrum",
    "stars up",
    "borba",
    "pipos",
    "albanian post",
    "koha",
    "cam",
    "myara",
    "promocija plus",
}

NON_PARTY_HEADERS = {
    "lead",
    "ref",
    "references",
    "source",
    "polling firm",
    "pollster",
    "date",
    "date of publication",
    "fieldwork date",
    "fieldwork",
    "sample size",
    "notes",
    "lead ±",
    "lead %",
    "margin",
    "change",
    "swing",
}

DATE_HEADER_HINTS = {
    "date",
    "date of publication",
    "fieldwork date",
    "fieldwork",
    "publication date",
    "published",
}

POLLSTER_HEADER_HINTS = {
    "polling firm",
    "pollster",
    "polling agency",
    "agency",
    "source",
    "firm",
    "pollster/source",
}

SAMPLE_HEADER_HINTS = {
    "sample size",
    "sample",
    "n",
}

GENERIC_NON_PARTY_HINTS = {
    "lead",
    "margin",
    "change",
    "swing",
    "others/undecided",
    "undecided",
    "turnout",
    "approval",
    "disapproval",
    "vote share",
}


def normalize_header(text: str) -> str:
    t = clean_text(text).lower()
    t = t.replace("–", "-").replace("—", "-")
    t = re.sub(r"\(.*?\)", "", t).strip()
    t = re.sub(r"\s+", " ", t)
    return HEADER_ALIASES.get(t, clean_text(text))


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
    resolved_url: Optional[str]
    active: bool
    fetched: bool
    ok: bool
    saved_raw_path: Optional[str]
    saved_csv_path: Optional[str]
    record_count: int
    error: Optional[str]


# ============================================================
# FILE HELPERS
# ============================================================

def ensure_dirs() -> None:
    MANUAL_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_POLLS_DIR.mkdir(parents=True, exist_ok=True)


def write_status_json(statuses: List[ScrapeStatus]) -> None:
    payload = {
        "generated_utc": utc_now_iso(),
        "source_count": len(statuses),
        "sources": [asdict(x) for x in statuses],
    }
    SCRAPE_STATUS_OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def save_raw_html(country: str, source_id: str, suffix: str, html: str) -> str:
    country_dir = RAW_POLLS_DIR / slugify(country)
    country_dir.mkdir(parents=True, exist_ok=True)
    path = country_dir / f"{slugify(source_id)}{suffix}.html"
    path.write_text(html, encoding="utf-8")
    return str(path.relative_to(REPO_ROOT))


def dedupe_records(records: List[PollRecord]) -> List[PollRecord]:
    seen: Set[Tuple[str, str, str, str, float]] = set()
    out: List[PollRecord] = []
    for r in sorted(records, key=lambda x: (x.country, x.date, x.source, x.party, x.value)):
        key = (r.country, r.date, r.source, r.party, r.value)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


# ============================================================
# DATE PARSING
# ============================================================

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

    m4 = re.search(r"\b(20\d{2})[-/](\d{2})[-/](\d{2})\b", raw)
    if m4:
        return f"{m4.group(1)}-{m4.group(2)}-{m4.group(3)}", None, None

    m5 = re.search(r"\b(20\d{2})[-/](\d{2})\b", raw)
    if m5:
        return f"{m5.group(1)}-{m5.group(2)}", None, None

    if year:
        return year, None, None

    return None, None, None


# ============================================================
# WIKIPEDIA RESOLVER
# ============================================================

def wikipedia_url_from_title(title: str) -> str:
    return f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'), safe=':_()%-')}"


def wikipedia_title_candidates(country: str) -> List[str]:
    hinted = COUNTRY_WIKIPEDIA_HINTS.get(country, [])

    generic = [
        f"Opinion polling for the next {country} parliamentary election",
        f"Opinion polling for the next {country} legislative election",
        f"Opinion polling for the next {country} general election",
        f"Opinion polling for the {country} parliamentary election",
        f"Opinion polling for the {country} legislative election",
        f"Opinion polling for the {country} general election",
        f"Next {country} parliamentary election",
        f"Next {country} legislative election",
        f"Next {country} general election",
    ]

    for year in ("2029", "2028", "2027", "2026", "2025", "2024"):
        generic.extend([
            f"Opinion polling for the {year} {country} parliamentary election",
            f"Opinion polling for the {year} {country} legislative election",
            f"Opinion polling for the {year} {country} general election",
            f"{year} {country} parliamentary election",
            f"{year} {country} legislative election",
            f"{year} {country} general election",
        ])

    seen = set()
    out: List[str] = []
    for item in hinted + generic:
        item = re.sub(r"\s+", " ", item).strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)

    return out


def resolve_wikipedia_page(session: requests.Session, country: str) -> Optional[Tuple[str, str]]:
    for title in wikipedia_title_candidates(country):
        url = wikipedia_url_from_title(title)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except Exception:
            continue

        if resp.status_code != 200:
            continue

        final_url = resp.url or url
        html = resp.text.lower()

        if "wikipedia does not have an article with this exact name" in html:
            continue

        if "election" not in html and "poll" not in html:
            continue

        return title, final_url

    return None


# ============================================================
# WIKIPEDIA TABLE PARSER
# ============================================================

def is_probably_pollster_value(text: str) -> bool:
    t = clean_text(text).lower()
    if not t:
        return False
    if t in KNOWN_POLLSTERS:
        return True
    return any(name in t for name in KNOWN_POLLSTERS)


def header_kind(header_text: str) -> str:
    h = normalize_header(header_text).lower()

    if h in POLLSTER_HEADER_HINTS:
        return "pollster"
    if h in DATE_HEADER_HINTS:
        return "date"
    if h in SAMPLE_HEADER_HINTS:
        return "sample"
    if h in NON_PARTY_HEADERS or h in GENERIC_NON_PARTY_HINTS:
        return "non_party"

    return "party"


def wikipedia_find_best_poll_table(soup: BeautifulSoup):
    preferred_section_ids = [
        "Party_polling",
        "Party_poll",
        "Opinion_polls",
        "Opinion_polls",
        "Opinion_polling",
        "Poll_results",
        "Polling",
        "Polls",
    ]

    for section_id in preferred_section_ids:
        headline = soup.find(id=section_id)
        if not headline:
            continue

        heading = headline.find_parent(["h2", "h3", "h4"])
        if not heading:
            continue

        node = heading
        while node:
            node = node.find_next_sibling()
            if node is None:
                break
            if getattr(node, "name", None) in {"h2", "h3"}:
                break
            if getattr(node, "name", None) == "table" and "wikitable" in (node.get("class") or []):
                return node

    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        return None

    def table_score(table) -> int:
        txt = clean_text(table.get_text(" ", strip=True)).lower()
        score = 0
        if "polling firm" in txt:
            score += 5
        if "sample size" in txt:
            score += 4
        if "date of publication" in txt:
            score += 3
        if "party polling" in txt:
            score += 5
        if "opinion polls" in txt:
            score += 5
        if "poll" in txt:
            score += 2
        if "party" in txt:
            score += 2
        if "%" in txt:
            score += 2
        score += min(len(table.find_all("tr")), 12)
        return score

    return sorted(tables, key=table_score, reverse=True)[0]


def extract_header_matrix(table) -> Tuple[List[str], int]:
    rows = table.find_all("tr")
    if not rows:
        return [], 0

    header_rows: List[List[str]] = []
    header_row_count = 0

    for tr in rows[:6]:
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if ths and (len(ths) >= len(tds)):
            current = [normalize_header(th.get_text(" ", strip=True)) for th in ths]
            if current:
                header_rows.append(current)
                header_row_count += 1
        else:
            break

    if not header_rows:
        return [], 0

    base = header_rows[-1][:]

    if len(header_rows) >= 2:
        upper = header_rows[-2]
        merged: List[str] = []
        for idx, item in enumerate(base):
            prefix = upper[idx] if idx < len(upper) else ""
            prefix = normalize_header(prefix)
            if prefix and prefix.lower() not in {"", item.lower()} and item.lower() not in NON_PARTY_HEADERS:
                merged.append(f"{prefix} {item}".strip())
            else:
                merged.append(item)
        base = merged

    return base, header_row_count


def infer_column_roles(headers: List[str], sample_row: List[str]) -> Dict[str, object]:
    roles: Dict[str, object] = {
        "pollster_idx": None,
        "date_idx": None,
        "sample_idx": None,
        "party_indices": [],
        "non_party_indices": [],
    }

    for idx, h in enumerate(headers):
        kind = header_kind(h)
        if kind == "pollster" and roles["pollster_idx"] is None:
            roles["pollster_idx"] = idx
        elif kind == "date" and roles["date_idx"] is None:
            roles["date_idx"] = idx
        elif kind == "sample" and roles["sample_idx"] is None:
            roles["sample_idx"] = idx
        elif kind == "party":
            roles["party_indices"].append(idx)
        else:
            roles["non_party_indices"].append(idx)

    if roles["pollster_idx"] is None:
        for idx, val in enumerate(sample_row):
            txt = clean_text(val)
            if txt and parse_float(txt) is None:
                roles["pollster_idx"] = idx
                break

    if roles["date_idx"] is None:
        for idx, val in enumerate(sample_row):
            publication_date, _, _ = normalize_date_cell(val)
            if publication_date:
                roles["date_idx"] = idx
                break

    if roles["sample_idx"] is None:
        for idx, val in enumerate(sample_row):
            iv = parse_int(val)
            if iv and iv >= 100:
                roles["sample_idx"] = idx
                break

    if not roles["party_indices"]:
        reserved = {roles["pollster_idx"], roles["date_idx"], roles["sample_idx"]}
        roles["party_indices"] = [i for i in range(len(headers)) if i not in reserved]

    return roles


def trim_cells_to_headers(cells: List[str], headers: List[str]) -> List[str]:
    if len(cells) == len(headers):
        return cells
    if len(cells) > len(headers):
        return cells[:len(headers)]
    return cells + [""] * (len(headers) - len(cells))


def wikipedia_parse_table(country: str, source_name: str, table) -> List[PollRecord]:
    headers, header_row_count = extract_header_matrix(table)
    if not headers:
        raise RuntimeError("Nem sikerült fejlécet kiolvasni a Wikipédia táblából.")

    rows = table.find_all("tr")
    records: List[PollRecord] = []

    sample_row_cells: Optional[List[str]] = None
    for tr in rows[header_row_count:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        sample_row_cells = [clean_text(td.get_text(" ", strip=True)) for td in tds]
        sample_row_cells = trim_cells_to_headers(sample_row_cells, headers)
        break

    if sample_row_cells is None:
        raise RuntimeError("Nincs feldolgozható adatsor a táblában.")

    roles = infer_column_roles(headers, sample_row_cells)
    pollster_idx = roles["pollster_idx"]
    date_idx = roles["date_idx"]
    sample_idx = roles["sample_idx"]
    party_indices = roles["party_indices"]

    if pollster_idx is None or date_idx is None or not party_indices:
        raise RuntimeError("Nem sikerült azonosítani a fő oszlopokat (pollster/date/party).")

    for tr in rows[header_row_count:]:
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [clean_text(td.get_text(" ", strip=True)) for td in tds]
        cells = trim_cells_to_headers(cells, headers)

        pollster = cells[pollster_idx] if pollster_idx < len(cells) else ""
        date_cell = cells[date_idx] if date_idx < len(cells) else ""
        sample_size_cell = cells[sample_idx] if sample_idx is not None and sample_idx < len(cells) else ""

        if not pollster:
            continue
        if not is_probably_pollster_value(pollster):
            if parse_float(pollster) is not None:
                continue

        publication_date, fieldwork_start, fieldwork_end = normalize_date_cell(date_cell)
        if not publication_date:
            continue

        sample_size = parse_int(sample_size_cell)

        for idx in party_indices:
            if idx >= len(cells) or idx >= len(headers):
                continue

            party = normalize_header(headers[idx])
            raw_value = cells[idx]

            if not party:
                continue

            party_l = party.lower()
            if party_l in NON_PARTY_HEADERS or party_l in GENERIC_NON_PARTY_HINTS:
                continue
            if party_l in {"approval", "disapproval", "turnout", "votes", "vote"}:
                continue

            value = parse_float(raw_value)
            if value is None:
                continue
            if value < 0 or value > 100:
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
                    notes="auto_scraped_from_wikipedia_hinted_resolver_v4",
                )
            )

    return records


def scrape_with_wikipedia_hinted_polling(
    session: requests.Session,
    country: str,
    source_id: str,
    source_name: str,
) -> Tuple[List[PollRecord], Optional[str], Optional[str]]:
    resolved = resolve_wikipedia_page(session, country)
    if resolved is None:
        raise RuntimeError("Nem találtam használható Wikipédia polling / election oldalt ország-hintekkel sem.")

    resolved_title, resolved_url = resolved
    resp = session.get(resolved_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    html = resp.text
    raw_path = save_raw_html(country, source_id, "", html)

    soup = BeautifulSoup(html, "html.parser")
    table = wikipedia_find_best_poll_table(soup)
    if table is None:
        raise RuntimeError(f"A feloldott oldalon nincs használható wikitable: {resolved_title}")

    records = wikipedia_parse_table(country, source_name, table)
    records = dedupe_records(records)

    if not records:
        raise RuntimeError(f"A feloldott oldalon nem sikerült használható poll rekordot kinyerni: {resolved_title}")

    return records, raw_path, resolved_url


# ============================================================
# PARSER REGISTRY
# ============================================================

def scrape_source(
    session: requests.Session,
    country: str,
    source_cfg: Dict[str, str]
) -> Tuple[List[PollRecord], Optional[str], Optional[str]]:
    parser = source_cfg.get("parser")

    if parser == "wikipedia_hinted_polling":
        return scrape_with_wikipedia_hinted_polling(
            session=session,
            country=country,
            source_id=str(source_cfg.get("source_id", "")),
            source_name=str(source_cfg.get("source_name", "")),
        )

    raise RuntimeError(f"Ismeretlen parser: {parser}")


# ============================================================
# MAIN
# ============================================================

def run() -> None:
    ensure_dirs()
    session = build_session()
    statuses: List[ScrapeStatus] = []

    print("=== Common Poll Scraper – Resolver v4 with Kosovo + Montenegro ===")

    for country in COUNTRIES:
        print(f"\n[COUNTRY] {country}")
        country_records: List[PollRecord] = []
        source_cfgs = COUNTRY_PARSER_PLAN.get(country, [])

        if not source_cfgs:
            statuses.append(
                ScrapeStatus(
                    country=country,
                    source_id="no_sources",
                    source_name="No sources configured",
                    parser="none",
                    resolved_url=None,
                    active=False,
                    fetched=False,
                    ok=False,
                    saved_raw_path=None,
                    saved_csv_path=None,
                    record_count=0,
                    error="no_sources_configured",
                )
            )
            print("  - no sources configured")
            continue

        for source_cfg in source_cfgs:
            source_id = str(source_cfg.get("source_id", "unknown"))
            source_name = str(source_cfg.get("source_name", "unknown"))
            parser = str(source_cfg.get("parser", ""))
            active = bool(source_cfg.get("active", True))

            status = ScrapeStatus(
                country=country,
                source_id=source_id,
                source_name=source_name,
                parser=parser,
                resolved_url=None,
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
                print(f"  - {source_id}: skipped (inactive)")
                continue

            try:
                records, raw_path, resolved_url = scrape_source(session, country, source_cfg)
                records = dedupe_records(records)

                status.fetched = True
                status.ok = True
                status.saved_raw_path = raw_path
                status.resolved_url = resolved_url
                status.record_count = len(records)

                country_records.extend(records)
                print(f"  - {source_id}: ok ({len(records)} rekord)")
                if resolved_url:
                    print(f"    resolved: {resolved_url}")

            except Exception as exc:
                status.fetched = True
                status.ok = False
                status.error = str(exc)
                print(f"  - {source_id}: FAIL ({exc})")

            statuses.append(status)

        country_records = dedupe_records(country_records)

        if country_records:
            csv_path = write_country_csv(country, country_records)
            for st in statuses:
                if st.country == country and st.ok:
                    st.saved_csv_path = csv_path
            print(f"  => wrote CSV: {csv_path} ({len(country_records)} rekord)")
        else:
            print("  => no usable records")

    write_status_json(statuses)

    ok_count = sum(1 for s in statuses if s.ok)
    fail_count = sum(1 for s in statuses if s.fetched and not s.ok)
    countries_with_data = len({s.country for s in statuses if s.ok and s.record_count > 0})

    print("\n=== Summary ===")
    print(f"sources total: {len(statuses)}")
    print(f"sources ok: {ok_count}")
    print(f"sources failed: {fail_count}")
    print(f"countries with data: {countries_with_data}/{len(COUNTRIES)}")
    print(f"status written: {SCRAPE_STATUS_OUT.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    run()
