#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrape_polls.py

KÖZÖS POLL SCRAPER RENDSZER – TELJES ORSZÁGLISTA

Mit csinál:
- végigmegy az összes figyelt országon
- automatikusan próbál Wikipédia polling oldalt találni
- ha talál használható wikitable poll táblát, abból rekordokat képez
- országonként automatikus CSV-t ír:
    docs/data/manual_polls/<country_slug>_auto.csv
- scrape státuszt ír:
    docs/data/processed/polls/scrape_status.json

Miért ilyen:
- nem országonként külön script
- parser + resolver alapú közös rendszer
- ha nincs adatforrás, nem omlik össze, hanem skipel

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
from urllib.parse import quote, urljoin

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

# A repo-ban használt 8 ország
COUNTRIES: List[str] = [
    "Serbia",
    "Romania",
    "Bulgaria",
    "Croatia",
    "Albania",
    "Kosovo",
    "North Macedonia",
    "Bosnia and Herzegovina",
]

# Parser stratégia minden országra ugyanaz:
# 1) wikipedia_auto_polling
# Később ide be lehet tenni más közös parser stratégiát is.
COUNTRY_PARSER_PLAN: Dict[str, List[Dict[str, str]]] = {
    country: [
        {
            "source_id": f"{slugify_seed(country)}_wikipedia_auto_polling",
            "source_name": f"Wikipedia auto polling resolver – {country}",
            "parser": "wikipedia_auto_polling",
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

# Általános és balkáni pollster nevekre fókuszáló lista.
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
    resolved_url: Optional[str]
    active: bool
    fetched: bool
    ok: bool
    saved_raw_path: Optional[str]
    saved_csv_path: Optional[str]
    record_count: int
    error: Optional[str]


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


def ensure_dirs() -> None:
    MANUAL_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_POLLS_DIR.mkdir(parents=True, exist_ok=True)


def clean_text(text: str) -> str:
    text = unescape(text or "")
    text = re.sub(r"\[[^\]]*\]", "", text)  # wikipedia refs
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

    # Tiltott minták
    if t in {"—", "-", "–", "N/A", "n/a"}:
        return None

    # az első számot vesszük
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

    if year:
        return year, None, None

    return None, None, None


# ============================================================
# WIKIPEDIA AUTO RESOLVER
# ============================================================

def wikipedia_title_candidates(country: str) -> List[str]:
    """
    Több lehetséges Wikipédia címet próbálunk automatikusan.
    Nem országonként külön scraper, hanem közös URL-feloldó logika.
    """
    c = country

    candidates = [
        f"Opinion polling for the next {c} parliamentary election",
        f"Opinion polling for the next {c} legislative election",
        f"Opinion polling for the next {c} general election",
        f"Opinion polling for the {c} parliamentary election",
        f"Opinion polling for the {c} legislative election",
        f"Opinion polling for the {c} general election",
        f"Next {c} parliamentary election",
        f"Next {c} legislative election",
        f"Next {c} general election",
    ]

    # bizonyos országoknál hasznos lehet a konkrét évre utaló fallback
    # nem garantált, de közös rendszerként jó tartalék
    for year in ("2028", "2027", "2026", "2025", "2024"):
        candidates.extend([
            f"Opinion polling for the {year} {c} parliamentary election",
            f"Opinion polling for the {year} {c} legislative election",
            f"{year} {c} parliamentary election",
            f"{year} {c} legislative election",
        ])

    # whitespace normalizálás és duplikátum-szűrés
    seen = set()
    out = []
    for item in candidates:
        item = re.sub(r"\s+", " ", item).strip()
        if item not in seen:
            seen.add(item)
            out.append(item)

    return out


def wikipedia_url_from_title(title: str) -> str:
    return f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'), safe=':_()%-')}"


def resolve_wikipedia_page(session: requests.Session, country: str) -> Optional[Tuple[str, str]]:
    """
    Visszaadja:
    - resolved_title
    - resolved_url

    A logika közös minden országra.
    """
    for title in wikipedia_title_candidates(country):
        url = wikipedia_url_from_title(title)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except Exception:
            continue

        if resp.status_code != 200:
            continue

        final_url = resp.url or url
        text = resp.text.lower()

        # ha egy sima 404 / no article page jön
        if "wikipedia does not have an article with this exact name" in text:
            continue

        # legalább a polling szó vagy election tartalom legyen benne
        if "poll" not in text and "election" not in text:
            continue

        return title, final_url

    return None


# ============================================================
# WIKIPEDIA TABLE PARSER
# ============================================================

def is_probably_pollster(text: str) -> bool:
    t = clean_text(text).lower()
    if not t:
        return False
    if t in KNOWN_POLLSTERS:
        return True
    return any(name in t for name in KNOWN_POLLSTERS)


def wikipedia_find_best_poll_table(soup: BeautifulSoup):
    """
    Közös logika:
    - először tipikus szekciókat keresünk
    - ha nincs, minden wikitable közül a legígéretesebbet választjuk
    """
    preferred_section_ids = [
        "Poll_results",
        "Opinion_polls",
        "Polling",
        "Opinion_polling",
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
        if "lead" in txt:
            score += 2
        if "poll" in txt:
            score += 2
        if "date of publication" in txt:
            score += 3
        score += min(len(table.find_all("tr")), 10)
        return score

    tables_sorted = sorted(tables, key=table_score, reverse=True)
    return tables_sorted[0]


def wikipedia_extract_headers(table) -> List[str]:
    rows = table.find_all("tr")
    if not rows:
        return []

    header_cells: List[str] = []
    for tr in rows[:5]:
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
            # fallback: ha a sor első cellája nagyon rövid/üres vagy nem pollster-szerű, kihagyjuk
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

        row_records = 0

        for i in range(max_len):
            party = normalize_header(party_headers[i])
            raw_value = party_values[i]

            if not party:
                continue
            if party.lower() in NON_PARTY_HEADERS:
                continue

            value = parse_float(raw_value)
            if value is None:
                continue

            # 0-100 tartományon kívüli értéket dobjuk
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
                    notes="auto_scraped_from_wikipedia_auto_resolver",
                )
            )
            row_records += 1

        # ha egy sorból nulla párt jött ki, az nem gond, csak nem használjuk
        _ = row_records

    return records


def scrape_with_wikipedia_auto_polling(
    session: requests.Session,
    country: str,
    source_id: str,
    source_name: str,
) -> Tuple[List[PollRecord], Optional[str], Optional[str]]:
    resolved = resolve_wikipedia_page(session, country)
    if resolved is None:
        raise RuntimeError("Nem találtam használható Wikipédia polling / election oldalt.")

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

    if parser == "wikipedia_auto_polling":
        return scrape_with_wikipedia_auto_polling(
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

    print("=== Common Poll Scraper – Full Country List ===")

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
