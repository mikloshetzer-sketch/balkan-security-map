#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_party_polls.py

Feladat:
- a poll_registry.py alapján végigmenni az országokon és forrásokon
- a nyers forrásoldalakat archiválni
- a kézi CSV importokat normalizálni
- egységes JSON outputot előállítani a későbbi aggregációhoz

Ez a verzió SZÁNDÉKOSAN konzervatív:
- nem használ BeautifulSoup-ot vagy pandas-t
- nem erőltet törékeny scrapinget minden forrásra
- biztosít egy stabil adatpipeline-alapot

Bemenetek:
- scripts/poll_registry.py
- data/manual_polls/*.csv   (opcionális kézi importok)

Kimenetek:
- data/raw/polls/<country>/<source_id>.html
- data/processed/polls/normalized_polls.json
- data/processed/polls/source_fetch_status.json

CSV elvárt minta:
country,date,source,party,value,sample_size,fieldwork_start,fieldwork_end,notes
Serbia,2026-03-15,Ipsos,SNS,42,1200,2026-03-10,2026-03-14,example
Serbia,2026-03-15,Ipsos,SPN,18,1200,2026-03-10,2026-03-14,example
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# poll_registry.py ugyanabban a scripts mappában van
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from poll_registry import build_registry  # noqa: E402


# ---------------------------------------------------------------------
# Útvonalak
# ---------------------------------------------------------------------

RAW_POLLS_DIR = REPO_ROOT / "data" / "raw" / "polls"
MANUAL_POLLS_DIR = REPO_ROOT / "data" / "manual_polls"
PROCESSED_POLLS_DIR = REPO_ROOT / "data" / "processed" / "polls"

NORMALIZED_POLLS_JSON = PROCESSED_POLLS_DIR / "normalized_polls.json"
SOURCE_FETCH_STATUS_JSON = PROCESSED_POLLS_DIR / "source_fetch_status.json"

DEFAULT_TIMEOUT_SECONDS = 25
DEFAULT_USER_AGENT = (
    "balkan-security-map/party-polls-fetcher "
    "(research automation; contact via repo maintainer)"
)


# ---------------------------------------------------------------------
# Adatmodellek
# ---------------------------------------------------------------------

@dataclass
class NormalizedPollRow:
    country: str
    date: str
    source: str
    source_id: str
    party: str
    value: float
    sample_size: Optional[int]
    fieldwork_start: Optional[str]
    fieldwork_end: Optional[str]
    notes: str
    import_method: str  # manual_csv | parsed | api | bootstrap
    raw_file: Optional[str] = None


@dataclass
class FetchStatus:
    country: str
    source_id: str
    source_name: str
    homepage: str
    parser_kind: str
    active: bool
    supports_party_polling: bool
    supports_trend_context: bool
    fetched: bool
    archived_raw: bool
    archived_path: Optional[str]
    http_status: Optional[int]
    error: Optional[str]
    fetched_utc: str


# ---------------------------------------------------------------------
# Segédek
# ---------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dirs() -> None:
    RAW_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_POLLS_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    s = (value or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().replace(" ", "")
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def normalize_date(value: str) -> Optional[str]:
    """
    Egyszerű, robusztus normalizálás.
    Támogatott főbb alakok:
    - YYYY-MM-DD
    - YYYY/MM/DD
    - DD.MM.YYYY
    - YYYY-MM
    """
    if not value:
        return None

    text = value.strip()

    patterns = [
        ("%Y-%m-%d", 10),
        ("%Y/%m/%d", 10),
        ("%d.%m.%Y", 10),
        ("%Y-%m", 7),
        ("%Y/%m", 7),
    ]

    for fmt, _ in patterns:
        try:
            dt = datetime.strptime(text, fmt)
            if fmt in ("%Y-%m", "%Y/%m"):
                return dt.strftime("%Y-%m")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue

    return text


def group_rows_by_poll_key(rows: Iterable[NormalizedPollRow]) -> Dict[Tuple[str, str, str, str], List[NormalizedPollRow]]:
    grouped: Dict[Tuple[str, str, str, str], List[NormalizedPollRow]] = {}
    for row in rows:
        key = (row.country, row.date, row.source, row.import_method)
        grouped.setdefault(key, []).append(row)
    return grouped


# ---------------------------------------------------------------------
# HTTP / archiválás
# ---------------------------------------------------------------------

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.8",
        }
    )
    return session


def fetch_url(session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> Tuple[Optional[requests.Response], Optional[str]]:
    try:
        response = session.get(url, timeout=timeout)
        return response, None
    except Exception as exc:
        return None, str(exc)


def archive_raw_content(country: str, source_id: str, content: str, extension: str = "html") -> str:
    country_dir = RAW_POLLS_DIR / slugify(country)
    country_dir.mkdir(parents=True, exist_ok=True)

    path = country_dir / f"{slugify(source_id)}.{extension}"
    path.write_text(content, encoding="utf-8")
    return str(path.relative_to(REPO_ROOT))


# ---------------------------------------------------------------------
# Kézi CSV import
# ---------------------------------------------------------------------

REQUIRED_CSV_COLUMNS = {
    "country",
    "date",
    "source",
    "party",
    "value",
}

OPTIONAL_CSV_COLUMNS = {
    "source_id",
    "sample_size",
    "fieldwork_start",
    "fieldwork_end",
    "notes",
}

ALL_CSV_COLUMNS = REQUIRED_CSV_COLUMNS | OPTIONAL_CSV_COLUMNS


def discover_manual_csv_files() -> List[Path]:
    if not MANUAL_POLLS_DIR.exists():
        return []
    return sorted(MANUAL_POLLS_DIR.glob("*.csv"))


def validate_csv_header(fieldnames: Optional[List[str]], path: Path) -> None:
    if not fieldnames:
        raise ValueError(f"Nincs fejléc a CSV-ben: {path}")

    normalized = {name.strip() for name in fieldnames if name and name.strip()}
    missing = REQUIRED_CSV_COLUMNS - normalized
    if missing:
        raise ValueError(
            f"Hiányzó oszlop(ok) a {path.name} fájlban: {', '.join(sorted(missing))}"
        )


def rows_from_manual_csv(path: Path) -> List[NormalizedPollRow]:
    rows: List[NormalizedPollRow] = []

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        validate_csv_header(reader.fieldnames, path)

        for i, raw in enumerate(reader, start=2):
            country = (raw.get("country") or "").strip()
            date = normalize_date((raw.get("date") or "").strip())
            source = (raw.get("source") or "").strip()
            party = (raw.get("party") or "").strip()
            value = safe_float(raw.get("value"))

            if not country or not date or not source or not party or value is None:
                # rossz sor kihagyása, de ne borítsa az egész pipeline-t
                continue

            source_id = (raw.get("source_id") or "").strip()
            if not source_id:
                source_id = slugify(source)

            row = NormalizedPollRow(
                country=country,
                date=date,
                source=source,
                source_id=source_id,
                party=party,
                value=value,
                sample_size=safe_int(raw.get("sample_size")),
                fieldwork_start=normalize_date((raw.get("fieldwork_start") or "").strip()),
                fieldwork_end=normalize_date((raw.get("fieldwork_end") or "").strip()),
                notes=(raw.get("notes") or "").strip(),
                import_method="manual_csv",
                raw_file=str(path.relative_to(REPO_ROOT)),
            )
            rows.append(row)

    return rows


def collect_manual_csv_rows() -> List[NormalizedPollRow]:
    all_rows: List[NormalizedPollRow] = []

    for path in discover_manual_csv_files():
        try:
            all_rows.extend(rows_from_manual_csv(path))
        except Exception:
            # Egy rossz CSV ne döntse be a teljes futást.
            continue

    return all_rows


# ---------------------------------------------------------------------
# Forrásoldalak archiválása
# ---------------------------------------------------------------------

def fetch_and_archive_sources(registry: Dict[str, Any]) -> List[FetchStatus]:
    session = build_session()
    statuses: List[FetchStatus] = []

    for country, cfg in registry.items():
        for source in cfg.sources:
            status = FetchStatus(
                country=country,
                source_id=source.source_id,
                source_name=source.source_name,
                homepage=source.homepage,
                parser_kind=source.parser_kind,
                active=bool(source.active),
                supports_party_polling=bool(source.supports_party_polling),
                supports_trend_context=bool(source.supports_trend_context),
                fetched=False,
                archived_raw=False,
                archived_path=None,
                http_status=None,
                error=None,
                fetched_utc=utc_now_iso(),
            )

            if not source.active:
                status.error = "source_inactive"
                statuses.append(status)
                continue

            if not source.homepage:
                status.error = "missing_homepage"
                statuses.append(status)
                continue

            response, error = fetch_url(session, source.homepage)
            if error:
                status.error = error
                statuses.append(status)
                time.sleep(0.5)
                continue

            status.fetched = True
            status.http_status = response.status_code

            if response.status_code >= 400:
                status.error = f"http_{response.status_code}"
                statuses.append(status)
                time.sleep(0.5)
                continue

            content_type = (response.headers.get("Content-Type") or "").lower()
            extension = "html"
            if "json" in content_type:
                extension = "json"
            elif "xml" in content_type:
                extension = "xml"

            try:
                archived_rel = archive_raw_content(
                    country=country,
                    source_id=source.source_id,
                    content=response.text,
                    extension=extension,
                )
                status.archived_raw = True
                status.archived_path = archived_rel
            except Exception as exc:
                status.error = f"archive_error: {exc}"

            statuses.append(status)
            time.sleep(0.6)

    return statuses


# ---------------------------------------------------------------------
# Alap parser hookok
# ---------------------------------------------------------------------

def parse_bootstrap_wikipedia_tables_from_archives(
    statuses: List[FetchStatus]
) -> List[NormalizedPollRow]:
    """
    Opcionális bootstrap parser.
    Jelenleg csak helykitöltő — direkt nem építünk törékeny táblaparsert ebben a körben.

    Később ide jöhet:
    - Wikipedia opinion polling táblák parser
    - Europe Elects custom parser
    - lokális intézeti HTML parser
    """
    _ = statuses
    return []


# ---------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------

def normalized_rows_to_export(rows: List[NormalizedPollRow]) -> Dict[str, Any]:
    grouped = group_rows_by_poll_key(rows)

    polls: List[Dict[str, Any]] = []

    for key in sorted(grouped.keys()):
        country, date, source, import_method = key
        members = grouped[key]

        source_id = members[0].source_id
        sample_size = members[0].sample_size
        fieldwork_start = members[0].fieldwork_start
        fieldwork_end = members[0].fieldwork_end
        notes = members[0].notes
        raw_file = members[0].raw_file

        parties: Dict[str, float] = {}
        for row in members:
            parties[row.party] = row.value

        polls.append(
            {
                "country": country,
                "date": date,
                "source": source,
                "source_id": source_id,
                "sample_size": sample_size,
                "fieldwork_start": fieldwork_start,
                "fieldwork_end": fieldwork_end,
                "notes": notes,
                "import_method": import_method,
                "raw_file": raw_file,
                "parties": parties,
            }
        )

    return {
        "generated_utc": utc_now_iso(),
        "poll_count": len(polls),
        "row_count": len(rows),
        "polls": polls,
    }


def fetch_statuses_to_export(statuses: List[FetchStatus]) -> Dict[str, Any]:
    return {
        "generated_utc": utc_now_iso(),
        "source_count": len(statuses),
        "sources": [asdict(x) for x in statuses],
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------
# Konzolos összegzés
# ---------------------------------------------------------------------

def print_summary(rows: List[NormalizedPollRow], statuses: List[FetchStatus]) -> None:
    print("=== Party Poll Fetch Summary ===")
    print(f"normalized rows: {len(rows)}")
    print(f"source statuses: {len(statuses)}")

    success = sum(1 for s in statuses if s.fetched and s.archived_raw)
    failed = sum(1 for s in statuses if s.error and s.error != "source_inactive")
    inactive = sum(1 for s in statuses if s.error == "source_inactive")

    print(f"archived sources: {success}")
    print(f"failed sources:   {failed}")
    print(f"inactive sources: {inactive}")

    by_country: Dict[str, int] = {}
    for row in rows:
        by_country[row.country] = by_country.get(row.country, 0) + 1

    if by_country:
        print("\nRows by country:")
        for country in sorted(by_country.keys()):
            print(f" - {country}: {by_country[country]}")

    manual_files = discover_manual_csv_files()
    print(f"\nmanual csv files: {len(manual_files)}")
    for path in manual_files:
        print(f" - {path.relative_to(REPO_ROOT)}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    ensure_dirs()

    registry = build_registry()

    # 1) Források archiválása
    statuses = fetch_and_archive_sources(registry)

    # 2) Kézi importok
    manual_rows = collect_manual_csv_rows()

    # 3) Későbbi parser hook
    parsed_rows = parse_bootstrap_wikipedia_tables_from_archives(statuses)

    all_rows = manual_rows + parsed_rows

    # 4) Export
    normalized_payload = normalized_rows_to_export(all_rows)
    statuses_payload = fetch_statuses_to_export(statuses)

    write_json(NORMALIZED_POLLS_JSON, normalized_payload)
    write_json(SOURCE_FETCH_STATUS_JSON, statuses_payload)

    # 5) Summary
    print_summary(all_rows, statuses)


if __name__ == "__main__":
    main()
