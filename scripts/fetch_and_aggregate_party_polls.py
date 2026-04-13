#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_and_aggregate_party_polls.py

Cél:
- kézi CSV pollfájlok beolvasása
- opcionális közvetlen JSON/CSV URL források beolvasása
- normalizálás
- aggregálás országonként
- frontend-kompatibilis JSON generálás

Kimenetek:
- docs/data/processed/polls/normalized_polls.json
- docs/data/processed/polls/party_poll_aggregates.json
- docs/data/processed/polls/source_fetch_status.json

Ez a fájl NEM próbál még általános HTML scrapinget végezni minden oldalról.
Azért nem, mert az ilyen oldalak országonként teljesen eltérnek és törékenyek.
Ez a verzió stabil adatcsatornát ad a rendszerhez.

Elvárt kézi CSV formátum:
country,date,source,party,value,sample_size,fieldwork_start,fieldwork_end,notes
Serbia,2026-04-01,Ipsos,SNS,47.2,1200,2026-03-26,2026-03-30,teszt
Serbia,2026-04-01,Ipsos,SPS,6.8,1200,2026-03-26,2026-03-30,teszt

Ajánlott mappák:
- docs/data/manual_polls/
- docs/data/processed/polls/

Futtatás:
python scripts/fetch_and_aggregate_party_polls.py
"""

from __future__ import annotations

import csv
import json
import math
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except Exception:
    requests = None


# ============================================================
# PATHS
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

DOCS_DIR = REPO_ROOT / "docs"
MANUAL_POLLS_DIR = DOCS_DIR / "data" / "manual_polls"
PROCESSED_DIR = DOCS_DIR / "data" / "processed" / "polls"
RAW_DIR = DOCS_DIR / "data" / "raw" / "polls"

NORMALIZED_OUT = PROCESSED_DIR / "normalized_polls.json"
AGGREGATES_OUT = PROCESSED_DIR / "party_poll_aggregates.json"
FETCH_STATUS_OUT = PROCESSED_DIR / "source_fetch_status.json"


# ============================================================
# CONFIG
# ============================================================

DEFAULT_MAX_POLL_AGE_DAYS = 180
DEFAULT_MIN_SOURCES_PER_COUNTRY = 3
DEFAULT_RECENCY_HALF_LIFE_DAYS = 45
DEFAULT_TREND_LOOKBACK_POLLS = 2
DEFAULT_FLAT_THRESHOLD = 1.0
DEFAULT_TIMEOUT_SECONDS = 25

USER_AGENT = "balkan-security-map/party-polls-pipeline"


# ============================================================
# OPTIONAL DIRECT SOURCES
# ============================================================
# Ezeket akkor használd, ha van közvetlen JSON vagy CSV URL-ed.
# A manual CSV ettől függetlenül mindig működik.
#
# Támogatott source_type:
# - "csv_url"
# - "json_url"
#
# CSV elvárt mezők:
# country,date,source,party,value,sample_size,fieldwork_start,fieldwork_end,notes
#
# JSON elvárt minta:
# [
#   {
#     "country": "Serbia",
#     "date": "2026-04-01",
#     "source": "Ipsos",
#     "parties": {"SNS": 47.2, "SPS": 6.8},
#     "sample_size": 1200,
#     "fieldwork_start": "2026-03-26",
#     "fieldwork_end": "2026-03-30",
#     "notes": "teszt"
#   }
# ]

DIRECT_SOURCES: List[Dict[str, Any]] = [
    # Példa:
    # {
    #     "source_id": "serbia_example_csv",
    #     "source_name": "Serbia Example CSV",
    #     "source_type": "csv_url",
    #     "country": "Serbia",
    #     "url": "https://example.com/serbia_polls.csv",
    #     "active": False
    # },
]


# ============================================================
# MODELS
# ============================================================

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
    import_method: str
    raw_file: Optional[str] = None


@dataclass
class FetchStatus:
    source_id: str
    source_name: str
    country: str
    source_type: str
    url: str
    active: bool
    fetched: bool
    ok: bool
    http_status: Optional[int]
    saved_raw_path: Optional[str]
    rows_imported: int
    error: Optional[str]
    fetched_utc: str


@dataclass
class PollEntry:
    country: str
    date_raw: str
    date_dt: datetime
    source: str
    source_id: str
    sample_size: Optional[int]
    fieldwork_start: Optional[str]
    fieldwork_end: Optional[str]
    notes: str
    import_method: str
    raw_file: Optional[str]
    parties: Dict[str, float]


# ============================================================
# HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dirs() -> None:
    MANUAL_POLLS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    txt = str(value).strip().replace("%", "").replace(",", ".")
    if not txt:
        return None
    try:
        return float(txt)
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    txt = str(value).strip().replace(" ", "")
    if not txt:
        return None
    try:
        return int(float(txt))
    except Exception:
        return None


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%Y-%m", "%Y/%m"):
        try:
            dt = datetime.strptime(text, fmt)
            if fmt in ("%Y-%m", "%Y/%m"):
                return dt.strftime("%Y-%m")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return text


def parse_date_flexible(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def days_between(newer: datetime, older: datetime) -> float:
    return (newer - older).total_seconds() / 86400.0


def round2(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 2)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ============================================================
# MANUAL CSV IMPORT
# ============================================================

REQUIRED_CSV_COLUMNS = {"country", "date", "source", "party", "value"}


def discover_manual_csv_files() -> List[Path]:
    return sorted(MANUAL_POLLS_DIR.glob("*.csv"))


def validate_csv_header(fieldnames: Optional[List[str]], path: Path) -> None:
    if not fieldnames:
        raise ValueError(f"Nincs fejléc: {path}")
    names = {str(x).strip() for x in fieldnames if x}
    missing = REQUIRED_CSV_COLUMNS - names
    if missing:
        raise ValueError(f"Hiányzó oszlop(ok) a {path.name} fájlban: {', '.join(sorted(missing))}")


def rows_from_manual_csv(path: Path) -> List[NormalizedPollRow]:
    rows: List[NormalizedPollRow] = []

    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        validate_csv_header(reader.fieldnames, path)

        for raw in reader:
            country = (raw.get("country") or "").strip()
            date = normalize_date(raw.get("date"))
            source = (raw.get("source") or "").strip()
            party = (raw.get("party") or "").strip()
            value = safe_float(raw.get("value"))

            if not country or not date or not source or not party or value is None:
                continue

            source_id = slugify(raw.get("source_id") or source)

            rows.append(
                NormalizedPollRow(
                    country=country,
                    date=date,
                    source=source,
                    source_id=source_id,
                    party=party,
                    value=value,
                    sample_size=safe_int(raw.get("sample_size")),
                    fieldwork_start=normalize_date(raw.get("fieldwork_start")),
                    fieldwork_end=normalize_date(raw.get("fieldwork_end")),
                    notes=(raw.get("notes") or "").strip(),
                    import_method="manual_csv",
                    raw_file=str(path.relative_to(REPO_ROOT)),
                )
            )

    return rows


def collect_manual_csv_rows() -> List[NormalizedPollRow]:
    out: List[NormalizedPollRow] = []
    for path in discover_manual_csv_files():
        try:
            out.extend(rows_from_manual_csv(path))
        except Exception as exc:
            print(f"[WARN] Manual CSV skipped: {path.name} ({exc})")
    return out


# ============================================================
# DIRECT URL SOURCES
# ============================================================

def build_session():
    if requests is None:
        return None
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/csv,text/plain,*/*",
    })
    return s


def archive_raw_text(country: str, source_id: str, text: str, ext: str) -> str:
    country_dir = RAW_DIR / slugify(country)
    country_dir.mkdir(parents=True, exist_ok=True)
    path = country_dir / f"{slugify(source_id)}.{ext}"
    path.write_text(text, encoding="utf-8")
    return str(path.relative_to(REPO_ROOT))


def rows_from_csv_text(text: str, raw_file: Optional[str], import_method: str) -> List[NormalizedPollRow]:
    rows: List[NormalizedPollRow] = []
    reader = csv.DictReader(text.splitlines())
    validate_csv_header(reader.fieldnames, Path(raw_file or "remote.csv"))

    for raw in reader:
        country = (raw.get("country") or "").strip()
        date = normalize_date(raw.get("date"))
        source = (raw.get("source") or "").strip()
        party = (raw.get("party") or "").strip()
        value = safe_float(raw.get("value"))

        if not country or not date or not source or not party or value is None:
            continue

        source_id = slugify(raw.get("source_id") or source)

        rows.append(
            NormalizedPollRow(
                country=country,
                date=date,
                source=source,
                source_id=source_id,
                party=party,
                value=value,
                sample_size=safe_int(raw.get("sample_size")),
                fieldwork_start=normalize_date(raw.get("fieldwork_start")),
                fieldwork_end=normalize_date(raw.get("fieldwork_end")),
                notes=(raw.get("notes") or "").strip(),
                import_method=import_method,
                raw_file=raw_file,
            )
        )

    return rows


def rows_from_json_data(data: Any, raw_file: Optional[str], import_method: str, fallback_source_id: str) -> List[NormalizedPollRow]:
    rows: List[NormalizedPollRow] = []

    if not isinstance(data, list):
        return rows

    for item in data:
        if not isinstance(item, dict):
            continue

        country = (item.get("country") or "").strip()
        date = normalize_date(item.get("date"))
        source = (item.get("source") or "").strip()
        parties = item.get("parties") or {}

        if not country or not date or not source or not isinstance(parties, dict):
            continue

        source_id = slugify(item.get("source_id") or fallback_source_id or source)
        sample_size = safe_int(item.get("sample_size"))
        fieldwork_start = normalize_date(item.get("fieldwork_start"))
        fieldwork_end = normalize_date(item.get("fieldwork_end"))
        notes = (item.get("notes") or "").strip()

        for party_name, party_value in parties.items():
            value = safe_float(party_value)
            if not party_name or value is None:
                continue

            rows.append(
                NormalizedPollRow(
                    country=country,
                    date=date,
                    source=source,
                    source_id=source_id,
                    party=str(party_name).strip(),
                    value=value,
                    sample_size=sample_size,
                    fieldwork_start=fieldwork_start,
                    fieldwork_end=fieldwork_end,
                    notes=notes,
                    import_method=import_method,
                    raw_file=raw_file,
                )
            )

    return rows


def fetch_direct_sources() -> Tuple[List[NormalizedPollRow], List[FetchStatus]]:
    rows: List[NormalizedPollRow] = []
    statuses: List[FetchStatus] = []

    if not DIRECT_SOURCES:
      return rows, statuses

    session = build_session()
    if session is None:
        for src in DIRECT_SOURCES:
            statuses.append(
                FetchStatus(
                    source_id=src.get("source_id", "unknown"),
                    source_name=src.get("source_name", "unknown"),
                    country=src.get("country", ""),
                    source_type=src.get("source_type", ""),
                    url=src.get("url", ""),
                    active=bool(src.get("active", False)),
                    fetched=False,
                    ok=False,
                    http_status=None,
                    saved_raw_path=None,
                    rows_imported=0,
                    error="requests_not_available",
                    fetched_utc=utc_now_iso(),
                )
            )
        return rows, statuses

    for src in DIRECT_SOURCES:
        source_id = str(src.get("source_id", "unknown"))
        source_name = str(src.get("source_name", source_id))
        source_type = str(src.get("source_type", ""))
        country = str(src.get("country", ""))
        url = str(src.get("url", ""))
        active = bool(src.get("active", False))

        status = FetchStatus(
            source_id=source_id,
            source_name=source_name,
            country=country,
            source_type=source_type,
            url=url,
            active=active,
            fetched=False,
            ok=False,
            http_status=None,
            saved_raw_path=None,
            rows_imported=0,
            error=None,
            fetched_utc=utc_now_iso(),
        )

        if not active:
            status.error = "inactive"
            statuses.append(status)
            continue

        if not url:
            status.error = "missing_url"
            statuses.append(status)
            continue

        try:
            resp = session.get(url, timeout=DEFAULT_TIMEOUT_SECONDS)
            status.fetched = True
            status.http_status = resp.status_code

            if resp.status_code >= 400:
                status.error = f"http_{resp.status_code}"
                statuses.append(status)
                continue

            if source_type == "csv_url":
                raw_path = archive_raw_text(country or "unknown", source_id, resp.text, "csv")
                imported = rows_from_csv_text(resp.text, raw_path, "remote_csv")
                status.saved_raw_path = raw_path
                status.rows_imported = len(imported)
                status.ok = True
                rows.extend(imported)

            elif source_type == "json_url":
                raw_path = archive_raw_text(country or "unknown", source_id, resp.text, "json")
                data = resp.json()
                imported = rows_from_json_data(data, raw_path, "remote_json", source_id)
                status.saved_raw_path = raw_path
                status.rows_imported = len(imported)
                status.ok = True
                rows.extend(imported)

            else:
                status.error = f"unsupported_source_type:{source_type}"

        except Exception as exc:
            status.error = str(exc)

        statuses.append(status)

    return rows, statuses


# ============================================================
# NORMALIZATION EXPORT
# ============================================================

def group_rows_to_polls(rows: List[NormalizedPollRow]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str, str], List[NormalizedPollRow]] = defaultdict(list)

    for row in rows:
        key = (row.country, row.date, row.source, row.import_method)
        grouped[key].append(row)

    polls: List[Dict[str, Any]] = []

    for key in sorted(grouped.keys()):
        country, date, source, import_method = key
        members = grouped[key]
        parties: Dict[str, float] = {}
        for m in members:
            parties[m.party] = m.value

        first = members[0]
        polls.append(
            {
                "country": country,
                "date": date,
                "source": source,
                "source_id": first.source_id,
                "sample_size": first.sample_size,
                "fieldwork_start": first.fieldwork_start,
                "fieldwork_end": first.fieldwork_end,
                "notes": first.notes,
                "import_method": import_method,
                "raw_file": first.raw_file,
                "parties": parties,
            }
        )

    return polls


def build_normalized_payload(rows: List[NormalizedPollRow]) -> Dict[str, Any]:
    polls = group_rows_to_polls(rows)
    return {
        "generated_utc": utc_now_iso(),
        "row_count": len(rows),
        "poll_count": len(polls),
        "polls": polls,
    }


# ============================================================
# AGGREGATION
# ============================================================

def parse_poll_entries(normalized_payload: Dict[str, Any]) -> List[PollEntry]:
    out: List[PollEntry] = []

    for item in normalized_payload.get("polls", []):
        country = (item.get("country") or "").strip()
        date_raw = (item.get("date") or "").strip()
        source = (item.get("source") or "").strip()
        source_id = (item.get("source_id") or "").strip()
        parties = item.get("parties") or {}

        date_dt = parse_date_flexible(date_raw)
        if not country or not date_dt or not source or not isinstance(parties, dict):
            continue

        clean_parties: Dict[str, float] = {}
        for k, v in parties.items():
            fv = safe_float(v)
            if k and fv is not None:
                clean_parties[str(k).strip()] = fv

        if not clean_parties:
            continue

        out.append(
            PollEntry(
                country=country,
                date_raw=date_raw,
                date_dt=date_dt,
                source=source,
                source_id=source_id or slugify(source),
                sample_size=safe_int(item.get("sample_size")),
                fieldwork_start=item.get("fieldwork_start"),
                fieldwork_end=item.get("fieldwork_end"),
                notes=item.get("notes") or "",
                import_method=item.get("import_method") or "unknown",
                raw_file=item.get("raw_file"),
                parties=clean_parties,
            )
        )

    return out


def is_fresh_enough(entry: PollEntry, now_dt: datetime, max_age_days: int) -> bool:
    return days_between(now_dt, entry.date_dt) <= max_age_days


def recency_weight(entry: PollEntry, now_dt: datetime, half_life_days: int) -> float:
    age = max(0.0, days_between(now_dt, entry.date_dt))
    if half_life_days <= 0:
        return 1.0
    return 0.5 ** (age / float(half_life_days))


def sample_weight(sample_size: Optional[int]) -> float:
    if not sample_size or sample_size <= 0:
        return 1.0
    return max(0.75, min(math.sqrt(sample_size / 1000.0), 1.5))


def combined_weight(entry: PollEntry, now_dt: datetime, half_life_days: int) -> float:
    return recency_weight(entry, now_dt, half_life_days) * sample_weight(entry.sample_size)


def mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def weighted_mean(values_and_weights: List[Tuple[float, float]]) -> Optional[float]:
    if not values_and_weights:
        return None
    total_weight = 0.0
    weighted_sum = 0.0
    for value, weight in values_and_weights:
        if weight <= 0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return weighted_sum / total_weight


def infer_party_trend(
    polls_sorted_desc: List[PollEntry],
    party_name: str,
    lookback_polls: int,
    flat_threshold: float,
) -> Dict[str, Any]:
    values: List[Tuple[datetime, float]] = []
    for poll in polls_sorted_desc:
        if party_name in poll.parties:
            values.append((poll.date_dt, poll.parties[party_name]))

    if len(values) < max(lookback_polls * 2, 2):
        return {
            "direction": "unknown",
            "delta": None,
            "recent_average": None,
            "older_average": None,
        }

    recent = [v for _, v in values[:lookback_polls]]
    older = [v for _, v in values[lookback_polls:lookback_polls * 2]]

    recent_avg = mean(recent)
    older_avg = mean(older)

    if recent_avg is None or older_avg is None:
        return {
            "direction": "unknown",
            "delta": None,
            "recent_average": round2(recent_avg),
            "older_average": round2(older_avg),
        }

    delta = recent_avg - older_avg
    if delta > flat_threshold:
        direction = "up"
    elif delta < -flat_threshold:
        direction = "down"
    else:
        direction = "flat"

    return {
        "direction": direction,
        "delta": round2(delta),
        "recent_average": round2(recent_avg),
        "older_average": round2(older_avg),
    }


def group_polls_by_country(entries: List[PollEntry]) -> Dict[str, List[PollEntry]]:
    grouped: Dict[str, List[PollEntry]] = defaultdict(list)
    for entry in entries:
        grouped[entry.country].append(entry)
    return grouped


def aggregate_country(
    country: str,
    polls: List[PollEntry],
    now_dt: datetime,
    min_sources_per_country: int,
    max_age_days: int,
    half_life_days: int,
    trend_lookback_polls: int,
    flat_threshold: float,
) -> Dict[str, Any]:
    fresh_polls = [p for p in polls if is_fresh_enough(p, now_dt, max_age_days)]
    fresh_polls_sorted_desc = sorted(fresh_polls, key=lambda x: x.date_dt, reverse=True)

    distinct_sources = sorted({p.source_id for p in fresh_polls_sorted_desc})
    latest_poll_dt = fresh_polls_sorted_desc[0].date_dt if fresh_polls_sorted_desc else None

    party_values_simple: Dict[str, List[float]] = defaultdict(list)
    party_values_weighted: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
    party_poll_counts: Dict[str, int] = defaultdict(int)
    party_sources: Dict[str, set] = defaultdict(set)

    for poll in fresh_polls_sorted_desc:
        weight = combined_weight(poll, now_dt, half_life_days)
        for party_name, value in poll.parties.items():
            party_values_simple[party_name].append(value)
            party_values_weighted[party_name].append((value, weight))
            party_poll_counts[party_name] += 1
            party_sources[party_name].add(poll.source_id)

    parties_out: List[Dict[str, Any]] = []

    for party_name in sorted(party_values_weighted.keys()):
        simple_avg = mean(party_values_simple[party_name])
        weighted_avg = weighted_mean(party_values_weighted[party_name])
        trend = infer_party_trend(
            fresh_polls_sorted_desc,
            party_name,
            trend_lookback_polls,
            flat_threshold,
        )

        parties_out.append(
            {
                "party": party_name,
                "simple_average": round2(simple_avg),
                "weighted_average": round2(weighted_avg),
                "poll_count": int(party_poll_counts[party_name]),
                "source_count": len(party_sources[party_name]),
                "sources": sorted(party_sources[party_name]),
                "trend": trend,
            }
        )

    parties_out.sort(
        key=lambda x: (
            x["weighted_average"] if x["weighted_average"] is not None else -9999,
            x["simple_average"] if x["simple_average"] is not None else -9999,
        ),
        reverse=True,
    )

    leader = parties_out[0] if parties_out else None
    ready_for_display = len(distinct_sources) >= min_sources_per_country and leader is not None

    polls_used = []
    for poll in fresh_polls_sorted_desc:
        polls_used.append(
            {
                "date": poll.date_raw,
                "source": poll.source,
                "source_id": poll.source_id,
                "sample_size": poll.sample_size,
                "fieldwork_start": poll.fieldwork_start,
                "fieldwork_end": poll.fieldwork_end,
                "import_method": poll.import_method,
                "raw_file": poll.raw_file,
                "parties": poll.parties,
            }
        )

    return {
        "country": country,
        "status": {
            "ready_for_display": ready_for_display,
            "readiness_reason": f"{len(distinct_sources)} distinct source(s) within {max_age_days} days; minimum required: {min_sources_per_country}",
            "distinct_source_count": len(distinct_sources),
            "distinct_sources": distinct_sources,
            "fresh_poll_count": len(fresh_polls_sorted_desc),
            "max_poll_age_days": max_age_days,
            "latest_poll_date": latest_poll_dt.strftime("%Y-%m-%d") if latest_poll_dt else None,
        },
        "leader": (
            {
                "party": leader["party"],
                "weighted_average": leader["weighted_average"],
                "simple_average": leader["simple_average"],
                "trend_direction": leader["trend"]["direction"],
            }
            if leader else None
        ),
        "parties": parties_out,
        "polls_used": polls_used,
    }


def build_aggregate_payload(entries: List[PollEntry]) -> Dict[str, Any]:
    now_dt = utc_now()
    grouped = group_polls_by_country(entries)
    countries_out: List[Dict[str, Any]] = []
    ready_count = 0

    for country in sorted(grouped.keys()):
        item = aggregate_country(
            country=country,
            polls=grouped[country],
            now_dt=now_dt,
            min_sources_per_country=DEFAULT_MIN_SOURCES_PER_COUNTRY,
            max_age_days=DEFAULT_MAX_POLL_AGE_DAYS,
            half_life_days=DEFAULT_RECENCY_HALF_LIFE_DAYS,
            trend_lookback_polls=DEFAULT_TREND_LOOKBACK_POLLS,
            flat_threshold=DEFAULT_FLAT_THRESHOLD,
        )
        if item["status"]["ready_for_display"]:
            ready_count += 1
        countries_out.append(item)

    return {
        "generated_utc": utc_now_iso(),
        "config": {
            "min_sources_per_country": DEFAULT_MIN_SOURCES_PER_COUNTRY,
            "max_poll_age_days": DEFAULT_MAX_POLL_AGE_DAYS,
            "recency_half_life_days": DEFAULT_RECENCY_HALF_LIFE_DAYS,
            "trend_lookback_polls": DEFAULT_TREND_LOOKBACK_POLLS,
            "flat_threshold": DEFAULT_FLAT_THRESHOLD,
        },
        "summary": {
            "country_count": len(countries_out),
            "countries_ready_for_display": ready_count,
            "countries_not_ready_for_display": len(countries_out) - ready_count,
            "total_poll_entries": len(entries),
        },
        "countries": countries_out,
    }


# ============================================================
# FETCH STATUS EXPORT
# ============================================================

def build_fetch_status_payload(statuses: List[FetchStatus]) -> Dict[str, Any]:
    return {
        "generated_utc": utc_now_iso(),
        "source_count": len(statuses),
        "sources": [asdict(x) for x in statuses],
    }


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    ensure_dirs()

    print("=== Party poll pipeline started ===")

    manual_rows = collect_manual_csv_rows()
    print(f"[INFO] manual CSV rows: {len(manual_rows)}")

    direct_rows, fetch_statuses = fetch_direct_sources()
    print(f"[INFO] direct source rows: {len(direct_rows)}")
    print(f"[INFO] direct source statuses: {len(fetch_statuses)}")

    all_rows = manual_rows + direct_rows
    if not all_rows:
        print("[WARN] No poll rows found. Creating empty outputs.")

    normalized_payload = build_normalized_payload(all_rows)
    write_json(NORMALIZED_OUT, normalized_payload)

    entries = parse_poll_entries(normalized_payload)
    aggregate_payload = build_aggregate_payload(entries)
    write_json(AGGREGATES_OUT, aggregate_payload)

    fetch_status_payload = build_fetch_status_payload(fetch_statuses)
    write_json(FETCH_STATUS_OUT, fetch_status_payload)

    print(f"[OK] wrote: {NORMALIZED_OUT.relative_to(REPO_ROOT)}")
    print(f"[OK] wrote: {AGGREGATES_OUT.relative_to(REPO_ROOT)}")
    print(f"[OK] wrote: {FETCH_STATUS_OUT.relative_to(REPO_ROOT)}")

    print("\n=== Summary ===")
    print(f"rows: {len(all_rows)}")
    print(f"entries: {len(entries)}")
    print(f"countries: {aggregate_payload.get('summary', {}).get('country_count')}")
    print(f"ready: {aggregate_payload.get('summary', {}).get('countries_ready_for_display')}")
    print(f"not ready: {aggregate_payload.get('summary', {}).get('countries_not_ready_for_display')}")

    for country_data in aggregate_payload.get("countries", []):
        country = country_data.get("country")
        leader = country_data.get("leader")
        ready = country_data.get("status", {}).get("ready_for_display")
        if leader:
            print(f" - {country}: {leader['party']} ({leader['weighted_average']}%), trend={leader['trend_direction']}, ready={ready}")
        else:
            print(f" - {country}: no leader, ready={ready}")


if __name__ == "__main__":
    main()
