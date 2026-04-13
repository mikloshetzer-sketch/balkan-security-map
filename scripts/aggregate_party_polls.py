#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
aggregate_party_polls.py

Feladat:
- a normalized_polls.json állományból országonként aggregált párttrendeket készít
- csak a kellően friss és értelmezhető adatokat veszi figyelembe
- kiszámolja:
    - pártonként az egyszerű átlagot
    - a frissebb mérésekkel súlyozott átlagot
    - a trend irányát (up / down / flat)
    - az ország vezető pártját
    - a forráskészültséget
- frontend-barát JSON-t ír ki

Bemenet:
- data/processed/polls/normalized_polls.json

Kimenet:
- data/processed/polls/party_poll_aggregates.json

Megjegyzés:
- Ez a verzió még nem "forecast engine".
- Nem becsül mandátumot.
- Nem normalizál pártszövetségeket automatikusan.
- Konzervatív aggregátor, stabil JSON outputtal.
"""

from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------
# Útvonalak
# ---------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

INPUT_PATH = REPO_ROOT / "data" / "processed" / "polls" / "normalized_polls.json"
OUTPUT_PATH = REPO_ROOT / "data" / "processed" / "polls" / "party_poll_aggregates.json"


# ---------------------------------------------------------------------
# Konfiguráció
# ---------------------------------------------------------------------

DEFAULT_MAX_POLL_AGE_DAYS = int(os.getenv("POLL_MAX_AGE_DAYS", "180"))
DEFAULT_MIN_SOURCES_PER_COUNTRY = int(os.getenv("POLL_MIN_SOURCES", "3"))
DEFAULT_RECENCY_HALF_LIFE_DAYS = int(os.getenv("POLL_HALF_LIFE_DAYS", "45"))
DEFAULT_TREND_LOOKBACK_POLLS = int(os.getenv("POLL_TREND_LOOKBACK_POLLS", "2"))
DEFAULT_FLAT_THRESHOLD = float(os.getenv("POLL_FLAT_THRESHOLD", "1.0"))


# ---------------------------------------------------------------------
# Segédek
# ---------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def slugify(value: str) -> str:
    text = (value or "").strip().lower()
    out = []
    prev_us = False

    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True

    result = "".join(out).strip("_")
    return result or "unknown"


def parse_date_flexible(value: Optional[str]) -> Optional[datetime]:
    """
    Támogatott fő formák:
    - YYYY-MM-DD
    - YYYY-MM
    """
    if not value:
        return None

    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def days_between(newer: datetime, older: datetime) -> float:
    return (newer - older).total_seconds() / 86400.0


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def round2(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 2)


# ---------------------------------------------------------------------
# Adatmodellek
# ---------------------------------------------------------------------

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


# ---------------------------------------------------------------------
# Beolvasás
# ---------------------------------------------------------------------

def load_normalized_polls(path: Path = INPUT_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Nem található a bemeneti fájl: {path}")

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_poll_entries(payload: Dict[str, Any]) -> List[PollEntry]:
    result: List[PollEntry] = []

    for item in payload.get("polls", []):
        country = (item.get("country") or "").strip()
        date_raw = (item.get("date") or "").strip()
        source = (item.get("source") or "").strip()
        source_id = (item.get("source_id") or "").strip()
        parties = item.get("parties") or {}

        if not country or not date_raw or not source:
            continue

        date_dt = parse_date_flexible(date_raw)
        if not date_dt:
            continue

        parsed_parties: Dict[str, float] = {}
        for party_name, party_value in parties.items():
            val = safe_float(party_value)
            if val is None:
                continue
            parsed_parties[str(party_name).strip()] = val

        if not parsed_parties:
            continue

        result.append(
            PollEntry(
                country=country,
                date_raw=date_raw,
                date_dt=date_dt,
                source=source,
                source_id=source_id or slugify(source),
                sample_size=item.get("sample_size"),
                fieldwork_start=item.get("fieldwork_start"),
                fieldwork_end=item.get("fieldwork_end"),
                notes=item.get("notes") or "",
                import_method=item.get("import_method") or "unknown",
                raw_file=item.get("raw_file"),
                parties=parsed_parties,
            )
        )

    return result


# ---------------------------------------------------------------------
# Szűrés / súlyozás
# ---------------------------------------------------------------------

def is_fresh_enough(entry: PollEntry, now_dt: datetime, max_age_days: int) -> bool:
    age_days = days_between(now_dt, entry.date_dt)
    return age_days <= max_age_days


def recency_weight(entry: PollEntry, now_dt: datetime, half_life_days: int) -> float:
    """
    Exponenciális lecsengés:
    age = 0 nap -> 1.0
    age = half_life -> 0.5
    """
    age = max(0.0, days_between(now_dt, entry.date_dt))
    if half_life_days <= 0:
        return 1.0
    return 0.5 ** (age / float(half_life_days))


def sample_weight(sample_size: Optional[int]) -> float:
    """
    Enyhe mintanagyság-hatás.
    Nem hagyjuk, hogy a minta domináljon.
    """
    if not sample_size or sample_size <= 0:
        return 1.0

    # sqrt-súly, visszafogottan
    return max(0.75, min(math.sqrt(sample_size / 1000.0), 1.5))


def combined_weight(entry: PollEntry, now_dt: datetime, half_life_days: int) -> float:
    return recency_weight(entry, now_dt, half_life_days) * sample_weight(entry.sample_size)


# ---------------------------------------------------------------------
# Matematikai segédek
# ---------------------------------------------------------------------

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


# ---------------------------------------------------------------------
# Trend számítás
# ---------------------------------------------------------------------

def infer_party_trend(
    polls_sorted_desc: List[PollEntry],
    party_name: str,
    lookback_polls: int,
    flat_threshold: float,
) -> Dict[str, Any]:
    """
    Egyszerű trend:
    - frissebb blokk átlaga vs korábbi blokk átlaga
    - ha nincs elég adat -> unknown
    """
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


# ---------------------------------------------------------------------
# Aggregáció országonként
# ---------------------------------------------------------------------

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
    # frissek
    fresh_polls = [p for p in polls if is_fresh_enough(p, now_dt, max_age_days)]
    fresh_polls_sorted_desc = sorted(fresh_polls, key=lambda x: x.date_dt, reverse=True)

    distinct_sources = sorted({p.source_id for p in fresh_polls_sorted_desc})
    latest_poll_dt = fresh_polls_sorted_desc[0].date_dt if fresh_polls_sorted_desc else None

    readiness = len(distinct_sources) >= min_sources_per_country

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
            polls_sorted_desc=fresh_polls_sorted_desc,
            party_name=party_name,
            lookback_polls=trend_lookback_polls,
            flat_threshold=flat_threshold,
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

    raw_polls_out = []
    for poll in fresh_polls_sorted_desc:
        raw_polls_out.append(
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
            "ready_for_display": readiness,
            "readiness_reason": (
                f"{len(distinct_sources)} distinct source(s) within {max_age_days} days; "
                f"minimum required: {min_sources_per_country}"
            ),
            "distinct_source_count": len(distinct_sources),
            "distinct_sources": distinct_sources,
            "fresh_poll_count": len(fresh_polls_sorted_desc),
            "max_poll_age_days": max_age_days,
            "latest_poll_date": (
                latest_poll_dt.strftime("%Y-%m-%d") if latest_poll_dt else None
            ),
        },
        "leader": (
            {
                "party": leader["party"],
                "weighted_average": leader["weighted_average"],
                "simple_average": leader["simple_average"],
                "trend_direction": leader["trend"]["direction"],
            }
            if leader
            else None
        ),
        "parties": parties_out,
        "polls_used": raw_polls_out,
    }


# ---------------------------------------------------------------------
# Országcsoportosítás
# ---------------------------------------------------------------------

def group_polls_by_country(entries: List[PollEntry]) -> Dict[str, List[PollEntry]]:
    grouped: Dict[str, List[PollEntry]] = defaultdict(list)
    for entry in entries:
        grouped[entry.country].append(entry)
    return grouped


# ---------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------

def build_export_payload(
    entries: List[PollEntry],
    min_sources_per_country: int,
    max_age_days: int,
    half_life_days: int,
    trend_lookback_polls: int,
    flat_threshold: float,
) -> Dict[str, Any]:
    now_dt = utc_now()
    grouped = group_polls_by_country(entries)

    countries_out: List[Dict[str, Any]] = []
    summary_ready = 0

    for country in sorted(grouped.keys()):
        aggregated = aggregate_country(
            country=country,
            polls=grouped[country],
            now_dt=now_dt,
            min_sources_per_country=min_sources_per_country,
            max_age_days=max_age_days,
            half_life_days=half_life_days,
            trend_lookback_polls=trend_lookback_polls,
            flat_threshold=flat_threshold,
        )
        countries_out.append(aggregated)
        if aggregated["status"]["ready_for_display"]:
            summary_ready += 1

    return {
        "generated_utc": utc_now_iso(),
        "config": {
            "min_sources_per_country": min_sources_per_country,
            "max_poll_age_days": max_age_days,
            "recency_half_life_days": half_life_days,
            "trend_lookback_polls": trend_lookback_polls,
            "flat_threshold": flat_threshold,
        },
        "summary": {
            "country_count": len(countries_out),
            "countries_ready_for_display": summary_ready,
            "countries_not_ready_for_display": len(countries_out) - summary_ready,
            "total_poll_entries": len(entries),
        },
        "countries": countries_out,
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

def print_summary(payload: Dict[str, Any]) -> None:
    print("=== Party Poll Aggregation Summary ===")
    print(f"generated_utc: {payload.get('generated_utc')}")
    print(f"country_count: {payload.get('summary', {}).get('country_count')}")
    print(
        "countries_ready_for_display: "
        f"{payload.get('summary', {}).get('countries_ready_for_display')}"
    )
    print(
        "countries_not_ready_for_display: "
        f"{payload.get('summary', {}).get('countries_not_ready_for_display')}"
    )
    print(f"total_poll_entries: {payload.get('summary', {}).get('total_poll_entries')}")

    print("\nPer-country leaders:")
    for country_data in payload.get("countries", []):
        country = country_data.get("country")
        leader = country_data.get("leader")
        ready = country_data.get("status", {}).get("ready_for_display")

        if leader:
            print(
                f" - {country}: {leader['party']} "
                f"({leader['weighted_average']}%), "
                f"trend={leader['trend_direction']}, ready={ready}"
            )
        else:
            print(f" - {country}: no leader, ready={ready}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    payload = load_normalized_polls(INPUT_PATH)
    entries = parse_poll_entries(payload)

    export_payload = build_export_payload(
        entries=entries,
        min_sources_per_country=DEFAULT_MIN_SOURCES_PER_COUNTRY,
        max_age_days=DEFAULT_MAX_POLL_AGE_DAYS,
        half_life_days=DEFAULT_RECENCY_HALF_LIFE_DAYS,
        trend_lookback_polls=DEFAULT_TREND_LOOKBACK_POLLS,
        flat_threshold=DEFAULT_FLAT_THRESHOLD,
    )

    write_json(OUTPUT_PATH, export_payload)
    print_summary(export_payload)


if __name__ == "__main__":
    main()
