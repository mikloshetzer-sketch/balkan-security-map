#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dateparser

from risk_taxonomy import (
    COUNTRIES,
    DIMENSIONS,
    classify_record,
)

# ============================================================
# PATHS
# ============================================================

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DATA_DIR = os.path.join(ROOT, "docs", "data")

GDELT_PATH = os.path.join(DOCS_DATA_DIR, "gdelt.geojson")
GDELT_LINKED_PATH = os.path.join(DOCS_DATA_DIR, "gdelt_linked.geojson")
GDACS_PATH = os.path.join(DOCS_DATA_DIR, "gdacs.geojson")
USGS_PATH = os.path.join(DOCS_DATA_DIR, "usgs.geojson")

OUTPUT_PATH = os.path.join(DOCS_DATA_DIR, "risk_daily.json")


# ============================================================
# HELPERS
# ============================================================

def ensure_dirs() -> None:
    os.makedirs(DOCS_DATA_DIR, exist_ok=True)


def to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def parse_time_iso(t: Optional[str]) -> Optional[datetime]:
    if not t:
        return None
    try:
        dt = dateparser.parse(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def load_geojson_features(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        feats = data.get("features") or []
        return feats if isinstance(feats, list) else []
    except Exception:
        return []


def safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def normalize_country(country: Optional[str]) -> Optional[str]:
    if not country:
        return None

    aliases = {
        "bosnia": "Bosnia and Herzegovina",
        "bosnia and herzegovina": "Bosnia and Herzegovina",
        "north macedonia": "North Macedonia",
        "macedonia": "North Macedonia",
        "kosovo": "Kosovo",
        "serbia": "Serbia",
        "albania": "Albania",
        "croatia": "Croatia",
        "slovenia": "Slovenia",
        "montenegro": "Montenegro",
        "greece": "Greece",
        "bulgaria": "Bulgaria",
        "romania": "Romania",
        "hungary": "Hungary",
        "moldova": "Moldova",
        "turkey": "Turkey",
    }

    key = country.strip().lower()
    return aliases.get(key, country)


def extract_coords(feature: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    coords = (feature.get("geometry") or {}).get("coordinates") or []
    if len(coords) < 2:
        return None, None
    lon = safe_float(coords[0])
    lat = safe_float(coords[1])
    return lon, lat


def compact_location_text(props: Dict[str, Any]) -> str:
    return str(
        props.get("location")
        or props.get("place")
        or props.get("country_hint")
        or ""
    )


# ============================================================
# TIME / SOURCE WEIGHTS
# ============================================================

def time_weight(dt: Optional[datetime], now: datetime) -> float:
    """
    Rövidtávú snapshothoz:
    - friss esemény számít a legtöbbet
    - 3 nap után gyorsabban csökken
    """
    if dt is None:
        return 0.45

    age_hours = (now - dt).total_seconds() / 3600.0
    if age_hours <= 6:
        return 1.00
    if age_hours <= 24:
        return 0.85
    if age_hours <= 72:
        return 0.60
    if age_hours <= 7 * 24:
        return 0.30
    return 0.12


def source_modifier(source: str, kind: str) -> float:
    """
    A pipeline jelenlegi forrásaira külön finomhangolás.
    """
    source = (source or "").strip()
    kind = (kind or "").strip()

    if source == "GDELT" and kind == "news_linked":
        return 1.00
    if source == "GDELT" and kind == "news_geo":
        return 0.85
    if source == "GDACS":
        return 0.35
    if source == "USGS":
        return 0.25
    return 0.90


def dimension_share(dimensions: List[str]) -> float:
    if not dimensions:
        return 1.0
    return 1.0 / float(len(dimensions))


# ============================================================
# COUNTRY INFERENCE
# ============================================================

def infer_country_from_feature(props: Dict[str, Any], title: str, summary: str) -> Optional[str]:
    candidates = [
        props.get("country"),
        props.get("country_hint"),
        props.get("location"),
        props.get("place"),
        title,
        summary,
    ]

    text = " | ".join([str(x) for x in candidates if x])

    aliases = {
        "Albania": ["albania", "tirana"],
        "Bosnia and Herzegovina": ["bosnia and herzegovina", "bosnia", "sarajevo", "republika srpska", "banja luka"],
        "Bulgaria": ["bulgaria", "sofia"],
        "Croatia": ["croatia", "zagreb"],
        "Greece": ["greece", "athens", "evros", "aegean"],
        "Kosovo": ["kosovo", "pristina", "mitrovica", "zvecan", "zubin potok", "leposavic"],
        "Montenegro": ["montenegro", "podgorica", "bar"],
        "North Macedonia": ["north macedonia", "skopje", "macedonia"],
        "Romania": ["romania", "bucharest"],
        "Serbia": ["serbia", "belgrade", "presevo", "bujanovac", "sandzak"],
        "Slovenia": ["slovenia", "ljubljana"],
        "Turkey": ["turkey", "ankara", "istanbul"],
        "Moldova": ["moldova", "chisinau", "transnistria"],
        "Hungary": ["hungary", "budapest"],
    }

    low = text.lower()
    for country, words in aliases.items():
        for word in words:
            if word in low:
                return country

    return None


# ============================================================
# LEVELS
# ============================================================

def dim_score_to_level(score: float) -> str:
    """
    Reálisabb dimenziós küszöbök.
    """
    if score >= 4.5:
        return "critical"
    if score >= 2.6:
        return "tense"
    if score >= 1.1:
        return "elevated"
    return "normal"


def overall_from_dimensions(dim_levels: Dict[str, str], dim_scores: Dict[str, float], incident_count: int) -> str:
    values = list(dim_levels.values())
    total_score = sum(dim_scores.values())

    critical_count = sum(1 for v in values if v == "critical")
    tense_count = sum(1 for v in values if v == "tense")
    elevated_count = sum(1 for v in values if v == "elevated")

    if critical_count >= 2:
        return "critical"
    if critical_count == 1 and tense_count >= 1:
        return "critical"
    if critical_count == 1:
        return "tense"
    if tense_count >= 2:
        return "tense"
    if tense_count == 1 and elevated_count >= 1:
        return "tense"
    if tense_count == 1:
        return "elevated"
    if elevated_count >= 2:
        return "elevated"
    if elevated_count == 1 and incident_count >= 3:
        return "elevated"
    if total_score >= 6.0:
        return "elevated"
    return "normal"


def regional_overall_from_countries(country_rows: List[Dict[str, Any]]) -> str:
    critical_count = sum(1 for row in country_rows if row.get("overall") == "critical")
    tense_count = sum(1 for row in country_rows if row.get("overall") == "tense")
    elevated_count = sum(1 for row in country_rows if row.get("overall") == "elevated")

    if critical_count >= 2:
        return "critical"
    if critical_count == 1 and tense_count >= 1:
        return "critical"
    if critical_count == 1:
        return "tense"
    if tense_count >= 2:
        return "tense"
    if tense_count == 1 and elevated_count >= 2:
        return "tense"
    if tense_count == 1:
        return "elevated"
    if elevated_count >= 3:
        return "elevated"
    if elevated_count >= 1:
        return "elevated"
    return "normal"


def confidence_label(confidence_value: float) -> str:
    if confidence_value >= 0.75:
        return "high"
    if confidence_value >= 0.55:
        return "medium"
    return "low"


# ============================================================
# FEATURE NORMALIZATION
# ============================================================

def summarize_feature(props: Dict[str, Any]) -> Tuple[str, str]:
    source = props.get("source") or ""
    kind = props.get("kind") or ""
    title = props.get("title") or props.get("place") or props.get("location") or "Untitled event"

    summary_parts: List[str] = []

    if props.get("location"):
        summary_parts.append(f"Location: {props.get('location')}")
    if props.get("place"):
        summary_parts.append(f"Place: {props.get('place')}")
    if props.get("category"):
        summary_parts.append(f"Category: {props.get('category')}")
    if props.get("event_root_code"):
        summary_parts.append(f"CAMEO root: {props.get('event_root_code')}")
    if props.get("sources_count"):
        summary_parts.append(f"Sources: {props.get('sources_count')}")
    if props.get("mag") is not None:
        summary_parts.append(f"Magnitude: {props.get('mag')}")
    if source:
        summary_parts.append(f"Source: {source}")
    if kind:
        summary_parts.append(f"Kind: {kind}")

    summary = " | ".join(summary_parts)
    return str(title), summary


def feature_to_incident(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = feature.get("properties") or {}
    lon, lat = extract_coords(feature)

    title, summary = summarize_feature(props)

    country = infer_country_from_feature(props, title, summary)
    country = normalize_country(country)

    event_time = parse_time_iso(props.get("time"))
    if event_time is None:
        return None

    source = props.get("source") or "unknown"
    kind = props.get("kind") or ""
    location_text = compact_location_text(props)

    classified = classify_record(
        title=title,
        summary=summary,
        source_name=source,
        country=country,
        location_text=location_text,
    )

    dims = classified.dimensions or ["political"]
    share = dimension_share(dims)

    base_score = float(classified.base_score)
    score = base_score * source_modifier(source, kind)
    score *= time_weight(event_time, datetime.now(timezone.utc))

    # További fék, hogy a teljes rendszer ne fusson el.
    score *= 0.75

    # Természeti és riasztási források ne vigyék el a biztonsági képet.
    if source == "USGS":
        score *= 0.40
    elif source == "GDACS":
        score *= 0.55

    score = round(score, 3)

    return {
        "time": to_utc_z(event_time),
        "source": source,
        "kind": kind,
        "country": country,
        "title": title,
        "summary": summary,
        "url": props.get("url"),
        "lon": lon,
        "lat": lat,
        "event_type": classified.event_type,
        "dimensions": dims,
        "dimension_share": round(share, 4),
        "severity": int(classified.severity),
        "confidence": round(float(classified.confidence), 3),
        "source_type": classified.source_type,
        "geo_weight": round(float(classified.geo_weight), 3),
        "score": score,
        "matched_keywords": classified.matched_keywords,
    }


def load_all_incidents() -> List[Dict[str, Any]]:
    features = []
    features.extend(load_geojson_features(GDELT_PATH))
    features.extend(load_geojson_features(GDELT_LINKED_PATH))
    features.extend(load_geojson_features(GDACS_PATH))
    features.extend(load_geojson_features(USGS_PATH))

    incidents: List[Dict[str, Any]] = []
    for feature in features:
        item = feature_to_incident(feature)
        if item is None:
            continue
        incidents.append(item)

    incidents.sort(key=lambda x: x.get("time", ""), reverse=True)
    return incidents


# ============================================================
# AGGREGATION
# ============================================================

def init_country_row(country: str) -> Dict[str, Any]:
    return {
        "country": country,
        "overall": "normal",
        "overall_score": 0.0,
        "confidence": "low",
        "confidence_value": 0.0,
        "dimensions": {dim: "normal" for dim in DIMENSIONS},
        "dimension_scores": {dim: 0.0 for dim in DIMENSIONS},
        "drivers": [],
        "incident_count": 0,
        "recent_incident_count": 0,
        "top_incidents": [],
    }


def aggregate_country_risk(incidents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    recent_cut = now - timedelta(hours=48)

    countries = sorted(set(COUNTRIES + ["Turkey", "Moldova", "Hungary"]))
    rows = {country: init_country_row(country) for country in countries}

    driver_scores: Dict[str, Dict[str, float]] = {country: {} for country in countries}
    confidence_acc: Dict[str, List[float]] = {country: [] for country in countries}

    for inc in incidents:
        country = inc.get("country")
        if not country or country not in rows:
            continue

        row = rows[country]
        row["incident_count"] += 1

        inc_dt = parse_time_iso(inc.get("time"))
        if inc_dt and inc_dt >= recent_cut:
            row["recent_incident_count"] += 1

        inc_score = float(inc.get("score") or 0.0)
        dims = inc.get("dimensions") or ["political"]
        share = float(inc.get("dimension_share") or 1.0)

        for dim in dims:
            if dim not in row["dimension_scores"]:
                continue
            row["dimension_scores"][dim] += inc_score * share

        row["overall_score"] += inc_score
        confidence_acc[country].append(float(inc.get("confidence") or 0.0))

        driver_key = inc.get("event_type") or "unknown"
        driver_scores[country][driver_key] = driver_scores[country].get(driver_key, 0.0) + inc_score

        row["top_incidents"].append({
            "time": inc.get("time"),
            "title": inc.get("title"),
            "event_type": inc.get("event_type"),
            "score": round(inc_score, 3),
            "source": inc.get("source"),
            "dimensions": inc.get("dimensions"),
        })

    out: List[Dict[str, Any]] = []

    for country in countries:
        row = rows[country]

        # Cap / normalizálás országos szinten
        normalized_dim_scores: Dict[str, float] = {}
        for dim, raw_score in row["dimension_scores"].items():
            # enyhe saturáció: sok eseménynél se menjen el kontroll nélkül
            norm_score = raw_score / (1.0 + 0.18 * raw_score)
            normalized_dim_scores[dim] = round(norm_score, 3)

        row["dimension_scores"] = normalized_dim_scores

        normalized_overall = row["overall_score"] / (2.0 + 0.35 * row["overall_score"])
        row["overall_score"] = round(normalized_overall, 3)

        row["dimensions"] = {
            dim: dim_score_to_level(score)
            for dim, score in row["dimension_scores"].items()
        }

        row["overall"] = overall_from_dimensions(
            row["dimensions"],
            row["dimension_scores"],
            row["incident_count"],
        )

        conf_vals = confidence_acc[country]
        conf_avg = sum(conf_vals) / len(conf_vals) if conf_vals else 0.0

        # Enyhe boost csak akkor, ha tényleg van adat
        if row["incident_count"] >= 3:
            conf_avg += 0.05
        if row["recent_incident_count"] >= 2:
            conf_avg += 0.04

        conf_avg = clamp(conf_avg, 0.0, 1.0)
        row["confidence_value"] = round(conf_avg, 3)
        row["confidence"] = confidence_label(conf_avg)

        top_drivers = sorted(
            driver_scores[country].items(),
            key=lambda x: x[1],
            reverse=True,
        )[:3]
        row["drivers"] = [name for name, _ in top_drivers]

        row["top_incidents"] = sorted(
            row["top_incidents"],
            key=lambda x: x["score"],
            reverse=True,
        )[:5]

        out.append(row)

    out.sort(key=lambda x: (x["overall_score"], x["recent_incident_count"]), reverse=True)
    return out


def aggregate_regional_risk(country_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not country_rows:
        return {
            "overall": "normal",
            "overall_score": 0.0,
            "confidence": "low",
            "watchlist": [],
            "top_drivers": [],
        }

    dim_totals = {dim: 0.0 for dim in DIMENSIONS}
    driver_scores: Dict[str, float] = {}
    conf_values: List[float] = []

    active_rows = [row for row in country_rows if row.get("incident_count", 0) > 0]

    for row in active_rows:
        conf_values.append(float(row.get("confidence_value") or 0.0))

        for dim, score in (row.get("dimension_scores") or {}).items():
            if dim in dim_totals:
                dim_totals[dim] += float(score or 0.0)

        for drv in row.get("drivers") or []:
            driver_scores[drv] = driver_scores.get(drv, 0.0) + 1.0

    # Régiós score ne összeg legyen, hanem átlagos aktív terhelés
    if active_rows:
        avg_overall = sum(float(r.get("overall_score") or 0.0) for r in active_rows) / len(active_rows)
    else:
        avg_overall = 0.0

    regional_dimensions = {}
    for dim, total in dim_totals.items():
        regional_dimensions[dim] = round((total / max(1, len(active_rows))), 3)

    regional_dim_levels = {
        dim: dim_score_to_level(score)
        for dim, score in regional_dimensions.items()
    }

    overall = regional_overall_from_countries(country_rows)

    conf_avg = sum(conf_values) / len(conf_values) if conf_values else 0.0

    watchlist = [
        {
            "country": row["country"],
            "overall": row["overall"],
            "overall_score": row["overall_score"],
            "drivers": row["drivers"],
        }
        for row in country_rows
        if row["overall"] in {"critical", "tense", "elevated"}
    ][:5]

    top_drivers = [
        name for name, _ in sorted(driver_scores.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    return {
        "overall": overall,
        "overall_score": round(avg_overall, 3),
        "confidence": confidence_label(conf_avg),
        "confidence_value": round(conf_avg, 3),
        "dimensions": regional_dim_levels,
        "dimension_scores": regional_dimensions,
        "watchlist": watchlist,
        "top_drivers": top_drivers,
    }


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    ensure_dirs()

    incidents = load_all_incidents()
    country_rows = aggregate_country_risk(incidents)
    regional = aggregate_regional_risk(country_rows)

    output = {
        "generated_utc": to_utc_z(datetime.now(timezone.utc)),
        "method_version": "risk_snapshot_v2",
        "sources": {
            "gdelt_geo": os.path.exists(GDELT_PATH),
            "gdelt_linked": os.path.exists(GDELT_LINKED_PATH),
            "gdacs": os.path.exists(GDACS_PATH),
            "usgs": os.path.exists(USGS_PATH),
        },
        "region": regional,
        "countries": country_rows,
        "stats": {
            "incident_count": len(incidents),
            "country_count": len(country_rows),
            "active_country_count": sum(1 for row in country_rows if row.get("incident_count", 0) > 0),
        },
        "legend": {
            "levels": ["normal", "elevated", "tense", "critical"],
            "dimensions": DIMENSIONS,
            "confidence": ["low", "medium", "high"],
        },
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
