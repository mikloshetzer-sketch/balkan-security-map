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
RSS_PATH = os.path.join(DOCS_DATA_DIR, "trusted_rss.json")

OUTPUT_PATH = os.path.join(DOCS_DATA_DIR, "risk_daily.json")

# ============================================================
# HELPERS
# ============================================================

def ensure_dirs():
    os.makedirs(DOCS_DATA_DIR, exist_ok=True)

def to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_time_iso(t):
    if not t:
        return None
    try:
        dt = dateparser.parse(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except:
        return None

def load_geojson_features(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (json.load(f) or {}).get("features", [])
    except:
        return []

def load_rss():
    if not os.path.exists(RSS_PATH):
        return []
    try:
        with open(RSS_PATH, "r", encoding="utf-8") as f:
            return (json.load(f) or {}).get("stories", [])
    except:
        return []

def normalize_country(c):
    if not c:
        return None
    return c

# ============================================================
# WEIGHTS
# ============================================================

def time_weight(dt, now):
    if not dt:
        return 0.4
    h = (now - dt).total_seconds() / 3600
    if h < 6: return 1.0
    if h < 24: return 0.8
    if h < 72: return 0.6
    if h < 168: return 0.3
    return 0.1

def dimension_share(dims):
    return 1.0 / len(dims) if dims else 1.0

# ============================================================
# INCIDENT CONVERSION
# ============================================================

def geo_to_incident(f):
    p = f.get("properties") or {}
    dt = parse_time_iso(p.get("time"))
    if not dt:
        return None

    classified = classify_record(
        title=p.get("title") or "",
        summary=str(p),
        source_name=p.get("source"),
        country=p.get("country"),
        location_text=str(p.get("place")),
    )

    dims = classified.dimensions or ["political"]

    score = float(classified.base_score)
    score *= time_weight(dt, datetime.now(timezone.utc))
    score *= 0.55

    return {
        "country": p.get("country"),
        "time": to_utc_z(dt),
        "dimensions": dims,
        "dimension_share": dimension_share(dims),
        "score": round(score,3),
        "confidence": classified.confidence,
        "event_type": classified.event_type,
    }

def rss_to_incident(item):
    dt = parse_time_iso(item.get("time"))
    if not dt:
        return None

    country = normalize_country(item.get("country_hint"))
    if not country:
        return None

    classified = classify_record(
        title=item.get("title"),
        summary=item.get("summary"),
        source_name="RSS",
        country=country,
        location_text=item.get("summary"),
    )

    dims = classified.dimensions or ["political"]

    score = float(classified.base_score) * 0.6
    score *= time_weight(dt, datetime.now(timezone.utc))
    score *= 0.4

    return {
        "country": country,
        "time": to_utc_z(dt),
        "dimensions": dims,
        "dimension_share": dimension_share(dims),
        "score": round(score,3),
        "confidence": classified.confidence * 1.1,
        "event_type": classified.event_type,
    }

# ============================================================
# LOAD INCIDENTS
# ============================================================

def load_all_incidents():
    incidents = []

    for f in load_geojson_features(GDELT_PATH):
        i = geo_to_incident(f)
        if i: incidents.append(i)

    for f in load_geojson_features(GDELT_LINKED_PATH):
        i = geo_to_incident(f)
        if i: incidents.append(i)

    for f in load_geojson_features(GDACS_PATH):
        i = geo_to_incident(f)
        if i: incidents.append(i)

    for f in load_geojson_features(USGS_PATH):
        i = geo_to_incident(f)
        if i: incidents.append(i)

    rss_items = load_rss()
    rss_count = 0

    for r in rss_items:
        i = rss_to_incident(r)
        if i:
            incidents.append(i)
            rss_count += 1

    print(f"[RSS] integrated: {rss_count}")

    return incidents

# ============================================================
# AGGREGATION
# ============================================================

def aggregate(incidents):
    rows = {}

    for c in COUNTRIES:
        rows[c] = {
            "country": c,
            "overall_score": 0,
            "dimension_scores": {d:0 for d in DIMENSIONS},
            "incident_count": 0,
            "confidence": 0,
        }

    for inc in incidents:
        c = inc.get("country")
        if c not in rows:
            continue

        rows[c]["incident_count"] += 1
        rows[c]["overall_score"] += inc["score"]

        for d in inc["dimensions"]:
            rows[c]["dimension_scores"][d] += inc["score"] * inc["dimension_share"]

        rows[c]["confidence"] += inc["confidence"]

    out = []

    for r in rows.values():
        if r["incident_count"] > 0:
            r["confidence"] /= r["incident_count"]

        score = r["overall_score"]

        if score > 5:
            level = "critical"
        elif score > 3:
            level = "tense"
        elif score > 1:
            level = "elevated"
        else:
            level = "normal"

        r["overall"] = level
        r["overall_score"] = round(score,3)
        r["confidence"] = round(r["confidence"],3)

        out.append(r)

    return out

# ============================================================
# MAIN
# ============================================================

def main():
    ensure_dirs()

    incidents = load_all_incidents()
    countries = aggregate(incidents)

    output = {
        "generated_utc": to_utc_z(datetime.now(timezone.utc)),
        "countries": countries,
        "stats": {
            "incident_count": len(incidents),
        }
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print("Risk updated with RSS ✔")

if __name__ == "__main__":
    main()
