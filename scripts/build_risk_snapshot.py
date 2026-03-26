#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "docs", "data")

OUTPUT_PATH = os.path.join(DATA_DIR, "risk_daily.json")

# ============================================================
# CONFIG
# ============================================================

DIMENSION_MAP = {
    "protest": "political",
    "security_politics": "political",

    "military": "military",
    "drone": "military",

    "police": "policing",

    "border": "migration",

    "cyber": "infrastructure",
    "energy": "infrastructure",
    "infrastructure": "infrastructure",
}

# base weights
BASE_WEIGHTS = {
    "news_linked": 1.3,
    "news_geo": 1.0,
    "disaster_alert": 0.6,
    "earthquake": 0.3,
}

# ============================================================
# HELPERS
# ============================================================

def load_geojson(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return (json.load(f) or {}).get("features", [])

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def weight_of(props: Dict) -> float:
    kind = props.get("kind")
    return BASE_WEIGHTS.get(kind, 0.2)

# ============================================================
# DIMENSION SCORING
# ============================================================

def compute_dimension_scores(features: List[Dict]) -> Dict[str, float]:
    scores = {
        "political": 0.0,
        "military": 0.0,
        "policing": 0.0,
        "migration": 0.0,
        "social": 0.0,
        "infrastructure": 0.0,
    }

    for f in features:
        props = f.get("properties", {})
        cat = props.get("category")
        dim = DIMENSION_MAP.get(cat)

        if not dim:
            continue

        scores[dim] += weight_of(props)

    return scores

# ============================================================
# DIMENSION LEVELS
# ============================================================

def level_from_score(score: float) -> str:
    if score < 1.5:
        return "normal"
    elif score < 3.5:
        return "elevated"
    elif score < 6.0:
        return "tense"
    else:
        return "critical"

# ============================================================
# OVERALL LOGIC (FIXED)
# ============================================================

def compute_overall(dim_levels: Dict[str, str], dim_scores: Dict[str, float]) -> str:
    levels = list(dim_levels.values())

    count_elevated = sum(1 for v in levels if v == "elevated")
    count_tense = sum(1 for v in levels if v == "tense")
    count_critical = sum(1 for v in levels if v == "critical")

    max_score = max(dim_scores.values()) if dim_scores else 0.0

    # ---- CRITICAL ----
    if count_critical >= 2:
        return "critical"
    if count_critical == 1 and max_score > 7:
        return "critical"

    # ---- TENSE ----
    if count_tense >= 2:
        return "tense"
    if count_tense == 1 and count_elevated >= 1:
        return "tense"
    if max_score > 5:
        return "tense"

    # ---- ELEVATED ----
    if count_elevated >= 2:
        return "elevated"
    if count_elevated == 1:
        return "elevated"

    return "normal"

# ============================================================
# CONFIDENCE
# ============================================================

def compute_confidence(total_events: int) -> Dict:
    if total_events < 20:
        return {"confidence": "low", "value": 0.4}
    elif total_events < 80:
        return {"confidence": "medium", "value": 0.6}
    else:
        return {"confidence": "high", "value": 0.8}

# ============================================================
# MAIN
# ============================================================

def main() -> int:
    gdelt = load_geojson(os.path.join(DATA_DIR, "gdelt.geojson"))
    gdelt_linked = load_geojson(os.path.join(DATA_DIR, "gdelt_linked.geojson"))
    gdacs = load_geojson(os.path.join(DATA_DIR, "gdacs.geojson"))
    usgs = load_geojson(os.path.join(DATA_DIR, "usgs.geojson"))

    all_features = gdelt + gdelt_linked + gdacs + usgs

    dim_scores = compute_dimension_scores(all_features)
    dim_levels = {k: level_from_score(v) for k, v in dim_scores.items()}

    overall = compute_overall(dim_levels, dim_scores)
    overall_score = sum(dim_scores.values())

    conf = compute_confidence(len(all_features))

    out = {
        "generated_utc": now_iso(),
        "method_version": "risk_snapshot_v3",
        "sources": {
            "gdelt_geo": True,
            "gdelt_linked": True,
            "gdacs": True,
            "usgs": True,
        },
        "region": {
            "overall": overall,
            "overall_score": round(overall_score, 3),
            "confidence": conf["confidence"],
            "confidence_value": conf["value"],
            "dimensions": dim_levels,
            "dimension_scores": {k: round(v, 3) for k, v in dim_scores.items()},
            "watchlist": [],
        },
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("[risk] risk_daily.json created.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
