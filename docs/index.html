#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as dateparser

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DATA_DIR = os.path.join(ROOT, "docs", "data")

# Balkán bounding box (durva)
BALKAN_BBOX = (13.0, 37.0, 30.0, 47.5)

# Vizsgált országok (kontúrhoz)
BALKAN_COUNTRIES = [
    "Albania",
    "Bosnia and Herzegovina",
    "Bulgaria",
    "Croatia",
    "Greece",
    "Kosovo",
    "Montenegro",
    "North Macedonia",
    "Romania",
    "Serbia",
    "Slovenia",
    "Turkey",
    "Moldova",
    "Hungary",
]

USER_AGENT = "balkan-security-map/1.5 (github actions; public blog)"
TIMEOUT = 30

CACHE_PATH = os.path.join(DOCS_DATA_DIR, "geocode_cache.json")


def ensure_dirs() -> None:
    os.makedirs(DOCS_DATA_DIR, exist_ok=True)


def http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> requests.Response:
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)

    backoff = 2
    last_exc: Optional[Exception] = None

    for attempt in range(1, 4):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"[http_get] retry {attempt}/3 status={r.status_code} url={url}")
                time.sleep(backoff)
                backoff *= 2
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            print(f"[http_get] error retry {attempt}/3: {e}")
            time.sleep(backoff)
            backoff *= 2

    raise last_exc if last_exc else RuntimeError("http_get failed")


def in_bbox(lon: float, lat: float, bbox: Tuple[float, float, float, float]) -> bool:
    lon_min, lat_min, lon_max, lat_max = bbox
    return (lon_min <= lon <= lon_max) and (lat_min <= lat <= lat_max)


def to_feature(lon: float, lat: float, props: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": props,
    }


def save_geojson(path: str, features: List[Dict[str, Any]]) -> None:
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)


# -------------------------
# Borders (weekly refresh)
# -------------------------
def ensure_balkan_borders() -> None:
    out_path = os.path.join(DOCS_DATA_DIR, "balkan_borders.geojson")

    if os.path.exists(out_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(out_path), tz=timezone.utc)
        if datetime.now(timezone.utc) - mtime < timedelta(days=7):
            return

    url = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
    print("[borders] downloading world countries geojson...")
    data = http_get(url).json()

    keep = set(BALKAN_COUNTRIES)
    out_feats = []
    for f in (data.get("features", []) or []):
        props = f.get("properties") or {}
        if props.get("name") in keep:
            out_feats.append(f)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": out_feats}, f, ensure_ascii=False, indent=2)

    print(f"[borders] saved {len(out_feats)} borders -> {out_path}")


# -------------------------
# Sources
# -------------------------
def fetch_usgs(days: int = 7, min_magnitude: float = 2.5) -> List[Dict[str, Any]]:
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    params = {
        "format": "geojson",
        "starttime": start.strftime("%Y-%m-%d"),
        "endtime": end.strftime("%Y-%m-%d"),
        "minmagnitude": str(min_magnitude),
    }
    data = http_get(url, params=params).json()

    out: List[Dict[str, Any]] = []
    for f in data.get("features", []):
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        if not in_bbox(lon, lat, BALKAN_BBOX):
            continue
        p = f.get("properties") or {}
        t_ms = p.get("time")
        dt = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat() if isinstance(t_ms, (int, float)) else None
        out.append(
            to_feature(
                lon, lat,
                {
                    "source": "USGS",
                    "kind": "earthquake",
                    "mag": p.get("mag"),
                    "place": p.get("place"),
                    "time": dt,
                    "url": p.get("url"),
                    "title": p.get("title"),
                },
            )
        )
    return out


def fetch_gdacs(days: int = 14) -> List[Dict[str, Any]]:
    url = "https://www.gdacs.org/xml/rss.xml"
    xml = http_get(url).text
    items = xml.split("<item>")[1:]
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    def get_tag(chunk: str, tag: str) -> Optional[str]:
        open_t = f"<{tag}>"
        close_t = f"</{tag}>"
        if open_t in chunk and close_t in chunk:
            return chunk.split(open_t, 1)[1].split(close_t, 1)[0].strip()
        return None

    out: List[Dict[str, Any]] = []
    for raw in items:
        chunk = raw.split("</item>")[0]
        title = get_tag(chunk, "title")
        link = get_tag(chunk, "link")
        pub = get_tag(chunk, "pubDate")
        point = get_tag(chunk, "georss:point") or get_tag(chunk, "point")
        if not pub or not point:
            continue
        try:
            pub_dt = dateparser.parse(pub).astimezone(timezone.utc)
        except Exception:
            continue
        if pub_dt < cutoff:
            continue
        try:
            lat_s, lon_s = point.split()
            lat, lon = float(lat_s), float(lon_s)
        except Exception:
            continue
        if not in_bbox(lon, lat, BALKAN_BBOX):
            continue

        out.append(
            to_feature(
                lon, lat,
                {
                    "source": "GDACS",
                    "kind": "disaster_alert",
                    "title": title,
                    "time": pub_dt.isoformat(),
                    "url": link,
                },
            )
        )
    return out


def fetch_gdelt(days: int = 2, max_records: int = 250) -> List[Dict[str, Any]]:
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    keywords = ["protest", "demonstration", "riot", "clash", "violence", "border", "checkpoint", "police", "attack", "explosion"]
    countries = [
        "Albania", "Bosnia", "Herzegovina", "Bulgaria", "Croatia", "Greece", "Kosovo",
        "Montenegro", "North Macedonia", "Romania", "Serbia", "Slovenia", "Turkey", "Moldova", "Hungary"
    ]
    query = "(" + " OR ".join(keywords) + ") AND (" + " OR ".join(countries) + ")"

    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end.strftime("%Y%m%d%H%M%S"),
        "sort": "HybridRel",
    }

    resp = http_get(url, params=params)
    try:
        data = resp.json()
    except Exception:
        snippet = (resp.text or "")[:250].replace("\n", " ")
        print(f"[GDELT] Non-JSON response. status={resp.status_code} head={snippet!r}")
        return []

    arts = data.get("articles", []) or []
    out: List[Dict[str, Any]] = []

    for a in arts:
        loc = a.get("location") or {}
        geo = loc.get("geo") or {}
        lat = geo.get("latitude")
        lon = geo.get("longitude")
        if lat is None or lon is None:
            continue

        try:
            lat_f, lon_f = float(lat), float(lon)
        except Exception:
            continue
        if not in_bbox(lon_f, lat_f, BALKAN_BBOX):
            continue

        seendate = a.get("seendate")
        time_iso = None
        if seendate:
            try:
                time_iso = dateparser.parse(seendate).astimezone(timezone.utc).isoformat()
            except Exception:
                time_iso = None

        out.append(
            to_feature(
                lon_f, lat_f,
                {
                    "source": "GDELT",
                    "kind": "news_event",
                    "title": a.get("title"),
                    "time": time_iso,
                    "url": a.get("url"),
                    "domain": a.get("domain"),
                    "language": a.get("language"),
                },
            )
        )

    seen = set()
    deduped = []
    for f in out:
        u = (f.get("properties") or {}).get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(f)
    return deduped


# -------------------------
# Scoring + time
# -------------------------
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


def score_feature(props: Dict[str, Any]) -> float:
    src = props.get("source")
    kind = props.get("kind")

    if src == "GDELT" and kind == "news_event":
        return 1.0
    if src == "GDACS":
        return 0.5
    if src == "USGS":
        try:
            m = float(props.get("mag"))
        except Exception:
            m = 0.0
        return 0.2 + min(0.6, max(0.0, (m - 3.0) * 0.15))
    return 0.1


def time_decay(dt: Optional[datetime], now: datetime) -> float:
    # félidő ~ 72 óra
    if dt is None:
        return 0.6
    age_hours = (now - dt).total_seconds() / 3600.0
    return 0.5 ** (age_hours / 72.0)


# -------------------------
# Hotspot aggregation + trend
# -------------------------
def grid_key(lon: float, lat: float, cell_deg: float) -> Tuple[int, int]:
    return (int(math.floor(lon / cell_deg)), int(math.floor(lat / cell_deg)))


def cell_center(ix: int, iy: int, cell_deg: float) -> Tuple[float, float]:
    return ((ix + 0.5) * cell_deg, (iy + 0.5) * cell_deg)


def trend_from(last7: float, prev7: float) -> Tuple[str, Optional[float], str]:
    """
    trend_code, change_pct, arrow
    """
    if last7 <= 0 and prev7 <= 0:
        return "na", 0.0, "·"
    if prev7 <= 0 and last7 > 0:
        return "new", None, "🆕"
    change = (last7 - prev7) / prev7 * 100.0

    # küszöbök blogra (nem túl “ideges”)
    if change >= 12:
        return "up", change, "🔺"
    if change <= -12:
        return "down", change, "🔻"
    return "flat", change, "▬"


def build_hotspots_with_trend(
    all_features: List[Dict[str, Any]],
    cell_deg: float = 0.5,
    top_n: int = 10,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    cutoff_7 = now - timedelta(days=7)
    cutoff_14 = now - timedelta(days=14)

    acc: Dict[Tuple[int, int], Dict[str, Any]] = {}

    for f in all_features:
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])

        props = f.get("properties") or {}
        dt = parse_time_iso(props.get("time"))

        base = score_feature(props)
        w = time_decay(dt, now)
        s = base * w  # össz-score (decay-elt)

        k = grid_key(lon, lat, cell_deg)
        bucket = acc.get(k)
        if bucket is None:
            acc[k] = {
                "score": 0.0,
                "count": 0,
                "sources": {"GDELT": 0, "USGS": 0, "GDACS": 0},
                "last7_score": 0.0,
                "prev7_score": 0.0,
            }
            bucket = acc[k]

        bucket["score"] += s
        bucket["count"] += 1
        src = props.get("source")
        if src in bucket["sources"]:
            bucket["sources"][src] += 1

        # trend score-k (csak ha van dt)
        if dt is not None:
            if dt >= cutoff_7:
                bucket["last7_score"] += s
            elif cutoff_14 <= dt < cutoff_7:
                bucket["prev7_score"] += s

    hotspot_features: List[Dict[str, Any]] = []
    rows: List[Dict[str, Any]] = []

    for (ix, iy), v in acc.items():
        lon_c, lat_c = cell_center(ix, iy, cell_deg)
        if not in_bbox(lon_c, lat_c, BALKAN_BBOX):
            continue

        last7 = float(v["last7_score"])
        prev7 = float(v["prev7_score"])
        trend_code, change_pct, arrow = trend_from(last7, prev7)

        props = {
            "type": "hotspot_cell",
            "score": round(float(v["score"]), 3),
            "count": int(v["count"]),
            "cell_deg": cell_deg,
            "sources": v["sources"],
            "last7_score": round(last7, 3),
            "prev7_score": round(prev7, 3),
            "trend": trend_code,
            "trend_arrow": arrow,
            "change_pct": None if change_pct is None else round(change_pct, 1),
        }

        hotspot_features.append(to_feature(lon_c, lat_c, props))
        rows.append({"lon": lon_c, "lat": lat_c, **props})

    rows_sorted = sorted(rows, key=lambda x: x["score"], reverse=True)
    return hotspot_features, rows_sorted[:top_n]


# -------------------------
# Reverse geocode for top hotspots (cached)
# -------------------------
def load_cache() -> Dict[str, Any]:
    if not os.path.exists(CACHE_PATH):
        return {}
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def save_cache(cache: Dict[str, Any]) -> None:
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def cache_key(lat: float, lon: float) -> str:
    return f"{lat:.2f},{lon:.2f}"


def reverse_geocode_osm(lat: float, lon: float, cache: Dict[str, Any]) -> str:
    k = cache_key(lat, lon)
    if k in cache:
        return cache[k]

    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"format": "jsonv2", "lat": str(lat), "lon": str(lon), "zoom": "10", "addressdetails": "1"}

    try:
        resp = http_get(url, params=params, headers={"Accept-Language": "en"})
        data = resp.json()
        addr = data.get("address") or {}

        # Blogbarátabb sorrend: admin/megye → város → falu
        name = (
            addr.get("county")
            or addr.get("state")
            or addr.get("municipality")
            or addr.get("city")
            or addr.get("town")
            or addr.get("village")
            or ""
        )
        country = addr.get("country") or ""
        if name and country and country not in name:
            place = f"{name}, {country}"
        else:
            place = name or country or "unknown"

        cache[k] = place
        time.sleep(1.0)  # udvarias throttling
        return place
    except Exception:
        cache[k] = "unknown"
        return "unknown"


# -------------------------
# Blog summary (hybrid)
# -------------------------
def pct_change(curr: float, prev: float) -> Optional[float]:
    if prev <= 0 and curr <= 0:
        return 0.0
    if prev <= 0:
        return None
    return (curr - prev) / prev * 100.0


def compute_total_score(features: List[Dict[str, Any]], now: datetime) -> float:
    total = 0.0
    for f in features:
        props = f.get("properties") or {}
        dt = parse_time_iso(props.get("time"))
        total += score_feature(props) * time_decay(dt, now)
    return total


def make_summary(all_features: List[Dict[str, Any]], top_hotspots: List[Dict[str, Any]], counts: Dict[str, int]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff_7 = now - timedelta(days=7)
    cutoff_14 = now - timedelta(days=14)

    last7: List[Dict[str, Any]] = []
    prev7: List[Dict[str, Any]] = []

    for f in all_features:
        dt = parse_time_iso((f.get("properties") or {}).get("time"))
        if dt is None:
            continue
        if dt >= cutoff_7:
            last7.append(f)
        elif cutoff_14 <= dt < cutoff_7:
            prev7.append(f)

    score_last7 = compute_total_score(last7, now)
    score_prev7 = compute_total_score(prev7, now)
    change = pct_change(score_last7, score_prev7)

    if change is None:
        trend_text = "Trend: nincs elég bázisadat az összehasonlításhoz."
    else:
        if change > 12:
            trend_text = f"Trend: emelkedő (+{change:.0f}%) az előző 7 naphoz képest."
        elif change < -12:
            trend_text = f"Trend: csökkenő ({change:.0f}%) az előző 7 naphoz képest."
        else:
            trend_text = f"Trend: nagyjából stagnáló ({change:+.0f}%) az előző 7 naphoz képest."

    if top_hotspots:
        h0 = top_hotspots[0]
        place = h0.get("place") or "ismeretlen térség"
        arrow = h0.get("trend_arrow", "")
        ch = h0.get("change_pct")
        ch_txt = "n/a" if ch is None else f"{ch:+.0f}%"
        top_text = (
            f"Legerősebb góc: {place} {arrow} (rácspont {h0['lat']:.2f}, {h0['lon']:.2f}; "
            f"score {float(h0['score']):.2f}; 7 napos változás: {ch_txt})."
        )
        note = "Megjegyzés: a hotspot híralapú jelzéseken alapul; érdemes a forrásokat kézzel ellenőrizni."
    else:
        top_text = "Legerősebb góc: jelenleg nincs elég geokódolt jelzés a térképes kiemeléshez."
        note = "Megjegyzés: a híralapú geokódolás hullámzó lehet; a rendszer automatikusan frissül."

    bullets = [
        top_text,
        trend_text,
        f"Forráskép: GDELT {counts.get('gdelt',0)}, USGS {counts.get('usgs',0)}, GDACS {counts.get('gdacs',0)}.",
        note,
    ]

    return {
        "generated_utc": now.isoformat(),
        "headline": "Balkán biztonsági helyzet – napi kivonat",
        "bullets": bullets,
        "stats": {
            "score_last7": round(score_last7, 3),
            "score_prev7": round(score_prev7, 3),
            "change_pct": None if change is None else round(change, 2),
        },
    }


def main() -> int:
    ensure_dirs()

    try:
        ensure_balkan_borders()
    except Exception as e:
        print(f"[borders] failed: {e}")

    print("Fetching USGS...")
    try:
        usgs = fetch_usgs(days=7, min_magnitude=2.5)
    except Exception as e:
        print(f"[USGS] fetch failed, continuing with empty layer: {e}")
        usgs = []
    print(f"USGS features: {len(usgs)}")

    print("Fetching GDACS...")
    try:
        gdacs = fetch_gdacs(days=14)
    except Exception as e:
        print(f"[GDACS] fetch failed, continuing with empty layer: {e}")
        gdacs = []
    print(f"GDACS features: {len(gdacs)}")

    print("Fetching GDELT...")
    try:
        gdelt = fetch_gdelt(days=2, max_records=250)
    except Exception as e:
        print(f"[GDELT] fetch failed, continuing with empty layer: {e}")
        gdelt = []
    print(f"GDELT features: {len(gdelt)}")

    save_geojson(os.path.join(DOCS_DATA_DIR, "usgs.geojson"), usgs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdacs.geojson"), gdacs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdelt.geojson"), gdelt)

    all_feats = gdelt + gdacs + usgs

    hotspot_geo, top_hotspots = build_hotspots_with_trend(all_feats, cell_deg=0.5, top_n=10)

    # place names for top list
    cache = load_cache()
    for h in top_hotspots:
        h["place"] = reverse_geocode_osm(float(h["lat"]), float(h["lon"]), cache)
    save_cache(cache)

    save_geojson(os.path.join(DOCS_DATA_DIR, "hotspots.geojson"), hotspot_geo)
    with open(os.path.join(DOCS_DATA_DIR, "hotspots.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_utc": datetime.now(timezone.utc).isoformat(), "top": top_hotspots}, f, ensure_ascii=False, indent=2)

    counts = {"usgs": len(usgs), "gdacs": len(gdacs), "gdelt": len(gdelt), "hotspot_cells": len(hotspot_geo)}
    summary = make_summary(all_feats, top_hotspots, counts)
    with open(os.path.join(DOCS_DATA_DIR, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "bbox": {"lon_min": BALKAN_BBOX[0], "lat_min": BALKAN_BBOX[1], "lon_max": BALKAN_BBOX[2], "lat_max": BALKAN_BBOX[3]},
    }
    with open(os.path.join(DOCS_DATA_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
