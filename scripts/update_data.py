#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import math
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as dateparser

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DATA_DIR = os.path.join(ROOT, "docs", "data")

BALKAN_BBOX = (13.0, 37.0, 30.0, 47.5)

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

USER_AGENT = "balkan-security-map/2.1 (github actions; public blog)"
TIMEOUT = 30
CACHE_PATH = os.path.join(DOCS_DATA_DIR, "geocode_cache.json")

# --- HIBRID EARLY WARNING zónák (egyszerű bbox alapú közelítés) ---
SENSITIVE_ZONES = [
    {"name": "Kosovo–Serbia", "bbox": (19.5, 42.0, 21.8, 43.6), "mult": 1.35},
    {"name": "Bosznia (entitásvonal tág)", "bbox": (16.0, 43.0, 19.8, 45.1), "mult": 1.20},
    {"name": "Görög–török Égei", "bbox": (24.0, 35.6, 28.9, 41.5), "mult": 1.25},
    {"name": "Moldova–Transznisztria", "bbox": (28.0, 46.0, 30.3, 48.2), "mult": 1.30},
]

# Rolling retention (days)
ROLLING_DAYS = 7
GDELT_DAYS = 7        # request window (plus rolling merge)
USGS_DAYS = 7
GDACS_DAYS = 14       # RSS is broader; rolling still trims to 7 for map layer if we want
# NOTE: GDACS rolling will also trim to ROLLING_DAYS for "events on map".
# If you want GDACS to show 14 days on map, set GDACS_KEEP_DAYS = 14.
GDACS_KEEP_DAYS = 7


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


def load_geojson_features(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        feats = data.get("features") or []
        if isinstance(feats, list):
            return feats
        return []
    except Exception:
        return []


# -------------------------
# Time helpers (stable UTC "Z")
# -------------------------
def to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    # keep seconds; drop microseconds for stable output
    dt = dt.replace(microsecond=0)
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


def clamp_and_normalize_times(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure props['time'] is UTC Z string if parseable."""
    out = []
    for f in features:
        p = (f.get("properties") or {})
        dt = parse_time_iso(p.get("time"))
        if dt is not None:
            p["time"] = to_utc_z(dt)
            f["properties"] = p
        out.append(f)
    return out


def trim_by_days(features: List[Dict[str, Any]], keep_days: int) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=keep_days)
    kept = []
    for f in features:
        dt = parse_time_iso((f.get("properties") or {}).get("time"))
        if dt is None:
            # if no time, keep but it's rare
            kept.append(f)
            continue
        if dt >= cutoff:
            kept.append(f)
    return kept


# -------------------------
# Dedup keys
# -------------------------
def dedup_key(feature: Dict[str, Any]) -> Optional[str]:
    p = feature.get("properties") or {}
    src = p.get("source") or ""
    url = p.get("url")
    title = p.get("title")
    tm = p.get("time")
    kind = p.get("kind") or ""

    # Prefer strong IDs
    if url:
        return f"{src}|{url}"

    # USGS sometimes has no url? (rare). Use title+time+mag
    if src == "USGS":
        mag = p.get("mag")
        place = p.get("place")
        return f"{src}|{kind}|{tm}|{mag}|{place}"

    # GDACS: title+time usually enough
    if src == "GDACS":
        return f"{src}|{kind}|{tm}|{title}"

    # fallback
    if title and tm:
        return f"{src}|{kind}|{tm}|{title}"

    return None


def merge_dedup(old_feats: List[Dict[str, Any]], new_feats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged = []
    seen = set()

    for f in (new_feats + old_feats):
        k = dedup_key(f)
        if not k:
            merged.append(f)
            continue
        if k in seen:
            continue
        seen.add(k)
        merged.append(f)

    # Sort by time desc (best-effort)
    def sort_key(feat: Dict[str, Any]) -> float:
        dt = parse_time_iso((feat.get("properties") or {}).get("time"))
        if dt is None:
            return 0.0
        return dt.timestamp()

    merged.sort(key=sort_key, reverse=True)
    return merged


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
        dt = None
        if isinstance(t_ms, (int, float)):
            dt = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc)
        out.append(
            to_feature(
                lon, lat,
                {
                    "source": "USGS",
                    "kind": "earthquake",
                    "mag": p.get("mag"),
                    "place": p.get("place"),
                    "time": to_utc_z(dt) if dt else None,
                    "url": p.get("url"),
                    "title": p.get("title"),
                    "type": "Earthquake",
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
                    "time": to_utc_z(pub_dt),
                    "url": link,
                    "type": "Alert",
                },
            )
        )
    return out


def fetch_gdelt(days: int = 7, max_records: int = 250) -> List[Dict[str, Any]]:
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
        geo = (loc.get("geo") or {})
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
                dt = dateparser.parse(seendate).astimezone(timezone.utc)
                time_iso = to_utc_z(dt)
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
                    "type": "News",
                },
            )
        )

    return out


# -------------------------
# Scoring
# -------------------------
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
    if last7 <= 0 and prev7 <= 0:
        return "na", 0.0, "·"
    if prev7 <= 0 and last7 > 0:
        return "new", None, "🆕"
    change = (last7 - prev7) / prev7 * 100.0
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
        s = score_feature(props) * time_decay(dt, now)

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
        place = f"{name}, {country}" if name and country and country not in name else (name or country or "unknown")

        cache[k] = place
        time.sleep(1.0)
        return place
    except Exception:
        cache[k] = "unknown"
        return "unknown"


# -------------------------
# Weekly summary (7 days) with examples
# -------------------------
STOP = {
    "the","a","an","and","or","to","of","in","on","for","with","as","at","by","from",
    "is","are","was","were","be","been","it","this","that","these","those",
    "over","after","before","into","about","amid","during","near",
    "says","say","new","up","down",
    "serbia","kosovo","greece","turkey","romania","bulgaria","croatia","albania",
    "bosnia","herzegovina","montenegro","slovenia","moldova","hungary","macedonia",
}
WORD_RE = re.compile(r"[a-zA-Z]{3,}")


def extract_topics(titles: List[str], top_k: int = 6) -> List[str]:
    freq: Dict[str, int] = {}
    for t in titles:
        for w in WORD_RE.findall((t or "").lower()):
            if w in STOP:
                continue
            freq[w] = freq.get(w, 0) + 1
    return [w for w, _ in sorted(freq.items(), key=lambda x: x[1], reverse=True)[:top_k]]


def build_weekly(all_features: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff_7 = now - timedelta(days=7)

    week: List[Tuple[datetime, Dict[str, Any]]] = []
    for f in all_features:
        p = f.get("properties") or {}
        dt = parse_time_iso(p.get("time"))
        if dt is None:
            continue
        if dt >= cutoff_7:
            week.append((dt, f))

    counts = {"GDELT": 0, "USGS": 0, "GDACS": 0}
    gdelt_items: List[Tuple[datetime, Dict[str, Any]]] = []

    for dt, f in week:
        src = (f.get("properties") or {}).get("source")
        if src in counts:
            counts[src] += 1
        if src == "GDELT":
            gdelt_items.append((dt, f))

    gdelt_items.sort(key=lambda x: x[0], reverse=True)

    examples = []
    for dt, f in gdelt_items[:5]:
        p = f.get("properties") or {}
        examples.append({
            "time_utc": to_utc_z(dt),
            "title": p.get("title"),
            "url": p.get("url"),
            "domain": p.get("domain"),
        })

    topics = extract_topics([((x[1].get("properties") or {}).get("title") or "") for x in gdelt_items[:50]])

    bullets = [
        f"Híralapú jelzések (GDELT): {counts['GDELT']} db az elmúlt 7 napban.",
        f"Természeti/ellátási stresszorok: USGS {counts['USGS']} esemény, GDACS {counts['GDACS']} riasztás (Balkán bbox-ban).",
    ]
    if topics:
        bullets.append("Gyakori témák a hírcímekben: " + ", ".join(topics) + ".")
    bullets.append("Megjegyzés: automatikus OSINT-kivonat; a linkelt források kézi ellenőrzése javasolt.")

    return {
        "generated_utc": to_utc_z(now),
        "headline": "Heti kivonat – elmúlt 7 nap",
        "bullets": bullets,
        "counts": counts,
        "examples": examples,
    }


# -------------------------
# Daily summary + banner alert
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


def alert_from_top(top: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not top:
        return None

    arrow = top.get("trend_arrow")
    ch = top.get("change_pct")
    score = float(top.get("score") or 0.0)
    place = top.get("place") or "ismeretlen térség"

    if arrow == "🆕":
        return {
            "level": "info",
            "title": "Új góc",
            "text": f"Új hotspot jelent meg: {place}. Érdemes követni a következő 24–72 órában.",
        }

    if arrow == "🔺":
        if ch is not None and ch >= 25:
            return {
                "level": "high",
                "title": "Emelkedő feszültség",
                "text": f"Erősödő hotspot: {place} (7 napos változás: +{ch:.0f}%).",
            }
        if ch is None or ch >= 12:
            return {
                "level": "medium",
                "title": "Emelkedő feszültség",
                "text": f"Felfutó jelzések: {place} (trend: emelkedő).",
            }

    if score >= 2.0:
        return {
            "level": "watch",
            "title": "Monitor",
            "text": f"Magas aktivitás: {place}. Trend nem egyértelmű, de érdemes figyelni.",
        }

    return None


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

    top = top_hotspots[0] if top_hotspots else None
    if top:
        place = top.get("place") or "ismeretlen térség"
        arrow = top.get("trend_arrow", "")
        ch = top.get("change_pct")
        ch_txt = "n/a" if ch is None else f"{ch:+.0f}%"
        top_text = (
            f"Legerősebb góc: {place} {arrow} (rácspont {top['lat']:.2f}, {top['lon']:.2f}; "
            f"score {float(top['score']):.2f}; 7 napos változás: {ch_txt})."
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
        "generated_utc": to_utc_z(now),
        "headline": "Balkán biztonsági helyzet – napi kivonat",
        "bullets": bullets,
        "alert": alert_from_top(top),
        "stats": {
            "score_last7": round(score_last7, 3),
            "score_prev7": round(score_prev7, 3),
            "change_pct": None if change is None else round(change, 2),
        },
    }


# -------------------------
# EARLY WARNING (hibrid)
# -------------------------
def zone_multiplier(lon: float, lat: float) -> Tuple[float, Optional[str]]:
    mult = 1.0
    zname: Optional[str] = None
    for z in SENSITIVE_ZONES:
        if in_bbox(lon, lat, z["bbox"]):
            if z["mult"] > mult:
                mult = float(z["mult"])
                zname = str(z["name"])
    return mult, zname


def neighbor_keys(k: Tuple[int, int]) -> List[Tuple[int, int]]:
    x, y = k
    return [(x-1,y-1),(x,y-1),(x+1,y-1),(x-1,y),(x+1,y),(x-1,y+1),(x,y+1),(x+1,y+1)]


def build_early_warning(
    all_features: List[Dict[str, Any]],
    cell_deg: float = 0.5,
    lookback_days: int = 7,
    recent_hours: int = 48,
    top_n: int = 10,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=lookback_days)
    recent_cut = now - timedelta(hours=recent_hours)

    acc: Dict[Tuple[int,int], Dict[str, Any]] = {}

    def get_bucket(k: Tuple[int,int]) -> Dict[str, Any]:
        b = acc.get(k)
        if b is None:
            b = {
                "recent": 0.0,
                "baseline": 0.0,
                "recent_cnt": 0,
                "baseline_cnt": 0,
                "src_recent": {"GDELT": 0, "USGS": 0, "GDACS": 0},
            }
            acc[k] = b
        return b

    for f in all_features:
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        if not in_bbox(lon, lat, BALKAN_BBOX):
            continue
        props = f.get("properties") or {}
        dt = parse_time_iso(props.get("time"))
        if dt is None or dt < cutoff:
            continue

        s = score_feature(props)  # no strong decay
        k = grid_key(lon, lat, cell_deg)
        b = get_bucket(k)

        src = props.get("source")
        if dt >= recent_cut:
            b["recent"] += s
            b["recent_cnt"] += 1
            if src in b["src_recent"]:
                b["src_recent"][src] += 1
        else:
            b["baseline"] += s
            b["baseline_cnt"] += 1

    raw: Dict[Tuple[int,int], float] = {}
    meta: Dict[Tuple[int,int], Dict[str, Any]] = {}

    for k, b in acc.items():
        lon_c, lat_c = cell_center(k[0], k[1], cell_deg)
        if not in_bbox(lon_c, lat_c, BALKAN_BBOX):
            continue

        recent = float(b["recent"])
        base = float(b["baseline"])

        if recent <= 0.75:
            continue

        ratio = (recent + 0.5) / (base + 1.5)

        src_mix = sum(1 for v in b["src_recent"].values() if v > 0)
        mix_boost = 1.0 + 0.08 * (src_mix - 1) if src_mix >= 2 else 1.0

        z_mult, z_name = zone_multiplier(lon_c, lat_c)

        esc = (recent * 10.0) * math.log1p(ratio) * mix_boost * z_mult

        raw[k] = esc
        meta[k] = {
            "recent": round(recent, 3),
            "baseline": round(base, 3),
            "ratio": round(ratio, 3),
            "src_mix": int(src_mix),
            "zone": z_name,
            "zone_mult": round(z_mult, 2),
            "recent_cnt": int(b["recent_cnt"]),
            "baseline_cnt": int(b["baseline_cnt"]),
            "src_recent": b["src_recent"],
        }

    if not raw:
        return [], []

    max_raw = max(raw.values()) or 1.0

    signals: List[Dict[str, Any]] = []
    rows: List[Dict[str, Any]] = []

    for k, esc_raw in raw.items():
        neigh = neighbor_keys(k)
        neigh_active = sum(1 for nk in neigh if nk in raw and raw[nk] >= 0.35 * max_raw)
        spread_boost = 1.0 + 0.06 * neigh_active

        score0_100 = min(100.0, (esc_raw * spread_boost) / max_raw * 100.0)
        lon_c, lat_c = cell_center(k[0], k[1], cell_deg)

        props = {
            "type": "early_warning",
            "escalation": round(score0_100, 1),
            "cell_deg": cell_deg,
            "neighbor_active": int(neigh_active),
            **meta[k],
        }

        signals.append(to_feature(lon_c, lat_c, props))
        rows.append({"lon": lon_c, "lat": lat_c, **props})

    rows_sorted = sorted(rows, key=lambda x: x["escalation"], reverse=True)
    return signals, rows_sorted[:top_n]


# -------------------------
# main
# -------------------------
def main() -> int:
    ensure_dirs()

    try:
        ensure_balkan_borders()
    except Exception as e:
        print(f"[borders] failed: {e}")

    # Load previous layers for rolling merge
    prev_usgs = load_geojson_features(os.path.join(DOCS_DATA_DIR, "usgs.geojson"))
    prev_gdacs = load_geojson_features(os.path.join(DOCS_DATA_DIR, "gdacs.geojson"))
    prev_gdelt = load_geojson_features(os.path.join(DOCS_DATA_DIR, "gdelt.geojson"))

    print("Fetching USGS...")
    try:
        usgs_new = fetch_usgs(days=USGS_DAYS, min_magnitude=2.5)
    except Exception as e:
        print(f"[USGS] fetch failed, continuing with rolling previous: {e}")
        usgs_new = []
    print(f"USGS fetched: {len(usgs_new)}")

    print("Fetching GDACS...")
    try:
        gdacs_new = fetch_gdacs(days=GDACS_DAYS)
    except Exception as e:
        print(f"[GDACS] fetch failed, continuing with rolling previous: {e}")
        gdacs_new = []
    print(f"GDACS fetched: {len(gdacs_new)}")

    print("Fetching GDELT...")
    try:
        gdelt_new = fetch_gdelt(days=GDELT_DAYS, max_records=250)
    except Exception as e:
        print(f"[GDELT] fetch failed, continuing with rolling previous: {e}")
        gdelt_new = []
    print(f"GDELT fetched: {len(gdelt_new)}")

    # Normalize time fields + merge rolling + trim
    usgs_merged = merge_dedup(clamp_and_normalize_times(prev_usgs), clamp_and_normalize_times(usgs_new))
    gdacs_merged = merge_dedup(clamp_and_normalize_times(prev_gdacs), clamp_and_normalize_times(gdacs_new))
    gdelt_merged = merge_dedup(clamp_and_normalize_times(prev_gdelt), clamp_and_normalize_times(gdelt_new))

    usgs = trim_by_days(usgs_merged, keep_days=ROLLING_DAYS)
    gdacs = trim_by_days(gdacs_merged, keep_days=GDACS_KEEP_DAYS)
    gdelt = trim_by_days(gdelt_merged, keep_days=ROLLING_DAYS)

    print(f"USGS kept(rolling {ROLLING_DAYS}d): {len(usgs)}")
    print(f"GDACS kept({GDACS_KEEP_DAYS}d): {len(gdacs)}")
    print(f"GDELT kept(rolling {ROLLING_DAYS}d): {len(gdelt)}")

    # Save source layers
    save_geojson(os.path.join(DOCS_DATA_DIR, "usgs.geojson"), usgs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdacs.geojson"), gdacs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdelt.geojson"), gdelt)

    all_feats = gdelt + gdacs + usgs

    # Hotspots
    hotspot_geo, top_hotspots = build_hotspots_with_trend(all_feats, cell_deg=0.5, top_n=10)

    cache = load_cache()
    for h in top_hotspots:
        h["place"] = reverse_geocode_osm(float(h["lat"]), float(h["lon"]), cache)
    save_cache(cache)

    save_geojson(os.path.join(DOCS_DATA_DIR, "hotspots.geojson"), hotspot_geo)
    with open(os.path.join(DOCS_DATA_DIR, "hotspots.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_utc": to_utc_z(datetime.now(timezone.utc)), "top": top_hotspots}, f, ensure_ascii=False, indent=2)

    # Daily summary + weekly summary
    counts = {"usgs": len(usgs), "gdacs": len(gdacs), "gdelt": len(gdelt), "hotspot_cells": len(hotspot_geo)}
    summary = make_summary(all_feats, top_hotspots, counts)
    with open(os.path.join(DOCS_DATA_DIR, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    weekly = build_weekly(all_feats)
    with open(os.path.join(DOCS_DATA_DIR, "weekly.json"), "w", encoding="utf-8") as f:
        json.dump(weekly, f, ensure_ascii=False, indent=2)

    # EARLY WARNING output
    early_geo, early_top = build_early_warning(all_feats, cell_deg=0.5, lookback_days=7, recent_hours=48, top_n=10)

    cache = load_cache()
    for e in early_top:
        e["place"] = reverse_geocode_osm(float(e["lat"]), float(e["lon"]), cache)
    save_cache(cache)

    save_geojson(os.path.join(DOCS_DATA_DIR, "early.geojson"), early_geo)
    with open(os.path.join(DOCS_DATA_DIR, "early.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_utc": to_utc_z(datetime.now(timezone.utc)), "top": early_top}, f, ensure_ascii=False, indent=2)

    # meta
    now = datetime.now(timezone.utc)
    meta = {
        "generated_utc": to_utc_z(now),
        "counts": counts,
        "bbox": {"lon_min": BALKAN_BBOX[0], "lat_min": BALKAN_BBOX[1], "lon_max": BALKAN_BBOX[2], "lat_max": BALKAN_BBOX[3]},
        "rolling_days": ROLLING_DAYS,
        "early": {"cells": len(early_geo), "recent_hours": 48, "lookback_days": 7},
    }
    with open(os.path.join(DOCS_DATA_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

