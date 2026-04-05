#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import re
import time
import zipfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

import requests
from dateutil import parser as dateparser

# ============================================================
# Optional integration: risk snapshot builder
# ============================================================
try:
    from build_risk_snapshot import main as build_risk_snapshot_main
except Exception:
    build_risk_snapshot_main = None

# ============================================================
# PATHS
# ============================================================
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DATA_DIR = os.path.join(ROOT, "docs", "data")
CACHE_PATH = os.path.join(DOCS_DATA_DIR, "geocode_cache.json")
COUNTRIES_CACHE_PATH = os.path.join(DOCS_DATA_DIR, "balkan_countries.geojson")
TRUSTED_RSS_PATH = os.path.join(DOCS_DATA_DIR, "trusted_rss.json")
TRUSTED_RSS_SIGNALS_PATH = os.path.join(DOCS_DATA_DIR, "trusted_rss_signals.geojson")

USER_AGENT = "balkan-security-map/4.1 (github actions)"
TIMEOUT = 30

# ============================================================
# REGION
# ============================================================
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

WESTERN_BALKANS_COUNTRIES = [
    "Serbia",
    "Kosovo",
    "Bosnia and Herzegovina",
    "Montenegro",
    "North Macedonia",
    "Albania",
]

BALKAN_BBOX = (13.0, 37.0, 30.0, 47.5)

ROLLING_DAYS = 7
USGS_DAYS = 7
GDACS_DAYS = 14
GDACS_KEEP_DAYS = 7

GDELT_EXPORT_DAYS = 14
MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
MAX_SOURCES_PER_EVENT = 8

# ============================================================
# TRUSTED RSS
# ============================================================
MAX_RSS_ITEMS_PER_FEED = 50
MAX_RSS_OUTPUT_ITEMS = 150
RSS_SIGNAL_DAYS = 3

TRUSTED_RSS_FEEDS = [
    {
        "id": "euronews_news",
        "name": "Euronews",
        "url": "https://www.euronews.com/rss?level=theme&name=news",
        "weight": 0.92,
        "language": "en",
        "scope": ["world", "europe", "balkans", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "guardian_world",
        "name": "The Guardian",
        "url": "https://www.theguardian.com/world/rss",
        "weight": 0.95,
        "language": "en",
        "scope": ["world", "europe", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "guardian_europe",
        "name": "The Guardian Europe",
        "url": "https://www.theguardian.com/world/europe-news/rss",
        "weight": 0.95,
        "language": "en",
        "scope": ["europe", "balkans", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "dw_world",
        "name": "DW",
        "url": "https://rss.dw.com/rdf/rss-en-world",
        "weight": 0.92,
        "language": "en",
        "scope": ["world", "europe", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "dw_europe",
        "name": "DW Europe",
        "url": "https://rss.dw.com/rdf/rss-en-eu",
        "weight": 0.93,
        "language": "en",
        "scope": ["europe", "balkans", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "politico_eu",
        "name": "Politico Europe",
        "url": "https://www.politico.eu/feed/",
        "weight": 0.88,
        "language": "en",
        "scope": ["eu", "nato", "policy", "politics"],
        "trusted": True,
    },
    {
        "id": "cnn_world",
        "name": "CNN World",
        "url": "http://rss.cnn.com/rss/edition_world.rss",
        "weight": 0.80,
        "language": "en",
        "scope": ["world", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "reuters_world",
        "name": "Reuters World",
        "url": "https://feeds.reuters.com/reuters/worldNews",
        "weight": 0.98,
        "language": "en",
        "scope": ["world", "europe", "security", "politics", "energy"],
        "trusted": True,
    },
    {
        "id": "reuters_europe",
        "name": "Reuters Europe",
        "url": "https://feeds.reuters.com/Reuters/worldNews",
        "weight": 0.96,
        "language": "en",
        "scope": ["europe", "security", "politics", "energy"],
        "trusted": True,
    },
]

BALKAN_COUNTRY_KEYWORDS = {
    "Albania": ["albania", "tirana", "albanian"],
    "Bosnia and Herzegovina": ["bosnia", "sarajevo", "republika srpska", "bih", "bosnian"],
    "Bulgaria": ["bulgaria", "sofia", "bulgarian"],
    "Croatia": ["croatia", "zagreb", "croatian"],
    "Greece": ["greece", "athens", "greek", "aegean"],
    "Kosovo": ["kosovo", "pristina", "priština", "kosovar"],
    "Montenegro": ["montenegro", "podgorica", "montenegrin"],
    "North Macedonia": ["north macedonia", "skopje", "macedonia", "macedonian"],
    "Romania": ["romania", "bucharest", "romanian"],
    "Serbia": ["serbia", "belgrade", "serbian", "vučić", "vucic", "kanjiza", "kanjiža"],
    "Slovenia": ["slovenia", "ljubljana", "slovenian"],
    "Turkey": ["turkey", "ankara", "istanbul", "turkish"],
    "Moldova": ["moldova", "chisinau", "chișinău", "moldovan", "transnistria"],
    "Hungary": ["hungary", "budapest", "hungarian"],
}

COUNTRY_SIGNAL_COORDS = {
    "Albania": (19.82, 41.33),
    "Bosnia and Herzegovina": (18.41, 43.86),
    "Bulgaria": (23.32, 42.70),
    "Croatia": (15.98, 45.81),
    "Greece": (23.73, 37.98),
    "Kosovo": (21.16, 42.67),
    "Montenegro": (19.26, 42.43),
    "North Macedonia": (21.43, 41.99),
    "Romania": (26.10, 44.43),
    "Serbia": (20.46, 44.82),
    "Slovenia": (14.51, 46.05),
    "Turkey": (32.85, 39.93),
    "Moldova": (28.86, 47.01),
    "Hungary": (19.04, 47.50),
}

DIMENSION_KEYWORDS = {
    "political": [
        "election", "government", "parliament", "president", "prime minister",
        "coalition", "opposition", "vote", "ballot", "minister", "resign",
        "cabinet", "diplomacy", "sanction", "eu summit", "commission"
    ],
    "military": [
        "military", "army", "troops", "defence", "defense", "nato", "exercise",
        "drone", "airstrike", "weapon", "armed forces", "missile", "navy", "fighter jet"
    ],
    "policing": [
        "police", "arrest", "raid", "investigation", "court", "prosecutor",
        "corruption", "crime", "smuggling", "detention", "trial"
    ],
    "migration": [
        "migrant", "migration", "refugee", "asylum", "border crossing",
        "border", "smuggling route", "detention camp", "migrants"
    ],
    "social": [
        "protest", "strike", "demonstration", "riot", "student", "union",
        "teachers", "workers", "civil society", "rally"
    ],
    "infrastructure": [
        "energy", "pipeline", "grid", "electricity", "gas", "oil",
        "port", "rail", "bridge", "airport", "blackout", "infrastructure",
        "terminal", "power plant", "explosive", "sabotage"
    ],
}

EXCLUDE_KEYWORDS = [
    "sport", "football", "soccer", "tennis", "basketball", "celebrity", "fashion",
    "movie", "music", "entertainment", "lifestyle", "travel tips", "horoscope"
]

# ============================================================
# CATEGORY BUCKETS
# ============================================================
CATEGORY_BUCKETS = [
    ("protest", ["protest", "demonstration", "strike", "riot", "clash", "violence"]),
    ("border", ["border", "checkpoint", "incursion", "smuggling", "migration", "asylum"]),
    ("police", ["police", "arrest", "detained", "court", "raid", "prosecutor"]),
    ("military", ["military", "troops", "deployment", "exercise", "drill", "mobilization"]),
    ("drone", ["drone", "uav", "unmanned", "quadcop", "shahed"]),
    ("cyber", ["cyber", "ransomware", "ddos", "hack", "malware", "disinformation"]),
    ("energy", ["energy", "pipeline", "power outage", "electricity", "grid", "gas", "explosive", "sabotage"]),
    ("infrastructure", ["infrastructure", "rail", "bridge", "port", "airport"]),
    ("security_politics", ["security", "intelligence", "sanctions", "terror", "extremism"]),
]

CAMEO_ROOT_TO_CAT = {
    "18": "violence",
    "19": "violence",
    "20": "violence",
    "14": "police",
    "15": "police",
    "01": "security_politics",
    "02": "security_politics",
}

# ============================================================
# EARLY WARNING zones
# ============================================================
SENSITIVE_ZONES = [
    {"name": "Kosovo–Serbia", "bbox": (19.5, 42.0, 21.8, 43.6), "mult": 1.35},
    {"name": "Bosznia (entitásvonal tág)", "bbox": (16.0, 43.0, 19.8, 45.1), "mult": 1.20},
    {"name": "Görög–török Égei", "bbox": (24.0, 35.6, 28.9, 41.5), "mult": 1.25},
    {"name": "Moldova–Transznisztria", "bbox": (28.0, 46.0, 30.3, 48.2), "mult": 1.30},
]

# ============================================================
# Basics
# ============================================================
def ensure_dirs() -> None:
    os.makedirs(DOCS_DATA_DIR, exist_ok=True)

def http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> requests.Response:
    h = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if headers:
        h.update(headers)

    backoff = 2
    last_exc: Optional[Exception] = None
    non_retry_statuses = {400, 401, 403, 404}

    for attempt in range(1, 4):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code in non_retry_statuses:
                r.raise_for_status()
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"[http_get] retry {attempt}/3 status={r.status_code}")
                time.sleep(backoff)
                backoff *= 2
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if isinstance(e, requests.HTTPError):
                status = e.response.status_code if e.response is not None else None
                if status in non_retry_statuses:
                    raise
            print(f"[http_get] error retry {attempt}/3: {e}")
            time.sleep(backoff)
            backoff *= 2

    raise last_exc if last_exc else RuntimeError("http_get failed")

def http_get_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")

def http_get_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=120) as r:
        return r.read()

def in_bbox(lon: float, lat: float, bbox: Tuple[float, float, float, float]) -> bool:
    lon_min, lat_min, lon_max, lat_max = bbox
    return (lon_min <= lon <= lon_max) and (lat_min <= lat <= lat_max)

def to_feature(lon: float, lat: float, props: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": props}

def save_geojson(path: str, features: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, indent=2)

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

# ============================================================
# Time helpers
# ============================================================
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

def clamp_times(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for f in features:
        p = f.get("properties") or {}
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
            continue
        if dt >= cutoff:
            kept.append(f)
    return kept

# ============================================================
# Dedup
# ============================================================
def dedup_key(feature: Dict[str, Any]) -> Optional[str]:
    p = feature.get("properties") or {}
    src = p.get("source") or ""
    url = p.get("url")
    title = p.get("title")
    tm = p.get("time")
    kind = p.get("kind") or ""
    if url:
        return f"{src}|{url}"
    if title and tm:
        return f"{src}|{kind}|{tm}|{title}"
    return None

def merge_dedup(old_feats: List[Dict[str, Any]], new_feats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
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

    def sort_key(feat: Dict[str, Any]) -> float:
        dt = parse_time_iso((feat.get("properties") or {}).get("time"))
        return dt.timestamp() if dt else 0.0

    merged.sort(key=sort_key, reverse=True)
    return merged

# ============================================================
# Point-in-polygon (no shapely)
# ============================================================
def point_in_ring(lon: float, lat: float, ring: List[List[float]]) -> bool:
    inside = False
    n = len(ring)
    if n < 4:
        return False
    x, y = lon, lat
    for i in range(n - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        if ((y1 > y) != (y2 > y)):
            xinters = (x2 - x1) * (y - y1) / (y2 - y1 + 1e-15) + x1
            if x < xinters:
                inside = not inside
    return inside

def point_in_polygon(lon: float, lat: float, poly_coords: List[List[List[float]]]) -> bool:
    if not poly_coords:
        return False
    outer = poly_coords[0]
    if not point_in_ring(lon, lat, outer):
        return False
    for hole in poly_coords[1:]:
        if point_in_ring(lon, lat, hole):
            return False
    return True

def point_in_feature(lon: float, lat: float, geom: Dict[str, Any]) -> bool:
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if not coords:
        return False
    if gtype == "Polygon":
        return point_in_polygon(lon, lat, coords)
    if gtype == "MultiPolygon":
        for poly in coords:
            if point_in_polygon(lon, lat, poly):
                return True
        return False
    return False

def load_or_build_country_geoms() -> Dict[str, Dict[str, Any]]:
    need_refresh = True
    if os.path.exists(COUNTRIES_CACHE_PATH):
        mtime = datetime.fromtimestamp(os.path.getmtime(COUNTRIES_CACHE_PATH), tz=timezone.utc)
        if datetime.now(timezone.utc) - mtime < timedelta(days=7):
            need_refresh = False

    if need_refresh:
        url = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
        print("[countries] downloading world countries geojson...")
        data = http_get(url).json()
        keep = set(BALKAN_COUNTRIES)
        feats = []
        for f in (data.get("features") or []):
            props = f.get("properties") or {}
            name = props.get("name")
            if name in keep:
                feats.append(f)
        with open(COUNTRIES_CACHE_PATH, "w", encoding="utf-8") as fp:
            json.dump({"type": "FeatureCollection", "features": feats}, fp, ensure_ascii=False, indent=2)

    with open(COUNTRIES_CACHE_PATH, "r", encoding="utf-8") as fp:
        cached = json.load(fp) or {}

    geoms: Dict[str, Dict[str, Any]] = {}
    for f in (cached.get("features") or []):
        props = f.get("properties") or {}
        name = props.get("name")
        geom = f.get("geometry") or {}
        if name and geom:
            geoms[name] = geom
    return geoms

def in_balkan_countries(lon: float, lat: float, geoms: Dict[str, Dict[str, Any]]) -> bool:
    if not in_bbox(lon, lat, BALKAN_BBOX):
        return False
    for name in BALKAN_COUNTRIES:
        geom = geoms.get(name)
        if geom and point_in_feature(lon, lat, geom):
            return True
    return False

# ============================================================
# Borders layer
# ============================================================
def ensure_balkan_borders(geoms: Dict[str, Dict[str, Any]]) -> None:
    out_path = os.path.join(DOCS_DATA_DIR, "balkan_borders.geojson")
    feats = []
    for name in BALKAN_COUNTRIES:
        if name in geoms:
            feats.append({"type": "Feature", "properties": {"name": name}, "geometry": geoms[name]})
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": feats}, f, ensure_ascii=False, indent=2)

# ============================================================
# Reverse geocode cache
# ============================================================
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

# ============================================================
# TRUSTED RSS helpers
# ============================================================
@dataclass
class TrustedStory:
    story_id: str
    source_id: str
    source_name: str
    source_weight: float
    trusted: bool
    title: str
    summary: str
    url: str
    published_utc: Optional[str]
    fetched_utc: str
    country_hint: Optional[str]
    dimensions: List[str]
    scope: List[str]
    signal_score: float
    confidence_boost: float
    match_terms: List[str]

def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def rss_blob(title: str, summary: str) -> str:
    return f"{title} {summary}".lower().strip()

def parse_rss_datetime(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        try:
            dt = dateparser.parse(value)
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None

def rss_recency_score(published_utc: Optional[str]) -> float:
    if not published_utc:
        return 0.55
    try:
        dt = datetime.fromisoformat(published_utc.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        hours = max(0.0, (now - dt).total_seconds() / 3600.0)
        if hours <= 6:
            return 1.15
        if hours <= 12:
            return 1.0
        if hours <= 24:
            return 0.92
        if hours <= 48:
            return 0.82
        if hours <= 72:
            return 0.72
        if hours <= 168:
            return 0.58
        return 0.42
    except Exception:
        return 0.55

def infer_country_from_text(blob: str) -> Tuple[Optional[str], List[str]]:
    matches: List[Tuple[str, str]] = []
    for country, keywords in BALKAN_COUNTRY_KEYWORDS.items():
        for kw in keywords:
            if kw in blob:
                matches.append((country, kw))
    if not matches:
        return None, []
    counts: Dict[str, int] = {}
    used_terms: List[str] = []
    for country, kw in matches:
        counts[country] = counts.get(country, 0) + 1
        used_terms.append(kw)
    best = sorted(counts.items(), key=lambda x: x[1], reverse=True)[0][0]
    return best, sorted(set(used_terms))

def infer_dimensions_from_text(blob: str) -> Tuple[List[str], List[str]]:
    dims: List[str] = []
    terms: List[str] = []
    for dim, keywords in DIMENSION_KEYWORDS.items():
        hit = False
        for kw in keywords:
            if kw in blob:
                hit = True
                terms.append(kw)
        if hit:
            dims.append(dim)
    if not dims:
        dims = ["political"]
    return dims, sorted(set(terms))

def should_exclude_rss(blob: str) -> bool:
    return any(kw in blob for kw in EXCLUDE_KEYWORDS)

def make_story_id(url: str, title: str) -> str:
    raw = f"{url}|{title}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:16]

def parse_rss_xml(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    items: List[Dict[str, Any]] = []

    for elem in root.findall(".//item"):
        items.append({
            "title": elem.findtext("title"),
            "link": elem.findtext("link"),
            "description": elem.findtext("description"),
            "summary": elem.findtext("{http://purl.org/rss/1.0/modules/content/}encoded"),
            "pubDate": elem.findtext("pubDate"),
        })

    if items:
        return items

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    for entry in root.findall(".//atom:entry", ns):
        link = ""
        for link_el in entry.findall("atom:link", ns):
            href = link_el.attrib.get("href")
            if href:
                link = href
                break

        summary = entry.findtext("atom:summary", default="", namespaces=ns)
        content = entry.findtext("atom:content", default="", namespaces=ns)

        items.append({
            "title": entry.findtext("atom:title", default="", namespaces=ns),
            "link": link,
            "description": summary,
            "summary": content,
            "published": entry.findtext("atom:updated", default="", namespaces=ns),
        })

    return items

def normalize_rss_item(feed: Dict[str, Any], item: Dict[str, Any]) -> Optional[TrustedStory]:
    title = strip_html(item.get("title", ""))
    summary = strip_html(item.get("summary", "") or item.get("description", ""))
    url = (item.get("link") or "").strip()

    if not title or not url:
        return None

    blob = rss_blob(title, summary)
    if should_exclude_rss(blob):
        return None

    country_hint, country_terms = infer_country_from_text(blob)
    dims, dim_terms = infer_dimensions_from_text(blob)

    if country_hint is None and not any(s in feed.get("scope", []) for s in ("balkans", "europe", "eu", "security", "politics")):
        return None

    published_utc = parse_rss_datetime(item.get("pubDate") or item.get("published"))
    recency = rss_recency_score(published_utc)

    source_weight = float(feed["weight"])
    dimension_factor = min(1.18, 0.85 + 0.08 * len(dims))
    country_factor = 1.15 if country_hint else 0.78

    signal_score = round(source_weight * recency * dimension_factor * country_factor, 4)
    confidence_boost = round(min(0.45, 0.14 + source_weight * 0.18 + (0.07 if country_hint else 0.0)), 4)

    return TrustedStory(
        story_id=make_story_id(url, title),
        source_id=feed["id"],
        source_name=feed["name"],
        source_weight=source_weight,
        trusted=bool(feed.get("trusted", True)),
        title=title,
        summary=summary[:500],
        url=url,
        published_utc=published_utc,
        fetched_utc=datetime.now(timezone.utc).isoformat(),
        country_hint=country_hint,
        dimensions=dims,
        scope=list(feed.get("scope", [])),
        signal_score=signal_score,
        confidence_boost=confidence_boost,
        match_terms=sorted(set(country_terms + dim_terms)),
    )

def fetch_rss_feed(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read()

def dedupe_trusted_stories(stories: List[TrustedStory]) -> List[TrustedStory]:
    seen_urls: set[str] = set()
    seen_title_keys: set[str] = set()
    out: List[TrustedStory] = []

    for story in stories:
        url_key = story.url.strip().lower()
        title_key = re.sub(r"\W+", "", story.title.lower())

        if url_key in seen_urls:
            continue
        if title_key in seen_title_keys:
            continue

        seen_urls.add(url_key)
        seen_title_keys.add(title_key)
        out.append(story)

    return out

def build_trusted_rss_output(stories: List[TrustedStory], errors: List[Dict[str, str]]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    by_country: Dict[str, int] = {}
    by_source: Dict[str, int] = {}

    for s in stories:
        if s.country_hint:
            by_country[s.country_hint] = by_country.get(s.country_hint, 0) + 1
        by_source[s.source_name] = by_source.get(s.source_name, 0) + 1

    return {
        "generated_utc": now,
        "count": len(stories),
        "stories": [asdict(s) for s in stories],
        "summary": {
            "top_countries": sorted(by_country.items(), key=lambda x: x[1], reverse=True)[:10],
            "top_sources": sorted(by_source.items(), key=lambda x: x[1], reverse=True),
        },
        "errors": errors,
    }

def fetch_trusted_rss() -> Dict[str, Any]:
    all_stories: List[TrustedStory] = []
    errors: List[Dict[str, str]] = []

    for feed in TRUSTED_RSS_FEEDS:
        try:
            xml_bytes = fetch_rss_feed(feed["url"])
            items = parse_rss_xml(xml_bytes)[:MAX_RSS_ITEMS_PER_FEED]
            for item in items:
                story = normalize_rss_item(feed, item)
                if story:
                    all_stories.append(story)
        except Exception as exc:
            errors.append({
                "feed_id": feed["id"],
                "feed_name": feed["name"],
                "error": str(exc),
            })

    all_stories = dedupe_trusted_stories(all_stories)
    all_stories.sort(
        key=lambda s: (
            s.country_hint is None,
            -(s.signal_score or 0.0),
            s.published_utc or "",
        )
    )
    all_stories = all_stories[:MAX_RSS_OUTPUT_ITEMS]

    return build_trusted_rss_output(all_stories, errors)

def save_trusted_rss(payload: Dict[str, Any]) -> None:
    with open(TRUSTED_RSS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def rss_story_to_feature(story: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    country = story.get("country_hint")
    if not country or country not in COUNTRY_SIGNAL_COORDS:
        return None

    dt = parse_time_iso(story.get("published_utc") or story.get("fetched_utc"))
    if dt is None:
        dt = datetime.now(timezone.utc)

    lon, lat = COUNTRY_SIGNAL_COORDS[country]
    dims = story.get("dimensions") or []
    category = dims[0] if dims else "political"
    title = (story.get("title") or "")[:220]

    score = float(story.get("signal_score") or 0.0)
    confidence = float(story.get("confidence_boost") or 0.0)

    return to_feature(
        lon,
        lat,
        {
            "source": "RSS",
            "kind": "trusted_rss_signal",
            "type": "NewsSignal",
            "time": to_utc_z(dt),
            "date": to_utc_z(dt)[:10],
            "title": title,
            "location": country,
            "country": country,
            "category": category,
            "dimensions": dims,
            "signal_score": round(score, 4),
            "confidence_boost": round(confidence, 4),
            "sources_count": 1,
            "sources": [story.get("url")] if story.get("url") else [],
            "url": story.get("url"),
            "source_name": story.get("source_name"),
            "anchor": "country_capital",
        },
    )

def build_trusted_rss_signal_features(payload: Optional[Dict[str, Any]], keep_days: int = RSS_SIGNAL_DAYS) -> List[Dict[str, Any]]:
    stories = (payload or {}).get("stories") or []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=keep_days)

    feats: List[Dict[str, Any]] = []
    for story in stories:
        dt = parse_time_iso(story.get("published_utc") or story.get("fetched_utc"))
        if dt is not None and dt < cutoff:
            continue
        feat = rss_story_to_feature(story)
        if feat:
            feats.append(feat)

    feats.sort(key=lambda f: parse_time_iso((f.get("properties") or {}).get("time")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return feats

# ============================================================
# Sources: USGS / GDACS
# ============================================================
def fetch_usgs(geoms: Dict[str, Dict[str, Any]], days: int = 7, min_magnitude: float = 2.5) -> List[Dict[str, Any]]:
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
    for f in data.get("features", []) or []:
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        if not in_balkan_countries(lon, lat, geoms):
            continue

        p = f.get("properties") or {}
        t_ms = p.get("time")
        dt = None
        if isinstance(t_ms, (int, float)):
            dt = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc)

        out.append(
            to_feature(
                lon,
                lat,
                {
                    "source": "USGS",
                    "kind": "earthquake",
                    "category": "natural",
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

def fetch_gdacs(geoms: Dict[str, Dict[str, Any]], days: int = 14) -> List[Dict[str, Any]]:
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

        if not in_balkan_countries(lon, lat, geoms):
            continue

        out.append(
            to_feature(
                lon,
                lat,
                {
                    "source": "GDACS",
                    "kind": "disaster_alert",
                    "category": "natural",
                    "title": title,
                    "time": to_utc_z(pub_dt),
                    "url": link,
                    "type": "Alert",
                },
            )
        )
    return out

# ============================================================
# GDELT GEO disabled intentionally
# ============================================================
def fetch_gdelt_geo_disabled() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return [], {
        "generated_utc": to_utc_z(datetime.now(timezone.utc)),
        "api": "geo",
        "enabled": False,
        "reason": "disabled_for_stability",
        "returned_total": 0,
        "runs": [],
    }

# ============================================================
# GDELT EXPORT (linked events)
# ============================================================
def parse_masterfilelist(master_text: str) -> List[str]:
    urls = []
    for line in master_text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[2].strip()
        if url.startswith("https://data.gdeltproject.org/"):
            url = "http://data.gdeltproject.org/" + url[len("https://data.gdeltproject.org/"):]
        if url.endswith(".export.CSV.zip") and "/gdeltv2/" in url:
            urls.append(url)
    return urls

def extract_timestamp_from_url(url: str) -> Optional[datetime]:
    base = url.split("/")[-1]
    ts = base.split(".")[0]
    if len(ts) != 14 or not ts.isdigit():
        return None
    return datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

def yyyymmdd_to_iso(s: str) -> str:
    if not s or len(s) != 8:
        return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def safe_float(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def norm_loc(s: str) -> str:
    if not s:
        return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst: List[str], url: str) -> None:
    if not url:
        return
    if url not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(url)

def classify_from_text(text: str) -> Optional[str]:
    t = (text or "").lower()
    for bucket_name, kw in CATEGORY_BUCKETS:
        for k in kw:
            if k.lower() in t:
                return bucket_name
    return None

def fetch_gdelt_export_linked(geoms: Dict[str, Dict[str, Any]], lookback_days: int = 14) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=lookback_days)

    master = http_get_text(MASTERFILELIST_URL)
    urls = parse_masterfilelist(master)

    recent = []
    for u in urls:
        ts = extract_timestamp_from_url(u)
        if ts and ts >= cutoff:
            recent.append((ts, u))
    recent.sort(key=lambda x: x[0])

    if not recent:
        return []

    live_agg: Dict[str, Dict[str, Any]] = {}

    for ts, url in recent:
        try:
            zbytes = http_get_bytes(url)
            zf = zipfile.ZipFile(io.BytesIO(zbytes))
            name = zf.namelist()[0]
            raw = zf.read(name).decode("utf-8", errors="replace")
        except Exception:
            continue

        reader = csv.reader(io.StringIO(raw), delimiter="\t")
        for row in reader:
            if len(row) < 61:
                continue

            gid = str(row[0]).strip()
            day = str(row[1]).strip()
            event_code = str(row[26]).strip()
            root = str(row[28]).strip()
            fullname = str(row[52]).strip()
            lat = safe_float(str(row[56]).strip())
            lon = safe_float(str(row[57]).strip())
            sourceurl = str(row[60]).strip()

            if lat is None or lon is None:
                continue
            if not in_balkan_countries(lon, lat, geoms):
                continue

            date_iso = yyyymmdd_to_iso(day)
            if not date_iso:
                continue

            category = CAMEO_ROOT_TO_CAT.get(root)
            if category is None:
                category = classify_from_text(fullname) or "other"

            loc_norm = norm_loc(fullname)
            key = f"{date_iso}|{category}|{loc_norm}"

            if key not in live_agg:
                live_agg[key] = {
                    "date": date_iso,
                    "time": f"{date_iso}T00:00:00Z",
                    "category": category,
                    "event_root_code": root,
                    "event_codes": set([event_code]) if event_code else set(),
                    "gdelt_ids": set([gid]) if gid else set(),
                    "location": fullname or "unknown",
                    "loc_norm": loc_norm,
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "sources": [sourceurl] if sourceurl else [],
                }
            else:
                ev = live_agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                if fullname and ev["location"] == "unknown":
                    ev["location"] = fullname
                if event_code:
                    ev["event_codes"].add(event_code)
                if gid:
                    ev["gdelt_ids"].add(gid)
                add_unique(ev["sources"], sourceurl)

    live_features: List[Dict[str, Any]] = []
    for ev in live_agg.values():
        lat = ev["lat_sum"] / max(1, ev["n"])
        lon = ev["lon_sum"] / max(1, ev["n"])
        live_features.append(
            to_feature(
                lon,
                lat,
                {
                    "source": "GDELT",
                    "kind": "news_linked",
                    "type": "News",
                    "time": ev["time"],
                    "date": ev["date"],
                    "title": ev["location"],
                    "location": ev["location"],
                    "category": ev["category"],
                    "event_root_code": ev["event_root_code"],
                    "event_codes": sorted([c for c in ev["event_codes"] if c]),
                    "gdelt_ids_count": len(ev["gdelt_ids"]),
                    "sources_count": len(ev["sources"]),
                    "sources": ev["sources"],
                    "url": ev["sources"][0] if ev["sources"] else None,
                },
            )
        )

    live_features.sort(
        key=lambda f: (f.get("properties", {}).get("date", ""), f.get("properties", {}).get("sources_count", 0)),
        reverse=True,
    )
    return live_features

# ============================================================
# Scoring + hotspots + trend
# ============================================================
def score_feature(props: Dict[str, Any]) -> float:
    src = props.get("source")
    kind = props.get("kind")
    if src == "RSS" and kind == "trusted_rss_signal":
        try:
            signal = float(props.get("signal_score") or 0.0)
            conf = float(props.get("confidence_boost") or 0.0)
        except Exception:
            signal = 0.0
            conf = 0.0
        return min(2.2, 0.9 + signal * 0.75 + conf)
    if src == "GDELT" and kind in ("news_linked",):
        return 1.3
    if src == "GDELT" and kind in ("news_geo", "news_event"):
        return 1.0
    if src == "GDACS":
        return 0.6
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

def build_hotspots_with_trend(all_features: List[Dict[str, Any]], cell_deg: float = 0.5, top_n: int = 10):
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
                "sources": {"GDELT": 0, "USGS": 0, "GDACS": 0, "RSS": 0},
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

# ============================================================
# EARLY WARNING
# ============================================================
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
    return [
        (x - 1, y - 1),
        (x, y - 1),
        (x + 1, y - 1),
        (x - 1, y),
        (x + 1, y),
        (x - 1, y + 1),
        (x, y + 1),
        (x + 1, y + 1),
    ]

def build_early_warning(all_features: List[Dict[str, Any]], cell_deg: float = 0.5, lookback_days: int = 7, recent_hours: int = 48, top_n: int = 10):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=lookback_days)
    recent_cut = now - timedelta(hours=recent_hours)

    acc: Dict[Tuple[int, int], Dict[str, Any]] = {}

    def get_bucket(k: Tuple[int, int]) -> Dict[str, Any]:
        b = acc.get(k)
        if b is None:
            b = {"recent": 0.0, "baseline": 0.0, "src_recent": {"GDELT": 0, "USGS": 0, "GDACS": 0, "RSS": 0}}
            acc[k] = b
        return b

    for f in all_features:
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        props = f.get("properties") or {}
        dt = parse_time_iso(props.get("time"))
        if dt is None or dt < cutoff:
            continue

        s = score_feature(props)
        k = grid_key(lon, lat, cell_deg)
        b = get_bucket(k)

        src = props.get("source")
        if dt >= recent_cut:
            b["recent"] += s
            if src in b["src_recent"]:
                b["src_recent"][src] += 1
        else:
            b["baseline"] += s

    raw: Dict[Tuple[int, int], float] = {}
    meta: Dict[Tuple[int, int], Dict[str, Any]] = {}

    for k, b in acc.items():
        lon_c, lat_c = cell_center(k[0], k[1], cell_deg)
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

# ============================================================
# Weekly topics
# ============================================================
STOP = {
    "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with", "as", "at", "by", "from",
    "is", "are", "was", "were", "be", "been", "it", "this", "that", "these", "those",
    "over", "after", "before", "into", "about", "amid", "during", "near",
    "says", "say", "new", "up", "down",
    "serbia", "kosovo", "greece", "turkey", "romania", "bulgaria", "croatia", "albania",
    "bosnia", "herzegovina", "montenegro", "slovenia", "moldova", "hungary", "macedonia",
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
    place = top.get("place") or "ismeretlen térség"

    if arrow == "🆕":
        return {"level": "info", "title": "Új góc", "text": f"Új hotspot jelent meg: {place}. Érdemes követni 24–72 órában."}
    if arrow == "🔺":
        if ch is not None and ch >= 25:
            return {"level": "high", "title": "Emelkedő feszültség", "text": f"Erősödő hotspot: {place} (+{ch:.0f}%)."}
        return {"level": "medium", "title": "Emelkedő feszültség", "text": f"Felfutó jelzések: {place}."}
    return None

def make_summary(
    all_features: List[Dict[str, Any]],
    top_hotspots: List[Dict[str, Any]],
    counts: Dict[str, int],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff_7 = now - timedelta(days=7)
    cutoff_14 = now - timedelta(days=14)

    last7, prev7 = [], []
    for f in all_features:
        dt = parse_time_iso((f.get("properties") or {}).get("time"))
        if not dt:
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
        chv = top.get("change_pct")
        ch_txt = "n/a" if chv is None else f"{chv:+.0f}%"
        top_text = (
            f"Legerősebb góc: {place} {arrow} "
            f"(rácspont {top['lat']:.2f}, {top['lon']:.2f}; score {float(top['score']):.2f}; 7 napos változás: {ch_txt})."
        )
    else:
        top_text = "Legerősebb góc: jelenleg nincs elég geokódolt jelzés a térképes kiemeléshez."

    rss_count = 0
    if trusted_rss_payload:
        rss_count = int(trusted_rss_payload.get("count") or 0)

    bullets = [
        top_text,
        trend_text,
        f"Forráskép: GDELT {counts.get('gdelt', 0)} + linked {counts.get('gdelt_linked', 0)}, USGS {counts.get('usgs', 0)}, GDACS {counts.get('gdacs', 0)}, RSS-jelek {counts.get('rss_signals', 0)}.",
    ]
    if rss_count:
        bullets.append(f"Trusted RSS forrásokból releváns sajtóanyag: {rss_count} db.")
    bullets.append("Megjegyzés: automatikus OSINT-kivonat; a linkelt források kézi ellenőrzése javasolt.")

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
        "rss_count": rss_count,
    }

# ============================================================
# Weekly brief helpers
# ============================================================
def collect_week_window(all_features: List[Dict[str, Any]], days: int = 7) -> List[Tuple[datetime, Dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    out: List[Tuple[datetime, Dict[str, Any]]] = []
    for f in all_features:
        p = f.get("properties") or {}
        dt = parse_time_iso(p.get("time"))
        if dt and dt >= cutoff:
            out.append((dt, f))
    return out

def country_signal_score_from_features(week_items: List[Tuple[datetime, Dict[str, Any]]], country: str) -> float:
    keywords = [x.lower() for x in BALKAN_COUNTRY_KEYWORDS.get(country, [])]
    score = 0.0
    for _, f in week_items:
        p = f.get("properties") or {}
        text = " ".join([
            str(p.get("title") or ""),
            str(p.get("location") or ""),
            str(p.get("place") or ""),
            str(p.get("country") or ""),
        ]).lower()
        if any(kw in text for kw in keywords):
            score += score_feature(p)
    return round(score, 2)

def country_signal_score_from_rss(stories: List[Dict[str, Any]], country: str) -> float:
    score = 0.0
    for s in stories:
        if (s.get("country_hint") or "") == country:
            score += float(s.get("signal_score") or 0.0)
    return round(score, 2)

def get_country_scores(
    week_items: List[Tuple[datetime, Dict[str, Any]]],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, float]]:
    stories = (trusted_rss_payload or {}).get("stories") or []
    out: Dict[str, Dict[str, float]] = {}
    for country in WESTERN_BALKANS_COUNTRIES:
        feat_score = country_signal_score_from_features(week_items, country)
        rss_score = country_signal_score_from_rss(stories, country)
        out[country] = {
            "feature_score": feat_score,
            "rss_score": rss_score,
            "total": round(feat_score + rss_score, 2),
        }
    return out

def dominant_dimensions(stories: List[Dict[str, Any]], country: Optional[str] = None) -> List[str]:
    c = Counter()
    for s in stories:
        if country and (s.get("country_hint") or "") != country:
            continue
        for d in (s.get("dimensions") or []):
            c[d] += 1
    return [name for name, _ in c.most_common(3)]

def overall_status_label(
    counts_upper: Dict[str, int],
    top_hotspots: List[Dict[str, Any]],
    early_top: List[Dict[str, Any]],
) -> str:
    pressure = 0.0
    pressure += counts_upper.get("GDELT", 0) * 0.01
    pressure += counts_upper.get("USGS", 0) * 0.2
    pressure += counts_upper.get("GDACS", 0) * 0.6
    pressure += counts_upper.get("RSS", 0) * 0.08

    if top_hotspots:
        pressure += float(top_hotspots[0].get("score") or 0.0)
        if top_hotspots[0].get("trend_arrow") == "🔺":
            pressure += 2.5
        elif top_hotspots[0].get("trend_arrow") == "🆕":
            pressure += 1.8

    if early_top:
        pressure += float(early_top[0].get("escalation") or 0.0) / 25.0

    if pressure >= 12:
        return "fokozódó feszültségekkel terhelt"
    if pressure >= 7:
        return "mérsékelten romló"
    return "alapvetően stabil, de törékeny"

def main_drivers_label(
    country_scores: Dict[str, Dict[str, float]],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> List[str]:
    stories = (trusted_rss_payload or {}).get("stories") or []
    dims = dominant_dimensions(stories)
    drivers: List[str] = []

    if "political" in dims:
        drivers.append("politikai instabilitás")
    if "social" in dims:
        drivers.append("társadalmi feszültségek")
    if "migration" in dims or "policing" in dims:
        drivers.append("határbiztonsági és rendészeti nyomás")
    if "military" in dims:
        drivers.append("biztonságpolitikai és katonai érzékenységek")
    if "infrastructure" in dims:
        drivers.append("kritikus infrastruktúrához kapcsolódó sérülékenységek")

    serbia = country_scores.get("Serbia", {}).get("total", 0.0)
    kosovo = country_scores.get("Kosovo", {}).get("total", 0.0)
    bosnia = country_scores.get("Bosnia and Herzegovina", {}).get("total", 0.0)

    if (serbia + kosovo) >= 4.5:
        drivers.append("a szerb–koszovói viszony tartós bizonytalansága")
    if bosnia >= 2.5:
        drivers.append("az etnopolitikai törésvonalak fennmaradása")
    if not drivers:
        drivers.append("politikai instabilitás")
        drivers.append("külső befolyási kísérletek")

    uniq = []
    for d in drivers:
        if d not in uniq:
            uniq.append(d)
    return uniq[:3]

def determine_no_major_shift(top_hotspots: List[Dict[str, Any]], early_top: List[Dict[str, Any]]) -> str:
    if early_top and float(early_top[0].get("escalation") or 0.0) >= 70:
        return "ugyanakkor lokális incidensek és eszkalációs kockázatok megfigyelhetők voltak"
    if top_hotspots and top_hotspots[0].get("trend_arrow") == "🔺":
        return "ugyanakkor lokális incidensek és retorikai eszkaláció megfigyelhető volt"
    return "ugyanakkor a feszültségek több ponton továbbra is fennmaradtak"

def intro_paragraph(
    counts_upper: Dict[str, int],
    top_hotspots: List[Dict[str, Any]],
    early_top: List[Dict[str, Any]],
    country_scores: Dict[str, Dict[str, float]],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> str:
    status = overall_status_label(counts_upper, top_hotspots, early_top)
    drivers = main_drivers_label(country_scores, trusted_rss_payload)
    no_shift = determine_no_major_shift(top_hotspots, early_top)

    return (
        f"A Nyugat-Balkán biztonsági helyzete az elmúlt héten összességében {status} képet mutatott. "
        f"A térségben zajló folyamatokat továbbra is {', '.join(drivers)} határozzák meg. "
        f"A vizsgált időszakban nem történt olyan esemény, amely alapjaiban változtatta volna meg a régió biztonsági dinamikáját, "
        f"{no_shift}."
    )

def country_tone_from_score(score: float) -> str:
    if score >= 3.8:
        return "romló"
    if score >= 2.0:
        return "stagnáló"
    return "óvatosan javuló"

def serbia_section(country_scores: Dict[str, Dict[str, float]], trusted_rss_payload: Optional[Dict[str, Any]] = None) -> str:
    score = country_scores.get("Serbia", {}).get("total", 0.0)
    kos_score = country_scores.get("Kosovo", {}).get("total", 0.0)
    tone = "erősödő nacionalista hangvételt" if score >= 2.8 else "mérséklődő retorikát"

    if (score + kos_score) >= 5.0:
        extra = " A koszovói kérdés továbbra is a legfontosabb biztonságpolitikai tényezőként jelenik meg."
    else:
        extra = " A koszovói kérdés továbbra is meghatározó referencia-pont a szerb biztonságpolitikai diskurzusban."

    return (
        "Szerbia esetében a belpolitikai folyamatok továbbra is jelentős hatást gyakorolnak a biztonsági környezetre. "
        f"A kormányzati kommunikációban megjelenő narratívák {tone} tükröznek."
        f"{extra}"
    )

def kosovo_section(country_scores: Dict[str, Dict[str, float]], top_hotspots: List[Dict[str, Any]]) -> str:
    serbia = country_scores.get("Serbia", {}).get("total", 0.0)
    kosovo = country_scores.get("Kosovo", {}).get("total", 0.0)
    trend = country_tone_from_score((serbia + kosovo) / 2.0)

    north_risk = "Az északi régióban fennálló feszültségek továbbra is potenciális eszkalációs kockázatot hordoznak."
    if top_hotspots and "Kosovo" in str(top_hotspots[0].get("place") or ""):
        north_risk = "Az északi régióhoz kapcsolódó jelzések a héten is potenciális eszkalációs kockázatot jeleztek."

    return (
        f"Koszovó és Szerbia viszonya a héten {trend} tendenciát mutatott. "
        f"{north_risk} "
        "A nemzetközi jelenlét stabilizáló szerepe továbbra is meghatározó."
    )

def bosnia_section(country_scores: Dict[str, Dict[str, float]]) -> str:
    score = country_scores.get("Bosnia and Herzegovina", {}).get("total", 0.0)
    middle = (
        "A Republika Srpska vezetésének lépései fokozzák a politikai bizonytalanságot."
        if score >= 2.3
        else "Az entitások közötti politikai bizalmatlanság továbbra is magas szinten maradt."
    )
    return (
        "Bosznia-Hercegovina esetében az etnopolitikai törésvonalak változatlanul a biztonsági helyzet központi elemét képezik. "
        f"{middle} "
        "Az államszintű intézmények működése továbbra is korlátozott hatékonyságot mutat."
    )

def montenegro_section(country_scores: Dict[str, Dict[str, float]]) -> str:
    score = country_scores.get("Montenegro", {}).get("total", 0.0)
    tone = (
        "A belpolitikai törésvonalak és az intézményi törékenység mérsékelten növelték a bizonytalanságot."
        if score >= 1.6
        else "A biztonsági helyzet alapvetően stabil maradt, ugyanakkor a belpolitikai polarizáció továbbra is jelen van."
    )
    return (
        "Montenegróban a biztonsági környezetet elsősorban a belpolitikai stabilitás és az intézményi működés minősége befolyásolja. "
        f"{tone}"
    )

def north_macedonia_section(country_scores: Dict[str, Dict[str, float]]) -> str:
    score = country_scores.get("North Macedonia", {}).get("total", 0.0)
    tone = (
        "A politikai napirendet meghatározó kérdések a társadalmi polarizáció fenntartásához járultak hozzá."
        if score >= 1.6
        else "Jelentős biztonsági eszkaláció nem volt megfigyelhető, de a politikai érzékenységek fennmaradtak."
    )
    return (
        "Észak-Macedónia esetében a belpolitikai stabilitás és az euroatlanti orientáció továbbra is meghatározó keret maradt. "
        f"{tone}"
    )

def albania_section(country_scores: Dict[str, Dict[str, float]]) -> str:
    score = country_scores.get("Albania", {}).get("total", 0.0)
    tone = (
        "A belpolitikai versengés és a kormányzati–ellenzéki dinamika mérsékelten növelte a politikai zajt."
        if score >= 1.6
        else "A biztonsági helyzet alapvetően kezelhető maradt, komolyabb destabilizáló incidens nélkül."
    )
    return (
        "Albániában a biztonsági környezetet elsősorban a belpolitikai folyamatok és a regionális diplomáciai pozicionálás befolyásolta. "
        f"{tone}"
    )

def build_country_assessments(
    country_scores: Dict[str, Dict[str, float]],
    top_hotspots: List[Dict[str, Any]],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    return [
        {"country": "Szerbia", "code": "RS", "text": serbia_section(country_scores, trusted_rss_payload)},
        {"country": "Koszovó", "code": "XK", "text": kosovo_section(country_scores, top_hotspots)},
        {"country": "Bosznia-Hercegovina", "code": "BA", "text": bosnia_section(country_scores)},
        {"country": "Montenegró", "code": "ME", "text": montenegro_section(country_scores)},
        {"country": "Észak-Macedónia", "code": "MK", "text": north_macedonia_section(country_scores)},
        {"country": "Albánia", "code": "AL", "text": albania_section(country_scores)},
    ]

def external_actors_paragraph(trusted_rss_payload: Optional[Dict[str, Any]] = None) -> str:
    stories = (trusted_rss_payload or {}).get("stories") or []
    blob = " ".join([(s.get("title") or "") + " " + (s.get("summary") or "") for s in stories]).lower()

    russia = "Oroszország"
    china = "Kína"
    if "russia" in blob or "moscow" in blob:
        russia = "Oroszország"
    if "china" in blob or "beijing" in blob:
        china = "Kína"

    return (
        f"A térségben aktív külső szereplők közül {russia} és {china} befolyása továbbra is érzékelhető. "
        "Az Európai Unió és a NATO stabilizáló szerepe fennmaradt. "
        "A geopolitikai versengés a Balkánon elsősorban politikai, gazdasági és információs eszközökön keresztül zajlik."
    )

def risk_paragraph(
    top_hotspots: List[Dict[str, Any]],
    early_top: List[Dict[str, Any]],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> str:
    high_ew = bool(early_top and float(early_top[0].get("escalation") or 0.0) >= 70)
    hotspot_up = bool(top_hotspots and top_hotspots[0].get("trend_arrow") == "🔺")

    first = (
        "A jelenlegi folyamatok rövid távon nem utalnak fegyveres konfliktus közvetlen kockázatára, ugyanakkor több ponton emelkedő eszkalációs nyomás érzékelhető."
        if (high_ew or hotspot_up)
        else "A jelenlegi folyamatok rövid távon nem utalnak fegyveres konfliktus közvetlen kockázatára, azonban a feszültségek fennmaradása strukturális instabilitást jelez."
    )

    return (
        f"{first} "
        "A legfőbb kockázatot továbbra is az alacsony intenzitású, de tartós politikai krízisek jelentik. "
        "A dezinformációs tevékenység és a polarizált médiakörnyezet növeli a társadalmi feszültségeket."
    )

def forecast_paragraph(
    country_scores: Dict[str, Dict[str, float]],
    top_hotspots: List[Dict[str, Any]],
    trusted_rss_payload: Optional[Dict[str, Any]] = None,
) -> str:
    serbia = country_scores.get("Serbia", {}).get("total", 0.0)
    kosovo = country_scores.get("Kosovo", {}).get("total", 0.0)
    bosnia = country_scores.get("Bosnia and Herzegovina", {}).get("total", 0.0)

    if serbia + kosovo >= bosnia and serbia + kosovo >= 3.8:
        key_issue = "szerb–koszovói párbeszéd"
    elif bosnia >= 2.3:
        key_issue = "boszniai belpolitikai stabilitás"
    else:
        key_issue = "EU-integrációs folyamat"

    if top_hotspots and top_hotspots[0].get("trend_arrow") == "🔺":
        start = "Rövid távon nem várható jelentős változás a biztonsági helyzetben, azonban a jelenlegi trendek fennmaradása esetén fokozatos romlás valószínűsíthető."
    else:
        start = "Rövid távon nem várható jelentős változás a biztonsági helyzetben, ugyanakkor több törékeny politikai és társadalmi dinamika továbbra is fennmarad."

    return (
        f"{start} "
        f"A következő időszak kulcskérdése a {key_issue} alakulása lesz. "
        "A nemzetközi közösség szerepvállalása továbbra is elengedhetetlen a status quo fenntartásához."
    )

def closing_paragraph() -> str:
    return (
        "Összességében a Nyugat-Balkán biztonsági helyzete továbbra is törékeny stabilitást mutat. "
        "A térségben fennálló konfliktuspotenciál kezelhető, azonban hosszú távon csak strukturális politikai megoldásokkal mérsékelhető."
    )

def methodology_paragraph() -> str:
    return (
        "A heti brief nyílt forrású információk strukturált feldolgozásán alapul. "
        "A rendszer GDELT eseményalapú híradatokat, trusted RSS sajtóforrásokat, valamint USGS és GDACS jelzéseket integrál. "
        "Az események időbeli súlyozással, forrásalapú pontozással és térbeli hotspot-azonosítással kerülnek értékelésre. "
        "Az országos helyzetértékelés automatizált, indikátor-alapú szöveggenerálással készül, ezért a kimenet tájékoztató jellegű; "
        "a kiemelt állítások esetében minden esetben javasolt a források manuális ellenőrzése és elemzői validálása."
    )

def htmlify_paragraphs(paragraphs: List[str]) -> str:
    return "\n".join([f"<p>{p}</p>" for p in paragraphs if p.strip()])

def build_weekly(all_features: List[Dict[str, Any]], trusted_rss_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    week = collect_week_window(all_features, days=7)

    counts_upper = {"GDELT": 0, "USGS": 0, "GDACS": 0, "RSS": 0}
    titles: List[str] = []

    for _, f in week:
        src = (f.get("properties") or {}).get("source")
        if src in counts_upper:
            counts_upper[src] += 1
        title = ((f.get("properties") or {}).get("title") or "")
        if title:
            titles.append(title)

    rss_count = 0
    rss_examples: List[Dict[str, Any]] = []
    rss_stories = (trusted_rss_payload or {}).get("stories") or []
    if trusted_rss_payload:
        rss_count = len(rss_stories)
        rss_examples = rss_stories[:5]
        for s in rss_stories[:40]:
            titles.append((s.get("title") or ""))

    topics = extract_topics(titles[:160])
    country_scores = get_country_scores(week, trusted_rss_payload=trusted_rss_payload)

    hotspots_path = os.path.join(DOCS_DATA_DIR, "hotspots.json")
    early_path = os.path.join(DOCS_DATA_DIR, "early.json")
    top_hotspots = []
    early_top = []

    try:
        if os.path.exists(hotspots_path):
            with open(hotspots_path, "r", encoding="utf-8") as f:
                top_hotspots = (json.load(f) or {}).get("top") or []
    except Exception:
        top_hotspots = []

    try:
        if os.path.exists(early_path):
            with open(early_path, "r", encoding="utf-8") as f:
                early_top = (json.load(f) or {}).get("top") or []
    except Exception:
        early_top = []

    intro = intro_paragraph(counts_upper, top_hotspots, early_top, country_scores, trusted_rss_payload)
    countries = build_country_assessments(country_scores, top_hotspots, trusted_rss_payload)
    external = external_actors_paragraph(trusted_rss_payload)
    risks = risk_paragraph(top_hotspots, early_top, trusted_rss_payload)
    forecast = forecast_paragraph(country_scores, top_hotspots, trusted_rss_payload)
    closing = closing_paragraph()
    methodology = methodology_paragraph()

    weekly_assessment_paragraphs = [
        intro,
        *[f"{c['country']}: {c['text']}" for c in countries],
        external,
        risks,
        forecast,
        closing,
    ]

    bullets = [
        intro,
        risks,
        forecast,
        closing,
    ]

    return {
        "generated_utc": to_utc_z(now),
        "headline": "Nyugat-Balkán heti biztonsági brief",
        "title": f"Weekly Balkan Security Brief – {now.strftime('%Y-%m-%d')}",
        "region": "Western Balkans",
        "weekly_assessment": htmlify_paragraphs(weekly_assessment_paragraphs),
        "weekly_assessment_plain": weekly_assessment_paragraphs,
        "country_assessments": countries,
        "external_actors": external,
        "risk_assessment": risks,
        "forecast": forecast,
        "closing": closing,
        "methodology": methodology,
        "methodology_html": htmlify_paragraphs([methodology]),
        "bullets": bullets,
        "counts": counts_upper,
        "rss_count": rss_count,
        "topics": topics,
        "country_scores": country_scores,
        "examples": [
            {
                "title": x.get("title"),
                "url": x.get("url"),
                "domain": x.get("source_name"),
            }
            for x in rss_examples
        ],
    }

# ============================================================
# MAIN
# ============================================================
def main() -> int:
    ensure_dirs()

    geoms = load_or_build_country_geoms()
    ensure_balkan_borders(geoms)

    prev_usgs = load_geojson_features(os.path.join(DOCS_DATA_DIR, "usgs.geojson"))
    prev_gdacs = load_geojson_features(os.path.join(DOCS_DATA_DIR, "gdacs.geojson"))
    prev_gdelt_linked = load_geojson_features(os.path.join(DOCS_DATA_DIR, "gdelt_linked.geojson"))
    prev_rss_signals = load_geojson_features(TRUSTED_RSS_SIGNALS_PATH)

    try:
        usgs_new = fetch_usgs(geoms, days=USGS_DAYS, min_magnitude=2.5)
    except Exception as e:
        print(f"[USGS] fetch failed: {e}")
        usgs_new = []

    try:
        gdacs_new = fetch_gdacs(geoms, days=GDACS_DAYS)
    except Exception as e:
        print(f"[GDACS] fetch failed: {e}")
        gdacs_new = []

    gdelt_geo_new, gdelt_debug = fetch_gdelt_geo_disabled()

    try:
        gdelt_linked_new = fetch_gdelt_export_linked(geoms, lookback_days=GDELT_EXPORT_DAYS)
    except Exception as e:
        print(f"[GDELT EXPORT] fetch failed: {e}")
        gdelt_linked_new = []

    try:
        trusted_rss_payload = fetch_trusted_rss()
        save_trusted_rss(trusted_rss_payload)
        print(f"[RSS] trusted_rss.json created with {trusted_rss_payload.get('count', 0)} stories.")
    except Exception as e:
        print(f"[RSS] fetch failed: {e}")
        trusted_rss_payload = {
            "generated_utc": to_utc_z(datetime.now(timezone.utc)),
            "count": 0,
            "stories": [],
            "summary": {"top_countries": [], "top_sources": []},
            "errors": [{"feed_id": "all", "feed_name": "trusted_rss", "error": str(e)}],
        }
        save_trusted_rss(trusted_rss_payload)

    rss_signal_new = build_trusted_rss_signal_features(trusted_rss_payload, keep_days=RSS_SIGNAL_DAYS)

    usgs_merged = merge_dedup(clamp_times(prev_usgs), clamp_times(usgs_new))
    gdacs_merged = merge_dedup(clamp_times(prev_gdacs), clamp_times(gdacs_new))
    gdelt_linked_merged = merge_dedup(clamp_times(prev_gdelt_linked), clamp_times(gdelt_linked_new))
    rss_signal_merged = merge_dedup(clamp_times(prev_rss_signals), clamp_times(rss_signal_new))

    usgs = trim_by_days(usgs_merged, keep_days=ROLLING_DAYS)
    gdacs = trim_by_days(gdacs_merged, keep_days=GDACS_KEEP_DAYS)
    gdelt = []
    gdelt_linked = trim_by_days(gdelt_linked_merged, keep_days=GDELT_EXPORT_DAYS)
    rss_signals = trim_by_days(rss_signal_merged, keep_days=RSS_SIGNAL_DAYS)

    save_geojson(os.path.join(DOCS_DATA_DIR, "usgs.geojson"), usgs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdacs.geojson"), gdacs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdelt.geojson"), gdelt)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdelt_linked.geojson"), gdelt_linked)
    save_geojson(TRUSTED_RSS_SIGNALS_PATH, rss_signals)

    with open(os.path.join(DOCS_DATA_DIR, "gdelt_debug.json"), "w", encoding="utf-8") as f:
        json.dump(gdelt_debug, f, ensure_ascii=False, indent=2)

    all_feats = gdelt + gdelt_linked + gdacs + usgs + rss_signals
    hotspot_geo, top_hotspots = build_hotspots_with_trend(all_feats, cell_deg=0.5, top_n=10)

    cache = load_cache()
    for h in top_hotspots:
        h["place"] = reverse_geocode_osm(float(h["lat"]), float(h["lon"]), cache)
    save_cache(cache)

    save_geojson(os.path.join(DOCS_DATA_DIR, "hotspots.geojson"), hotspot_geo)
    with open(os.path.join(DOCS_DATA_DIR, "hotspots.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_utc": to_utc_z(datetime.now(timezone.utc)), "top": top_hotspots}, f, ensure_ascii=False, indent=2)

    early_geo, early_top = build_early_warning(all_feats, cell_deg=0.5, lookback_days=7, recent_hours=48, top_n=10)
    cache = load_cache()
    for e in early_top:
        e["place"] = reverse_geocode_osm(float(e["lat"]), float(e["lon"]), cache)
    save_cache(cache)

    save_geojson(os.path.join(DOCS_DATA_DIR, "early.geojson"), early_geo)
    with open(os.path.join(DOCS_DATA_DIR, "early.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_utc": to_utc_z(datetime.now(timezone.utc)), "top": early_top}, f, ensure_ascii=False, indent=2)

    counts = {
        "usgs": len(usgs),
        "gdacs": len(gdacs),
        "gdelt": len(gdelt),
        "gdelt_linked": len(gdelt_linked),
        "rss_signals": len(rss_signals),
        "hotspot_cells": len(hotspot_geo),
        "rss_trusted": int(trusted_rss_payload.get("count") or 0),
    }

    summary = make_summary(all_feats, top_hotspots, counts, trusted_rss_payload=trusted_rss_payload)
    with open(os.path.join(DOCS_DATA_DIR, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    weekly = build_weekly(all_feats, trusted_rss_payload=trusted_rss_payload)
    with open(os.path.join(DOCS_DATA_DIR, "weekly.json"), "w", encoding="utf-8") as f:
        json.dump(weekly, f, ensure_ascii=False, indent=2)

    meta = {
        "generated_utc": to_utc_z(datetime.now(timezone.utc)),
        "counts": counts,
        "rolling_days": ROLLING_DAYS,
        "countries": BALKAN_COUNTRIES,
        "western_balkans_core": WESTERN_BALKANS_COUNTRIES,
        "bbox": {
            "lon_min": BALKAN_BBOX[0],
            "lat_min": BALKAN_BBOX[1],
            "lon_max": BALKAN_BBOX[2],
            "lat_max": BALKAN_BBOX[3],
        },
        "gdelt": {
            "geo_enabled": False,
            "export_days": GDELT_EXPORT_DAYS,
        },
        "early": {"recent_hours": 48, "lookback_days": 7},
        "rss": {
            "enabled": True,
            "feeds": [f["id"] for f in TRUSTED_RSS_FEEDS],
            "output": "trusted_rss.json",
            "signal_output": "trusted_rss_signals.geojson",
            "signal_days": RSS_SIGNAL_DAYS,
        },
        "weekly_brief": {
            "enabled": True,
            "structure": [
                "intro",
                "country_assessments",
                "external_actors",
                "risk_assessment",
                "forecast",
                "closing",
                "methodology",
            ],
        },
    }
    with open(os.path.join(DOCS_DATA_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if build_risk_snapshot_main is not None:
        try:
            print("[risk] building risk_daily.json ...")
            build_risk_snapshot_main()
            print("[risk] risk_daily.json created.")
        except Exception as e:
            print(f"[risk] build failed: {e}")
    else:
        print("[risk] build_risk_snapshot not available, skipping risk_daily.json generation.")

    print("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

