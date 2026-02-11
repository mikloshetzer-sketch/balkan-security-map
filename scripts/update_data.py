#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as dateparser

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DATA_DIR = os.path.join(ROOT, "docs", "data")

# Balkán "doboz" (bounding box) – később finomítható
# lon_min, lat_min, lon_max, lat_max
BALKAN_BBOX = (13.0, 37.0, 30.0, 47.5)

USER_AGENT = "balkan-security-map/1.1 (github actions)"
TIMEOUT = 30


def ensure_dirs() -> None:
    os.makedirs(DOCS_DATA_DIR, exist_ok=True)


def http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> requests.Response:
    """
    Robusztus GET: retry + backoff átmeneti hibákra / rate limitre.
    """
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)

    backoff = 2
    last_exc: Optional[Exception] = None

    for attempt in range(1, 4):  # 3 próbálkozás
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
# USGS Earthquakes (GeoJSON)
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

    resp = http_get(url, params=params)
    data = resp.json()

    out: List[Dict[str, Any]] = []
    for f in data.get("features", []):
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates") or []
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
                lon,
                lat,
                {
                    "source": "USGS",
                    "type": "earthquake",
                    "mag": p.get("mag"),
                    "place": p.get("place"),
                    "time": dt,
                    "url": p.get("url"),
                    "title": p.get("title"),
                },
            )
        )
    return out


# -------------------------
# GDACS (RSS)
# -------------------------
def fetch_gdacs(days: int = 14) -> List[Dict[str, Any]]:
    url = "https://www.gdacs.org/xml/rss.xml"
    xml = http_get(url).text

    items = xml.split("<item>")[1:]
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    out: List[Dict[str, Any]] = []
    for raw in items:
        chunk = raw.split("</item>")[0]

        def get_tag(tag: str) -> Optional[str]:
            open_t = f"<{tag}>"
            close_t = f"</{tag}>"
            if open_t in chunk and close_t in chunk:
                return chunk.split(open_t, 1)[1].split(close_t, 1)[0].strip()
            return None

        title = get_tag("title")
        link = get_tag("link")
        pub = get_tag("pubDate")
        point = get_tag("georss:point") or get_tag("point")

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
                lon,
                lat,
                {
                    "source": "GDACS",
                    "type": "disaster_alert",
                    "title": title,
                    "time": pub_dt.isoformat(),
                    "url": link,
                },
            )
        )
    return out


# -------------------------
# GDELT 2 DOC (JSON) – híralapú jelzések
# -------------------------
def fetch_gdelt(days: int = 2, max_records: int = 250) -> List[Dict[str, Any]]:
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    keywords = [
        "protest",
        "demonstration",
        "riot",
        "clash",
        "violence",
        "border",
        "checkpoint",
        "police",
        "attack",
        "explosion",
    ]
    countries = [
        "Albania",
        "Bosnia",
        "Herzegovina",
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

    # GDELT néha nem JSON-t ad vissza (HTML/üres), ezt toleráljuk
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
                lon_f,
                lat_f,
                {
                    "source": "GDELT",
                    "type": "news_event",
                    "title": a.get("title"),
                    "time": time_iso,
                    "url": a.get("url"),
                    "domain": a.get("domain"),
                    "language": a.get("language"),
                },
            )
        )

    # duplikátum szűrés URL alapján
    seen = set()
    deduped = []
    for f in out:
        u = (f.get("properties") or {}).get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(f)

    return deduped


def main() -> int:
    ensure_dirs()

    # USGS
    print("Fetching USGS...")
    try:
        usgs = fetch_usgs(days=7, min_magnitude=2.5)
    except Exception as e:
        print(f"[USGS] fetch failed, continuing with empty layer: {e}")
        usgs = []
    print(f"USGS features: {len(usgs)}")

    # GDACS
    print("Fetching GDACS...")
    try:
        gdacs = fetch_gdacs(days=14)
    except Exception as e:
        print(f"[GDACS] fetch failed, continuing with empty layer: {e}")
        gdacs = []
    print(f"GDACS features: {len(gdacs)}")

    # GDELT
    print("Fetching GDELT...")
    try:
        gdelt = fetch_gdelt(days=2, max_records=250)
    except Exception as e:
        print(f"[GDELT] fetch failed, continuing with empty layer: {e}")
        gdelt = []
    print(f"GDELT features: {len(gdelt)}")

    # mentés
    save_geojson(os.path.join(DOCS_DATA_DIR, "usgs.geojson"), usgs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdacs.geojson"), gdacs)
    save_geojson(os.path.join(DOCS_DATA_DIR, "gdelt.geojson"), gdelt)

    meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "counts": {"usgs": len(usgs), "gdacs": len(gdacs), "gdelt": len(gdelt)},
        "bbox": {
            "lon_min": BALKAN_BBOX[0],
            "lat_min": BALKAN_BBOX[1],
            "lon_max": BALKAN_BBOX[2],
            "lat_max": BALKAN_BBOX[3],
        },
    }
    with open(os.path.join(DOCS_DATA_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
