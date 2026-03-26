from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from rss_registry import (
    TRUSTED_RSS_FEEDS,
    BALKAN_COUNTRY_KEYWORDS,
    DIMENSION_KEYWORDS,
    EXCLUDE_KEYWORDS,
)

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "docs" / "data" / "trusted_rss.json"

USER_AGENT = "BalkanSecurityMonitor/1.0 (+trusted-rss-ingest)"
TIMEOUT = 20
MAX_ITEMS_PER_FEED = 50
MAX_OUTPUT_ITEMS = 150


@dataclass
class Story:
    story_id: str
    source_id: str
    source_name: str
    source_weight: float
    trusted: bool
    title: str
    summary: str
    url: str
    published_utc: str | None
    fetched_utc: str
    country_hint: str | None
    dimensions: list[str]
    scope: list[str]
    signal_score: float
    confidence_boost: float
    match_terms: list[str]


def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def text_blob(title: str, summary: str) -> str:
    return f"{title} {summary}".lower().strip()


def parse_datetime(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def score_recency(published_utc: str | None) -> float:
    if not published_utc:
        return 0.55
    try:
        dt = datetime.fromisoformat(published_utc)
        now = datetime.now(timezone.utc)
        hours = max(0.0, (now - dt).total_seconds() / 3600.0)
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


def infer_country(blob: str) -> tuple[str | None, list[str]]:
    matches: list[tuple[str, str]] = []
    for country, keywords in BALKAN_COUNTRY_KEYWORDS.items():
        for kw in keywords:
            if kw in blob:
                matches.append((country, kw))
    if not matches:
        return None, []
    counts: dict[str, int] = {}
    used_terms: list[str] = []
    for country, kw in matches:
        counts[country] = counts.get(country, 0) + 1
        used_terms.append(kw)
    best = sorted(counts.items(), key=lambda x: x[1], reverse=True)[0][0]
    return best, sorted(set(used_terms))


def infer_dimensions(blob: str) -> tuple[list[str], list[str]]:
    dims: list[str] = []
    terms: list[str] = []
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


def should_exclude(blob: str) -> bool:
    return any(kw in blob for kw in EXCLUDE_KEYWORDS)


def make_story_id(url: str, title: str) -> str:
    raw = f"{url}|{title}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:16]


def normalize_item(feed: dict[str, Any], item: dict[str, Any]) -> Story | None:
    title = strip_html(item.get("title", ""))
    summary = strip_html(item.get("summary", "") or item.get("description", ""))
    url = (item.get("link") or "").strip()

    if not title or not url:
        return None

    blob = text_blob(title, summary)
    if should_exclude(blob):
        return None

    country_hint, country_terms = infer_country(blob)
    dims, dim_terms = infer_dimensions(blob)

    if country_hint is None and "balkans" not in feed.get("scope", []):
        return None

    published_utc = parse_datetime(item.get("pubDate") or item.get("published"))
    recency = score_recency(published_utc)

    source_weight = float(feed["weight"])
    dimension_factor = min(1.15, 0.85 + 0.08 * len(dims))
    country_factor = 1.1 if country_hint else 0.8

    signal_score = round(source_weight * recency * dimension_factor * country_factor, 4)
    confidence_boost = round(min(0.35, 0.14 + source_weight * 0.18 + (0.05 if country_hint else 0.0)), 4)

    fetched_utc = datetime.now(timezone.utc).isoformat()

    return Story(
        story_id=make_story_id(url, title),
        source_id=feed["id"],
        source_name=feed["name"],
        source_weight=source_weight,
        trusted=bool(feed.get("trusted", True)),
        title=title,
        summary=summary[:500],
        url=url,
        published_utc=published_utc,
        fetched_utc=fetched_utc,
        country_hint=country_hint,
        dimensions=dims,
        scope=list(feed.get("scope", [])),
        signal_score=signal_score,
        confidence_boost=confidence_boost,
        match_terms=sorted(set(country_terms + dim_terms)),
    )


def parse_rss_xml(xml_bytes: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    items: list[dict[str, Any]] = []

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

    # Atom fallback
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


def fetch_feed(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read()


def dedupe_stories(stories: list[Story]) -> list[Story]:
    seen_urls: set[str] = set()
    seen_title_keys: set[str] = set()
    out: list[Story] = []

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


def build_output(stories: list[Story], errors: list[dict[str, str]]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()

    by_country: dict[str, int] = {}
    by_source: dict[str, int] = {}
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


def main() -> None:
    all_stories: list[Story] = []
    errors: list[dict[str, str]] = []

    for feed in TRUSTED_RSS_FEEDS:
        try:
            xml_bytes = fetch_feed(feed["url"])
            items = parse_rss_xml(xml_bytes)[:MAX_ITEMS_PER_FEED]
            for item in items:
                story = normalize_item(feed, item)
                if story:
                    all_stories.append(story)
        except Exception as exc:
            errors.append({
                "feed_id": feed["id"],
                "feed_name": feed["name"],
                "error": str(exc),
            })

    all_stories = dedupe_stories(all_stories)
    all_stories.sort(
        key=lambda s: (
            s.country_hint is None,
            -s.signal_score,
            s.published_utc or "",
        )
    )
    all_stories = all_stories[:MAX_OUTPUT_ITEMS]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(
        json.dumps(build_output(all_stories, errors), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[ok] wrote {OUT_PATH} with {len(all_stories)} stories")


if __name__ == "__main__":
    main()
