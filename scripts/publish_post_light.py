#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import requests


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DATA_DIR = os.path.join(ROOT, "docs", "data")

WP_API_BASE = "https://public-api.wordpress.com/rest/v1.1"


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def esc(s: Any) -> str:
    # minimal HTML escaping
    txt = "" if s is None else str(s)
    return (
        txt.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def fmt_dt(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        d = d.astimezone(timezone.utc)
        return d.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(iso)


def build_html(summary: Dict[str, Any], weekly: Dict[str, Any], hotspots: Dict[str, Any], meta: Dict[str, Any]) -> str:
    gen = meta.get("generated_utc") or summary.get("generated_utc") or weekly.get("generated_utc")
    counts = (meta.get("counts") or {}) if isinstance(meta, dict) else {}

    map_url = os.getenv("MAP_URL", "").strip()
    map_block = ""
    if map_url:
        map_block = f"""
        <p><b>Térkép:</b> <a href="{esc(map_url)}" target="_blank" rel="noopener">Balkán Biztonsági Monitor</a></p>
        """

    # top hotspots
    top = (hotspots.get("top") or []) if isinstance(hotspots, dict) else []
    top = top[:8]

    def li_list(items: List[Any]) -> str:
        if not items:
            return "<li>—</li>"
        return "\n".join([f"<li>{esc(x)}</li>" for x in items[:12]])

    daily_bullets = summary.get("bullets") or []
    weekly_bullets = weekly.get("bullets") or []

    hs_rows = ""
    if top:
        hs_rows = "<ol>" + "\n".join(
            [
                "<li>"
                f"<b>{esc(h.get('place') or 'ismeretlen térség')}</b>"
                f" — score: {esc(h.get('score'))}"
                f" — ({esc(round(float(h.get('lat',0)),2))}, {esc(round(float(h.get('lon',0)),2))})"
                f"{' ' + esc(h.get('trend_arrow')) if h.get('trend_arrow') else ''}"
                f")"
                "</li>"
                for h in top
            ]
        ) + "</ol>"
    else:
        hs_rows = "<p>— (nincs elég adat)</p>"

    html = f"""
    <div>
      <p><b>Frissítés:</b> {esc(fmt_dt(gen))}</p>
      <p>
        <b>Források (7 nap):</b>
        GDELT: {esc(counts.get("gdelt", 0))} + linked: {esc(counts.get("gdelt_linked", 0))},
        USGS: {esc(counts.get("usgs", 0))},
        GDACS: {esc(counts.get("gdacs", 0))}
      </p>
      {map_block}

      <h3>{esc(summary.get("headline") or "Napi kivonat")}</h3>
      <ul>
        {li_list(daily_bullets)}
      </ul>

      <h3>{esc(weekly.get("headline") or "Heti kivonat")}</h3>
      <ul>
        {li_list(weekly_bullets)}
      </ul>

      <h3>Top hotspotok</h3>
      {hs_rows}

      <hr/>
      <p style="font-size:12px;color:#666;">
        Megjegyzés: automatikus OSINT-kivonat; a linkelt források kézi ellenőrzése javasolt.
      </p>
    </div>
    """
    return html


def wp_request(method: str, url: str, token: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "balkan-security-map/wordpress-poster",
    }
    r = requests.request(method, url, headers=headers, data=payload, timeout=45)
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"WP non-JSON response ({r.status_code}): {r.text[:300]}")
    if r.status_code >= 400:
        raise RuntimeError(f"WP error ({r.status_code}): {data}")
    return data


def main() -> int:
    token = os.getenv("WP_ACCESS_TOKEN", "").strip()
    blog_id = os.getenv("WP_BLOG_ID", "").strip()

    if not token or not blog_id:
        print("Missing env: WP_ACCESS_TOKEN and/or WP_BLOG_ID", file=sys.stderr)
        return 2

    post_status = os.getenv("POST_STATUS", "draft").strip()  # draft/publish/private
    post_id = os.getenv("WP_POST_ID", "").strip()  # optional: update existing post

    # load prepared data (this is the key: NO GDELT calls here)
    summary = load_json(os.path.join(DOCS_DATA_DIR, "summary.json"))
    weekly = load_json(os.path.join(DOCS_DATA_DIR, "weekly.json"))
    hotspots = load_json(os.path.join(DOCS_DATA_DIR, "hotspots.json"))
    meta = load_json(os.path.join(DOCS_DATA_DIR, "meta.json"))

    title_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    title = f"Balkán biztonsági monitor – heti/napi kivonat ({title_date})"
    html = build_html(summary, weekly, hotspots, meta)

    if post_id:
        url = f"{WP_API_BASE}/sites/{blog_id}/posts/{post_id}"
        payload = {"title": title, "content": html, "status": post_status}
        out = wp_request("POST", url, token, payload)
        print(f"Updated post_id={out.get('ID')} status={out.get('status')} URL={out.get('URL')}")
    else:
        url = f"{WP_API_BASE}/sites/{blog_id}/posts/new"
        payload = {"title": title, "content": html, "status": post_status}
        out = wp_request("POST", url, token, payload)
        print(f"Created post_id={out.get('ID')} status={out.get('status')} URL={out.get('URL')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
