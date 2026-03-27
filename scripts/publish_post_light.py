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


def build_hotspot_rows(top: List[Dict[str, Any]]) -> str:
    if not top:
        return """
        <p style="margin:0;font-size:16px;line-height:1.8;color:#f1f5f9;text-align:justify;">
          — nincs elég adat
        </p>
        """

    rows = []
    for h in top[:6]:
        place = esc(h.get("place") or "ismeretlen térség")
        score = esc(h.get("score"))
        lat = esc(round(float(h.get("lat", 0)), 2))
        lon = esc(round(float(h.get("lon", 0)), 2))
        trend = esc(h.get("trend_arrow") or "")
        change_pct = h.get("change_pct")
        change_html = ""
        if change_pct is not None:
            change_html = f"<br/><strong>7 napos változás:</strong> {esc(change_pct)}%"

        trend_html = ""
        if trend:
            trend_html = f"""
            <div style="font-size:14px;color:#475569;margin-top:6px;">
              Trend: {trend}
            </div>
            """

        rows.append(
            f"""
            <div style="
                background:#f8fafc;
                color:#1e293b;
                border-radius:16px;
                padding:18px 20px;
                box-shadow:0 8px 20px rgba(0,0,0,0.14);
                margin:0 0 14px 0;
            ">
              <div style="font-size:18px;font-weight:700;line-height:1.35;color:#0f172a;">
                {place}
              </div>
              <div style="font-size:15px;line-height:1.7;color:#334155;margin-top:8px;">
                <strong>Score:</strong> {score}<br/>
                <strong>Koordináták:</strong> ({lat}, {lon})
                {change_html}
              </div>
              {trend_html}
            </div>
            """
        )
    return "\n".join(rows)


def card_section(title: str, body_html: str) -> str:
    return f"""
    <section style="margin:0 0 26px 0;">
      <div style="
          background:#e5e7eb;
          color:#0f172a;
          padding:18px 22px;
          border-radius:16px;
          box-shadow:0 6px 18px rgba(0,0,0,0.16);
          margin:0 0 14px 0;
      ">
        <div style="font-size:22px;font-weight:700;line-height:1.3;">
          {esc(title)}
        </div>
      </div>

      <div style="padding:2px 8px 0 8px;">
        <div style="
            background:rgba(255,255,255,0.08);
            border:1px solid rgba(255,255,255,0.12);
            border-radius:16px;
            padding:22px 24px;
            box-shadow:0 8px 20px rgba(0,0,0,0.12);
            color:#f1f5f9;
            font-size:16px;
            line-height:1.85;
            text-align:justify;
        ">
          {body_html}
        </div>
      </div>
    </section>
    """


def paragraphize_text(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return '<p style="margin:0;">—</p>'
    return f'<p style="margin:0;">{esc(text)}</p>'


def build_overview_html(gen: str, counts: Dict[str, Any], map_url: str) -> str:
    generated_text = esc(fmt_dt(gen))
    gdelt = esc(counts.get("gdelt", 0))
    gdelt_linked = esc(counts.get("gdelt_linked", 0))
    usgs = esc(counts.get("usgs", 0))
    gdacs = esc(counts.get("gdacs", 0))

    map_block = ""
    if map_url:
        map_block = f"""
        <p style="margin:0 0 16px 0;font-size:16px;line-height:1.8;color:#f1f5f9;text-align:justify;">
          <strong>Térkép:</strong>
          <a href="{esc(map_url)}" target="_blank" rel="noopener"
             style="color:#bfdbfe;text-decoration:underline;">
             Balkán Biztonsági Monitor
          </a>
        </p>
        """

    return f"""
    <p style="margin:0 0 16px 0;font-size:16px;line-height:1.8;color:#f1f5f9;text-align:justify;">
      <strong>Frissítés:</strong> {generated_text}
    </p>

    <p style="margin:0 0 16px 0;font-size:16px;line-height:1.8;color:#f1f5f9;text-align:justify;">
      <strong>Források (7 nap):</strong>
      GDELT: {gdelt} + linked: {gdelt_linked},
      USGS: {usgs},
      GDACS: {gdacs}
    </p>

    {map_block}
    """


def build_html(summary: Dict[str, Any], weekly: Dict[str, Any], hotspots: Dict[str, Any], meta: Dict[str, Any]) -> str:
    gen = meta.get("generated_utc") or summary.get("generated_utc") or weekly.get("generated_utc")
    counts = (meta.get("counts") or {}) if isinstance(meta, dict) else {}

    map_url = os.getenv("MAP_URL", "").strip()

    top = (hotspots.get("top") or []) if isinstance(hotspots, dict) else []

    report_title = weekly.get("title") or "Weekly Balkan Security Brief"
    weekly_assessment_html = weekly.get("weekly_assessment") or ""
    methodology_html = weekly.get("methodology_html") or ""
    methodology_text = weekly.get("methodology") or ""
    weekly_headline = weekly.get("headline") or "Nyugat-Balkán heti biztonsági brief"

    if not weekly_assessment_html:
        fallback_bullets = weekly.get("bullets") or []
        if fallback_bullets:
            weekly_assessment_html = "\n".join(
                [f'<p style="margin:0 0 16px 0;">{esc(x)}</p>' for x in fallback_bullets]
            )
        else:
            weekly_assessment_html = '<p style="margin:0;">—</p>'

    if not methodology_html:
        methodology_html = paragraphize_text(methodology_text or "Automatikus OSINT-kivonat; a linkelt források kézi ellenőrzése minden esetben javasolt.")

    hotspot_html = build_hotspot_rows(top)
    overview_html = build_overview_html(gen, counts, map_url)

    return f"""
    <div style="background:#4b5563;padding:40px 20px;">
      <div style="max-width:1000px;margin:0 auto;display:flex;flex-direction:column;gap:18px;">

        <div style="
            background:linear-gradient(135deg,#475569,#334155);
            padding:26px 28px;
            border-radius:22px;
            color:#f8fafc;
            box-shadow:0 12px 30px rgba(0,0,0,0.22);
            margin-bottom:8px;
        ">
          <div style="font-size:12px;text-transform:uppercase;letter-spacing:1.4px;color:#cbd5e1;">
            Balkan Security Monitor
          </div>
          <div style="font-size:30px;font-weight:700;line-height:1.2;margin-top:8px;">
            {esc(report_title)}
          </div>
          <div style="font-size:15px;line-height:1.7;margin-top:12px;color:#cbd5e1;">
            {esc(weekly_headline)}
          </div>
        </div>

        <section style="margin:0 0 26px 0;">
          <div style="
              background:#e5e7eb;
              color:#0f172a;
              padding:18px 22px;
              border-radius:16px;
              box-shadow:0 6px 18px rgba(0,0,0,0.16);
              margin:0 0 14px 0;
          ">
            <div style="font-size:22px;font-weight:700;line-height:1.3;">
              Áttekintés
            </div>
          </div>

          <div style="padding:2px 8px 0 8px;">
            {overview_html}
          </div>
        </section>

        {card_section("Heti részletes jelentés", weekly_assessment_html)}

        {card_section("Módszertan", methodology_html)}

        <section style="margin:0 0 26px 0;">
          <div style="
              background:#e5e7eb;
              color:#0f172a;
              padding:18px 22px;
              border-radius:16px;
              box-shadow:0 6px 18px rgba(0,0,0,0.16);
              margin:0 0 14px 0;
          ">
            <div style="font-size:22px;font-weight:700;line-height:1.3;">
              Top hotspotok
            </div>
          </div>

          <div style="padding:2px 8px 0 8px;">
            {hotspot_html}
          </div>
        </section>

      </div>
    </div>
    """


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

    post_status = os.getenv("POST_STATUS", "draft").strip()
    post_id = os.getenv("WP_POST_ID", "").strip()

    summary = load_json(os.path.join(DOCS_DATA_DIR, "summary.json"))
    weekly = load_json(os.path.join(DOCS_DATA_DIR, "weekly.json"))
    hotspots = load_json(os.path.join(DOCS_DATA_DIR, "hotspots.json"))
    meta = load_json(os.path.join(DOCS_DATA_DIR, "meta.json"))

    title = weekly.get("title") or f"Weekly Balkan Security Brief – {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
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
