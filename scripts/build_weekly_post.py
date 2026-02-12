#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iso_to_local_hu(iso: Optional[str]) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # Budapest időzóna nélkül: csak “szép” formátum UTC-vel (GitHub-on egyszerűbb)
        # Ha kell valódi Europe/Budapest konverzió, szólj és rakok bele zoneinfo-t.
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_examples(examples: List[Dict[str, Any]]) -> str:
    if not examples:
        return "<p><em>Most nincs kiemelt példa.</em></p>"
    items = []
    for ex in examples[:5]:
        t = html_escape(ex.get("title") or "")
        url = html_escape(ex.get("url") or "")
        dom = html_escape(ex.get("domain") or "")
        time_utc = iso_to_local_hu(ex.get("time_utc"))
        line = f'<li><a href="{url}" target="_blank" rel="noopener">{t}</a>'
        meta = " • ".join([x for x in [dom, time_utc] if x])
        if meta:
            line += f'<br><small>{meta}</small>'
        line += "</li>"
        items.append(line)
    return "<ul>" + "\n".join(items) + "</ul>"


def responsive_iframe(map_url: str) -> str:
    # Gutenberg-kompatibilis: HTML blokkban beágyazva
    map_url = map_url.rstrip("/")
    return f"""
<!-- wp:html -->
<div style="max-width: 1100px; margin: 16px auto;">
  <div style="position: relative; width: 100%; padding-top: 62.5%; border-radius: 12px; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,0.12);">
    <iframe
      src="{html_escape(map_url)}/"
      title="Balkán biztonsági monitor – interaktív térkép"
      style="position:absolute; inset:0; width:100%; height:100%; border:0;"
      loading="lazy"
      referrerpolicy="no-referrer-when-downgrade"
      allowfullscreen>
    </iframe>
  </div>
  <p style="margin:10px 0 0; font-size: 13px; opacity: 0.85;">
    Tipp: mobilon a „Rétegek” panellel tudsz szűrni (hotspot / early / GDACS / USGS / GDELT).
  </p>
</div>
<!-- /wp:html -->
""".strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--weekly", required=True, help="docs/data/weekly.json")
    ap.add_argument("--meta", required=False, help="docs/data/meta.json")
    ap.add_argument("--map-url", required=True, help="GitHub Pages map URL")
    ap.add_argument("--out", required=True, help="Output HTML")
    args = ap.parse_args()

    weekly = load_json(args.weekly)
    meta = load_json(args.meta) if args.meta else {}

    generated = iso_to_local_hu(weekly.get("generated_utc"))
    headline = weekly.get("headline") or "Heti kivonat – elmúlt 7 nap"
    bullets = weekly.get("bullets") or []
    counts = (weekly.get("counts") or {}) if isinstance(weekly.get("counts"), dict) else {}
    examples = weekly.get("examples") or []

    # meta info (ha van)
    map_generated = ""
    try:
        map_generated = iso_to_local_hu((meta.get("generated_utc") or ""))
    except Exception:
        map_generated = ""

    # Build HTML
    b = []
    b.append(f"<h2>{html_escape(headline)}</h2>")
    if generated:
        b.append(f"<p><small>Generálva: {html_escape(generated)}</small></p>")

    if map_generated and map_generated != generated:
        b.append(f"<p><small>Térképadatok frissítve: {html_escape(map_generated)}</small></p>")

    b.append("<hr>")

    b.append("<h3>Összefoglaló</h3>")
    if bullets:
        b.append("<ul>")
        for x in bullets:
            b.append(f"<li>{html_escape(str(x))}</li>")
        b.append("</ul>")
    else:
        b.append("<p><em>Nincs összefoglaló.</em></p>")

    # Számlálók
    if counts:
        b.append("<p><strong>Heti forrásszámlálók:</strong> "
                 f"GDELT: {counts.get('GDELT',0)} • USGS: {counts.get('USGS',0)} • GDACS: {counts.get('GDACS',0)}</p>")

    b.append("<hr>")

    b.append("<h3>Kiemelt példák (hírcímek)</h3>")
    b.append(build_examples(examples))

    b.append("<hr>")

    b.append("<h3>Interaktív térkép</h3>")
    b.append("<p>Az alábbi térkép mutatja a friss jelzéseket és a hotspot-összesítést.</p>")
    b.append(responsive_iframe(args.map_url))

    # Finom jogi/OSINT megjegyzés
    b.append("<p><small><em>Megjegyzés: automatikus OSINT-kivonat. A linkelt források kézi ellenőrzése javasolt.</em></small></p>")

    out_html = "\n".join(b)

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(out_html)

    print(f"Wrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
