#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from datetime import datetime

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def fmt_dt(iso: str) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso

def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: build_weekly_post.py docs/data/weekly.json docs/data/meta.json", file=sys.stderr)
        return 2

    weekly_path = sys.argv[1]
    meta_path = sys.argv[2]

    weekly = load_json(weekly_path)
    meta = load_json(meta_path)

    headline = weekly.get("headline") or "Balkán biztonsági monitor – heti jelentés"
    generated = weekly.get("generated_utc") or meta.get("generated_utc") or ""
    bullets = weekly.get("bullets") or []
    examples = weekly.get("examples") or []

    # HTML output to stdout (workflow > post.html)
    html = []
    html.append(f"<h2>{headline}</h2>")
    if generated:
        html.append(f"<p><em>Generálva: {fmt_dt(generated)}</em></p>")

    if bullets:
        html.append("<h3>Heti kivonat</h3>")
        html.append("<ul>")
        for b in bullets:
            html.append(f"<li>{b}</li>")
        html.append("</ul>")

    if examples:
        html.append("<h3>Friss hírcím példák (válogatás)</h3>")
        html.append("<ul>")
        for ex in examples:
            t = fmt_dt(ex.get("time_utc") or "")
            title = (ex.get("title") or "").strip() or "(cím nélkül)"
            url = ex.get("url") or ""
            domain = ex.get("domain") or ""
            if url:
                html.append(f'<li><strong>{t}</strong> – <a href="{url}" target="_blank" rel="noopener">{title}</a> <em>({domain})</em></li>')
            else:
                html.append(f"<li><strong>{t}</strong> – {title} <em>({domain})</em></li>")
        html.append("</ul>")

    # a térkép iframe-et a postoló script fogja hozzáadni (MAP_URL-ből),
    # itt csak a szöveg jön ki.
    print("\n".join(html))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
