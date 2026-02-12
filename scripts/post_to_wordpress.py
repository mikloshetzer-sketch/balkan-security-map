#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import requests
from datetime import datetime

def main() -> int:
    if len(sys.argv) < 5:
        print("Usage: post_to_wordpress.py post.html WP_SITE WP_ACCESS_TOKEN MAP_URL", file=sys.stderr)
        return 2

    post_html_path = sys.argv[1]
    wp_site = sys.argv[2].strip()
    token = sys.argv[3].strip()
    map_url = sys.argv[4].strip()

    if not wp_site or not token or not map_url:
        print("Missing WP_SITE / WP_ACCESS_TOKEN / MAP_URL", file=sys.stderr)
        return 2

    with open(post_html_path, "r", encoding="utf-8") as f:
        body_html = f.read()

    # Beágyazott térkép (reszponzív)
    map_embed = f"""
<hr/>
<h3>Interaktív térkép</h3>
<div style="position:relative;width:100%;padding-top:62.5%;">
  <iframe
    src="{map_url}"
    title="Balkán biztonsági térkép"
    style="position:absolute;top:0;left:0;width:100%;height:100%;border:0;"
    loading="lazy"
    referrerpolicy="no-referrer-when-downgrade"
  ></iframe>
</div>
<p><em>Megjegyzés: automatikus OSINT-kivonat. A linkelt források kézi ellenőrzése javasolt.</em></p>
"""

    content = body_html + map_embed

    # cím: heti dátummal
    today = datetime.utcnow().strftime("%Y-%m-%d")
    title = f"Balkán biztonsági monitor – heti jelentés ({today})"

    # WP.com REST API (WordPress.com hosted site)
    endpoint = f"https://public-api.wordpress.com/rest/v1.2/sites/{wp_site}/posts/new"

    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "balkan-security-map/weekly-post",
    }

    data = {
        "title": title,
        "content": content,
        "status": "publish",
        # "tags": "Balkán, biztonságpolitika, OSINT",
        # "categories": "Elemzés",
    }

    r = requests.post(endpoint, headers=headers, data=data, timeout=30)

    if r.status_code >= 400:
        print(f"[WP] ERROR status={r.status_code} body={r.text[:500]}", file=sys.stderr)
        return 2

    resp = r.json()
    url = resp.get("URL") or resp.get("url") or ""
    print(f"[WP] OK: {url}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
