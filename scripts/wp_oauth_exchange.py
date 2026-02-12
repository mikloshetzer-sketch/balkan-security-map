#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import requests

TOKEN_URL = "https://public-api.wordpress.com/oauth2/token"
TIMEOUT = 30

def req_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"[ERROR] Missing env var: {name}", file=sys.stderr)
        raise SystemExit(2)
    return v

def main() -> int:
    client_id = req_env("WP_CLIENT_ID")
    client_secret = req_env("WP_CLIENT_SECRET")
    redirect_uri = req_env("WP_REDIRECT_URI")
    code = req_env("WP_CODE")

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
        "code": code,
    }

    r = requests.post(TOKEN_URL, data=data, timeout=TIMEOUT)
    try:
        payload = r.json()
    except Exception:
        print(f"[ERROR] Non-JSON response: {r.status_code} {r.text[:200]}", file=sys.stderr)
        return 1

    if r.status_code >= 400 or "error" in payload:
        print("[ERROR] Token exchange failed:", json.dumps(payload, indent=2, ensure_ascii=False), file=sys.stderr)
        return 1

    # Biztonság: nem írunk ki tokeneket a logba
    bundle = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "blog_id": payload.get("blog_id"),
        "blog_url": payload.get("blog_url"),
        "token_type": payload.get("token_type"),
        "scope": payload.get("scope"),
        "refresh_token": payload.get("refresh_token"),
        "access_token": payload.get("access_token"),
        "expires_in": payload.get("expires_in"),
    }

    # Mentsük artifactba
    with open("wp_token_bundle.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    print("[OK] Token bundle saved to wp_token_bundle.json (uploaded as artifact).")
    print("[NEXT] Download artifact, then set repo secrets: WP_REFRESH_TOKEN and WP_BLOG_ID.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
