#!/usr/bin/env python3
import os
import requests

CLIENT_ID = os.environ["WP_CLIENT_ID"]
CLIENT_SECRET = os.environ["WP_CLIENT_SECRET"]
REDIRECT_URI = os.environ["WP_REDIRECT_URI"]
CODE = os.environ["WP_CODE"]

r = requests.post(
    "https://public-api.wordpress.com/oauth2/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "code": CODE,
        "grant_type": "authorization_code",
    },
    timeout=30,
)
r.raise_for_status()
print(r.json())
