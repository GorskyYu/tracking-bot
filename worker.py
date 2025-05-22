import os
import time
import hmac
import hashlib
import base64
import requests
import json
from urllib.parse import quote
from datetime import datetime
from zoneinfo import ZoneInfo

# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID        = os.getenv("TE_APP_ID")           # TripleEagle App ID
APP_SECRET    = os.getenv("TE_SECRET")           # TripleEagle App Secret
LINE_TOKEN    = os.getenv("LINE_TOKEN")          # Channel Access Token
LINE_GROUP_ID = os.getenv("LINE_GROUP_ID")       # LINE groupId to push messages
TIMEZONE      = "America/Vancouver"
CACHE_FILE    = "status_cache.json"

# ─── Signature Generator ──────────────────────────────────────────────────────
def generate_sign(params: dict, secret: str) -> str:
    parts = []
    for k in sorted(params.keys()):
        v = params[k]
        parts.append(f"{k}={quote(str(v), safe='~')}")
    qs = "&".join(parts)
    sig_bytes = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig_bytes).decode('utf-8')

# ─── TripleEagle API Caller ───────────────────────────────────────────────────
def call_api(action: str, payload: dict = None) -> dict:
    ts = str(int(time.time()))
    params = {"id": APP_ID, "timestamp": ts, "format": "json", "action": action}
    params["sign"] = generate_sign(params, APP_SECRET)
    url = "https://eship.tripleeaglelogistics.com/api?" + "&".join(
        f"{k}={quote(str(params[k]), safe='~')}" for k in params
    )
    headers = {"Content-Type": "application/json"}
    if payload:
        r = requests.post(url, json=payload, headers=headers)
    else:
        r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# ─── Load / Save Cache ─────────────────────────────────────────────────────────
def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ─── Time Guard ────────────────────────────────────────────────────────────────
def within_business_hours() -> bool:
    now = datetime.now(ZoneInfo(TIMEZONE))
    # Monday=0 … Sunday=6
    if now.weekday() == 6:
        return False
    return 7 <= now.hour < 18

# ─── Main Polling Logic ───────────────────────────────────────────────────────
def main():
    if not within_business_hours():
        print("Outside Mon–Sat 07:00–18:00 PST, skipping.")
        return

    cache = load_cache()
    # 1) Fetch all active shipments
    resp = call_api("shipment/list")
    lst  = resp.get("response", {}).get("list") or resp.get("response") or []
    order_ids = [o["id"] for o in lst if "id" in o]

    # 2) Filter Yumi’s orders
    yumi = []
    for oid in order_ids:
        det = call_api("shipment/detail", {"id": oid}).get("response", {})
        if isinstance(det, list):
            det = det[0]
        init = det.get("initiation", {})
        loc  = next(iter(init), None)
        name = init.get(loc, {}).get("name", "").lower() if loc else ""
        if "yumi" in name or "shu-yen" in name:
            # also pull tracking number if available
            num = det.get("number", "")
            yumi.append((oid, num))

    # 3) For each, fetch tracking and push changes
    for oid, num in yumi:
        td = call_api("shipment/tracking", {
            "keyword": oid,
            "rsync": 0,
            "timezone": TIMEZONE
        })
        events = td.get("response", [])[0].get("list", [])
        if not events:
            continue
        latest = max(events, key=lambda e: int(e["timestamp"]))
        status = latest.get("context", "UNKNOWN")
        prev   = cache.get(oid)
        if status != prev:
            # build and send LINE push
            message = f"📦 {oid} ({num}) status changed → {status}"
            push_payload = {
                "to": LINE_GROUP_ID,
                "messages": [{"type": "text", "text": message}]
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_TOKEN}"
            }
            resp = requests.post(
                "https://api.line.me/v2/bot/message/push",
                headers=headers,
                json=push_payload
            )
            print(f"Pushed for {oid}: {resp.status_code} {resp.text}")
            cache[oid] = status

    save_cache(cache)

if __name__ == "__main__":
    main()
