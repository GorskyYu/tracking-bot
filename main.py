import os
import time
import hmac
import hashlib
import requests
import json

# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID       = os.getenv("TE_APP_ID")       # e.g. "584"
APP_SECRET   = os.getenv("TE_SECRET")       # e.g. "da5f3bec04ed5ec082d8a0a7f04e12e7"
LINE_TOKEN   = os.getenv("LINE_TOKEN")      # Your channel access token
LINE_GROUP_ID= os.getenv("LINE_GROUP_ID")   # Captured from webhook.site
LINE_USER_ID = os.getenv("LINE_USER_ID")    # Fallback or 1:1 user ID
CACHE_FILE   = "status_cache.json"
TIMEZONE     = "America/Vancouver"

# ─── Signature Generator ──────────────────────────────────────────────────────
def generate_sign(params: dict, secret: str) -> str:
    # Build sorted querystring
    qs = "&".join(f"{k}={requests.utils.quote(str(params[k]))}" for k in sorted(params))
    # HMAC-SHA256 hex digest
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

# ─── Generic API Caller ───────────────────────────────────────────────────────
def call_api(action: str, payload: dict=None) -> dict:
    timestamp = str(int(time.time()))
    params = {
        "id":        APP_ID,
        "timestamp": timestamp,
        "format":    "json",
        "action":    action
    }
    params["sign"] = generate_sign(params, APP_SECRET)
    # Build URL
    url = "https://eship.tripleeaglelogistics.com/api?" + "&".join(
        f"{k}={requests.utils.quote(str(params[k]))}" for k in params
    )
    if payload:
        resp = requests.post(url, json=payload, headers={"Content-Type":"application/json"})
    else:
        resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

# ─── Fetch Helpers ────────────────────────────────────────────────────────────
def fetch_active_order_ids() -> list:
    data = call_api("shipment/list")
    lst = data.get("response", {}).get("list") or data.get("response")
    return [o["id"] for o in lst] if isinstance(lst, list) else []

def fetch_tracking(order_ids: list) -> dict:
    return call_api("shipment/tracking", {
        "keyword": ",".join(order_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    })

def fetch_detail(order_id: str) -> dict:
    return call_api("shipment/detail", {"id": order_id})

# ─── LINE Push ────────────────────────────────────────────────────────────────
def send_line_message(text: str):
    target = LINE_GROUP_ID or LINE_USER_ID
    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json"
    }
    body = {
        "to": target,
        "messages": [{"type":"text","text": text}]
    }
    r = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=body)
    print(f"[LINE {r.status_code}] {r.text}")

# ─── Cache ────────────────────────────────────────────────────────────────────
def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}

def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ─── Main Logic ───────────────────────────────────────────────────────────────
def main():
    cache = load_cache()
    order_ids = fetch_active_order_ids()
    if not order_ids:
        print("No active orders.")
        return

    tracking_data = fetch_tracking(order_ids)
    details_map   = {oid: fetch_detail(oid) for oid in order_ids}

    for item in tracking_data.get("response", []):
        latest = max(item["list"], key=lambda ev: int(ev["timestamp"]))
        oid       = item["id"]
        number    = item.get("number", "")
        status    = latest.get("context", "UNKNOWN")
        prev_stat = cache.get(oid)

        if status != prev_stat:
            # Optional: include sender name from detail
            detail = details_map.get(oid, {})
            sender = "Unknown"
            init = detail.get("response") or detail.get("response", {})
            if isinstance(init, dict):
                loc = next(iter(init), None)
                sender = init.get(loc, {}).get("name", sender)

            msg = (
                f"📦 Order {oid} ({number})\n"
                f"Sender: {sender}\n"
                f"Update: {status}\n"
                f"Time: {latest['datetime'].get(TIMEZONE, '')}"
            )
            print("[CHANGE]", msg)
            send_line_message(msg)
            cache[oid] = status
        else:
            print(f"[NO CHANGE] {oid} still '{status}'")

    save_cache(cache)

if __name__ == "__main__":
    main()
