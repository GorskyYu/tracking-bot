import os
import time
import hmac
import hashlib
import requests
import json

APP_ID = os.getenv("TE_APP_ID")               # e.g., 584
APP_SECRET = os.getenv("TE_SECRET")           # e.g., da5f3bec...
LINE_TOKEN = os.getenv("LINE_TOKEN")
LINE_USER_ID = os.getenv("LINE_USER_ID")
CACHE_FILE = "status_cache.json"
TIMEZONE = "America/Vancouver"

def generate_sign(params: dict, secret: str) -> str:
    query = '&'.join(f"{k}={requests.utils.quote(str(v))}" for k in sorted(params))
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).digest().hex()

def fetch_api(action, payload=None):
    timestamp = int(time.time())
    params = {
        "id": APP_ID,
        "timestamp": str(timestamp),
        "format": "json",
        "action": action
    }
    params["sign"] = generate_sign(params, APP_SECRET)
    url = "https://eship.tripleeaglelogistics.com/api?" + '&'.join(f"{k}={requests.utils.quote(str(v))}" for k,v in params.items())
    options = {
        "headers": {
            "Content-Type": "application/json"
        },
        "timeout": 20
    }
    if payload:
        options["json"] = payload
        resp = requests.post(url, **options)
    else:
        resp = requests.get(url, **options)
    resp.raise_for_status()
    return resp.json()

def fetch_active_order_ids():
    data = fetch_api("shipment/list")
    raw_list = data.get("response", {}).get("list", [])
    return [s.get("id") for s in raw_list if "id" in s]

def fetch_tracking_data(order_ids):
    return fetch_api("shipment/tracking", {
        "keyword": ",".join(order_ids),
        "rsync": 0,
        "timezone": TIMEZONE
    })

def fetch_shipment_detail(order_id):
    return fetch_api("shipment/detail", {"id": order_id})

def send_line_message(msg):
    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "to": LINE_USER_ID,
        "messages": [{"type": "text", "text": msg}]
    }
    resp = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=data)
    print(f"[LINE] {resp.status_code}: {resp.text}")

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cache(data):
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f)

def main():
    cache = load_cache()
    order_ids = fetch_active_order_ids()
    tracking_data = fetch_tracking_data(order_ids)
    details_map = {oid: fetch_shipment_detail(oid) for oid in order_ids}

    for item in tracking_data.get("response", []):
        latest = max(item["list"], key=lambda ev: int(ev["timestamp"]))
        order_id = item["id"]
        tracking_number = item["number"]
        current_status = latest["context"]
        cached_status = cache.get(order_id)

        if current_status != cached_status:
            detail = details_map.get(order_id, {})
            sender = "Unknown"
            if "initiation" in detail:
                loc = next(iter(detail["initiation"]), None)
                if loc:
                    sender = detail["initiation"][loc].get("name", "Unknown")
            msg = f"📦 Order {order_id} ({tracking_number})\nSender: {sender}\nUpdate: {current_status}\nTime: {latest['datetime'][TIMEZONE]}"
            send_line_message(msg)
            cache[order_id] = current_status
        else:
            print(f"[No Change] {order_id} - {current_status}")

    save_cache(cache)

if __name__ == "__main__":
    main()
