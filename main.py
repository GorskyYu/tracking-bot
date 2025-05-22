import os
import time
import hmac
import hashlib
import requests
import json

# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID     = os.getenv("TE_APP_ID")
APP_SECRET = os.getenv("TE_SECRET")

TIMEZONE   = "America/Vancouver"
CACHE_FILE = "status_cache.json"

# ─── Signature Generator ──────────────────────────────────────────────────────
def generate_sign(params: dict, secret: str) -> str:
    qs = "&".join(f"{k}={requests.utils.quote(str(params[k]))}" for k in sorted(params))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

# ─── Generic API ──────────────────────────────────────────────────────────────
def call_api(action: str, payload: dict = None) -> dict:
    timestamp = str(int(time.time()))
    params = {
        "id": APP_ID,
        "timestamp": timestamp,
        "format": "json",
        "action": action
    }
    params["sign"] = generate_sign(params, APP_SECRET)
    url = "https://eship.tripleeaglelogistics.com/api?" + "&".join(
        f"{k}={requests.utils.quote(str(params[k]))}" for k in params
    )
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers) if payload else requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

# ─── Cache Handling ───────────────────────────────────────────────────────────
def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}

def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ─── Helpers ──────────────────────────────────────────────────────────────────
def fetch_active_order_ids() -> list:
    data = call_api("shipment/list")
    lst = data.get("response", {}).get("list") or data.get("response")
    return [o["id"] for o in lst if "id" in o] if isinstance(lst, list) else []

def fetch_detail(order_id: str) -> dict:
    return call_api("shipment/detail", {"id": order_id})

def fetch_tracking(order_ids: list) -> dict:
    return call_api("shipment/tracking", {
        "keyword": ",".join(order_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    })

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    cache = load_cache()
    order_ids = fetch_active_order_ids()
    if not order_ids:
        print("No active orders.")
        return

    yumi_orders = []
    sender_map = {}
    for oid in order_ids:
        try:
            detail = fetch_detail(oid)
            response = detail.get("response", {})
            if isinstance(response, list):
                response = response[0]
            initiation = response.get("initiation", {})
            if isinstance(initiation, dict):
                loc = next(iter(initiation), None)
                sender = initiation.get(loc, {}).get("name", "")
                if "Yumi" in sender or "Shu-Yen" in sender:
                    yumi_orders.append(oid)
                    sender_map[oid] = sender
        except Exception as e:
            print(f"Error loading detail for {oid}: {e}")

    if not yumi_orders:
        print("No active orders from Yumi.")
        return

    tracking_data = fetch_tracking(yumi_orders)
    for item in tracking_data.get("response", []):
        oid = item.get("id")
        tracking_number = item.get("number", "N/A")
        if not item.get("list"):
            print(f"{oid} ({tracking_number}) - No tracking history")
            continue
        latest = max(item["list"], key=lambda ev: int(ev["timestamp"]))
        context = latest.get("context", "No context")
        time_str = latest["datetime"].get(TIMEZONE, latest["datetime"].get("GMT", "N/A"))

        prev_status = cache.get(oid)
        if context != prev_status:
            print(f"⚠️ status changed for {oid} ({tracking_number})")
        print(f"{oid} ({tracking_number}) - Sender: {sender_map.get(oid)}")
        print(f"  Status: {context}")
        print(f"  Time:   {time_str}")
        print()

        cache[oid] = context

    save_cache(cache)

if __name__ == "__main__":
    main()
