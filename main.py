import os
import time
import hmac
import hashlib
import requests
import json
from urllib.parse import quote
from flask import Flask, request

# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID      = os.getenv("TE_APP_ID")          # e.g. "584"
APP_SECRET  = os.getenv("TE_SECRET")          # your TE App Secret
LINE_TOKEN  = os.getenv("LINE_TOKEN")         # Channel access token
TIMEZONE    = "America/Vancouver"

# ─── Signature Generator ──────────────────────────────────────────────────────
def generate_sign(params: dict, secret: str) -> str:
    parts = []
    for k in sorted(params.keys()):
        v = params[k]
        parts.append(f"{k}={quote(str(v), safe='~')}")
    qs = "&".join(parts)
    sig_bytes = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).digest()
    return requests.utils.b64encode(sig_bytes).decode()

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

# ─── Business Logic ───────────────────────────────────────────────────────────
def get_yumi_statuses() -> list:
    # 1) list all active orders
    resp = call_api("shipment/list")
    lst = resp.get("response", {}).get("list") or resp.get("response") or []
    order_ids = [o["id"] for o in lst if "id" in o]

    # 2) filter Yumi’s orders
    yumi_ids = []
    for oid in order_ids:
        det = call_api("shipment/detail", {"id": oid}).get("response", {})
        if isinstance(det, list):
            det = det[0]
        init = det.get("initiation", {})
        loc = next(iter(init), None)
        name = init.get(loc, {}).get("name", "").lower()
        if "yumi" in name or "shu-yen" in name:
            yumi_ids.append(oid)

    if not yumi_ids:
        return ["📦 沒有 Yumi 的有效訂單"]

    # 3) fetch tracking updates
    td = call_api("shipment/tracking", {
        "keyword": ",".join(yumi_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    })

    # 4) format reply
    lines = [f"📦 {time.strftime('%Y-%m-%d %H:%M', time.localtime())}"]
    for item in td.get("response", []):
        oid = item.get("id")
        num = item.get("number", "")
        events = item.get("list") or []
        if not events:
            lines.append(f"{oid} ({num}) – 尚無追蹤紀錄")
            continue
        ev = max(events, key=lambda e: int(e["timestamp"]))
        ctx = ev.get("context", "")
        tme = ev["datetime"].get(TIMEZONE, ev["datetime"].get("GMT", ""))
        lines.append(f"{oid} ({num}) → {ctx}  @ {tme}")
    return lines

# ─── Flask Webhook ────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == 'GET':
        # Respond 200 OK to LINE’s Verify
        return "OK"
    data = request.get_json()
    for event in data.get("events", []):
        if event.get("type") == "message" and event["message"].get("type") == "text":
            text = event["message"]["text"].strip()
            if text == "追蹤包裹":
                reply_token = event["replyToken"]
                messages = get_yumi_statuses()
                payload = {
                    "replyToken": reply_token,
                    "messages": [{"type": "text", "text": m} for m in messages]
                }
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {LINE_TOKEN}"
                }
                requests.post(
                    "https://api.line.me/v2/bot/message/reply",
                    headers=headers,
                    json=payload
                )
    return "OK"

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
