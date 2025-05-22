import os
import time
import hmac
import hashlib
import requests
import json
import base64
from urllib.parse import quote
from flask import Flask, request

# ─── Status Translations ──────────────────────────────────────────────────────
TRANSLATIONS = {
    "out for delivery today":         "今日派送中",
    "processing at ups facility":     "UPS 處理中",
    "arrived at facility":            "已到達中心",
    "departed from facility":         "已離開中心",
    "pickup scan":                    "取件掃描",
    "your package is currently at the ups access point™ and is scheduled to be tendered to ups.": 
                                      "貨件目前在 UPS 取貨點，稍後將交予 UPS",
    "drop-off":                       "已寄件",
    "order created at triple eagle":  "已在系統建立訂單",
    "shipper created a label, ups has not received the package yet.": 
                                      "已建立運單，UPS 尚未收件",
    "delivered":                      "已送達",
}


# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID      = os.getenv("TE_APP_ID")          # e.g. "584"
APP_SECRET  = os.getenv("TE_SECRET")          # your TE App Secret
LINE_TOKEN  = os.getenv("LINE_TOKEN")         # Channel access token
TIMEZONE    = "America/Vancouver"

# ─── Signature Generator ──────────────────────────────────────────────────────
def generate_sign(params: dict, secret: str) -> str:
    # Build encodeURIComponent-style querystring
    parts = []
    for k in sorted(params.keys()):
        v = params[k]
        parts.append(f"{k}={quote(str(v), safe='~')}")
    qs = "&".join(parts)

    # HMAC-SHA256 and Base64-encode
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
    
    lines = [f"📦 {time.strftime('%Y-%m-%d %H:%M', time.localtime())}"]
    for item in td.get("response", []):
        oid = item.get("id")
        num = item.get("number", "")
        events = item.get("list") or []
        if not events:
            lines.append(f"{oid} ({num}) – 尚無追蹤紀錄")
            continue

                # pick the latest event
        ev = max(events, key=lambda e: int(e["timestamp"]))

        # extract and format location
        loc_raw = ev.get("location", "")
        if loc_raw:
            loc = loc_raw.replace(",", ", ")
            loc_str = f"[{loc}] "
        else:
            loc_str = ""

        # original English context
        ctx = ev.get("context", "").strip()

        # normalize and translate (fallback: swap Triple Eagle→system)
        ctx_lc = ctx.lower()
        translated = TRANSLATIONS.get(ctx_lc)
        if not translated:
            # replace any “Triple Eagle” mentions
            translated = ctx.replace("Triple Eagle", "system")

        # timestamp
        tme = ev["datetime"].get(TIMEZONE, ev["datetime"].get("GMT", ""))

        # append final line in Traditional Chinese
        # e.g. "U110236870 (…) → [Richmond, Canada] 已送達 @ …"
        lines.append(f"{oid} ({num}) → {loc_str}{translated}  @ {tme}")


    return lines    

    # 4) format reply
    lines = [f"📦 {time.strftime('%Y-%m-%d %H:%M', time.localtime())}"]
    for item in td.get("response", []):
        oid = item.get("id")
        num = item.get("number", "")
        events = item.get("list") or []
        if not events:
            lines.append(f"{oid} ({num}) – 尚無追蹤紀錄")
            continue

        # pick the latest event
        ev = max(events, key=lambda e: int(e["timestamp"]))

        # include location
        location = ev.get("location", "")  # e.g. "[Concord,Canada]"
        ctx      = ev.get("context", "")
        tme      = ev["datetime"].get(TIMEZONE, ev["datetime"].get("GMT", ""))

        # build the line with location first
        lines.append(f"{location} {oid} ({num}) → {ctx}  @ {tme}")
    return lines

# ─── Flask Webhook ────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Log every incoming request
    print(f"[Webhook] Received {request.method} to /webhook")
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Webhook] Payload:", json.dumps(data))

    for event in data.get("events", []):
        if event.get("type") == "message" and event["message"].get("type") == "text":
            text = event["message"]["text"].strip()
            print(f"[Webhook] Received text: {text}")

            if text == "追蹤包裹":
                print("[Webhook] Trigger matched, fetching statuses…")
                reply_token = event["replyToken"]
                messages = get_yumi_statuses()
                print("[Webhook] Reply messages:", messages)

                # Combine all lines into one message to avoid the 5-message limit
                combined = "\n\n".join(messages)
                payload = {
                  "replyToken": reply_token,
                  "messages": [{"type": "text", "text": combined}]
                }

                headers = {
                  "Content-Type":"application/json",
                  "Authorization":f"Bearer {LINE_TOKEN}"
                }
                resp = requests.post(
                  "https://api.line.me/v2/bot/message/reply",
                  headers=headers,
                  json=payload
                )
                print(f"[Webhook] LINE reply status: {resp.status_code}, body: {resp.text}")

    return "OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)