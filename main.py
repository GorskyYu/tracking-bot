import os
import time
import hmac
import hashlib
import requests
import json
import base64
from urllib.parse import quote
from flask import Flask, request, jsonify


# ─── Customer Mapping ──────────────────────────────────────────────────────────
# Map each LINE group to the list of lowercase keywords you filter on
CUSTOMER_FILTERS = {
    os.getenv("LINE_GROUP_ID_YUMI"):   ["yumi", "shu-yen"],
    os.getenv("LINE_GROUP_ID_VICKY"):  ["vicky","chia-chi"]
}


# ─── Status Translations ──────────────────────────────────────────────────────
TRANSLATIONS = {
    "out for delivery today":         "今日派送中",
    "processing at ups facility":     "UPS處理中",
    "arrived at facility":            "已到達派送中心",
    "departed from facility":         "已離開派送中心",
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
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
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
def get_statuses_for(keywords: list[str]) -> list[str]:
    # 1) list all active orders
    resp = call_api("shipment/list")
    lst  = resp.get("response", {}).get("list") or resp.get("response") or []
    order_ids = [o["id"] for o in lst if "id" in o]

    # 2) filter by these keywords
    cust_ids = []
    for oid in order_ids:
        det = call_api("shipment/detail", {"id": oid}).get("response", {})
        if isinstance(det, list): det = det[0]
        init = det.get("initiation", {})
        loc  = next(iter(init), None)
        name = init.get(loc,{}).get("name","").lower() if loc else ""
        if any(kw in name for kw in keywords):
            cust_ids.append(oid)

    if not cust_ids:
        return ["📦 沒有此客戶的有效訂單"]

    # 3) fetch tracking updates
    td = call_api("shipment/tracking", {
        "keyword": ",".join(cust_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    })

    # 4) format reply exactly as before, with translation & location
    lines = [f"📦 {time.strftime('%Y-%m-%d %H:%M', time.localtime())}"]
    for item in td.get("response", []):
        oid = item.get("id"); num = item.get("number","")
        events = item.get("list") or []
        if not events:
            lines.append(f"{oid} ({num}) – 尚無追蹤紀錄"); continue

        ev = max(events, key=lambda e: int(e["timestamp"]))
        loc_raw = ev.get("location","")
        loc     = f"[{loc_raw.replace(',',', ')}] " if loc_raw else ""
        ctx_lc  = ev.get("context","").strip().lower()
        translated = TRANSLATIONS.get(ctx_lc, ev.get("context","").replace("Triple Eagle","system"))
        tme     = ev["datetime"].get(TIMEZONE, ev["datetime"].get("GMT",""))
        lines.append(f"{oid} ({num}) → {loc}{translated}  @ {tme}")

    return lines


# ─── Flask Webhook ────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Log incoming methods
    print(f"[Webhook] Received {request.method} to /webhook")
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Webhook] Payload:", json.dumps(data, ensure_ascii=False))

    for event in data.get("events", []):
        # Only handle text messages
        if event.get("type") == "message" and event["message"].get("type") == "text":
            group_id = event["source"].get("groupId")
            text     = event["message"]["text"].strip()
            print(f"[Webhook] Detected groupId: {group_id}, text: {text}")

            if text == "追蹤包裹":
                keywords = CUSTOMER_FILTERS.get(group_id)
                if not keywords:
                    print(f"[Webhook] No keywords configured for group {group_id}, skipping.")
                    continue

                # Now safe to extract reply_token
                reply_token = event["replyToken"]
                print("[Webhook] Trigger matched, fetching statuses…")
                messages = get_statuses_for(keywords)
                print("[Webhook] Reply messages:", messages)

                # Combine lines into one multi-line text
                combined = "\n\n".join(messages)
                payload = {
                    "replyToken": reply_token,
                    "messages": [{"type": "text", "text": combined}]
                }

                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {LINE_TOKEN}"
                }
                resp = requests.post(
                    "https://api.line.me/v2/bot/message/reply",
                    headers=headers,
                    json=payload
                )
                print(f"[Webhook] LINE reply status: {resp.status_code}, body: {resp.text}")

    return "OK", 200
    
# ─── Monday.com Webhook ────────────────────────────────────────────────────────
from flask import Flask, request, jsonify
import os, json, requests

app = Flask(__name__)
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
LINE_TOKEN       = os.getenv("LINE_TOKEN")

# Map the two allowed client texts to their LINE group IDs
CLIENT_TO_GROUP = {
    "Yumi":  os.getenv("LINE_GROUP_ID_YUMI"),
    "Vicky": os.getenv("LINE_GROUP_ID_VICKY"),
}

@app.route("/monday-webhook", methods=["GET", "POST"])
def monday_webhook():
    # 1️⃣ Allow Monday’s GET check
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Monday] Raw payload:", json.dumps(data, ensure_ascii=False))

    # Unwrap if wrapped under "event"
    evt = data.get("event", data)

    # 2️⃣ Handle the initial challenge
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]}), 200

    # 3️⃣ Extract item and status
    pulse_id  = evt.get("pulseId") or evt.get("itemId")
    item_name = evt.get("pulseName") or evt.get("itemName") or str(pulse_id)
    new_value = evt.get("value", {}).get("label", {}).get("text")
    print(f"[Monday] pulse_id={pulse_id}, item_name={item_name}, new_value={new_value}")

    # Only continue for 國際運輸
    if new_value != "國際運輸" or not pulse_id:
        return "OK", 200

    # 4️⃣ Fetch just the Client Name column (ID = formula8__1)
    gql = '''
    query ($itemId: [Int]) {
      items (ids: $itemId) {
        column_values (ids: ["formula8__1"]) {
          text
        }
      }
    }'''
    vars = {"itemId": int(pulse_id)}
    resp = requests.post(
        "https://api.monday.com/v2",
        json={"query": gql, "variables": vars},
        headers={
          "Authorization": MONDAY_API_TOKEN,
          "Content-Type":  "application/json"
        }
    )
    data2 = resp.json()
    print("[Monday API] response:", data2)

    # 5️⃣ Pull the Client Name text
    try:
        client = data2["data"]["items"][0]["column_values"][0]["text"]
    except Exception as e:
        print("[Monday API] error fetching Client Name:", e)
        return "OK", 200

    # 6️⃣ Look up the LINE group for this client
    group_id = CLIENT_TO_GROUP.get(client)
    if not group_id:
        print(f"[Monday→LINE] no mapping for client “{client}” – skipping.")
        return "OK", 200

    # 7️⃣ Send the push notification
    text = f"📦 {item_name} 已送往機場，準備進行國際運輸。"
    r2 = requests.post(
        "https://api.line.me/v2/bot/message/push",
        headers={
          "Authorization": f"Bearer {LINE_TOKEN}",
          "Content-Type":  "application/json"
        },
        json={"to": group_id, "messages":[{"type":"text","text":text}]}
    )
    print(f"[Monday→LINE] pushed to {client}: {r2.status_code}, {r2.text}")

    return "OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)