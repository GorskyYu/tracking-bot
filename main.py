import os
import time
import hmac
import hashlib
import requests
import json
import base64
from urllib.parse import quote
from flask import Flask, request, jsonify

from apscheduler.schedulers.background import BackgroundScheduler
import threading


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

# ─── Client → LINE Group Mapping ───────────────────────────────────────────────
CLIENT_TO_GROUP = {
    "yumi":  os.getenv("LINE_GROUP_ID_YUMI"),
    "vicky": os.getenv("LINE_GROUP_ID_VICKY"),
}

# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID      = os.getenv("TE_APP_ID")          # e.g. "584"
APP_SECRET  = os.getenv("TE_SECRET")          # your TE App Secret
LINE_TOKEN  = os.getenv("LINE_TOKEN")         # Channel access token
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
TIMEZONE    = "America/Vancouver"

STATE_FILE = os.getenv("STATE_FILE", "last_seen.json")
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
LINE_HEADERS = {
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

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
            
            print(f"[Debug] incoming groupId: {group_id!r}")
            print(f"[Debug] CUSTOMER_FILTERS keys: {list(CUSTOMER_FILTERS.keys())!r}")
            
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
@app.route("/monday-webhook", methods=["GET", "POST"])
def monday_webhook():
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    evt  = data.get("event", data)
    # respond to Monday’s handshake
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]}), 200

    sub_id    = evt.get("pulseId") or evt.get("itemId")
    parent_id = evt.get("parentItemId")
    lookup_id = parent_id or sub_id
    new_txt   = evt.get("value", {}).get("label", {}).get("text")

    # only act when Location flips to 國際運輸
    if new_txt != "國際運輸" or not lookup_id:
        return "OK", 200

    # fetch just the formula column:
    gql = '''
    query ($itemIds: [ID!]!) {
      items(ids: $itemIds) {
        column_values(ids: ["formula8__1"]) {
          id
          text
          ... on FormulaValue { display_value }
        }
      }
    }'''
    variables = {"itemIds": [str(lookup_id)]}
    resp = requests.post(
      "https://api.monday.com/v2",
      json={"query": gql, "variables": variables},
      headers={
        "Authorization": MONDAY_API_TOKEN,
        "Content-Type":  "application/json"
      }
    )
    data2 = resp.json()

    # grab that single column_value
    cv = data2["data"]["items"][0]["column_values"][0]
    client = (cv.get("text") or cv.get("display_value") or "").strip()
    key    = client.lower()     # e.g. "yumi" or "vicky"

    group_id = CLIENT_TO_GROUP.get(key)
    if not group_id:
        print(f"[Monday→LINE] no mapping for “{client}” → {key}, skipping.")
        return "OK", 200

    item_name = evt.get("pulseName") or str(lookup_id)
    message   = f"📦 {item_name} 已送往機場，準備進行國際運輸。"

    push = requests.post(
      "https://api.line.me/v2/bot/message/push",
      headers={
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type":  "application/json"
      },
      json={"to": group_id, "messages":[{"type":"text","text":message}]}
    )
    print(f"[Monday→LINE] sent to {client}: {push.status_code}", push.text)

    return "OK", 200
    
# ─── Poller State Helpers & Job ───────────────────────────────────────────────
def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def check_te_updates():
    """Poll TE API every interval; push only newly changed statuses."""
    state = load_state()
    for group_id, keywords in CUSTOMER_FILTERS.items():
        lines = get_statuses_for(keywords)
        # skip header line[0], parse each “… @ timestamp”
        for line in lines[1:]:
            parts = line.rsplit("@", 1)
            if len(parts) != 2:
                continue
            order_key = parts[0]
            ts = parts[1].strip()
            if state.get(order_key) != ts:
                state[order_key] = ts
                payload = {
                    "to": group_id,
                    "messages": [{"type": "text", "text": line}]
                }
                requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    save_state(state)    
    
if __name__ == "__main__":
    # ─── start the TE poller ──────────────────────────────────────────────
    sched = BackgroundScheduler(timezone="America/Vancouver")
    # run every 30 minutes, Mon–Sat, between 04:00 (7 am ET) and 19:00 (7 pm PT)
    sched.add_job(
        check_te_updates,
        trigger="cron",
        day_of_week="mon-sat",
        hour="4-19",
        minute="0,30"
    )
    sched.start()

    # on some platforms APScheduler needs this to keep the main thread alive
    threading.Event().wait()

    # ─── start Flask ───────────────────────────────────────────────────────
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)))