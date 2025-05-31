import os
import time
import hmac
import hashlib
import requests
import json
import base64
import redis
import logging
import re
from urllib.parse import quote
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dateutil.parser import parse as parse_date
from datetime import timedelta


SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly","https://www.googleapis.com/auth/drive.metadata.readonly"]

# load your Google service account credentials from the env var
GA_SVC_INFO = json.loads(os.environ["GOOGLE_SVCKEY_JSON"])
# build a fully-authorized client
GC = gspread.service_account_from_dict(GA_SVC_INFO)
creds = ServiceAccountCredentials.from_json_keyfile_dict(GA_SVC_INFO, SCOPES)
gs = gspread.authorize(creds)



# ─── Structured Logging Setup ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ─── Customer Mapping ──────────────────────────────────────────────────────────
# Map each LINE group to the list of lowercase keywords you filter on
CUSTOMER_FILTERS = {
    os.getenv("LINE_GROUP_ID_YUMI"):   ["yumi", "shu-yen"],
    os.getenv("LINE_GROUP_ID_VICKY"):  ["vicky","chia-chi"]
}

# ─── Status Translations ──────────────────────────────────────────────────────
TRANSLATIONS = {
    "out for delivery today":         "今日派送中",
    "out for delivery":               "派送中",
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

# ─── Ace schedule config ──────────────────────────────────────────────────────
ACE_GROUP_ID   = os.getenv("LINE_GROUP_ID_ACE")
VICKY_GROUP_ID = os.getenv("LINE_GROUP_ID_VICKY")
VICKY_USER_ID  = os.getenv("VICKY_USER_ID") 
YVES_USER_ID   =os.getenv("YVES_USER_ID") 
YUMI_GROUP_ID  = os.getenv("LINE_GROUP_ID_YUMI")

# Trigger when you see “週四出貨”/“週日出貨” + “麻煩請” + an ACE or 250N code,
# or when you see the exact phrase “這幾位還沒有按申報相符”
CODE_TRIGGER_RE = re.compile(r"\b(?:ACE|250N)\d+[A-Z0-9]*\b")
MISSING_CONFIRM = "這幾位還沒有按申報相符"

# Names to look for in each group’s list
VICKY_NAMES = {"顧家琪","顧志忠","周佩樺","顧郭蓮梅","廖芯儀","林寶玲"}
YUMI_NAMES  = {"劉淑燕","竇永裕","劉淑玫","劉淑茹","陳富美","劉福祥","郭淨崑"}
EXCLUDED_SENDERS = {"Yves Lai", "Yves KT Lai", "Yves MM Lai", "Yumi Liu", "Vicky Ku"}

# ─── Redis for state persistence ───────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is required for state persistence")
r = redis.from_url(REDIS_URL, decode_responses=True)

# — set up Google sheets client once:
SCOPES = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(os.environ["GOOGLE_SVCKEY_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
gs = gspread.authorize(creds)

# pull your sheet URL / ID from env
VICKY_SHEET_URL = os.getenv("VICKY_SHEET_URL")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
TIMEZONE    = "America/Vancouver"

#STATE_FILE = os.getenv("STATE_FILE", "last_seen.json")
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

# ─── Vicky-reminder helpers ───────────────────────────────────────────────────    
def vicky_has_active_orders() -> list[str]:
    """
    Return a list of Vicky’s active UPS tracking numbers (the 1Z… codes).
    """
    # 1) Get all active TE order IDs
    resp_list = call_api("shipment/list")
    lst = resp_list.get("response", {}).get("list") or resp_list.get("response", [])
    te_ids: list[str] = [str(o["id"]) for o in lst if "id" in o]
    # 2) Filter those down to only Vicky’s orders
    vicky_ids: list[str] = []
    for oid in te_ids:
        det = call_api("shipment/detail", {"id": oid}).get("response", {})
        if isinstance(det, list):
            det = det[0]
        init = det.get("initiation", {})
        loc  = next(iter(init), None)
        name = init.get(loc, {}).get("name","").lower() if loc else ""
        if any(kw in name for kw in CUSTOMER_FILTERS[VICKY_GROUP_ID]):
            vicky_ids.append(oid)
    if not vicky_ids:
        return []

    # 3) Fetch raw tracking info for exactly those TE IDs
    resp_tr = call_api("shipment/tracking", {
        "keyword": ",".join(vicky_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    }).get("response", [])

    # 4) Extract the UPS “number” field
    tracking_numbers = [
        item.get("number", "").strip()
        for item in resp_tr
        if item.get("number")
    ]
    return tracking_numbers


def vicky_sheet_recently_edited():
    # 1) build a credentials object from your SERVICE_ACCOUNT JSON
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SVCKEY_JSON"]),
        scopes=SCOPES
    )

    # 2) fetch the spreadsheet’s Drive metadata
    drive = build("drive", "v3", credentials=creds)
    sheet_url = os.environ["VICKY_SHEET_URL"]
    file_id = sheet_url.split("/")[5]            # extract the ID from the URL
    meta = drive.files().get(
        fileId=file_id,
        fields="modifiedTime"
    ).execute()

    # 3) parse the ISO timestamp into a datetime
    last_edit = datetime.fromisoformat(meta["modifiedTime"].replace("Z","+00:00"))

    # 4) compare against now (UTC)
    age = datetime.now(timezone.utc) - last_edit
    return age.days < 3
  
def handle_ace_ezway_check_and_push(event):
    """
    For ACE messages containing “麻煩請” + “收到EZ way通知後” + (週四出貨 or 週日出貨),
    find any senders in the last 14 days from the Google Sheet who are NOT in
    VICKY_NAMES or YUMI_NAMES, and push them directly to Yves privately.
    """
    text = event["message"]["text"]

    # Only trigger if the message has all the required keywords
    if (
        "麻煩請" in text
        and "收到EZ way通知後" in text
        and ("週四出貨" in text or "週日出貨" in text)
    ):
        
        # Use the new ACE sheet URL from your environment
        ACE_SHEET_URL = os.getenv("ACE_SHEET_URL")
        sheet = gs.open_by_url(ACE_SHEET_URL).sheet1
        data = sheet.get_all_values()

        # Find the closest date to today
        today = datetime.now(timezone.utc).date()
        
        # Find the closest date
        closest_date = None
        closest_diff = timedelta(days=99999)  # large initial value

        # First, determine the closest date
        for row_idx, row in enumerate(data[1:], start=2):
            row_date_str = row[0].strip()
            if not row_date_str:
                print(f"DEBUG: Row {row_idx} skipped (empty date cell).")
                continue

            try:
                row_date = parse_date(row_date_str).date()
            except Exception as e:
                print(f"DEBUG: Row {row_idx} skipped (could not parse date '{row_date_str}'): {e}")
                continue

            diff = abs(row_date - today)
            # Debug: show parsed date and difference
            print(f"DEBUG: Row {row_idx} date parsed as {row_date}, diff from today: {diff}")
            
            if diff < closest_diff:
                closest_diff = diff
                closest_date = row_date
        
        # Debug: show which date was chosen as closest       
        print(f"Closest date (date only): {closest_date}")
        
        if closest_date is None:
            print("DEBUG: No valid dates found in sheet; exiting.")
            return        

        # 2) Collect senders on that closest date (excluding any names in your lists)
        results = set()
        
        for row_idx, row in enumerate(data[1:], start=2):
            row_date_str = row[0].strip()
            if not row_date_str:
                continue
                
            try:
                row_date = parse_date(row_date_str).date()
            except Exception:
                continue

            # Debug: show which rows exactly match the closest_date
            if row_date == closest_date:
                sender = row[2].strip() if len(row) > 2 else ""
                print(f"DEBUG: Row {idx} matches closest_date ({closest_date}). Sender name cell: '{sender}'")

                # Check exclusion lists
                if not sender:
                    print(f"DEBUG: Row {idx} has empty sender; skipping.")
                elif sender in VICKY_NAMES:
                    print(f"DEBUG: Row {idx} sender '{sender}' is in VICKY_NAMES; skipping.")
                elif sender in YUMI_NAMES:
                    print(f"DEBUG: Row {idx} sender '{sender}' is in YUMI_NAMES; skipping.")
                elif sender in EXCLUDED_SENDERS:
                    print(f"DEBUG: Row {idx} sender '{sender}' is in EXCLUDED_SENDERS; skipping.")
                else:
                    print(f"DEBUG: Row {idx} sender '{sender}' added to results.")
                    results.add(sender)                

#            if row_date == closest_date:
#                sender = row[2].strip()
#                if sender and sender not in VICKY_NAMES and sender not in YUMI_NAMES and sender not in EXCLUDED_SENDERS:
#                    results.add(sender)

        if results:
            # Push header to Yves privately
            header_payload = {
                "to": YVES_USER_ID,  # Push directly to your private LINE chat
                "messages": [{"type": "text", "text": "Ace散客EZWay需提醒以下寄件人："}]
            }
            requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=header_payload)

            # Push each sender name to Yves as separate messages
            for sender in sorted(results):
                payload = {
                    "to": YVES_USER_ID,
                    "messages": [{"type": "text", "text": sender}]
                }
                requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)

            print(f"DEBUG: Pushed {len(results)} filtered sender(s) to Yves privately: {sorted(results)}")
            log.info(f"Pushed {len(results)} filtered sender names to Yves privately.")
        else:
            print("DEBUG: No filtered senders found for the closest date.")
            log.info("No filtered senders found for the closest date.")
  
# ─── Wednesday/Friday reminder callback ───────────────────────────────────────
def remind_vicky(day_name: str):
    """Send Vicky a reminder at 5 PM PST on Wednesday/Friday if needed."""
    
    resp = None  # Initialize!
    
    # 1) Gather her active orders and check sheet edits
    oids = vicky_has_active_orders()
    if not oids or vicky_sheet_recently_edited():
        return

    # 2) Build the header 
    placeholder = "{user1}"
    header = (
        f"{placeholder} 您好，溫哥華倉庫{day_name}預計出貨。"
        "系統未偵測到内容物清單有異動，"
        "請麻煩填寫以下包裹的内容物清單。謝謝！"
    )

    # 3) Body is the list of tracking IDs (one per line)
    body = "\n".join(oids)

    # 4) Footer is your Google Sheet URL from env
    footer = os.getenv("VICKY_SHEET_URL")
    
    # 5) Assemble full text in one message
    full_text = "\n\n".join([header, body, footer])
    
    # 6) Build the substitution map for the mention
    substitution = {
        "user1": {
            "type": "mention",
            "mentionee": {
                "type":   "user",
                "userId": VICKY_USER_ID
            }
        }
    }    

    # 7) Send as a textV2 message
    payload = {
      "to": VICKY_GROUP_ID,
      "messages": [{
        "type":        "textV2",
        "text":        full_text,
        "substitution": substitution
      }]
    }
    
#    print("VICKY_GROUP_ID:", VICKY_GROUP_ID)
#    print("VICKY_USER_ID:", VICKY_USER_ID)
#    print("LINE_PUSH_URL:", LINE_PUSH_URL)
#    print("LINE_HEADERS:", LINE_HEADERS)
#    print("Payload:\n", json.dumps(payload, ensure_ascii=False, indent=2))
    
    try:
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent Vicky reminder for {day_name}: {len(oids)} orders (status {resp.status_code})")
    except Exception as e:
        log.error(f"Error sending LINE push: {e}")

    # Robust logging
    if resp:
        log.debug("Payload: %s", json.dumps(payload, ensure_ascii=False, indent=2))
        log.debug("Response body: %s", resp.text)
        log.debug("Response status: %s", resp.status_code)
        log.debug("Response headers:\n%s", resp.headers)




# ─── Ace schedule handler ─────────────────────────────────────────────────────
def handle_ace_schedule(event):
    """
    Extracts the Ace message, filters lines for Yumi/Vicky,
    and pushes a cleaned summary into their groups with the names
    inserted between 麻煩請 and 收到EZ way通知後…
    """
    text     = event["message"]["text"]
    # split into lines
    lines = text.splitlines()

    # find the index of the “麻煩請” line
    try:
        idx_m = next(i for i,l in enumerate(lines) if "麻煩請" in l)
    except StopIteration:
        idx_m = 1  # fallback just after the first line

    # find the index of the “收到EZ way通知後” line
    try:
        idx_r = next(i for i,l in enumerate(lines) if l.startswith("收到EZ way通知後"))
    except StopIteration:
        idx_r = len(lines)

    # header before names: up through 麻煩請
    header = lines[: idx_m+1 ]

    # footer after names: from 收到EZ way通知後 onward
    footer = lines[ idx_r: ]

    # collect only the code lines (ACE/250N+name)
    code_lines = [l for l in lines if CODE_TRIGGER_RE.search(l)]

    # strip off the code prefix from each
    cleaned = [ CODE_TRIGGER_RE.sub("", l).strip() for l in code_lines ]
    
    # strip the code prefix and any stray quotes
    cleaned = [
        CODE_TRIGGER_RE.sub("", l).strip().strip('"')
        for l in code_lines
    ]    

    # now split into per-group lists
    vicky_batch = [c for c in cleaned if any(name in c for name in VICKY_NAMES)]
    yumi_batch  = [c for c in cleaned if any(name in c for name in YUMI_NAMES )]

    def push_to(group, batch):
        if not batch:
            return
        
        # first, strip out any pure-quote lines and remove quotes from the rest
        clean_batch = []
        for line in batch:
            # remove leading/trailing whitespace and quotation marks
            stripped = line.strip().strip('"')
            if stripped:                   # skip empty / quote-only lines
                clean_batch.append(stripped)
        
        # build the new message: header, blank line, names, blank line, footer
        message = []
        message += header
        message += [""]       # blank line
        message += batch
        message += [""]       # blank line
        message += footer

        payload = {
            "to": group,
            "messages": [{"type":"text","text":"\n".join(message)}]
        }
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Pushed Ace summary to {group}: {resp.status_code}")

    push_to(VICKY_GROUP_ID, vicky_batch)
    push_to(YUMI_GROUP_ID,  yumi_batch)

# ─── Ace shipment-block handler ────────────────────────────────────────────────
def handle_ace_shipments(event):
    """
    Splits the text into blocks starting with '出貨單號:', then
    forwards each complete block to Yumi or Vicky based on the
    recipient name.
    """
    text = event["message"]["text"]
    # split into shipment‐blocks
    parts = re.split(r'(?=出貨單號:)', text)
    vicky, yumi = [], []

    for blk in parts:
        if "出貨單號:" not in blk or "宅配單號:" not in blk:
            continue
        lines = [l.strip() for l in blk.strip().splitlines() if l.strip()]
        if len(lines) < 4:
            continue
        # recipient name is on line 3
        recipient = lines[2].split()[0]
        full_msg  = "\n".join(lines)
        if recipient in VICKY_NAMES:
            vicky.append(full_msg)
        elif recipient in YUMI_NAMES:
            yumi.append(full_msg)

    def push(group, messages):
        if not messages:
            return
        payload = {
            "to": group,
            "messages":[{"type":"text","text":"\n\n".join(messages)}]
        }
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent {len(messages)} shipment blocks to {group}: {resp.status_code}")

    push(VICKY_GROUP_ID, vicky)
    push(YUMI_GROUP_ID,  yumi)


# ─── Flask Webhook ────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Log incoming methods
    print(f"[Webhook] Received {request.method} to /webhook")
    log.info(f"Received {request.method} to /webhook")
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Webhook] Payload:", json.dumps(data, ensure_ascii=False))
    log.info(f"Payload: {json.dumps(data, ensure_ascii=False)}")

    for event in data.get("events", []):
        # Only handle text messages
        if event.get("type") != "message" or event["message"].get("type") != "text":
            continue
            
        group_id = event["source"].get("groupId")
        text     = event["message"]["text"].strip()
        
        print(f"[Debug] incoming groupId: {group_id!r}")
        print(f"[Debug] CUSTOMER_FILTERS keys: {list(CUSTOMER_FILTERS.keys())!r}")
        
        print(f"[Webhook] Detected groupId: {group_id}, text: {text}")
        
        # ——— 1) Ace schedule / missing-confirmation trigger ——————————
        is_schedule = (
            ("週四出貨" in text or "週日出貨" in text)
            and "麻煩請" in text
            and CODE_TRIGGER_RE.search(text)
        )
        is_missing = MISSING_CONFIRM in text
        
        # NEW: detect pure-shipment blocks
        is_shipment = (
            "出貨單號" in text
            and "宅配單號" in text
            and CODE_TRIGGER_RE.search(text)
        )    

        if group_id == ACE_GROUP_ID:
            # 2a) schedule-style notice
            if is_schedule or is_missing:
                handle_ace_schedule(event)
                handle_ace_ezway_check_and_push(event)
                continue
            # 2b) shipment-block notice
            if is_shipment:
                handle_ace_shipments(event)
                continue            

        # 2) Your existing “追蹤包裹” logic
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
            log.info(f"LINE reply status={resp.status_code}, body={resp.text}")

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
        log.warning(f"No mapping for client={client} key={key}, skipping.")
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
    log.info(f"Monday→LINE push status={push.status_code}, body={push.text}")

    return "OK", 200
    
# ─── Poller State Helpers & Job ───────────────────────────────────────────────
# ─── Helpers for parsing batch lines ─────────────────────────────────────────
def extract_order_key(line: str) -> str:
    return line.rsplit("@",1)[0].strip()

def extract_timestamp(line: str) -> str:
    return line.rsplit("@",1)[1].strip()

def load_state():
    """Fetch the JSON-encoded map of order_key→timestamp from Redis."""
    data = r.get("last_seen")
    return json.loads(data) if data else {}

def save_state(state):
    """Persist the map of order_key→timestamp back to Redis."""
    r.set("last_seen", json.dumps(state))

def check_te_updates():
    """Poll TE API every interval; push only newly changed statuses."""
    state = load_state()
    for group_id, keywords in CUSTOMER_FILTERS.items():
        lines = get_statuses_for(keywords)
        new_lines = []
        for line in lines[1:]:
            ts = extract_timestamp(line)
            key = extract_order_key(line)
            if state.get(key) != ts:
                state[key] = ts
                new_lines.append(line)
        if new_lines:
            payload = {
                "to": group_id,
                "messages": [{
                    "type": "text",
                    "text": "\n\n".join(new_lines)
                }]
            }
            requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    save_state(state)   

# ─── Poller + Scheduler Bootstrap ────────────────────────────────────────────
if __name__ == "__main__":
    sched = BackgroundScheduler(timezone="America/Vancouver")
    sched.add_job(
        check_te_updates,
        trigger="cron",
        day_of_week="mon-sat",
        hour="4-19",
        minute="0,30"
    )
    
    # ——— Vicky reminders ——————————————————————
    sched.add_job(lambda: remind_vicky("星期四"),
                  trigger="cron", day_of_week="wed", hour=17, minute=0)
    sched.add_job(lambda: remind_vicky("週末"),
                  trigger="cron", day_of_week="fri", hour=17, minute=0)    
    
    sched.start()
    log.info("Scheduler started")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
