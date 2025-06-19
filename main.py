import os
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
from datetime import timedelta, datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dateutil.parser import parse as parse_date
import openai
from collections import defaultdict
import threading
from typing import Optional

import io
from io import BytesIO
from PIL import Image, ImageFilter
from pyzbar.pyzbar import decode, ZBarSymbol
from pdf2image import convert_from_bytes  # 新增：將 PDF 頁面轉為影像供條碼掃描
from PyPDF2 import PdfReader  # 新增：解析 PDF 文字內容
import fitz  # PyMuPDF

import pytz

# Requires:
# pip install pymupdf pillow openai

# ─── Google Sheets 認證 ─────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
GA_SVC_INFO = json.loads(os.environ["GOOGLE_SVCKEY_JSON"])  # load your Google service account credentials from the env var
GC = gspread.service_account_from_dict(GA_SVC_INFO) # build a fully-authorized client
creds = ServiceAccountCredentials.from_json_keyfile_dict(GA_SVC_INFO, SCOPES)
gs = gspread.authorize(creds)
# 打開 Tracking 工作表
ws_tracking = gs.open_by_key("1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q") \
                .worksheet("Tracking")


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

# ─── LINE & ACE/SQ 設定 ──────────────────────────────────────────────────────
ACE_GROUP_ID     = os.getenv("LINE_GROUP_ID_ACE")
SOQUICK_GROUP_ID = os.getenv("LINE_GROUP_ID_SQ")
VICKY_GROUP_ID   = os.getenv("LINE_GROUP_ID_VICKY")
VICKY_USER_ID    = os.getenv("VICKY_USER_ID") 
YVES_USER_ID     = os.getenv("YVES_USER_ID") 
YUMI_GROUP_ID    = os.getenv("LINE_GROUP_ID_YUMI")

SQ_SHEET_URL     = os.getenv("SQ_SHEET_URL")


# Trigger when you see “週四出貨”/“週日出貨” + “麻煩請” + an ACE or 250N code,
# or when you see the exact phrase “這幾位還沒有按申報相符”
CODE_TRIGGER_RE = re.compile(r"\b(?:ACE|250N)\d+[A-Z0-9]*\b")
MISSING_CONFIRM = "這幾位還沒有按申報相符"

# Names to look for in each group’s list
VICKY_NAMES = {"顧家琪","顧志忠","周佩樺","顧郭蓮梅","廖芯儀","林寶玲"}
YUMI_NAMES  = {"劉淑燕","竇永裕","劉淑玫","劉淑茹","陳富美","劉福祥","郭淨崑"}
YVES_NAMES = {
    "梁穎琦",
    "張詠凱",
    "劉育伶",
    "羅唯英",
    "陳品茹",
    "張碧蓮",
    "吳政融",
    "解瑋庭",
    "洪君豪",
    "洪芷翎",
    "羅木癸",
    "洪金珠",
    "林憶慧",
    "葉怡秀",
    "葉詹明",
    "廖聰毅",
    "蔡英豪",
    "魏媴蓁",
    "黃淑芬",
    "解佩頴",
    "曹芷茜",
    "王詠皓",
    "曹亦芳",
    "李慧芝",
    "李錦祥",
    "詹欣陵",
    "陳志賢",
    "曾惠玲",
    "李白秀",
    "陳聖玄",
    "柯雅甄",
    "游玉慧",
    "游繼堯",
    "游承哲",
    "游傳杰",
    "陳秀華",
    "陳秀玲",
    "陳恒楷"
}
EXCLUDED_SENDERS = {"Yves Lai", "Yves KT Lai", "Yves MM Lai", "Yumi Liu", "Vicky Ku"}

# ─── Redis for state persistence ───────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is required for state persistence")
r = redis.from_url(REDIS_URL, decode_responses=True)

# pull your sheet URL / ID from env
VICKY_SHEET_URL = os.getenv("VICKY_SHEET_URL")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
TIMEZONE    = "America/Vancouver"

AIR_BOARD_ID = os.getenv("AIR_BOARD_ID")
AIR_PARENT_BOARD_ID = os.getenv("AIR_PARENT_BOARD_ID")

#STATE_FILE = os.getenv("STATE_FILE", "last_seen.json")
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
LINE_HEADERS = {
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

# ─── ADDED: Configure OpenAI API key ───────────────────────────────────────────
openai.api_key = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

OCR_SHIPPING_PROMPT = """
Task: Extract the following information from this shipping ticket
- Information of Sender on the `top-right corner`:
  - name
  - phone
  - client ID (the text on the third line between phone and address)
  - address
- Information of Receiver in the `SHIP TO` section
  - postal code(format `SNS NSN`, N stand for number and S stand for english character)
- Reference Number at the bottom after `Reference No.1:`
  - reference number
Response Format: {"sender": {"name": "", "phone": "", "client_id": "", "address": ""}, "receiver": {"postal_code": ""}, "reference number": ""}
* Do not include any extra text, explanation, or JSON outside of this format.
"""

TRACKING_PROMPT = """
Task: From this image of a shipping ticket page, extract ONLY the UPS tracking number.
The tracking number always starts with "1Z" and is alphanumeric.

Response Format (pure JSON):
{"tracking_number": ""}
* Do not include extra text or other fields.
"""

# keep an in-memory buffer of successfully updated tracking IDs per group
_pending = defaultdict(list)
_scheduled = set()

def strip_mention(line):
    # Remove an @mention at the very start of the line (e.g. "@Gorsky ")
    return re.sub(r"^@\S+\s*", "", line)

def _schedule_summary(group_id):
    """Called once per 30m window to send the summary and clear the buffer."""
    ids = _pending.pop(group_id, [])
    _scheduled.discard(group_id)
    if not ids:
        return
    # dedupe and format
    uniq = sorted(set(ids))
    text = "✅ Updated packages:\n" + "\n".join(f"- {tid}" for tid in uniq)
    payload = {
        "to": group_id,
        "messages": [{"type": "text", "text": text}]
    }
    requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)


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
    ts = str(int(datetime.now().timestamp()))
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
    # 4) format reply using each event’s own timestamp
    lines: list[str] = []
    for item in td.get("response", []):
        oid = item.get("id"); num = item.get("number","")
        events = item.get("list") or []
        if not events:
            lines.append(f"📦 {oid} ({num}) – 尚無追蹤紀錄")
            continue
        # pick the most recent event
        ev = max(events, key=lambda e: int(e["timestamp"]))
        loc_raw    = ev.get("location","")
        loc        = f"[{loc_raw.replace(',',', ')}] " if loc_raw else ""
        ctx_lc     = ev.get("context","").strip().lower()
        translated = TRANSLATIONS.get(ctx_lc, ev.get("context","").replace("Triple Eagle","system"))

        # derive the *real* event time from its epoch timestamp
        # 1) parse the numeric timestamp
        event_ts = int(ev["timestamp"])
        # 2) convert to a timezone‐aware datetime
        #    (make sure you have `import pytz` and `from datetime import datetime` at the top)
        tzobj = pytz.timezone(TIMEZONE)
        dt = datetime.fromtimestamp(event_ts, tz=tzobj)
        # 3) format it exactly like "Wed, 11 Jun 2025 15:05:46 -0700"
        tme = dt.strftime('%a, %d %b %Y %H:%M:%S %z')

        lines.append(f"📦 {oid} ({num}) → {loc}{translated}  @ {tme}")
    return lines

MONDAY_API_URL    = "https://api.monday.com/v2"
MONDAY_TOKEN      = os.getenv("MONDAY_TOKEN")
VICKY_SUBITEM_BOARD_ID = 9359342766    # 請填你 Vicky 子任務所在的 Board ID
VICKY_STATUS_COLUMN_ID = "status__1"   # 請填溫哥華收款那個欄位的 column_id

# ─── Vicky-reminder helpers ───────────────────────────────────────────────────    
def vicky_has_active_orders() -> list[str]:
    """
    Return a list of Vicky’s active UPS tracking numbers (the 1Z… codes).
    """
    # ── 1.1) 從 Monday 拿所有「狀態＝溫哥華收款」的 Subitem 名稱當 Tracking IDs ────────────────
    query = '''
    query ($boardId: ID!, $columnId: String!, $value: String!) {
      items_page_by_column_values(
        board_id: $boardId,
        limit: 100,
        columns: [{ column_id: $columnId, column_values: [$value] }]
      ) {
        items { name }
      }
    }
    '''
    variables = {
      "boardId": VICKY_SUBITEM_BOARD_ID,
      "columnId": VICKY_STATUS_COLUMN_ID,
      "value": "溫哥華收款"
    }
    resp = requests.post(
      MONDAY_API_URL,
      headers={ "Authorization": MONDAY_TOKEN, "Content-Type": "application/json" },
      json={ "query": query, "variables": variables }
    )
    data = resp.json().get("data", {}) \
                   .get("items_page_by_column_values", {}) \
                   .get("items", [])
    to_remind = [ item["name"].strip() for item in data if item.get("name") ]
    if not to_remind:
      return

    # 3) Fetch raw tracking info for exactly those TE IDs
    resp_tr = call_api("shipment/tracking", {
        "keyword": ",".join(to_remind),
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
    For any ACE message that contains “麻煩請” + “收到EZ way通知後” + (週四出貨 or 週日出貨),
    we will look up the *sheet* for the row whose date is closest to today, but ONLY
    for those “declaring persons” that actually appeared in the ACE text.  For each
    matching row, we pull the “sender” (column C) and push it privately if it's not in
    VICKY_NAMES or YUMI_NAMES or EXCLUDED_SENDERS.
    """
    text = event["message"]["text"]

    # Only trigger on the exact keywords
    if not (
        "麻煩請" in text
        and "收到EZ way通知後" in text
        and ("週四出貨" in text or "週日出貨" in text)
    ):
        return

    # ── 1) Extract declarer‐names from the ACE text ────────────────────────
    lines = text.splitlines()

    # find the line index that contains “麻煩請”
    try:
        idx_m = next(i for i, l in enumerate(lines) if "麻煩請" in l)
    except StopIteration:
        # If we can't find it, default to the top
        idx_m = 0

    # find the line index that starts with “收到EZ way通知後”
    try:
        idx_r = next(i for i, l in enumerate(lines) if l.startswith("收到EZ way通知後"))
    except StopIteration:
        idx_r = len(lines)

    # declarer lines are everything strictly between “麻煩請” and “收到EZ way通知後”
    raw_declarer_lines = lines[idx_m+1 : idx_r]
    declarer_names = set()

    for line in raw_declarer_lines:
        # Remove any ACE‐style code prefix (e.g. “ACE250605YL04 ”)
        cleaned = CODE_TRIGGER_RE.sub("", line).strip().strip('"')
        if not cleaned:
            continue

        # Take the first “token” as the actual name (before any phone or other columns)
        name_token = cleaned.split()[0]
        if name_token:
            declarer_names.add(name_token)

    if not declarer_names:
        # No valid declarers found in the message → nothing to do
        return

    # ── 2) Open the ACE sheet and find the “closest‐date” row ─────────────
    ACE_SHEET_URL = os.getenv("ACE_SHEET_URL")
    sheet = gs.open_by_url(ACE_SHEET_URL).sheet1
    data = sheet.get_all_values()  # raw rows as lists of strings

    today = datetime.now(timezone.utc).date()
    closest_date = None
    closest_diff = timedelta(days=9999)

    # Assume column A is date; skip header row at index 0, so start at row 2 in the sheet
    for row_idx, row in enumerate(data[1:], start=2):
        date_str = row[0].strip()
        if not date_str:
            continue
        try:
            row_date = parse_date(date_str).date()
        except Exception:
            continue

        diff = abs(row_date - today)
        if diff < closest_diff:
            closest_diff = diff
            closest_date = row_date

    if closest_date is None:
        # No parseable dates in sheet → bail out
        return

    # ── 3) Scan only the rows on that closest_date, and only if column B (declarer)
    #         is in our declarer_names set.  Then we grab column C (sender) for private push.
    results = set()

    for row_idx, row in enumerate(data[1:], start=2):
        date_str = row[0].strip()
        if not date_str:
            continue
        try:
            row_date = parse_date(date_str).date()
        except Exception:
            continue

        if row_date != closest_date:
            continue

        # Column B is at index 1 in 'row'
        declarer = row[1].strip() if len(row) > 1 else ""
        if not declarer or declarer not in declarer_names:
            continue

        # Column C is at index 2 in 'row' → this is the “sender” we want to notify
        sender = row[2].strip() if len(row) > 2 else ""
        if not sender:
            continue

        # Skip anyone already in VICKY_NAMES, YUMI_NAMES, or EXCLUDED_SENDERS
        if sender in VICKY_NAMES or sender in YUMI_NAMES or sender in EXCLUDED_SENDERS:
            continue

        results.add(sender)

    # ── 4) Push to Yves privately if any senders remain ────────────────────
    if results:
        header_payload = {
            "to": YVES_USER_ID,
            "messages": [{"type": "text", "text": "Ace散客EZWay需提醒以下寄件人："}]
        }
        requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=header_payload)

        for sender in sorted(results):
            payload = {
                "to": YVES_USER_ID,
                "messages": [{"type": "text", "text": sender}]
            }
            requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)

        print(f"DEBUG: Pushed {len(results)} sender(s) to Yves: {sorted(results)}")
    else:
        print("DEBUG: No matching senders found for any declarer in the ACE message.")

# ─── Soquick shipment-block handler ────────────────────────────────────────────
def handle_soquick_shipments(event):
    """
    Parse Soquick text containing "上周六出貨包裹的派件單號",
    split out lines of tracking+code+recipient, then push
    only the matching Vicky/Yumi lines + footer.
    """
    raw = event["message"]["text"]
    if "上周六出貨包裹的派件單號" not in raw:
        return

    # Split into non-empty lines
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    # Locate footer (starts with “您好”)
    footer_idx = next((i for i,l in enumerate(lines) if l.startswith("您好")), len(lines))
    header = lines[:footer_idx]
    footer = "\n".join(lines[footer_idx:])

    vicky, yumi = [], []
    for line in header:
        parts = line.split()
        if len(parts) < 3:
            continue
        recipient = parts[-1]
        if recipient in VICKY_NAMES:
            vicky.append(line)
        elif recipient in YUMI_NAMES:
            yumi.append(line)

    def push(group, msgs):
        if not msgs:
            return
        text = "\n".join(msgs) + "\n\n" + footer
        payload = {"to": group, "messages":[{"type":"text","text": text}]}
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent {len(msgs)} Soquick blocks to {group}: {resp.status_code}")

    push(VICKY_GROUP_ID, vicky)
    push(YUMI_GROUP_ID,  yumi)

def handle_soquick_full_notification(event):
    log.info(f"[SOQ FULL] invoked on text={event['message']['text']!r}")
    text = event["message"]["text"]
    """
    1) Parse the incoming text for “您好，請…” + “按申報相符”
    2) Split off the footer and extract all recipient names
    3) Push Vicky/Yumi group messages with their names + footer
    4) Look up those same names in col M of your Soquick sheet
       to find the corresponding senders in col C, and privately
       notify Yves of any senders not already in Vicky/Yumi/Excluded.
    """
    text = event["message"]["text"]
    if not ("您好，請" in text and "按" in text and "申報相符" in text):
        return

    # 1) extract lines & footer
    # split into non-empty lines and strip any leading @mention
    lines = [
        strip_mention(l.strip())
        for l in text.splitlines()
        if l.strip()
    ]
    try:
        footer_idx = next(i for i,l in enumerate(lines) if "您好，請" in l)
    except StopIteration:
        footer_idx = len(lines)
    recipients = lines[:footer_idx]
    footer     = "\n".join(lines[footer_idx:])

    # 2) split into Vicky / Yumi / “others” batches
    vicky_batch = [r for r in recipients if r in VICKY_NAMES]
    yumi_batch  = [r for r in recipients if r in YUMI_NAMES]
    other_recipients = [
        r for r in recipients
        if r not in VICKY_NAMES
           and r not in YUMI_NAMES
           and r not in EXCLUDED_SENDERS
    ]

    # ===== 插入這裡：列印 other_recipients =====
    log.info(f"[SOQ FULL][DEBUG] other_recipients = {other_recipients!r}")

    # dedupe
    def dedupe(seq):
        seen = set(); out=[]
        for x in seq:
            if x not in seen:
                seen.add(x); out.append(x)
        return out
    vicky_batch = dedupe(vicky_batch)
    yumi_batch  = dedupe(yumi_batch)
    other_recipients = dedupe(other_recipients)

    # 3) push the group notifications
    def push_group(group, batch):
        if not batch: return
        standard_footer = "您好，請提醒以上認證人按申報相符"
        msg = "\n".join(batch) + "\n\n" + standard_footer
        requests.post(
            LINE_PUSH_URL,
            headers=LINE_HEADERS,
            json={"to": group, "messages":[{"type":"text","text":msg}]}
        )

    # 這行取消註解就不會推給 Vicky
    push_group(VICKY_GROUP_ID, vicky_batch)
    push_group(YUMI_GROUP_ID,  yumi_batch)

    # ── Private “other” pushes ─────────────────────
    other_recipients = dedupe([
        r for r in recipients
        if r not in VICKY_NAMES
           and r not in YUMI_NAMES
           and r not in EXCLUDED_SENDERS
    ])
    log.info(f"[SOQ FULL][DEBUG] other_recipients = {other_recipients!r}")

    if other_recipients:
        # 依照訊息日期動態選分頁：前3天到後2天
        import datetime
        ts = event["timestamp"]                              # ms
        dt = datetime.datetime.fromtimestamp(ts/1000,         # +08:00
            tz=datetime.timezone(datetime.timedelta(hours=8)))
        # 候選日期字串：e.g. ['250611','250612','250613','250614','250615','250616']
        base = dt.date()
        candidates = [(base + datetime.timedelta(days=d)).strftime("%y%m%d")
                      for d in range(-3, 3)]
        ss = gs.open_by_url(SQ_SHEET_URL)
        found = [ws.title for ws in ss.worksheets() if ws.title in candidates]
        if len(found) == 1:
            sheet = ss.worksheet(found[0])
            log.info(f"[SOQ FULL][DEBUG] 使用分頁 {found[0]}")
        else:
            log.error(f"[SOQ FULL] 分頁數量不唯一，expected=1 got={len(found)}; candidates={candidates}, found={found}")
            return
        rows = sheet.get_all_values()[1:]  # skip header
        senders = set()

        for idx, row in enumerate(rows, start=2):
            # 印每一列 E 欄
            name_in_sheet = row[4].strip() if len(row) > 4 else ""
            log.info(f"[SOQ FULL][DEBUG] row {idx} colE = {name_in_sheet!r}")

            if name_in_sheet in other_recipients:
                sender = row[2].strip() if len(row) > 2 else ""
                log.info(f"[SOQ FULL][DEBUG] matched recipient {name_in_sheet!r} → sender {sender!r}")
                if sender and sender not in (VICKY_NAMES | YUMI_NAMES | EXCLUDED_SENDERS):
                    senders.add(sender)

        if senders:
            # header notification
            requests.post(
                LINE_PUSH_URL, headers=LINE_HEADERS,
                json={
                  "to": YVES_USER_ID,
                  "messages":[{"type":"text","text":"Soquick散客EZWay需提醒以下寄件人："}]
                }
            )
            for s in sorted(senders):
                requests.post(
                    LINE_PUSH_URL, headers=LINE_HEADERS,
                    json={"to": YVES_USER_ID, "messages":[{"type":"text","text":s}]}
                )
            log.info(f"[SOQ FULL] Privately pushed {len(senders)} senders to Yves")

 
# ─── 新增：處理「申報相符」提醒 ─────────────────────────
def handle_missing_confirm(event):
    text = event["message"]["text"]
    
    # 如果這是原始 EZ-Way 通知，就跳過
    if "收到EZ way通知後" in text:
        return
    
    # 如果訊息裡沒有「申報相符」，就跳過
    if "申報相符" not in text:
        return
        
    # 逐行找 ACE/250N 單號
    for l in text.splitlines():
        if CODE_TRIGGER_RE.search(l):
            parts = re.split(r"\s+", l.strip())
            # 確保至少有三段：單號、姓名、電話
            if len(parts) < 2:
                continue
            name = parts[1]
            if name in VICKY_NAMES:
                target = VICKY_GROUP_ID
            elif name in YUMI_NAMES:
                target = YUMI_GROUP_ID
            else:
                # 不是 Vicky 也不是 Yumi 的人，直接跳過
                continue
                
            # 推播姓名（你可以改成更完整的訊息）
            requests.post(
                LINE_PUSH_URL,
                headers=LINE_HEADERS,
                json={"to": target, "messages":[{"type":"text","text": f"{name} 尚未按申報相符"}]}
            )
 
# ─── Wednesday/Friday reminder callback ───────────────────────────────────────
def remind_vicky(day_name: str):
    """Send Vicky a one-per-day reminder at 17:30 if there are packages 
       beyond the two 'just created' statuses."""
    # ── 0) Idempotency guard: only once per day per day_name ───────────────
    tz = pytz.timezone(TIMEZONE)
    today_str = datetime.now(tz).date().isoformat()
    guard_key = f"vicky_reminder_{day_name}_{today_str}"
    if r.get(guard_key):
        return   
        
    # ── 1) Gather all Vicky order IDs ────────────────────────────────────
    resp_list = call_api("shipment/list")
    all_orders = resp_list.get("response", {}).get("list", []) or []
    vicky_ids = []
    for o in all_orders:
        oid = o.get("id")
        if not oid:
            continue
        det = call_api("shipment/detail", {"id": oid}).get("response", {})
        if isinstance(det, list): det = det[0]
        init = det.get("initiation", {})
        loc  = next(iter(init), None)
        name = init.get(loc, {}).get("name", "").lower() if loc else ""
        if any(kw in name for kw in CUSTOMER_FILTERS[VICKY_GROUP_ID]):
            vicky_ids.append(str(oid))
    if not vicky_ids:
        return

    # ── 2) Fetch tracking events and filter by status ───────────────────
    resp_tr = call_api("shipment/tracking", {
        "keyword": ",".join(vicky_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    }).get("response", []) or []

    SKIP_STATUSES = {
        "order created at triple eagle",
        "shipper created a label, ups has not received the package yet."
    }
    to_remind = []
    for item in resp_tr:
        num = item.get("number", "").strip()
        evs = item.get("list") or []
        if not num or not evs:
            continue
        latest = max(evs, key=lambda e: int(e.get("timestamp", 0)))
        ctx = latest.get("context", "").strip().lower()
        if ctx not in SKIP_STATUSES:
            to_remind.append(num)

    if not to_remind:
        return

    # ── 3) Assemble and send reminder (no sheet link) ──────────────────
    placeholder = "{user1}"
    header = (
        f"{placeholder} 您好，溫哥華倉庫預計{day_name}出貨，"
        "請麻煩填寫以下包裹的内容物清單。謝謝！"
    )
    body = "\n".join(to_remind)
    payload = {
        "to": VICKY_GROUP_ID,
        "messages": [{
            "type":        "textV2",
            "text":        "\n\n".join([header, body]),
            "substitution": {
                "user1": {
                    "type": "mention",
                    "mentionee": {
                        "type":   "user",
                        "userId": VICKY_USER_ID
                    }
                }
            }
        }]
    }
    try:
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        if resp.status_code == 200:
            # mark as sent for today
            r.set(guard_key, "1", ex=24*3600)
            log.info(f"Sent Vicky reminder for {day_name}: {len(to_remind)} packages")
        else:
            log.error(f"Failed to send Vicky reminder: {resp.status_code} {resp.text}")
    except Exception as e:
        log.error(f"Error sending Vicky reminder: {e}")


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
    # “others” = not in Vicky, not in Yumi, and not an excluded sender
    other_batch = [
        cleaned[i] for i, nm in enumerate(names_only)
        if nm not in VICKY_NAMES
           and nm not in YUMI_NAMES
           and nm not in YVES_NAMES
    ]    

    def push_to(group, batch, label):
        if not batch:
            # log.info(f"[ACE_SCHEDULE:{label}] batch empty, skipping")
            return
        
        # --- TEST MODE: just log the would-be message ---
        # rebuild the mini-message: header + blank + batch + blank + footer
        msg_lines = header + [""] + batch + [""] + footer
        final = "\n".join(msg_lines)
        # —– LOG instead —–
        # log.info(f"[ACE_SCHEDULE:{label}] to {group}:\n{final}")
        
        # real push to the group here
        requests.post(
            LINE_PUSH_URL,
            headers=LINE_HEADERS,
            json={"to": group, "messages":[{"type":"text","text": body }]}
        )
    
    # log.info(f"[ACE_SCHEDULE] vicky_batch={vicky_batch!r}, yumi_batch={yumi_batch!r}, others={other_batch!r}")
    push_to(VICKY_GROUP_ID, vicky_batch, label="VICKY")
    push_to(YUMI_GROUP_ID, yumi_batch,  label="YUMI")
    # your personal chat
    push_to(YVES_USER_ID, other_batch,  label="OTHERS")    

# ─── Ace shipment-block handler ────────────────────────────────────────────────
def handle_ace_shipments(event):
    """
    Splits the text into blocks starting with '出貨單號:', then
    forwards each complete block to Yumi or Vicky based on the
    recipient name.
    """
    # 1) Grab & clean the raw text
    raw = event["message"]["text"]
    log.info(f"[ACE SHIP] raw incoming text: {repr(raw)}")        # DEBUG log
    text = raw.replace('"', '').strip()                         # strip stray quotes
    
    # split into shipment‐blocks
    parts = re.split(r'(?=出貨單號:)', text)
    log.info(f"[ACE SHIP] split into {len(parts)} parts")         # DEBUG log
    
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

class LLMAgent:
    def __init__(self):
        # Initialize OpenAI client using env var
        import os
        api_key = os.getenv("OPENAI_API_KEY")
        self.client = openai.Client(api_key=api_key)

    def inference(self, messages):
        try:
            response = self.client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=messages,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error during inference: {e}")
            return None

# Convert PDF pages to PIL Images using PyMuPDF
def pdf_to_image(pdf_input, dpi=300):
    """
    Convert all pages of a PDF (path or bytes) to a list of PIL Image objects using PyMuPDF.
    """
    # 如果傳入的是 bytes 或 BytesIO，就用 stream 模式開啟
    if isinstance(pdf_input, (bytes, BytesIO)):
        doc = fitz.open(stream=pdf_input, filetype="pdf")
    else:
        doc = fitz.open(pdf_input)
        
    images = []
    # Calculate zoom factor to achieve desired DPI (default is 72)
    zoom = dpi / 72
    matrix = fitz.Matrix(zoom, zoom)

    for page in doc:
        pix = page.get_pixmap(matrix=matrix)
        img = Image.frombytes(
            "RGB",
            [pix.width, pix.height],
            pix.samples
        )
        images.append(img)
        
    # 走完所有頁後一次回傳完整列表
    return images  

# Extract text from images using the OpenAI API
def extract_text_from_images(image, prompt="Please extract text from this image."):
    """
    Sends each image to the LLM via base64-encoded data URI and returns a list of responses.
    Also saves each temporary image to disk under temp_images/.
    """
    agent = LLMAgent()
    
    # Corp second bar code for tracking number
    x, y, w, h = 120, 995, 750, 260
    cropped_img = image.crop((x, y, x + w, y + h))        
    decoded_objects = decode(cropped_img)
    tracking_number = decoded_objects[0].data.decode('utf-8')

    # Serialize image to JPEG bytes
    buf = BytesIO()
    image.save(buf, format="JPEG")
    img_bytes = buf.getvalue()
    buf.close()

    # Base64 encode
    b64 = base64.b64encode(img_bytes).decode("utf-8")
    data_uri = f"data:image/jpeg;base64,{b64}"

    # Build chat payload
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": data_uri}},
            ],
        }
    ]

    # Inference
    text = agent.inference(messages)
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        log.error(f"[PDF OCR] JSON parse failed, raw output → {text!r}")
        # 回傳原始字串以便後續檢查
        return {"_raw": text, "tracking number": tracking_number}
    result["tracking number"] = tracking_number
    return result

# ─── lookup_full_tracking 定義 ───────────────────
def lookup_full_tracking(ups_last4: str) -> Optional[str]:
    """
    在 Tracking 工作表的 S/T/U 欄找唯一尾號匹配，回傳完整追蹤碼或 None。
    """
    cols = [19, 20, 21]  # S=19, T=20, U=21
    matches = []
    for col_idx in cols:
        vals = ws_tracking.col_values(col_idx)
        for v in vals[1:]:
            v = v.strip()
            if len(v) >= 4 and v[-4:] == ups_last4:
                matches.append(v)
    if len(matches) != 1:
        log.warn(f"UPS尾號 {ups_last4} 找到 {len(matches)} 筆，不唯一，跳過")
        return None
    return matches[0]

# CLI entrypoint
def main():
    pdf_path = "U110252577.pdf"
    dpi = 300
    prompt = OCR_SHIPPING_PROMPT

    # Convert and extract
    images = pdf_to_image(pdf_path, dpi=dpi)
    text = extract_text_from_images(images, prompt=prompt)
    print(text)

# ─── Flask Webhook ────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    import re
    # Log incoming methods
    # print(f"[Webhook] Received {request.method} to /webhook")
    # log.info(f"Received {request.method} to /webhook")
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Webhook] Payload:", json.dumps(data, ensure_ascii=False))
    # log.info(f"Payload: {json.dumps(data, ensure_ascii=False)}")

    for event in data.get("events", []):
        # ignore non‐message events (eg. unsend)
        if event.get("type") != "message":
            continue
            
        # 立刻抓 source / group_id
        src = event["source"]
        group_id = src.get("groupId")
        msg      = event["message"]
        text     = msg.get("text", "").strip()
        mtype    = msg.get("type")
    
        # ─── PDF OCR trigger (only for a specific group) ─────────────────────
        if (
            msg.get("type") == "file"
            and msg.get("fileName", "").lower().endswith(".pdf")
            and src.get("type") == "group"
            and src.get("groupId") == "C1f77f5ef1fe48f4782574df449eac0cf"  # ← your group ID
        ):
            file_id = msg["id"]
            try:
                # 1) 下載 PDF 檔案
                resp = requests.get(
                    f"https://api-data.line.me/v2/bot/message/{file_id}/content",
                    headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                )
                resp.raise_for_status()

                # 2) PDF 轉圖像，拿到所有頁面
                from pdf2image import convert_from_bytes
                from io import BytesIO
                # 先試 pdf2image，若回傳空，就用 fallback
                images = convert_from_bytes(resp.content, dpi=300)
                if not images:
                    log.warning("[PDF OCR] convert_from_bytes returned empty, fallback to PyMuPDF")
                    images = pdf_to_image(BytesIO(resp.content), dpi=300)

                # 逐頁跑 OCR
                full_data = {}
                tracking_numbers = []
                for idx, img in enumerate(images, start=1):
                    try:
                        if idx == 1:
                            # 第1頁：抽全部欄位
                            full_data = extract_text_from_images(img, prompt=OCR_SHIPPING_PROMPT)
                            # 如果回傳 key 是 "tracking number"，改成下劃線
                            if "tracking number" in full_data:
                                full_data["tracking_number"] = full_data.pop("tracking number")
                            # 將第一頁的 tracking 也加入列表
                            tn = full_data.get("tracking_number")
                            if tn:
                                tracking_numbers.append(tn)
                            # 同理處理 reference number
                            if "reference number" in full_data:
                                full_data["reference_number"] = full_data.pop("reference number")
                        else:
                            # 後續頁：只抽 tracking_number
                            res = extract_text_from_images(img, prompt=TRACKING_PROMPT)
                            tn = res.get("tracking_number")
                            if not tn and "_raw" in res:
                                # e.g. res["_raw"] == '```json\n{"tracking_number": "1Z HF0 ..."}\n```'
                                raw = res["_raw"]
                                m = re.search(r"(1Z[\sA-Za-z0-9]+)", raw)
                                if m:
                                    tn = m.group(1).replace(" ", "")
                            if tn:
                                tracking_numbers.append(tn)
                    except Exception as e:
                        log.error(f"[PDF OCR] page {idx} failed: {e}", exc_info=True)

                # 合併並去重所有 tracking numbers
                full_data["all_tracking_numbers"] = sorted(dict.fromkeys(tracking_numbers))
                log.info(f"[PDF OCR] final data → {full_data}")                        

                ##5) 回傳同群組
                # requests.post(
                    # "https://api.line.me/v2/bot/message/push",
                    # headers=LINE_HEADERS,
                    # json={
                        # "to": src["groupId"],
                        # "messages": [{
                            # "type": "text",
                            # "text": json.dumps(full_data, ensure_ascii=False)
                        # }]
                    # }
                # )
                log.info(f"[PDF OCR] extracted → {full_data}")
                
                # ─── 6) Sheet 更新：以 reference_number 當作 Timestamp 搜尋，寫入追蹤碼 & 檢查 ABB ID
                ref_str = full_data.get("reference_number", "").strip()
                try:
                    # 確認是合法 timestamp
                    ts = parse_date(ref_str)
                except Exception:
                    log.error(f"[PDF OCR] reference_number '{ref_str}' is not a valid timestamp, abort sheet update.")
                else:
                    # 開啟試算表與 Tracking 試算表頁籤
                    SHEET_ID = "1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q"
                    WS_TITLE = "Tracking"
                    ss = gs.open_by_key(SHEET_ID)
                    ws = ss.worksheet(WS_TITLE)
                    # 在 A 欄找比對
                    values = ws.col_values(1)
                    row_idx = next((i for i,v in enumerate(values, start=1) if v.strip()==ref_str), None)
                    if not row_idx:
                        log.error(f"[PDF OCR] timestamp '{ref_str}' not found in sheet, abort sheet update.")
                    else:
                        # 填追蹤碼到 S(19), T(20), U(21)
                        for i, tn in enumerate(full_data.get("all_tracking_numbers", []), start=1):
                            if i>3: break
                            ws.update_cell(row_idx, 18+i, tn)
                            requests.post(
                                "https://api.line.me/v2/bot/message/push",
                                headers=LINE_HEADERS,
                                json={
                                    "to": "C1f77f5ef1fe48f4782574df449eac0cf",
                                    "messages": [{"type": "text", "text": "已上傳到Tracking Tab"}]
                                }
                            )                            
                        # 檢查 ABB會員帳號 (F=6)
                        sheet_abb = (ws.cell(row_idx,6).value or "").strip().lower()
                        client_id = full_data.get("client_id","").strip().lower()
                        # 只高亮 F<row_idx> 這個儲存格
                        cell = f"F{row_idx}"
                        fmt = {
                            "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}
                        }
                        ws.format(cell, fmt)

            except Exception as e:
                log.error(f"[PDF OCR] Failed to process PDF: {e}", exc_info=True)
                requests.post(
                    "https://api.line.me/v2/bot/message/push",
                    headers=LINE_HEADERS,
                    json={
                        "to": "C1f77f5ef1fe48f4782574df449eac0cf",
                        "messages": [{"type": "text", "text": "⚠️ 無法處理 PDF，請確認格式或內容是否清晰"}]
                    }
                )

            # ─── Create parent / subitems in Monday from PDF data ─────────────
            def adjust_caps(s: str) -> str:
                if s.isupper():
                    return " ".join(w.capitalize() for w in s.split())
                return s

            today       = datetime.now().strftime("%Y%m%d")
            adj_client  = adjust_caps(client_id)
            adj_name    = adjust_caps(name)
            parent_name = f"{today} {adj_client} - {adj_name}"

            headers = {"Authorization": MONDAY_API_TOKEN,"Content-Type":  "application/json"}

            # 1) lookup or create parent
            find_parent_q = f'''
            query {{
            items_by_column_values(
             board_id: {os.getenv("AIR_PARENT_BOARD_ID")},
             column_id: "name",
             column_value: "{parent_name}"
            ) {{ id }}
            }}
            '''
            resp = requests.post("https://api.monday.com/v2", headers=headers, json={"query": find_parent_q})
            items = resp.json().get("data", {}).get("items_by_column_values", [])
            if items:
                parent_id = items[0]["id"]
            else:
                create_parent_m = f'''
                mutation {{
                  create_item(
                    board_id: {os.getenv("AIR_PARENT_BOARD_ID")},
                    item_name: "{parent_name}"
                  ) {{ id }}
                }}
                '''
                resp = requests.post("https://api.monday.com/v2", headers=headers, json={"query": create_parent_m})
                parent_id = resp.json()["data"]["create_item"]["id"]

            # 2) create one subitem per tracking number
            for tn in full_data["all_tracking_numbers"]:
                create_sub_m = f'''
                mutation {{
                  create_subitem(
                    parent_item_id: {parent_id},
                    item_name: "{tn}"
                  ) {{ id }}
                }}
                '''
                requests.post("https://api.monday.com/v2", headers=headers, json={"query": create_sub_m})

            # 3) set 客人種類 to “早期代購” if name matches your Yumi/Liu or Vicky/Ku patterns
            if (("Yumi" in adj_name or "Shu-Yen" in adj_name) and "Liu" in adj_name) or (("Vicky" in adj_name or "Chia-Chi" in adj_name) and "Ku" in adj_name):
                set_type_m = f'''
                mutation {{
                  change_simple_column_value(
                    item_id: {parent_id},
                    board_id: {os.getenv("AIR_PARENT_BOARD_ID")},
                    column_id: "status_11__1",
                    value: "{{\\"label\\":\\"早期代購\\"}}"
                  ) {{ id }}
                }}
                '''
                requests.post("https://api.monday.com/v2", headers=headers, json={"query": set_type_m})

            # ─── Done parent/subitem creation ───────────────────────────────────
 
            # 中止後續處理這個 event
            return jsonify({}), 200
 
        # ─── If image, run ONLY the barcode logic and then continue ──────────
        if mtype == "image":
            is_from_me      = src.get("type") == "user"  and src.get("userId")  == YVES_USER_ID
            is_from_ace     = src.get("type") == "group" and src.get("groupId") == ACE_GROUP_ID
            is_from_soquick = src.get("type") == "group" and src.get("groupId") == SOQUICK_GROUP_ID
            if not (is_from_me or is_from_ace or is_from_soquick):
                continue

            try:
                # (1) Download raw image bytes from LINE
                message_id = event["message"]["id"]
                stream_resp = requests.get(
                    f"https://api-data.line.me/v2/bot/message/{message_id}/content",
                    headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                    stream=True
                )
                stream_resp.raise_for_status()
                chunks = []
                for chunk in stream_resp.iter_content(chunk_size=4096):
                    if chunk:
                        chunks.append(chunk)
                raw_bytes = b"".join(chunks)
                # log.info(f"[OCR] Downloaded {len(raw_bytes)} bytes from LINE")
                log.info(f"[BARCODE] Downloaded {len(raw_bytes)} bytes from LINE")

                # (2) Load into Pillow and auto‐crop to dark (text/barcode) region
                img = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
                                
                # ── DEBUG CHANGE: use full-resolution image, no thumbnail ──
                img_crop = img
                log.info(f"[BARCODE] Decoding full‐resolution image size {img_crop.size}")

                # (4) Decode any barcodes in the PIL image
                # Instead of decoding only CODE128, we now include multiple symbologies:
                decoded_objs = decode(
                    img_crop,
                    symbols=[ZBarSymbol.CODE128, ZBarSymbol.CODE39, ZBarSymbol.EAN13, ZBarSymbol.UPCA]
                )

                if not decoded_objs:
                    log.info("[BARCODE] No barcode detected in the image.")
                    # reply_payload = {
                        # "replyToken": event["replyToken"],
                        # "messages": [
                            # {
                                # "type": "text",
                                # "text": "No barcode detected. Please try again with a clearer image."
                            # }
                        # ]
                    # }
                    # requests.post(
                        # "https://api.line.me/v2/bot/message/reply",
                        # headers={
                            # "Content-Type": "application/json",
                            # "Authorization": f"Bearer {LINE_TOKEN}"
                        # },
                        # json=reply_payload
                    # )
                else:
                    # 1. Take the first decoded barcode as the Tracking ID
                    for obj in decoded_objs:
                        log.info(f"[BARCODE] Detected: {obj.type} → {obj.data.decode('utf-8')}")
                    tracking_raw = next(
                        (obj.data.decode("utf-8") for obj in decoded_objs if obj.data.decode("utf-8").startswith("1Z")),
                        decoded_objs[0].data.decode("utf-8")  # fallback
                    )

                    log.info(f"[BARCODE] First decoded raw data (tracking): {tracking_raw}")

                    # 2. If there is a tracking ID (we already decode it)
                    # tracking_id = decoded_objs[0].data.decode("utf-8").strip()
                    tracking_id = tracking_raw.strip()
                    log.info(f"[BARCODE] Decoded tracking ID: {tracking_id}")

                    # ─── Lookup the subitem directly on the subitem board via items_page_by_column_values ──────────────────────────────
                    q_search = """
                    query (
                      $boardId: ID!
                      $columnId: String!
                      $value: String!
                    ) {
                      items_page_by_column_values(
                        board_id: $boardId,
                        limit: 1,
                        columns: [
                          { column_id: $columnId, column_values: [$value] }
                        ]
                      ) {
                        items {
                          id
                          name
                        }
                      }
                    }
                    """
                    vars_search = {
                      "boardId":  os.getenv("AIR_BOARD_ID"),  # must be your subitem‐board ID
                      "columnId": "name",
                      "value":    tracking_id
                    }
                    r_search = requests.post(
                      "https://api.monday.com/v2",
                      headers={
                        "Authorization": MONDAY_API_TOKEN,
                        "Content-Type":  "application/json"
                      },
                      json={ "query": q_search, "variables": vars_search }
                    )
                    if r_search.status_code != 200:
                        log.error("[MONDAY] search failed %s: %s", r_search.status_code, r_search.text)
                        continue

                    items_page = r_search.json().get("data", {}) \
                                          .get("items_page_by_column_values", {}) \
                                          .get("items", [])
                    if not items_page:
                        log.warning(f"Tracking ID {tracking_id} not found in subitem board")
                        requests.post(
                          LINE_PUSH_URL, headers=LINE_HEADERS,
                          json={
                            "to": YVES_USER_ID,
                            "messages": [
                              {
                                "type": "text",
                                "text": f"⚠️ Tracking ID {tracking_id} not found in Monday."
                              }
                            ]
                          }
                        )
                        continue

                    found_subitem_id = items_page[0]["id"]
                    log.info(f"Found subitem {found_subitem_id} for {tracking_id}")
                                 
                    # STORE for next text event
                    pending_key = f"last_subitem_for_{group_id}"
                    r.set(pending_key, found_subitem_id, ex=300)
                    log.info(f"Stored subitem ID {found_subitem_id} for next text parsing (group {group_id})")
                    # ── END STORE ───────────────────────────────────────────────────────────────
###
                    # first decide location text based on which group this came from
                    src = event.get("source", {})

                    if group_id == ACE_GROUP_ID:
                        loc = "溫哥華倉A"
                    elif group_id == SOQUICK_GROUP_ID:
                        loc = "溫哥華倉S"
                    else:
                        # fallback or skip summary tracking if you prefer
                        loc = "Yves/Simply"

                    # ─── Update Location & Status ─────────────────────────────────────────
                    mutation = """
                    mutation ($itemId: ID!, $boardId: ID!, $columnVals: JSON!) {
                      change_multiple_column_values(
                        item_id: $itemId,
                        board_id: $boardId,
                        column_values: $columnVals
                      ) { id }
                    }
                    """
                    variables = {
                      "itemId":    found_subitem_id,
                      "boardId":   os.getenv("AIR_BOARD_ID"),  # same subitem‐board
                      "columnVals": json.dumps({
                        "location__1": { "label": loc },
                        "status__1":    { "label": "測量" }
                      })
                    }
                    up = requests.post(
                      "https://api.monday.com/v2",
                      headers={
                        "Authorization": MONDAY_API_TOKEN,
                        "Content-Type":  "application/json"
                      },
                      json={ "query": mutation, "variables": variables }
                    )
                    if up.status_code != 200:
                        log.error("[MONDAY] update failed %s: %s", up.status_code, up.text)
                    else:
                        log.info(f"Updated subitem {found_subitem_id}: location & status set")

                        # ─── BATCH SUMMARY TRACKING ───────────────────────────────────────
                        _pending[group_id].append(tracking_id)
                        if group_id not in _scheduled:
                            _scheduled.add(group_id)
                            # schedule the summary for this group in 30 minutes
                            threading.Timer(30*60, _schedule_summary, args=[group_id]).start()

                    # 3. If there is a second decoded value, extract the postal code portion
                    if len(decoded_objs) > 1:
                        postal_raw = decoded_objs[1].data.decode("utf-8")  # e.g. "420V6X1Z7"
                        # Extract everything after the first three characters:
                        postal_code = postal_raw[3:]  # yields "V6X1Z7"
                        log.info(f"[BARCODE] Extracted postal code (not printed): {postal_code}")

                        # 4. Save postal_code into memory (bio)
                        #    This call uses the 'bio' tool so that future conversations can recall it.
                        #    We do not print it to the user now.
                        # 
                        # Format: just the fact we want to remember, e.g. "Postal code V6X1Z7"
                        #
                        # (A separate tool call below will persist this memory.)

                        # ◆ ◆ ◆ Tool call follows below ◆ ◆ ◆

            except Exception:
                # Log any barcode or Monday API errors without replying to the chat
                log.error("[BARCODE] Error during image handling", exc_info=True)
                # log.error("[BARCODE] Error decoding barcode", exc_info=True)
                # Optionally, reply “NONE” or a helpful message:
                # error_payload = {
                    # "replyToken": event["replyToken"],
                    # "messages": [
                        # {
                            # "type": "text",
                            # "text": "An error occurred while reading the image. Please try again."
                        # }
                    # ]
                # }
                # requests.post(
                    # "https://api.line.me/v2/bot/message/reply",
                    # headers={
                        # "Content-Type": "application/json",
                        # "Authorization": f"Bearer {LINE_TOKEN}"
                    # },
                    # json=error_payload
                # )
            # now that images are handled, skip text logic
            continue
    
        # 0) 只處理文字
        if mtype != "text":
            continue
        
        # 1) 多筆 UPS 末四碼＋重量＋尺寸 一次處理
        # 同時支援「*」「×」「x」或「空白」分隔
        multi_pat = re.compile(
            r'(\d{4})\s+'             # 4位UPS尾號
            r'([\d.]+)kg\s+'          # 重量 (kg)
            r'(\d+)'                  # 寬
            r'(?:[×x*\s]+)'           # 允許 × x * 或空白 作為分隔
            r'(\d+)'                  # 高
            r'(?:[×x*\s]+)'           # 再次允許各種分隔
            r'(\d+)'                  # 深
            r'(?:cm)?',               # 可選的「cm」
            re.IGNORECASE
        )
        matches = multi_pat.findall(text)  # 找出所有符合格式的 tuple 列表

        if matches:
            for ups4, wt_str, w, h, d in matches:
                # —(1) 從 Google Sheets 找回完整追蹤碼
                full_no = lookup_full_tracking(ups4)
                if not full_no:
                    # 如果找不到或不唯一，跳過本筆
                    continue

                # —(2) 解析重量與尺寸
                weight_kg = float(wt_str)      # 將字串轉為 float
                dims_norm = f"{w}*{h}*{d}"    # 組成 "長*寬*高" 字串

                # —(3) 用完整追蹤碼到 Monday 查 subitem (Name 欄)
                find_q = f'''
                query {{
                  items_by_column_values(
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "name",
                    column_value: "{full_no}"
                  ) {{ id }}
                }}'''
                resp = requests.post(
                    "https://api.monday.com/v2",
                    headers={ "Authorization": MONDAY_API_TOKEN,
                              "Content-Type":  "application/json" },
                    json={ "query": find_q }
                )
                items = resp.json().get("data", {}) \
                                 .get("items_by_column_values", [])
                if not items:
                    log.warn(f"Monday: subitem 名稱={full_no} 找不到，跳過")
                    continue

                sub_id = items[0]["id"]  # 取第一個 match 的 subitem ID

                # —(4) 上傳尺寸 (__1__cm__1 欄)
                dim_mut = f'''
                mutation {{
                  change_simple_column_value(
                    item_id: {sub_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "__1__cm__1",
                    value: "{dims_norm}"
                  ) {{ id }}
                }}'''
                requests.post(
                    "https://api.monday.com/v2",
                    headers={ "Authorization": MONDAY_API_TOKEN,
                              "Content-Type":  "application/json" },
                    json={ "query": dim_mut }
                )

                # —(5) 上傳重量 (numeric__1 欄)
                wt_mut = f'''
                mutation {{
                  change_simple_column_value(
                    item_id: {sub_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "numeric__1",
                    value: "{weight_kg:.2f}"
                  ) {{ id }}
                }}'''
                requests.post(
                    "https://api.monday.com/v2",
                    headers={ "Authorization": MONDAY_API_TOKEN,
                              "Content-Type":  "application/json" },
                    json={ "query": wt_mut }
                )

                # —(6) 翻轉狀態到「溫哥華收款」(status__1 欄)
                stat_mut = f'''
                mutation {{
                  change_simple_column_value(
                    item_id: {sub_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "status__1",
                    value: "{{\\"label\\":\\"溫哥華收款\\"}}"
                  ) {{ id }}
                }}'''
                requests.post(
                    "https://api.monday.com/v2",
                    headers={ "Authorization": MONDAY_API_TOKEN,
                              "Content-Type":  "application/json" },
                    json={ "query": stat_mut }
                )

                # —(7) 日誌：確認更新完畢
                log.info(f"[UPS→Monday] {full_no} 更新: 重量={weight_kg}kg, 尺寸={dims_norm}")

            # 處理完所有多筆 UPS 後，跳過後續任何 handler
            continue

        # 2) pending_key 單筆 size/weight parser
        pending_key = f"last_subitem_for_{group_id}"
        sub_id = r.get(pending_key)
        if sub_id:
            size_text = text
            log.info(f"Parsing size_text for subitem {sub_id!r}: {size_text!r}")

            # parse weight
            wm = re.search(r"(\d+(?:\.\d+)?)\s*(kg|公斤|lbs?)", size_text, re.IGNORECASE)
            if wm:
                qty, unit = float(wm.group(1)), wm.group(2).lower()
                weight_kg = qty * (0.453592 if unit.startswith("lb") else 1.0)
                log.info(f"  → Parsed weight_kg: {weight_kg:.2f} kg")
            else:
                weight_kg = None

            # parse dimensions
            dm = re.search(
              # allow ×, x, *, or any whitespace between numbers
              r"(\d+(?:\.\d+)?)[×x*\s]+(\d+(?:\.\d+)?)[×x*\s]+(\d+(?:\.\d+)?)(?:\s*)(cm|公分|in|吋)?",
              size_text, re.IGNORECASE
            )
            if dm:
                # capture groups: 1=width, 2=height, 3=depth, 4=unit (optional)
                w, h, d = map(float, dm.group(1,2,3))
                unit = (dm.group(4) or "cm").lower()
                factor = 2.54 if unit.startswith(("in","吋")) else 1.0
                # use '*' between numbers, always
                dims_norm = f"{int(w*factor)}*{int(h*factor)}*{int(d*factor)}"
                log.info(f"  → Parsed dims_norm: {dims_norm}")
            else:
                dims_norm = None
                log.debug("  → No dimensions match")

            # helper to build the mutation
            def mutate(colId, val):
                return f'''
                mutation {{
                  change_simple_column_value(
                    item_id: {sub_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "{colId}",
                    value: "{val}"
                  ) {{ id }}
                }}'''

            # push dimensions if found
            if dims_norm:
                requests.post(
                  "https://api.monday.com/v2",
                  headers={ "Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json" },
                  json={ "query": mutate("__1__cm__1", dims_norm) }
                )

            # push weight if found
            if weight_kg is not None:
                requests.post(
                  "https://api.monday.com/v2",
                  headers={ "Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json" },
                  json={ "query": mutate("numeric__1", f"{weight_kg:.2f}") }
                )
                
            # now that we got weight, clear pending so we don't parse again
            r.delete(pending_key)
            log.info(f"Cleared pending for subitem {sub_id}")

            # ── if dims+weight and status is “測量”, bump to “溫哥華收款” ─────
            if dims_norm is not None and weight_kg is not None:
                status_mut = f'''
                mutation {{
                  change_column_value(
                    item_id: {sub_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "status__1",
                    value: "{{\\"label\\":\\"溫哥華收款\\"}}"
                  ) {{ id }}
                }}'''
                resp = requests.post(
                  "https://api.monday.com/v2",
                  headers={
                    "Authorization": MONDAY_API_TOKEN,
                    "Content-Type":  "application/json"
                  },
                  json={ "query": status_mut }
                )
                if resp.status_code == 200:
                    log.info(f"Updated status to 溫哥華收款 for subitem {sub_id}")
                else:
                    log.error(f"Failed to update status for subitem {sub_id}: {resp.text}")

            # whether dims or weight or both, log final
            log.info(f"Finished size/weight sync for subitem {sub_id}: dims={dims_norm!r}, weight={weight_kg!r}")
            continue
        
        # 3) Ace schedule (週四／週日出貨)
        if group_id == ACE_GROUP_ID and ("週四出貨" in text or "週日出貨" in text):
            handle_ace_schedule(event)
            continue

        # 4) ACE EZ-Way check
        if group_id == ACE_GROUP_ID:
            handle_ace_ezway_check_and_push(event)
            continue
        
        # ——— New: Richmond-arrival triggers content-request to Vicky —————————
        if group_id == VICKY_GROUP_ID and "[Richmond, Canada] 已到達派送中心" in text:
            # extract the tracking ID inside parentheses
            import re
            m = re.search(r"\(([^)]+)\)", text)
            if m:
                tracking_id = m.group(1)
            else:
                # no ID found, skip
                continue

            # build the mention message
            placeholder = "{user1}"
            msg = f"{placeholder} 請提供此包裹的內容物清單：{tracking_id}"
            substitution = {
                "user1": {
                    "type": "mention",
                    "mentionee": {
                        "type":   "user",
                        "userId": VICKY_USER_ID
                    }
                }
            }
            payload = {
                "to": VICKY_GROUP_ID,
                "messages": [{
                    "type":        "textV2",
                    "text":        msg,
                    "substitution": substitution
                }]
            }
            requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
            log.info(f"Requested contents list from Vicky for {tracking_id}")
            continue
                
        # ——— Soquick “上周六出貨包裹的派件單號” blocks ——————————————
        if group_id == SOQUICK_GROUP_ID and "上周六出貨包裹的派件單號" in text:
            handle_soquick_shipments(event)
            continue

        # ——— Soquick “請通知…申報相符” messages ——————————————
        log.info(
            "[SOQ DEBUG] group_id=%r, SOQUICK_GROUP_ID=%r, "
            "has_您好=%r, has_按=%r, has_申報相符=%r",
            group_id,
            SOQUICK_GROUP_ID,
            "您好，請" in text,
            "按" in text,
            "申報相符" in text,
        )        
        if (group_id == SOQUICK_GROUP_ID
            and "您好，請" in text
            and "按" in text
            and "申報相符" in text):
            handle_soquick_full_notification(event)
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
sched = BackgroundScheduler(timezone="America/Vancouver")

# ——— Vicky reminders (Wed & Fri at 18:00) ——————————————————————
sched.add_job(lambda: remind_vicky("星期四"),
              trigger="cron", day_of_week="wed", hour=18, minute=00)
sched.add_job(lambda: remind_vicky("週末"),
              trigger="cron", day_of_week="fri", hour=18, minute=00)

sched.start()
log.info("Scheduler started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
