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

from services.ocr_engine import OCRAgent
from services.monday_service import MondaySyncService
from services.shipment_parser import ShipmentParserService

from services.twws_service import get_twws_value_by_name

from jobs.ace_tasks import push_ace_today_shipments
from jobs.sq_tasks import push_sq_weekly_shipments

# Requires:
# pip install pymupdf pillow openai

# ─── Google Sheets 認證（環境變數 + lazy init）─────────────────────────
import os, json, base64, gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_gs = None  # lazy singleton, avoid authenticating at import time

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
GORSKY_USER_ID   = os.getenv("GORSKY_USER_ID")
SOQUICK_GROUP_ID = os.getenv("LINE_GROUP_ID_SQ")
VICKY_GROUP_ID   = os.getenv("LINE_GROUP_ID_VICKY")
VICKY_USER_ID    = os.getenv("VICKY_USER_ID") 
YVES_USER_ID     = os.getenv("YVES_USER_ID") 
YUMI_GROUP_ID    = os.getenv("LINE_GROUP_ID_YUMI")
JOYCE_GROUP_ID   = os.getenv("LINE_GROUP_ID_JOYCE")
IRIS_GROUP_ID    = os.getenv("LINE_GROUP_ID_IRIS")
PDF_GROUP_ID     = os.getenv("LINE_GROUP_ID_PDF")

SQ_SHEET_URL     = os.getenv("SQ_SHEET_URL")
ACE_SHEET_URL = os.getenv("ACE_SHEET_URL")

# --- Timezone (used by schedulers) ---
TIMEZONE = os.getenv("TIMEZONE", "America/Vancouver")

# Trigger when you see “週四出貨”/“週日出貨” + “麻煩請” + an ACE or 250N code,
# or when you see the exact phrase “這幾位還沒有按申報相符”
CODE_TRIGGER_RE = re.compile(r"\b(?:ACE|\d+N)\d*[A-Z0-9]*\b")
MISSING_CONFIRM = "這幾位還沒有按申報相符"

# Names to look for in each group’s list
VICKY_NAMES = {"顧家琪","顧志忠","周佩樺","顧郭蓮梅","廖芯儀","林寶玲","高懿欣","崔書鳳","周志明"}
YUMI_NAMES  = {"劉淑燕","竇永裕","劉淑玫","劉淑茹","陳富美","劉福祥","郭淨崑","陳卉怡","洪瑜駿","李祈霈","邱啓倫","許霈珩"}
IRIS_NAMES  = {"廖偉廷","廖本堂","李成艷"}
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
    "鄭詠渝",
    "鄭芸婷",
    "游繼堯",
    "游承哲",
    "游傳杰",
    "陳秀華",
    "陳秀玲",
    "陳恒楷"
}
EXCLUDED_SENDERS = {"Yves Lai", "Yves KT Lai", "Yves MM Lai", "Yumi Liu", "Vicky Ku"}

# ──────────────────────────────────────────────────────────────────────────────
# ACE「今日出貨」：排程 + 手動觸發
# - 來源：ACE_SHEET_URL 指向的 Google Sheet
# - 規則：找出「今天」在欄 A 的所有列 → 取該列的欄 B（Box ID），組成訊息推播
# - 時間：每週四、週日下午 4:00（America/Vancouver）
# - 手動：在 ACE 群組輸入「已上傳資料可出貨」立即觸發（不受每日防重複限制）
# ──────────────────────────────────────────────────────────────────────────────

# ─── Redis for state persistence ───────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is required for state persistence")
r = redis.from_url(REDIS_URL, decode_responses=True)

# pull your sheet URL / ID from env
VICKY_SHEET_URL = os.getenv("VICKY_SHEET_URL")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

AIR_BOARD_ID = os.getenv("AIR_BOARD_ID")
AIR_PARENT_BOARD_ID = os.getenv("AIR_PARENT_BOARD_ID")

#STATE_FILE = os.getenv("STATE_FILE", "last_seen.json")
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
LINE_HEADERS = {
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}
LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"


from apscheduler.schedulers.background import BackgroundScheduler
import atexit

# ── APScheduler 註冊：每週六 09:00 America/Vancouver 觸發 ────────────────
_sq_scheduler = None
def _ensure_scheduler_for_sq_weekly():
    """
    以背景排程方式，固定每週六 09:00（America/Vancouver）執行
    push_sq_weekly_shipments(force=False)。
    """
    global _sq_scheduler
    if _sq_scheduler is not None:
        return _sq_scheduler

    tz = pytz.timezone(TIMEZONE)
    sched = BackgroundScheduler(timezone=tz)
    sched.add_job(
        push_sq_weekly_shipments,
        trigger="cron",
        day_of_week="sat",
        hour=9,
        minute=0,
        kwargs={"force": False},
        id="sq_weekly_shipments_sat_9am",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))
    log.info("[SQ Weekly] Scheduler started (Sat 09:00 America/Vancouver).")

    _sq_scheduler = sched
    return _sq_scheduler

def get_gspread_client():
    """Authorize gspread using env vars. Prefers GCP_SA_JSON_BASE64; falls back to GOOGLE_SVCKEY_JSON."""
    global _gs
    if _gs is not None:
        return _gs

    # Prefer the base64 var you added on Heroku
    b64 = os.getenv("GCP_SA_JSON_BASE64", "")
    json_inline = os.getenv("GOOGLE_SVCKEY_JSON", "")

    if b64:
        info = json.loads(base64.b64decode(b64))
    elif json_inline:
        # Back-compat: if you're still providing raw JSON text in GOOGLE_SVCKEY_JSON
        info = json.loads(json_inline)
    else:
        raise RuntimeError("Missing credentials: set GCP_SA_JSON_BASE64 (preferred) or GOOGLE_SVCKEY_JSON")

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)

    # Only if you intentionally use Workspace domain-wide delegation:
    delegate = os.getenv("GSUITE_DELEGATE")
    if delegate:
        creds = creds.with_subject(delegate)

    _gs = gspread.authorize(creds)
    return _gs

# --- Debug: print SA client_email once on startup (safe) ---
try:
    import os, json, base64
    sa_json = None
    if os.getenv("GCP_SA_JSON_BASE64"):
        sa_json = base64.b64decode(os.getenv("GCP_SA_JSON_BASE64")).decode("utf-8", "ignore")
    elif os.getenv("GOOGLE_SVCKEY_JSON"):
        sa_json = os.getenv("GOOGLE_SVCKEY_JSON")
    if sa_json:
        client_email = json.loads(sa_json).get("client_email")
        if client_email:
            print(f"[GSHEET] service account email = {client_email}")
except Exception as _e:
    print("[GSHEET] could not print service account email:", _e)

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

# 模組載入時就確保 SQ 排程啟動（與 ACE 的 _ensure_scheduler_for_ace_today 並存）
try:
    _ensure_scheduler_for_sq_weekly()
except Exception as _e:
    log.error(f"[SQ Weekly] Scheduler init failed: {_e}")

# ── APScheduler 註冊：每週四＆週日 16:00 America/Vancouver 觸發 ────────────────
_scheduler = None
def _ensure_scheduler_for_ace_today():
    """
    以背景排程方式，固定在每週四與週日的 16:00（America/Vancouver）執行
    push_ace_today_shipments(force=False)。
    - 使用 coalesce / max_instances 來避免重啟造成的堆疊觸發
    - 使用 misfire_grace_time 允許短暫喚醒延遲
    """
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    tz = pytz.timezone(TIMEZONE)
    sched = BackgroundScheduler(timezone=tz)
    sched.add_job(
        push_ace_today_shipments,
        trigger="cron",
        day_of_week="thu,sun",
        hour=16,
        minute=0,
        kwargs={"force": False},  # 排程呼叫，一律非 force
        id="ace_today_shipments_thu_sun_4pm",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))
    log.info("[ACE Today] Scheduler started (Thu/Sun 16:00 America/Vancouver).")

    _scheduler = sched
    return _scheduler

# 模組匯入時就確保排程啟動（多次匯入也安全）
try:
    _ensure_scheduler_for_ace_today()
except Exception as _e:
    log.error(f"[ACE Today] Scheduler init failed: {_e}")

# ─── ADDED: Configure OpenAI API key ───────────────────────────────────────────
openai.api_key = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

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

# Helper for sending single final LINE message for uploading PDF
def _line_push(to: str, text: str):
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers=LINE_HEADERS,
            json={"to": to, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        log.error(f"[LINE PUSH] failed: {e}")


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
VICKY_SUBITEM_BOARD_ID = 4815120249    # 請填你 Vicky 子任務所在的 Board ID
VICKY_STATUS_COLUMN_ID = "status__1"   # 請填溫哥華收款那個欄位的 column_id

# ─── Vicky-reminder helpers ───────────────────────────────────────────────────(under construction)    
def vicky_has_active_orders() -> list[str]:
    """
    Return a list of Vicky’s active UPS tracking numbers (the 1Z… codes).
    """
    # include parent_item.name so we can filter only Vicky’s
    query = '''
    query ($boardId: ID!, $columnId: String!, $value: String!) {
      items_page_by_column_values(
        board_id: $boardId,
        limit: 100,
        columns: [{ column_id: $columnId, column_values: [$value] }]
      ) {
        items {
          name
          parent_item { name }
        }
      }
    }
    '''
    # 查詢多種需提醒的狀態
    statuses = ["收包裹", "測量", "重新包裝", "提供資料", "溫哥華收款"]
    to_remind = []
    
    for status in statuses:
        log.info(f"[vicky_has_active_orders] querying status {status!r}")
        resp = requests.post(
            MONDAY_API_URL,
            headers={ "Authorization": f"Bearer {MONDAY_TOKEN}", "Content-Type": "application/json" },
            json={ "query": query, "variables": {
                "boardId": VICKY_SUBITEM_BOARD_ID,
                "columnId": VICKY_STATUS_COLUMN_ID,
                "value": status
            }}
        )
        items = resp.json()\
                   .get("data", {})\
                   .get("items_page_by_column_values", {})\
                   .get("items", [])

        # keep only Vicky’s
        filtered = [
            itm["name"].strip()
            for itm in items
            if itm.get("parent_item", {}).get("name", "").find("Vicky") != -1
        ]
        log.info(f"[vicky_has_active_orders] {len(filtered)} of {len(items)} are Vicky’s for {status!r}")
        to_remind.extend(filtered)
    
    # 去重排序
    to_remind = sorted(set(to_remind))
    
    if not to_remind:
      return []

    # 3) We already have the subitem names (tracking IDs) in to_remind:
    return to_remind

# ─── Wednesday/Friday reminder callback ───────────────────────────────────────
def remind_vicky(day_name: str):
    log.info(f"[remind_vicky] Called for {day_name}")
    tz = pytz.timezone(TIMEZONE)
    today_str = datetime.now(tz).date().isoformat()
    guard_key = f"vicky_reminder_{day_name}_{today_str}"
    log.info(f"[remind_vicky] guard_key={guard_key!r}, existing={r.get(guard_key)!r}")
    if r.get(guard_key):
        log.info("[remind_vicky] Skipping because guard is set")
        return  
         
    # 1) Grab Monday subitems in the statuses you care about
    to_remind_ids = vicky_has_active_orders()  # returns list of TE IDs from Monday
    log.info(f"[remind_vicky] vicky_has_active_orders → {to_remind_ids!r}")
    if not to_remind_ids:
        log.info("[remind_vicky] No subitems in statuses to remind, exiting")
        return

    # 2) Use the subitem names directly as the list to remind
    to_remind = to_remind_ids

    if not to_remind:
        log.info("[remind_vicky] No tracking numbers found, exiting")
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

# def vicky_sheet_recently_edited():
    ##1) build a credentials object from your SERVICE_ACCOUNT JSON
    # creds = Credentials.from_service_account_info(
        # json.loads(os.environ["GOOGLE_SVCKEY_JSON"]),
        # scopes=SCOPES
    # )

    ##2) fetch the spreadsheet’s Drive metadata
    # drive = build("drive", "v3", credentials=creds)
    # sheet_url = os.environ["VICKY_SHEET_URL"]
    # file_id = sheet_url.split("/")[5]            # extract the ID from the URL
    # meta = drive.files().get(
        # fileId=file_id,
        # fields="modifiedTime"
    # ).execute()

    ##3) parse the ISO timestamp into a datetime
    # last_edit = datetime.fromisoformat(meta["modifiedTime"].replace("Z","+00:00"))

    ##4) compare against now (UTC)
    # age = datetime.now(timezone.utc) - last_edit
    # return age.days < 3
  
def handle_ace_ezway_check_and_push_to_yves(event):
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
    gs = get_gspread_client()
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

# ─── Soquick & Ace shipment-block handler ────────────────────────────────────────────
def handle_soquick_and_ace_shipments(event):
    """
    Parse Soquick & Ace text containing "上周六出貨包裹的派件單號", "出貨單號", "宅配單號"
    split out lines of tracking+code+recipient, then push
    only the matching Vicky/Yumi lines + footer.
    """
    raw = event["message"]["text"]
    if "上周六出貨包裹的派件單號" not in raw and not ("出貨單號" in raw and "宅配單號" in raw):
        return

    vicky, yumi = [], []

    # — Soquick flow —
    if "上周六出貨包裹的派件單號" in raw:
        # Split into non-empty lines
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        # Locate footer (starts with “您好”)
        footer_idx = next((i for i,l in enumerate(lines) if l.startswith("您好")), len(lines))
        header = lines[:footer_idx]
        footer = "\n".join(lines[footer_idx:])

        for line in header:
            parts = line.split()
            if len(parts) < 3:
                continue
            recipient = parts[-1]
            if recipient in VICKY_NAMES:
                vicky.append(line)
            elif recipient in YUMI_NAMES:
                yumi.append(line)

    # — Ace flow —
    else:
        # split into one block per “出貨單號:” line
        blocks = [b.strip().strip('"') for b in re.split(r'(?=出貨單號:)', raw) if b.strip()]
        
        for blk in blocks:
            # strip whitespace and any wrapping quotes
            block = blk.strip().strip('"')
            if not block:
                continue
            # must contain both 出貨單號 and 宅配單號
            if "出貨單號" not in block or "宅配單號" not in block:
                continue
            lines = block.splitlines()
            if len(lines) < 3:
                continue
            recipient = lines[2].split()[0]
            if recipient in VICKY_NAMES:
                vicky.append(block)
            elif recipient in YUMI_NAMES:
                yumi.append(block)

    def push(group, msgs):
        if not msgs:
            return
        
        # choose formatting per flow
        if "上周六出貨包裹的派件單號" in raw:
            text = "\n".join(msgs) + "\n\n" + footer
        else:
            text = "\n\n".join(msgs)
        payload = {"to": group, "messages":[{"type":"text","text": text}]}
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent {len(msgs)} Soquick blocks to {group}: {resp.status_code}")

    push(VICKY_GROUP_ID, vicky)
    push(YUMI_GROUP_ID,  yumi)

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

# ─── UPS tracking normalization ───────────────────
def normalize_ups(trk: str) -> str:
    s = re.sub(r'[^A-Za-z0-9]', '', trk or '').upper()
    if s.startswith('1Z'):
        head, tail = s[:2], s[2:]
        tail = tail.replace('O', '0')  # OCR fix: O→0 after 1Z
        s = head + tail
    return s

# ─── lookup_full_tracking 定義 ───────────────────
def lookup_full_tracking(ups_last4: str) -> Optional[str]:
    """
    在 Tracking 工作表的 S/T/U 欄找唯一尾號匹配，回傳完整追蹤碼或 None。
    """
    SHEET_ID = "1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q"
    gs = get_gspread_client()
    ss = gs.open_by_key(SHEET_ID)
    ws = ss.worksheet("Tracking")

    cols = [19, 20, 21]  # S=19, T=20, U=21
    matches = []
    for col_idx in cols:
        vals = ws.col_values(col_idx)
        for v in vals[1:]:
            v = (v or "").strip()
            if len(v) >= 4 and v[-4:] == ups_last4:
                matches.append(v)

    if len(matches) != 1:
        log.warning(f"UPS尾號 {ups_last4} 找到 {len(matches)} 筆，不唯一，跳過")
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

ocr_helper = OCRAgent()

CONFIG = {
    'VICKY_GROUP_ID': VICKY_GROUP_ID,
    'YUMI_GROUP_ID': YUMI_GROUP_ID,
    'IRIS_GROUP_ID': IRIS_GROUP_ID,
    'YVES_USER_ID': YVES_USER_ID,
    'GORSKY_USER_ID': GORSKY_USER_ID,
    'VICKY_NAMES': VICKY_NAMES,
    'YUMI_NAMES': YUMI_NAMES,
    'IRIS_NAMES': IRIS_NAMES,
    'YVES_NAMES': YVES_NAMES,
    'CODE_TRIGGER_RE': CODE_TRIGGER_RE,
    'ACE_SHEET_URL': ACE_SHEET_URL
}

shipment_parser = ShipmentParserService(CONFIG, get_gspread_client, _line_push)

monday_service = MondaySyncService(
    api_token=MONDAY_API_TOKEN,
    gspread_client_func=get_gspread_client,
    line_push_func=_line_push
)

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
    
        # ─── NEW & CLEANED PDF OCR Trigger ────────────────────────────────────
        if (
            msg.get("type") == "file"
            and msg.get("fileName", "").lower().endswith(".pdf")
            and src.get("groupId") in {VICKY_GROUP_ID, YUMI_GROUP_ID, JOYCE_GROUP_ID, IRIS_GROUP_ID, PDF_GROUP_ID}
        ):
            file_id = msg["id"]
            original_filename = msg.get("fileName", "uploaded.pdf")
            
            try:
                # 1) Download the PDF bytes from LINE
                resp = requests.get(
                    f"https://api-data.line.me/v2/bot/message/{file_id}/content",
                    headers={"Authorization": f"Bearer {LINE_TOKEN}"},
                )
                resp.raise_for_status()
                pdf_bytes = resp.content

                # 2) Use the isolated OCR Engine
                # This calls the class we created in ocr_engine.py
                full_data = ocr_helper.process_shipment_pdf(pdf_bytes)

                if not full_data:
                    log.error("[PDF OCR] Engine returned no data")
                    return "OK", 200

                log.info(f"[PDF OCR] Extracted data: {full_data}")

                # 3) Run Monday.com sync in a background thread to prevent timeouts
                import threading
                threading.Thread(
                    target=monday_service.run_sync,
                    args=(full_data, pdf_bytes, original_filename, r, group_id),
                    daemon=True
                ).start()

            except Exception as e:
                log.error(f"[PDF OCR] Critical failure: {e}", exc_info=True)
                _line_push(YVES_USER_ID, f"⚠️ PDF System Error: {str(e)}")

            return "OK", 200
 
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

        # 🟢 NEW: TWWS 兩段式互動邏輯
        twws_state_key = f"twws_wait_{group_id}" # 針對不同群組紀錄狀態
        
        # 檢查是否正在等待使用者輸入「子項目名稱」
        if r.get(twws_state_key):
            # 如果有狀態存在，把這次輸入的 text 當作名稱去查
            amount = get_twws_value_by_name(text)
            _line_push(group_id, f"🔍 查詢結果 ({text}):\n💰 應付金額: {amount}")
            r.delete(twws_state_key) # 查完後刪除狀態，回到一般模式
            continue

        # 觸發第一階段：使用者輸入 twws
        if text.lower() == "twws":
            # 設定狀態並給予 5 分鐘 (300秒) 的時限
            r.set(twws_state_key, "active", ex=300)
            _line_push(group_id, "好的，請輸入要查詢的子項目名稱 (例如: 1Z...):")
            continue

        # --- 金額自動錄入邏輯：僅限 PDF Scanning 群組觸發 ---
        if group_id == PDF_GROUP_ID:
            # 檢查是否為純數字金額 (如 43.10)
            if re.match(r'^\d+(\.\d{1,2})?$', text):
                # 從全局 Key 抓取最後一次上傳的 PDF 項目 ID, 取得包含 ID 與 Board 的組合字串
                redis_val = r.get("global_last_pdf_parent")

                if redis_val and "|" in redis_val:
                    # 拆分出項目 ID 與板塊 ID
                    last_pid, last_bid = redis_val.split("|")

                    # 呼叫時多傳入板塊 ID
                    ok, msg, item_name = monday_service.update_domestic_expense(last_pid, text, group_id, last_bid)

                    if ok:
                        _line_push(group_id, f"✅ 已成功登記境內支出: ${text}\n📌 項目: {item_name}")
                        r.delete("global_last_pdf_parent")
                    else:
                        _line_push(group_id, f"❌ 登記失敗: {msg}\n📌 項目: {item_name if item_name else '未知'}")
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
                    log.warning(f"Monday: subitem 名稱={full_no} 找不到，跳過")
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
 
        # 3) Ace schedule (週四／週日出貨) & ACE EZ-Way check
        if group_id == ACE_GROUP_ID and ("週四出貨" in text or "週日出貨" in text):
            shipment_parser.handle_ace_schedule(event)
            handle_ace_ezway_check_and_push_to_yves(event)
            continue

        # 4) 處理「申報相符」提醒
        if "申報相符" in text and CODE_TRIGGER_RE.search(text):
            shipment_parser.handle_missing_confirm(event)
            continue
        
        # 5) Richmond-arrival triggers content-request to Vicky —————————
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
                
        # 6) Soquick “上周六出貨包裹的派件單號” & Ace "出貨單號" blocks ——————————————
        if (group_id == SOQUICK_GROUP_ID and "上周六出貨包裹的派件單號" in text) or (group_id == ACE_GROUP_ID and "出貨單號" in text and "宅配單號" in text):
            handle_soquick_and_ace_shipments(event)
            continue

        # 7) Soquick “請通知…申報相符” messages ——————————————
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
            shipment_parser.handle_soquick_full_notification(event)
            continue          

        # 8) Your existing “追蹤包裹” logic
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

        # 9) Your existing “下個國定假日” logic
        if text == "下個國定假日":
            from holiday_reminder import get_next_holiday
            msg = get_next_holiday()
            reply_token = event["replyToken"]
            requests.post(
                "https://api.line.me/v2/bot/message/reply",
                headers={
                    "Authorization": f"Bearer {LINE_TOKEN}",
                    "Content-Type": "application/json"
                },
                json={
                    "replyToken": reply_token,
                    "messages": [{"type": "text", "text": msg}]
                }
            )

        # 🟢 NEW: ACE manual trigger “已上傳資料可出貨”
        if (
            event.get("source", {}).get("type") == "group"
            and event["source"].get("groupId") == ACE_GROUP_ID
            and text.strip() == "已上傳資料可出貨"
        ):
            reply_token = event.get("replyToken")
            push_ace_today_shipments(force=True, reply_token=reply_token)
            return "OK", 200

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

##——— Vicky reminders (Wed & Fri at 18:00) ——————————————————————
# sched.add_job(lambda: remind_vicky("星期四"),
              # trigger="cron", day_of_week="wed", hour=18, minute=00)
# sched.add_job(lambda: remind_vicky("週末"),
              # trigger="cron", day_of_week="fri", hour=17, minute=00)

# sched.start()
# log.info("Scheduler started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))