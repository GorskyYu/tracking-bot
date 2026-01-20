import os
import requests
import json
import redis
import logging
import re
import threading
from flask import Flask, request, jsonify
from datetime import datetime, timezone, timedelta
import pytz
import openai

# 基礎配置與工具
import config
from redis_client import r
from log import log

# 核心服務層
from services.ocr_engine import OCRAgent
from services.monday_service import MondaySyncService
from services.te_api_service import get_statuses_for, call_api
from services.barcode_service import handle_barcode_image
from services.twws_service import get_twws_value_by_name
from services.shipment_parser import ShipmentParserService

# 業務邏輯處理器
from handlers.handlers import (
    handle_soquick_and_ace_shipments,
    handle_ace_shipments,
    handle_soquick_full_notification
)
from handlers.unpaid_handler import handle_unpaid_event
from handlers.vicky_handler import remind_vicky

# 工作排程
from jobs.ace_tasks import push_ace_today_shipments
from jobs.sq_tasks import push_sq_weekly_shipments

from sheets import get_gspread_client

from collections import defaultdict
from typing import Optional, List, Dict, Any


# ─── Client → LINE Group Mapping ───────────────────────────────────────────────
CLIENT_TO_GROUP = {
    "yumi":  os.getenv("LINE_GROUP_ID_YUMI"),
    "vicky": os.getenv("LINE_GROUP_ID_VICKY"),
}

# ─── Environment Variables ────────────────────────────────────────────────────
APP_ID      = os.getenv("TE_APP_ID")          # e.g. "584"
APP_SECRET  = os.getenv("TE_SECRET")          # your TE App Secret

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
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
r = redis_client

# pull your sheet URL / ID from env
VICKY_SHEET_URL = os.getenv("VICKY_SHEET_URL")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

AIR_BOARD_ID = os.getenv("AIR_BOARD_ID")
AIR_PARENT_BOARD_ID = os.getenv("AIR_PARENT_BOARD_ID")

#STATE_FILE = os.getenv("STATE_FILE", "last_seen.json")
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
LINE_HEADERS = {
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {config.LINE_TOKEN}"
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

MONDAY_API_URL    = "https://api.monday.com/v2"
MONDAY_TOKEN      = os.getenv("MONDAY_TOKEN")
VICKY_SUBITEM_BOARD_ID = 4815120249    # 請填你 Vicky 子任務所在的 Board ID
VICKY_STATUS_COLUMN_ID = "status__1"   # 請填溫哥華收款那個欄位的 column_id



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
  

def _line_push(target_id, text):
    """通用 LINE PUSH 函式"""
    payload = {
        "to": target_id,
        "messages": [{"type": "text", "text": text}]
    }
    resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    log.info(f"[_line_push] to {target_id}: {resp.status_code}")
    return resp

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
    'VICKY_NAMES': config.VICKY_NAMES,
    'YUMI_NAMES': config.YUMI_NAMES,
    'IRIS_NAMES': config.IRIS_NAMES,
    'YVES_NAMES': config.YVES_NAMES,
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

        # 🟢 新增：圖片條碼辨識邏輯
        if mtype == "image":
            # 呼叫 barcode_service 處理，傳入所需的緩存與回呼函式
            if handle_barcode_image(event, group_id, r, _pending, _scheduled, _schedule_summary):
                continue # 如果處理成功（是條碼圖片），則跳過後續邏輯

        # 🟢 NEW: TWWS 兩段式互動邏輯 (限定個人私訊且限定 Yves 使用)
        user_id = src.get("userId")
        twws_state_key = f"twws_wait_{user_id}" # 使用 userId 確保狀態唯一
        
        # 檢查是否為「個人私訊」且為「指定的管理員 (Yves)」
        if src.get("type") == "user" and user_id == YVES_USER_ID:
            # 檢查是否正在等待使用者輸入「子項目名稱」
            if r.get(twws_state_key):
                # 如果有狀態存在，把這次輸入的 text 當作名稱去查
                amount = get_twws_value_by_name(text)
                # 使用 user_id 作為推播對象，確保私訊回傳
                _line_push(user_id, f"🔍 查詢結果 ({text}):\n💰 應付金額: {amount}")
                r.delete(twws_state_key) # 查完後刪除狀態
                continue

            # 觸發第一階段：使用者輸入 twws
            if text.lower() == "twws":
                # 設定狀態並給予 5 分鐘 (300秒) 的時限
                r.set(twws_state_key, "active", ex=300)
                _line_push(user_id, "好的，請輸入子項目名稱：")
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

        # 新的 Unpaid 邏輯
        if text.lower().startswith("unpaid"):
            user_id = src.get("userId")
            group_id = src.get("groupId")

            # 1. 判斷是否為管理員
            is_admin = (user_id == YVES_USER_ID or user_id == GORSKY_USER_ID)
            
            # 2. 判斷是否為有效的自動查詢群組
            is_valid_group = group_id in {VICKY_GROUP_ID, YUMI_GROUP_ID, IRIS_GROUP_ID}

            # 🟢 新邏輯：管理員隨時可用；一般成員僅限在指定群組內輸入 "unpaid"
            can_trigger = is_admin or (is_valid_group and text.lower() == "unpaid")

            if can_trigger:
                handle_unpaid_event(
                    sender_id=group_id if group_id else user_id,
                    message_text=text,
                    reply_token=event["replyToken"],
                    user_id=user_id,
                    group_id=group_id
                )
                continue

        # 1) 處理 UPS 批量更新與單筆尺寸錄入
        from handlers.ups_handler import handle_ups_logic
        if handle_ups_logic(event, text, group_id, redis_client):
            continue
 
        # 3) Ace schedule (週四／週日出貨) & ACE EZ-Way check
        if group_id == ACE_GROUP_ID and ("週四出貨" in text or "週日出貨" in text):
            # 使用 ShipmentParserService 實例呼叫邏輯
            shipment_parser.handle_ace_schedule(event)      # 負責發送到各負責人小群
            shipment_parser.handle_missing_confirm(event)   # 負責 Iris 分流與發送 Sender 給 Yves
            continue

        # 4) 處理「申報相符」通知分流 (包含 Danny 自動觸發與管理員手動觸發)
        from handlers.handlers import dispatch_confirmation_notification
        if dispatch_confirmation_notification(event, text, user_id):
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

##——— Vicky reminders (Wed & Fri at 18:00) ——————————————————————
# sched.add_job(lambda: remind_vicky("星期四"),
              # trigger="cron", day_of_week="wed", hour=18, minute=00)
# sched.add_job(lambda: remind_vicky("週末"),
              # trigger="cron", day_of_week="fri", hour=17, minute=00)

# sched.start()
# log.info("Scheduler started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))