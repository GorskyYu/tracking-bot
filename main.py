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
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from collections import defaultdict
from typing import Optional, List, Dict, Any
import base64

# ???????
import config
from config import (
    # LINE API (LINE_TOKEN used via config.LINE_TOKEN for file downloads)
    LINE_TOKEN,
    # Monday API
    MONDAY_API_URL, MONDAY_API_TOKEN,
    # Redis
    REDIS_URL,
    # Group IDs
    ACE_GROUP_ID, SOQUICK_GROUP_ID, VICKY_GROUP_ID, YUMI_GROUP_ID,
    IRIS_GROUP_ID, JOYCE_GROUP_ID, PDF_GROUP_ID,
    # User IDs
    YVES_USER_ID, GORSKY_USER_ID, VICKY_USER_ID,
    # Sheet URLs
    ACE_SHEET_URL, SQ_SHEET_URL, VICKY_SHEET_URL,
    # Board IDs
    AIR_BOARD_ID, AIR_PARENT_BOARD_ID, VICKY_SUBITEM_BOARD_ID, VICKY_STATUS_COLUMN_ID,
    # Mappings
    CLIENT_TO_GROUP, CUSTOMER_FILTERS,
    # Patterns & Constants
    CODE_TRIGGER_RE, MISSING_CONFIRM, TIMEZONE,
    # OpenAI
    OPENAI_API_KEY, OPENAI_MODEL,
)
from redis_client import r
from log import log

# ?????
from services.ocr_engine import OCRAgent
from services.monday_service import MondaySyncService
from services.te_api_service import get_statuses_for, call_api
from services.barcode_service import handle_barcode_image
from services.twws_service import get_twws_value_by_name
from services.shipment_parser import ShipmentParserService
from services.line_service import line_push, line_reply, line_push_mention

# ???????
from handlers.handlers import (
    handle_soquick_and_ace_shipments,
    handle_ace_shipments,
    handle_soquick_full_notification,
    dispatch_confirmation_notification
)
from handlers.unpaid_handler import handle_unpaid_event, handle_bill_event, handle_paid_bill_event, handle_paid_event
from handlers.vicky_handler import remind_vicky
from handlers.ups_handler import handle_ups_logic

# ????
from jobs.ace_tasks import push_ace_today_shipments
from jobs.sq_tasks import push_sq_weekly_shipments

from sheets import get_gspread_client
from holiday_reminder import get_next_holiday


# --- Redis Client -------------------------------------------------------------
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is required for state persistence")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# --- OpenAI Configuration -----------------------------------------------------
openai.api_key = OPENAI_API_KEY

# -- APScheduler ??:??? 09:00 America/Vancouver ?? ----------------
_sq_scheduler = None
def _ensure_scheduler_for_sq_weekly():
    """
    ???????,????? 09:00(America/Vancouver)??
    push_sq_weekly_shipments(force=False)?
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
    import base64
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

# --- Structured Logging Setup -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
# Note: 'log' is imported from log.py - don't redefine it

# --- Customer Mapping ----------------------------------------------------------
# Map each LINE group to the list of lowercase keywords you filter on
CUSTOMER_FILTERS = {
    os.getenv("LINE_GROUP_ID_YUMI"):   ["yumi", "shu-yen"],
    os.getenv("LINE_GROUP_ID_VICKY"):  ["vicky","chia-chi"]
}

# ???????? SQ ????(? ACE ? _ensure_scheduler_for_ace_today ??)
try:
    _ensure_scheduler_for_sq_weekly()
except Exception as _e:
    log.error(f"[SQ Weekly] Scheduler init failed: {_e}")

# -- APScheduler ??:???&?? 16:00 America/Vancouver ?? ----------------
_scheduler = None
def _ensure_scheduler_for_ace_today():
    """
    ???????,?????????? 16:00(America/Vancouver)??
    push_ace_today_shipments(force=False)?
    - ?? coalesce / max_instances ????????????
    - ?? misfire_grace_time ????????
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
        kwargs={"force": False},  # ????,??? force
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

# ????????????(???????)
try:
    _ensure_scheduler_for_ace_today()
except Exception as _e:
    log.error(f"[ACE Today] Scheduler init failed: {_e}")

# --- In-memory buffers for batch updates --------------------------------------
_pending: Dict[str, List[str]] = defaultdict(list)
_scheduled: set = set()


def strip_mention(line: str) -> str:
    """Remove an @mention at the very start of the line (e.g. '@Gorsky ')."""
    return re.sub(r"^@\S+\s*", "", line)


def _schedule_summary(group_id: str) -> None:
    """Called once per 30m window to send the summary and clear the buffer."""
    ids = _pending.pop(group_id, [])
    _scheduled.discard(group_id)
    if not ids:
        return
    # dedupe and format
    uniq = sorted(set(ids))
    text = "? Updated packages:\n" + "\n".join(f"- {tid}" for tid in uniq)
    line_push(group_id, text)


# --- Flask Webhook ------------------------------------------------------------
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

shipment_parser = ShipmentParserService(CONFIG, get_gspread_client, line_push)

monday_service = MondaySyncService(
    api_token=MONDAY_API_TOKEN,
    gspread_client_func=get_gspread_client,
    line_push_func=line_push
)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Log incoming methods
    # print(f"[Webhook] Received {request.method} to /webhook")
    # log.info(f"Received {request.method} to /webhook")
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Webhook] Payload:", json.dumps(data, ensure_ascii=False))
    # log.info(f"Payload: {json.dumps(data, ensure_ascii=False)}")

    for event in data.get("events", []):
        # ignore non-message events (eg. unsend)
        if event.get("type") != "message":
            continue
            
        # ??? source / group_id
        src = event["source"]
        group_id = src.get("groupId")
        msg      = event["message"]
        text     = msg.get("text", "").strip()
        mtype    = msg.get("type")
    
        # --- NEW & CLEANED PDF OCR Trigger ------------------------------------
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
                    headers={"Authorization": f"Bearer {config.LINE_TOKEN}"},
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
                threading.Thread(
                    target=monday_service.run_sync,
                    args=(full_data, pdf_bytes, original_filename, r, group_id),
                    daemon=True
                ).start()

            except Exception as e:
                log.error(f"[PDF OCR] Critical failure: {e}", exc_info=True)
                line_push(YVES_USER_ID, f"?? PDF System Error: {str(e)}")

            return "OK", 200

        # ?? ??:????????
        if mtype == "image":
            # ?? barcode_service ??,????????????
            if handle_barcode_image(event, group_id, r, _pending, _scheduled, _schedule_summary):
                continue # ??????(?????),???????

        # ?? NEW: TWWS ??????? (????????? Yves ??)
        user_id = src.get("userId")
        twws_state_key = f"twws_wait_{user_id}" # ?? userId ??????
        
        # ???????????????????? (Yves)?
        if src.get("type") == "user" and user_id == YVES_USER_ID:
            # ????????????????????
            if r.get(twws_state_key):
                # ???????,?????? text ??????
                amount = get_twws_value_by_name(text)
                # ?? user_id ??????,??????
                line_push(user_id, f"?? ???? ({text}):\n?? ????: {amount}")
                r.delete(twws_state_key) # ???????
                continue

            # ??????:????? twws
            if text.lower() == "twws":
                # ??????? 5 ?? (300?) ???
                r.set(twws_state_key, "active", ex=300)
                line_push(user_id, "??,????????:")
                continue

        # --- ????????:?? PDF Scanning ???? ---
        if group_id == PDF_GROUP_ID:
            # ?????????? (? 43.10)
            if re.match(r'^\d+(\.\d{1,2})?$', text):
                # ??? Key ????????? PDF ?? ID, ???? ID ? Board ?????
                redis_val = r.get("global_last_pdf_parent")

                if redis_val and "|" in redis_val:
                    # ????? ID ??? ID
                    last_pid, last_bid = redis_val.split("|")

                    # ???????? ID
                    ok, msg, item_name = monday_service.update_domestic_expense(last_pid, text, group_id, last_bid)

                    if ok:
                        line_push(group_id, f"? ?????????: ${text}\n?? ??: {item_name}")
                        r.delete("global_last_pdf_parent")
                    else:
                        line_push(group_id, f"? ????: {msg}\n?? ??: {item_name if item_name else '??'}")
                    continue


        # --- ???????? ---
        if text.startswith("????"):
            handle_bill_event(
                sender_id=group_id if group_id else user_id,
                message_text=text,
                reply_token=event["replyToken"],
                user_id=user_id,
                group_id=group_id
            )
            continue
        
        # ?????? (???????)
        if text.strip() == "????":
            current_user_id = src.get("userId")
            current_group_id = src.get("groupId")
            is_admin = (current_user_id == YVES_USER_ID or current_user_id == GORSKY_USER_ID)
            
            if is_admin and not current_group_id:
                handle_unpaid_event(
                    current_user_id,  # sender_id (positional)
                    text,             # message_text (positional)
                    event["replyToken"],  # reply_token (positional)
                    user_id=current_user_id,
                    group_id=None
                )
                continue
        
        # ?? Unpaid ??
        if text.lower().startswith("unpaid"):
            user_id = src.get("userId")
            group_id = src.get("groupId")

            # 1. ????????
            is_admin = (user_id == YVES_USER_ID or user_id == GORSKY_USER_ID)
            
            # 2. ??????????????
            is_valid_group = group_id in {VICKY_GROUP_ID, YUMI_GROUP_ID, IRIS_GROUP_ID}

            # ?? ???:???????;?????????????? "unpaid"
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
            
        # Paid ????:??????
        # 1. ???????:paid YYMMDD [AbowbowID]
        # 2. ??????:paid ?? [ntd|twd]
        if text.lower().startswith("paid"):
            parts = text.split()
            # ?????????????? (paid YYMMDD ...)
            if len(parts) >= 2 and re.match(r"^\d{6}$", parts[1]):
                handle_paid_bill_event(
                    sender_id=group_id if group_id else user_id,
                    message_text=text,
                    reply_token=event["replyToken"],
                    user_id=user_id,
                    group_id=group_id
                )
            else:
                # ???????? (paid ?? [ntd|twd])
                handle_paid_event(
                    sender_id=group_id if group_id else user_id,
                    message_text=text,
                    reply_token=event["replyToken"],
                    user_id=user_id,
                    group_id=group_id
                )
            continue

        # 1) ?? UPS ???????????
        if handle_ups_logic(event, text, group_id, redis_client):
            continue
 
        # 3) Ace schedule (??/????) & ACE EZ-Way check
        if group_id == ACE_GROUP_ID and ("????" in text or "????" in text):
            # ?? ShipmentParserService ??????
            shipment_parser.handle_ace_schedule(event)      # ???????????
            shipment_parser.handle_missing_confirm(event)   # ?? Iris ????? Sender ? Yves
            continue

        # 4) ???????????? (?? Danny ????????????)
        if dispatch_confirmation_notification(event, text, user_id):
            continue
        
        # 5) Richmond-arrival triggers content-request to Vicky ���������
        if group_id == VICKY_GROUP_ID and "[Richmond, Canada] ???????" in text:
            # extract the tracking ID inside parentheses
            m = re.search(r"\(([^)]+)\)", text)
            if m:
                tracking_id = m.group(1)
            else:
                # no ID found, skip
                continue

            # build the mention message using line_push_mention helper
            msg = "{user1} ?????????????" + tracking_id
            line_push_mention(VICKY_GROUP_ID, msg, {"user1": VICKY_USER_ID})
            log.info(f"Requested contents list from Vicky for {tracking_id}")
            continue
                
        # 6) Soquick �????????????� & Ace "????" blocks ��������������
        if (group_id == SOQUICK_GROUP_ID and "????????????" in text) or (group_id == ACE_GROUP_ID and "????" in text and "????" in text):
            handle_soquick_and_ace_shipments(event)
            continue

        # 7) Soquick �???�????� messages ��������������

        if (group_id in (SOQUICK_GROUP_ID, ACE_GROUP_ID)
            and "??,?" in text
            and "?" in text
            and "????" in text):
            shipment_parser.handle_soquick_full_notification(event)
            continue          

        # 8) Your existing "????" logic
        if text == "????":
            keywords = CUSTOMER_FILTERS.get(group_id)
            if not keywords:
                log.warning(f"[Webhook] No keywords configured for group {group_id}, skipping.")
                continue

            log.info("[Webhook] Trigger matched, fetching statuses�")
            messages = get_statuses_for(keywords)
            combined = "\n\n".join(messages)
            line_reply(event["replyToken"], combined)

        # 9) Your existing "??????" logic
        if text == "??????":
            msg = get_next_holiday()
            line_reply(event["replyToken"], msg)

        # ?? NEW: ACE manual trigger �????????�
        if (
            event.get("source", {}).get("type") == "group"
            and event["source"].get("groupId") == ACE_GROUP_ID
            and text.strip() == "????????"
        ):
            reply_token = event.get("replyToken")
            push_ace_today_shipments(force=True, reply_token=reply_token)
            return "OK", 200

    return "OK", 200
    
# --- Monday.com Webhook --------------------------------------------------------
@app.route("/monday-webhook", methods=["GET", "POST"])
def monday_webhook():
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    evt  = data.get("event", data)
    # respond to Monday�s handshake
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]}), 200

    sub_id    = evt.get("pulseId") or evt.get("itemId")
    parent_id = evt.get("parentItemId")
    lookup_id = parent_id or sub_id
    new_txt   = evt.get("value", {}).get("label", {}).get("text")

    # only act when Location flips to ????
    if new_txt != "????" or not lookup_id:
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
        print(f"[Monday?LINE] no mapping for �{client}� ? {key}, skipping.")
        log.warning(f"No mapping for client={client} key={key}, skipping.")
        return "OK", 200

    item_name = evt.get("pulseName") or str(lookup_id)
    message   = f"?? {item_name} ?????,?????????"

    line_push(group_id, message)
    log.info(f"MondayLINE push sent to {client}")

    return "OK", 200
 
# --- Poller State Helpers & Job -----------------------------------------------


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))