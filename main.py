import os
import requests
import json
import redis
import logging
import re
import threading
from flask import Flask, request, jsonify
from datetime import datetime, timezone, timedelta
import openai
from collections import defaultdict
from typing import Optional, List, Dict, Any
import base64

# 基礎配置與工具
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
    CUSTOMER_FILTERS,
    # Patterns & Constants
    CODE_TRIGGER_RE, MISSING_CONFIRM, TIMEZONE,
    # OpenAI
    OPENAI_API_KEY, OPENAI_MODEL,
)
from redis_client import r
from log import log

# 核心服務層
from services.ocr_engine import OCRAgent
from services.monday_service import MondaySyncService
from services.te_api_service import get_statuses_for, call_api
from services.barcode_service import handle_barcode_image
from services.twws_service import get_twws_value_by_name
from services.shipment_parser import ShipmentParserService
from services.line_service import line_push, line_reply, line_push_mention

# 業務邏輯處理器
from handlers.handlers import (
    handle_soquick_and_ace_shipments,
    handle_ace_shipments,
    handle_soquick_full_notification,
    dispatch_confirmation_notification
)
from handlers.unpaid_handler import handle_unpaid_event, handle_bill_event, handle_paid_bill_event, handle_paid_event, handle_rate_update
from handlers.vicky_handler import remind_vicky
from handlers.ups_handler import handle_ups_logic
from handlers.monday_webhook_handler import handle_monday_webhook

# 工作排程
from jobs.ace_tasks import push_ace_today_shipments
from jobs.sq_tasks import push_sq_weekly_shipments
from jobs.scheduler import init_all_schedulers

from sheets import get_gspread_client
from holiday_reminder import get_next_holiday


# ─── Redis Client ─────────────────────────────────────────────────────────────
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is required for state persistence")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# --- OpenAI Configuration -----------------------------------------------------
openai.api_key = OPENAI_API_KEY

#  Initialize Background Schedulers 
init_all_schedulers()


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
    text = " Updated packages:\n" + "\n".join(f"- {tid}" for tid in uniq)
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
            
        # 立刻抓 source / group_id
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
                line_push(YVES_USER_ID, f" PDF System Error: {str(e)}")

            return "OK", 200

        # 🟢 新增：圖片條碼辨識邏輯
        if mtype == "image":
            # 呼叫 barcode_service 處理，傳入所需的緩存與回呼函式
            if handle_barcode_image(event, group_id, r, _pending, _scheduled, _schedule_summary):
                continue  # 如果處理成功（是條碼圖片），則跳過後續邏輯

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
                line_push(user_id, f"🔍 查詢結果 ({text}):\n💰 應付金額: {amount}")
                r.delete(twws_state_key)  # 查完後刪除狀態
                continue

            # 觸發第一階段：使用者輸入 twws
            if text.lower() == "twws":
                # 設定狀態並給予 5 分鐘 (300秒) 的時限
                r.set(twws_state_key, "active", ex=300)
                line_push(user_id, "好的，請輸入子項目名稱：")
                continue

        # --- 金額自動錄入邏輯：僅限 PDF Scanning 群組觸發 ---
        if group_id == PDF_GROUP_ID:
            # 檢查是否為純數字金額 (如 43.10)
            if re.match(r'^\d+(\.\d{1,2})?$', text):
                # 從全局 Key 抓取最後一次上傳的 PDF 項目 ID
                redis_val = r.get("global_last_pdf_parent")

                if redis_val and "|" in redis_val:
                    # 拆分出項目 ID 與板塊 ID
                    last_pid, last_bid = redis_val.split("|")

                    # 呼叫時多傳入板塊 ID
                    ok, msg, item_name = monday_service.update_domestic_expense(last_pid, text, group_id, last_bid)

                    if ok:
                        line_push(group_id, f"✅ 已成功登記境內支出: ${text}\n📌 項目: {item_name}")
                        r.delete("global_last_pdf_parent")
                    else:
                        line_push(group_id, f"❌ 登記失敗: {msg}\n📌 項目: {item_name if item_name else '未知'}")
                    continue


        # ─── 查看帳單觸發入口 ───
        if text.startswith("查看帳單"):
            handle_bill_event(
                sender_id=group_id if group_id else user_id,
                message_text=text,
                reply_token=event["replyToken"],
                user_id=user_id,
                group_id=group_id
            )
            continue
        
        # ─── 運費單價更新：管理員回覆兩個數字更新零單價項目 ───
        # Format: number number (e.g., "2.5 10" or "2.5, 10")
        if re.match(r'^\d+(?:\.\d+)?\s*[,;\s]\s*\d+(?:\.\d+)?$', text.strip()):
            current_user_id = src.get("userId")
            current_group_id = src.get("groupId")
            is_admin = (current_user_id == YVES_USER_ID or current_user_id == GORSKY_USER_ID)
            
            if is_admin:
                if handle_rate_update(
                    sender_id=current_group_id if current_group_id else current_user_id,
                    message_text=text,
                    reply_token=event["replyToken"],
                    user_id=current_user_id,
                    group_id=current_group_id
                ):
                    continue
        
        # 目前功能指令 (僅限管理員私訊)
        if text.strip() == "目前功能":
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
            
        # Paid 指令處理：分為兩種情況
        # 1. 查看已付款帳單：paid YYMMDD [AbowbowID]
        # 2. 錄入實收金額：paid 金額 [ntd|twd]
        if text.lower().startswith("paid"):
            parts = text.split()
            # 檢查是否為查看已付款帳單格式 (paid YYMMDD ...)
            if len(parts) >= 2 and re.match(r"^\d{6}$", parts[1]):
                handle_paid_bill_event(
                    sender_id=group_id if group_id else user_id,
                    message_text=text,
                    reply_token=event["replyToken"],
                    user_id=user_id,
                    group_id=group_id
                )
            else:
                # 錄入實收金額格式 (paid 金額 [ntd|twd])
                handle_paid_event(
                    sender_id=group_id if group_id else user_id,
                    message_text=text,
                    reply_token=event["replyToken"],
                    user_id=user_id,
                    group_id=group_id
                )
            continue

        # 1) 處理 UPS 批量更新與單筆尺寸錄入
        if handle_ups_logic(event, text, group_id, redis_client):
            continue
 
        # 3) Ace schedule (週四/週日出貨) & ACE EZ-Way check
        if group_id == ACE_GROUP_ID and ("週四出貨" in text or "週日出貨" in text):
            # 使用 ShipmentParserService 實例呼叫邏輯
            shipment_parser.handle_ace_schedule(event)      # 負責發送到各負責人小群
            shipment_parser.handle_missing_confirm(event)   # 負責 Iris 分流與發送 Sender 給 Yves
            continue

        # 4) 處理「申報相符」通知分流 (包含 Danny 自動觸發與管理員手動觸發)
        if dispatch_confirmation_notification(event, text, user_id):
            continue
        
        # 5) Richmond-arrival triggers content-request to Vicky ���������
        if group_id == VICKY_GROUP_ID and "[Richmond, Canada] 已到達派送中心" in text:
            # extract the tracking ID inside parentheses
            m = re.search(r"\(([^)]+)\)", text)
            if m:
                tracking_id = m.group(1)
            else:
                # no ID found, skip
                continue

            # build the mention message using line_push_mention helper
            msg = "{user1} 請提供此包裹的內容物清單：" + tracking_id
            line_push_mention(VICKY_GROUP_ID, msg, {"user1": VICKY_USER_ID})
            log.info(f"Requested contents list from Vicky for {tracking_id}")
            continue
                
        # 6) Soquick "上周六出貨包裹的派件單號" & Ace "出貨單號" blocks
        if (group_id == SOQUICK_GROUP_ID and "上周六出貨包裹的派件單號" in text) or (group_id == ACE_GROUP_ID and "出貨單號" in text and "宅配單號" in text):
            handle_soquick_and_ace_shipments(event)
            continue

        # 7) Soquick "請通知…申報相符" messages

        if (group_id in (SOQUICK_GROUP_ID, ACE_GROUP_ID)
            and "您好，請" in text
            and "按" in text
            and "申報相符" in text):
            shipment_parser.handle_soquick_full_notification(event)
            continue          

        # 8) Your existing "追蹤包裹" logic
        if text == "追蹤包裹":
            keywords = CUSTOMER_FILTERS.get(group_id)
            if not keywords:
                log.warning(f"[Webhook] No keywords configured for group {group_id}, skipping.")
                continue

            log.info("[Webhook] Trigger matched, fetching statuses�")
            messages = get_statuses_for(keywords)
            combined = "\n\n".join(messages)
            line_reply(event["replyToken"], combined)

        # 9) Your existing "下個國定假日" logic
        if text == "下個國定假日":
            msg = get_next_holiday()
            line_reply(event["replyToken"], msg)

        # 🟢 NEW: ACE manual trigger "已上傳資料可出貨"
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
    return handle_monday_webhook()

# ─── ACE Red Row Trigger Webhook ───────────────────────────────────────────────
@app.route("/ace-trigger", methods=["POST"])
def ace_trigger():
    """
    Webhook endpoint for Google Apps Script to trigger ACE shipment push.
    Called when a row is marked red in the ACE Google Sheet.
    
    Expects JSON body with optional 'secret' field for basic auth.
    """
    # Basic authentication via shared secret
    data = request.get_json(silent=True) or {}
    expected_secret = os.getenv("ACE_TRIGGER_SECRET", "")
    if expected_secret and data.get("secret") != expected_secret:
        log.warning("[ACE Trigger] Invalid or missing secret")
        return jsonify({"error": "Unauthorized"}), 401
    
    log.info("[ACE Trigger] Received trigger from Google Sheet")
    
    # Run in background thread to respond quickly
    def run_push():
        try:
            push_ace_today_shipments(force=False)
        except Exception as e:
            log.error(f"[ACE Trigger] Push failed: {e}", exc_info=True)
    
    threading.Thread(target=run_push, daemon=True).start()
    return jsonify({"status": "triggered"}), 200

# --- Poller State Helpers & Job -----------------------------------------------


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))