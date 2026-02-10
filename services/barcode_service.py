import io
import json
import logging
import requests
import threading
import os
import base64
import time
import openai
from datetime import datetime
from PIL import Image
from pyzbar.pyzbar import decode, ZBarSymbol
from io import BytesIO

from config import (
    LINE_TOKEN, MONDAY_API_TOKEN, LINE_PUSH_URL, LINE_HEADERS,
    ACE_GROUP_ID, SOQUICK_GROUP_ID, YVES_USER_ID
)

log = logging.getLogger(__name__)

# 已知寄件人 → 自動建立 Parent Item 的對照表
SENDER_PARENT_MAP = {
    "karl lagerfeld": {"client": "Yumi", "name": "Shu-Yen Liu"}
}


def _check_sender_from_label(img):
    """Quick OpenAI vision check to identify sender company on a shipping label."""
    try:
        client = openai.Client(api_key=os.getenv("OPENAI_API_KEY"))
        img_small = img.copy()
        img_small.thumbnail((800, 800))
        buf = BytesIO()
        img_small.save(buf, format="JPEG", quality=70)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "What is the SENDER (FROM) company or brand name on this shipping label? Reply with ONLY the name, nothing else."},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
            ]}],
            max_tokens=50
        )
        return resp.choices[0].message.content.strip().lower()
    except Exception as e:
        log.error(f"[BARCODE] Sender OCR failed: {e}")
        return ""


def _handle_unknown_fedex(img, tracking_number, group_id, redis_client, pending_buffer, scheduled_buffer, summary_callback):
    """Background: OCR check sender → auto-create Monday items if matched."""
    try:
        sender_text = _check_sender_from_label(img)
        log.info(f"[BARCODE] Sender OCR result: '{sender_text}'")

        # Check against known sender mappings
        mapping = None
        for key, m in SENDER_PARENT_MAP.items():
            if key in sender_text:
                mapping = m
                break

        if not mapping:
            requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json={
                "to": YVES_USER_ID,
                "messages": [{"type": "text", "text": f"⚠️ 找不到單號: {tracking_number}\n(寄件人: {sender_text})"}]
            })
            return

        # --- Auto-create parent + subitem in Air Board ---
        headers = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
        air_parent_board = os.getenv("AIR_PARENT_BOARD_ID")
        air_subitem_board = os.getenv("AIR_BOARD_ID")
        today = datetime.now().strftime("%Y%m%d")
        parent_name = f"{today} {mapping['client']} - {mapping['name']}"

        # 1) Find or create parent
        find_q = f'query {{ items_by_column_values(board_id: {air_parent_board}, column_id: "name", column_value: "{parent_name}") {{ id }} }}'
        r1 = requests.post("https://api.monday.com/v2", headers=headers, json={"query": find_q}, timeout=10)
        found = (r1.json().get("data", {}) or {}).get("items_by_column_values", []) or []

        if found:
            parent_id = found[0]["id"]
            log.info(f"[BARCODE] Found existing parent: {parent_name} (ID: {parent_id})")
        else:
            create_q = f'mutation {{ create_item(board_id: {air_parent_board}, item_name: "{parent_name}") {{ id }} }}'
            r2 = requests.post("https://api.monday.com/v2", headers=headers, json={"query": create_q}, timeout=10)
            parent_id = r2.json()["data"]["create_item"]["id"]
            log.info(f"[BARCODE] Created new parent: {parent_name} (ID: {parent_id})")
            # Set customer type: 早期代購
            type_q = f'mutation {{ change_column_value(item_id: {parent_id}, board_id: {air_parent_board}, column_id: "status_11__1", value: "{{\\"label\\":\\"早期代購\\"}}") {{ id }} }}'
            requests.post("https://api.monday.com/v2", headers=headers, json={"query": type_q}, timeout=10)

        # 2) Create subitem
        sub_q = f'mutation {{ create_subitem(parent_item_id: {parent_id}, item_name: "{tracking_number}") {{ id }} }}'
        r3 = requests.post("https://api.monday.com/v2", headers=headers, json={"query": sub_q}, timeout=10)
        sub_id = r3.json()["data"]["create_subitem"]["id"]

        # 3) Set subitem columns
        loc = "溫哥華倉A" if group_id == ACE_GROUP_ID else ("溫哥華倉S" if group_id == SOQUICK_GROUP_ID else "Yves/Simply")
        mutation = """
        mutation ($itemId: ID!, $boardId: ID!, $columnVals: JSON!) {
          change_multiple_column_values(item_id: $itemId, board_id: $boardId, column_values: $columnVals) { id }
        }
        """
        requests.post("https://api.monday.com/v2", headers=headers, json={
            "query": mutation,
            "variables": {
                "itemId": sub_id,
                "boardId": air_subitem_board,
                "columnVals": json.dumps({
                    "status__1": {"label": "測量"},
                    "location__1": {"label": loc},
                    "status_18__1": {"label": "Ace"},
                    "status_19__1": {"label": "ACE大嘴鳥"}
                })
            }
        }, timeout=10)

        # 4) 等待 Monday automation 完成後，覆蓋加拿大單價為 0
        time.sleep(8)
        requests.post("https://api.monday.com/v2", headers=headers, json={
            "query": mutation,
            "variables": {
                "itemId": sub_id,
                "boardId": air_subitem_board,
                "columnVals": json.dumps({"numeric9__1": "0"})
            }
        }, timeout=10)
        log.info(f"[BARCODE] Set 加拿大單價=0 for subitem {sub_id}")

        # 5) Store in redis + batch buffer
        redis_client.set(f"last_subitem_for_{group_id}", sub_id, ex=300)
        pending_buffer[group_id].append(tracking_number)
        if group_id not in scheduled_buffer:
            scheduled_buffer.add(group_id)
            threading.Timer(30 * 60, summary_callback, args=[group_id]).start()

        # 6) Notify
        requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json={
            "to": YVES_USER_ID,
            "messages": [{"type": "text", "text": f"📦 自動建立 {parent_name}\n📋 子項目: {tracking_number}\n📍 {loc}"}]
        })
        log.info(f"[BARCODE] Auto-created: {parent_name} / {tracking_number}")

    except Exception as e:
        log.error(f"[BARCODE] Auto-create failed: {e}", exc_info=True)
        requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json={
            "to": YVES_USER_ID,
            "messages": [{"type": "text", "text": f"❌ 自動建立失敗: {tracking_number}\n{str(e)}"}]
        })


def handle_barcode_image(event, group_id, redis_client, pending_buffer, scheduled_buffer, summary_callback):
    """
    處理條碼圖片。
    回傳 True 表示訊息已作為條碼處理完成，主迴圈應結束處理此事件。
    """
    msg = event["message"]
    src = event["source"]
    
    # 權限檢查
    is_from_me = src.get("type") == "user" and src.get("userId") == YVES_USER_ID
    is_from_ace = src.get("type") == "group" and group_id == ACE_GROUP_ID
    is_from_soquick = src.get("type") == "group" and group_id == SOQUICK_GROUP_ID
    
    if not (is_from_me or is_from_ace or is_from_soquick):
        return False

    try:
        # (1) 下載圖片
        message_id = msg["id"]
        resp = requests.get(
            f"https://api-data.line.me/v2/bot/message/{message_id}/content",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"}, stream=True
        )
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")

        # (2) 解碼
        decoded_objs = decode(img, symbols=[ZBarSymbol.CODE128, ZBarSymbol.CODE39, ZBarSymbol.EAN13, ZBarSymbol.UPCA])
        if not decoded_objs:
            log.info("[BARCODE] No barcode detected.")
            return True

        # (3) 提取追蹤碼
        tracking_raw = next(
            (obj.data.decode("utf-8") for obj in decoded_objs if obj.data.decode("utf-8").startswith("1Z")),
            decoded_objs[0].data.decode("utf-8")
        ).strip()

        # 🟢 FedEx 追蹤碼清洗：非 UPS (1Z) 的長條碼，只取末尾 12 位
        if not tracking_raw.startswith("1Z") and len(tracking_raw) > 12:
            log.info(f"[BARCODE] FedEx cleanup: {tracking_raw} → {tracking_raw[-12:]}")
            tracking_raw = tracking_raw[-12:]

        # (4) Monday.com 搜尋與更新
        q_search = """
        query ($boardId: ID!, $columnId: String!, $value: String!) {
          items_page_by_column_values(board_id: $boardId, limit: 1, columns: [{ column_id: $columnId, column_values: [$value] }]) {
            items { id name }
          }
        }
        """
        vars_search = {"boardId": os.getenv("AIR_BOARD_ID"), "columnId": "name", "value": tracking_raw}
        r_search = requests.post("https://api.monday.com/v2", headers={"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"},
                                 json={"query": q_search, "variables": vars_search})
        
        items = r_search.json().get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        if not items:
            # 🟢 找不到單號：若為 FedEx，背景執行 OCR 辨識寄件人 → 自動建立
            if not tracking_raw.startswith("1Z"):
                log.info(f"[BARCODE] FedEx {tracking_raw} not found. Running sender OCR in background...")
                threading.Thread(
                    target=_handle_unknown_fedex,
                    args=(img, tracking_raw, group_id, redis_client, pending_buffer, scheduled_buffer, summary_callback),
                    daemon=True
                ).start()
            else:
                requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json={"to": YVES_USER_ID, "messages": [{"type": "text", "text": f"⚠️ 找不到單號: {tracking_raw}"}]})
            return True

        found_id = items[0]["id"]
        redis_client.set(f"last_subitem_for_{group_id}", found_id, ex=300)

        # 更新狀態
        loc = "溫哥華倉A" if group_id == ACE_GROUP_ID else ("溫哥華倉S" if group_id == SOQUICK_GROUP_ID else "Yves/Simply")
        mutation = """
        mutation ($itemId: ID!, $boardId: ID!, $columnVals: JSON!) {
          change_multiple_column_values(item_id: $itemId, board_id: $boardId, column_values: $columnVals) { id }
        }
        """
        requests.post("https://api.monday.com/v2", headers={"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"},
                      json={"query": mutation, "variables": {"itemId": found_id, "boardId": os.getenv("AIR_BOARD_ID"), 
                      "columnVals": json.dumps({"location__1": {"label": loc}, "status__1": {"label": "測量"}})}})

        # 批次匯總提醒
        pending_buffer[group_id].append(tracking_raw)
        if group_id not in scheduled_buffer:
            scheduled_buffer.add(group_id)
            threading.Timer(30*60, summary_callback, args=[group_id]).start()

        return True

    except Exception as e:
        log.error(f"[BARCODE] Error: {e}", exc_info=True)
        return True