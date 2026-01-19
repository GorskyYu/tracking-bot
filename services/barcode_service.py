import io
import json
import logging
import requests
import threading
from PIL import Image
from pyzbar.pyzbar import decode, ZBarSymbol
import os

from config import (
    LINE_TOKEN, MONDAY_API_TOKEN, LINE_PUSH_URL, LINE_HEADERS,
    ACE_GROUP_ID, SOQUICK_GROUP_ID, YVES_USER_ID
)

log = logging.getLogger(__name__)

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