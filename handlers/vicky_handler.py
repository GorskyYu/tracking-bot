import logging
import pytz
import requests
from datetime import datetime

# 從 config.py 匯入必要的常數
# 注意：確保 MONDAY_API_URL 也在 config.py 中定義，或直接在此定義
from config import (
    MONDAY_TOKEN,
    TIMEZONE,
    VICKY_GROUP_ID,
    VICKY_USER_ID,
    LINE_PUSH_URL,
    LINE_HEADERS
)

# 從 redis_client.py 匯入 redis 客戶端
from redis_client import r

# 如果 config.py 沒定義，這裡可以補上
MONDAY_API_URL = "https://api.monday.com/v2"
VICKY_SUBITEM_BOARD_ID = 4815120249
VICKY_STATUS_COLUMN_ID = "status__1"

log = logging.getLogger(__name__)

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