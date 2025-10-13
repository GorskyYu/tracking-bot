from datetime import datetime
import pytz
from dateutil.parser import parse as parse_date
from config import TIMEZONE, ACE_SHEET_URL, ACE_GROUP_ID
from sheets import get_gspread_client
from redis_client import r
from line_api import line_push, line_reply
from log import log

def _ace_collect_today_box_ids(sheet_url: str) -> list[str]:
    """
    讀取指定 Google Sheet（使用 service account），找出欄 A 等於「今天」的列，
    回傳所有該列欄 B（Box ID）的清單，但【同列的欄 C（寄件人）也必須非空】。

    - 欄 A：日期（可能是顯示文字或原始值，使用 dateutil.parse 盡量解析）
    - 欄 B：Box ID
    - 欄 C：寄件人（必須非空才收集）
    """
    gs = get_gspread_client()
    # 預設取第一個工作表（sheet1）。若你的 ACE sheet 不是第一個，可以改開特定 title。
    ws = gs.open_by_url(sheet_url).sheet1
    rows = ws.get_all_values()  # 包含表頭的二維陣列，每列皆為字串清單

    # 今天（以系統設定的 TIMEZONE＝America/Vancouver 為準）
    tz = pytz.timezone(TIMEZONE)
    today_local = datetime.now(tz).date()

    box_ids: list[str] = []
    # 假設第一列是表頭，從第二列開始掃描；若無表頭可改成 enumerate(rows, start=1)
    for i, row in enumerate(rows[1:], start=2):
        # row 至少要有 A、B、C 三欄
        if not row or len(row) < 3:
            continue

        col_a = (row[0] or "").strip()  # 日期
        col_b = (row[1] or "").strip()  # Box ID
        col_c = (row[2] or "").strip()  # 寄件人

        if not col_a:
            continue

        # 解析欄 A 日期文字：可能是 "2025-10-10"、"10/10/2025"、"Oct 10, 2025" 等
        try:
            d = parse_date(col_a).date()
        except Exception:
            # 若無法解析，直接略過該列（不拋錯以避免中斷全流程）
            continue

        # 日期比對（以「同一天」為準，不含時間）
        if d == today_local and col_b and col_c:
            box_ids.append(col_b)

    # 去重（保留首次出現的順序）
    seen = set()
    unique_box_ids = []
    for x in box_ids:
        if x not in seen:
            seen.add(x)
            unique_box_ids.append(x)

    return unique_box_ids

def push_ace_today_shipments(*, force: bool = False, reply_token: str | None = None):
    """
    推播 ACE「今日出貨」訊息到 ACE 群組。

    參數：
      - force: True 時「不」使用每日防重複機制（適用手動測試）
               False 時啟用每日防重複（排程呼叫）
      - reply_token: 若提供，會先用 reply API 回覆測試者，再進行推播（體驗較即時）

    邏輯：
      1) （非 force）用 Redis 設定當日防重複 key，避免重複推送。
      2) 讀取今天的 Box IDs；若為空，記錄 log 並（非 force）也設 guard，避免重複嘗試。
      3) 推送訊息格式：
         - 有資料： "今日出貨：ID1, ID2, ID3"
         - 無資料（force 手動測）：回覆 "今日出貨：目前無資料（測試）"
      4) （force）不寫入 guard；（非 force）成功後寫入 guard（48h 過期）。
    """
    tz = pytz.timezone(TIMEZONE)
    today_str = datetime.now(tz).strftime("%Y-%m-%d")
    guard_key = f"ace_today_shipments_pushed_{today_str}"

    # 手動測試時，先回一則短訊讓操作者知道已觸發
    if reply_token:
        try:
            requests.post(
                LINE_REPLY_URL,
                headers=LINE_HEADERS,
                json={"replyToken": reply_token, "messages": [{"type": "text", "text": "已接收，正在檢查今日出貨…"}]},
                timeout=10,
            )
        except Exception as e:
            log.warning(f"[ACE Today] reply (pre-ack) failed: {e}")

    # 非 force（排程）→ 開啟每日防重複
    if not force and r.get(guard_key):
        log.info("[ACE Today] Already pushed for today; skipping.")
        return

    try:
        ids = _ace_collect_today_box_ids(ACE_SHEET_URL)

        # 組出訊息
        if ids:
            base_text = "今日出貨：" + ", ".join(ids)
            text = base_text if not force else (base_text + "（測試）")
        else:
            # 沒有資料：排程（非 force）時不推群組；手動（force）時回測試訊息
            if force:
                try:
                    # 手動測試：若有 reply_token 用 reply 回傳，否則也可直接用推播（但避免打擾全群）
                    if reply_token:
                        requests.post(
                            LINE_REPLY_URL,
                            headers=LINE_HEADERS,
                            json={"replyToken": reply_token, "messages": [{"type": "text", "text": "今日出貨：目前無資料（測試）"}]},
                            timeout=10,
                        )
                    else:
                        # 沒有 reply_token 才使用推播（較吵），一般不建議
                        requests.post(
                            LINE_PUSH_URL,
                            headers=LINE_HEADERS,
                            json={"to": ACE_GROUP_ID, "messages": [{"type": "text", "text": "今日出貨：目前無資料（測試）"}]},
                            timeout=10,
                        )
                except Exception as e:
                    log.error(f"[ACE Today] Manual test (no data) notify failed: {e}")
                return
            else:
                log.info("[ACE Today] No box IDs for today; nothing to push.")
                # 排程時：仍然寫入 guard 避免重覆查詢／推送
                r.set(guard_key, "1", ex=48 * 3600)
                return

        # 送出群組推播
        payload = {"to": ACE_GROUP_ID, "messages": [{"type": "text", "text": text}]}
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload, timeout=10)

        if resp.status_code == 200:
            log.info(f"[ACE Today] Pushed {len(ids)} box IDs to ACE group. force={force}")
            # 排程（非 force）成功才寫 guard；手動（force）不寫，避免影響當日正式推播
            if not force:
                r.set(guard_key, "1", ex=48 * 3600)
        else:
            log.error(f"[ACE Today] Push failed: {resp.status_code} {resp.text}")

    except Exception as e:
        log.error(f"[ACE Today] Error: {e}", exc_info=True)

# ─── Heroku Scheduler hourly tick for ACE (stat-holiday style) ───────────────
def ace_today_cron_tick():
    """
    被 Heroku Scheduler【每小時】呼叫一次。
    只有在『週四或週日』且『16:00（America/Vancouver）』時，才真正呼叫
    push_ace_today_shipments(force=False)。去重交給函式內建的 Redis guard。
    """
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)

    # 週四=3、週日=6；僅在當地 16:00 時觸發
    if now.weekday() not in (3, 6) or now.hour != 16:
        log.info(f"[ACE Today TICK] skip at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return

    log.info("[ACE Today TICK] due window hit; calling push_ace_today_shipments(force=False)")
    try:
        push_ace_today_shipments(force=False)
    except Exception as e:
        log.error(f"[ACE Today TICK] invoke failed: {e}", exc_info=True)