from datetime import datetime
import pytz
from .config import TIMEZONE, SQ_SHEET_URL, SOQUICK_GROUP_ID
from .sheets import get_gspread_client
from .redis_client import r
from .line_api import line_push, line_reply
from .log import log

def _sq_collect_today_box_ids_by_tab(sheet_url: str) -> list[str]:
    """
    開 SQ 試算表，依今天日期（America/Vancouver），找同名分頁（YYMMDD），
    掃描該分頁：若「欄 C 不為空」，收集「欄 A」作為 Box ID。回傳去重後清單。
    """
    gs = get_gspread_client()
    ss = gs.open_by_url(sheet_url)

    # 今天 → 轉 tab 名稱（YYMMDD），例如 2025-10-10 → 251010
    tz = pytz.timezone(TIMEZONE)
    today_local = datetime.now(tz).date()
    tab_name = today_local.strftime("%y%m%d")

    try:
        ws = ss.worksheet(tab_name)
    except Exception:
        # 找不到今天分頁就回空
        return []

    rows = ws.get_all_values()  # 2D array（含表頭）
    box_ids = []
    # 若第一列是表頭可從第二列開始；無表頭可從第一列開始
    for row in rows[1:]:
        # 欄位保護
        col_a = (row[0] if len(row) > 0 else "").strip()  # A: Box ID
        col_c = (row[2] if len(row) > 2 else "").strip()  # C: 不為空才算
        if col_a and col_c:
            box_ids.append(col_a)

    # 去重（保留順序）
    seen = set()
    uniq = []
    for x in box_ids:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def push_sq_weekly_shipments(*, force: bool = False, reply_token: str | None = None):
    """
    推播 SQ「本週出貨」訊息到 SQ 群組。
    - 來源：SQ_SHEET_URL 指向之 Google Sheet，找今天日期的 tab（YYMMDD）
    - 邏輯：讀取 tab 內 欄A（Box ID），同行欄C不為空者，收集欄A
    - 時間：每週六 09:00 America/Vancouver（排程會以 force=False 呼叫）
    - force=True：不寫 guard，可用 reply_token 提供回覆；無資料也會回「（測試）」字樣
    - force=False：寫 guard（48h 過期），無資料時不吵群組
    """
    tz = pytz.timezone(TIMEZONE)
    today_str = datetime.now(tz).strftime("%Y-%m-%d")
    guard_key = f"sq_weekly_shipments_pushed_{today_str}"

    # 手動測試：先秒回（體驗較即時）
    if reply_token:
        try:
            requests.post(
                LINE_REPLY_URL,
                headers=LINE_HEADERS,
                json={"replyToken": reply_token, "messages": [{"type": "text", "text": "已接收，正在檢查 SQ 今日出貨…"}]},
                timeout=10,
            )
        except Exception as e:
            log.warning(f"[SQ Weekly] reply (pre-ack) failed: {e}")

    # 排程模式避免重複
    if not force and r.get(guard_key):
        log.info("[SQ Weekly] Already pushed for today; skipping.")
        return

    try:
        if not SQ_SHEET_URL:
            log.error("[SQ Weekly] SQ_SHEET_URL not set")
            return

        ids = _sq_collect_today_box_ids_by_tab(SQ_SHEET_URL)

        # 訊息內容
        if ids:
            base_text = "今日出貨：" + ", ".join(ids)
            text = base_text if not force else (base_text + "（測試）")
        else:
            # 無資料：force 時回測試訊息；非 force 僅寫 guard
            if force:
                try:
                    if reply_token:
                        requests.post(
                            LINE_REPLY_URL,
                            headers=LINE_HEADERS,
                            json={"replyToken": reply_token, "messages": [{"type": "text", "text": "今日出貨：目前無資料（測試）"}]},
                            timeout=10,
                        )
                    else:
                        requests.post(
                            LINE_PUSH_URL,
                            headers=LINE_HEADERS,
                            json={"to": SOQUICK_GROUP_ID, "messages": [{"type": "text", "text": "今日出貨：目前無資料（測試）"}]},
                            timeout=10,
                        )
                except Exception as e:
                    log.error(f"[SQ Weekly] Manual test (no data) notify failed: {e}")
                return
            else:
                log.info("[SQ Weekly] No box IDs for today; nothing to push.")
                r.set(guard_key, "1", ex=48 * 3600)
                return

        # 送出推播（SQ 群）
        payload = {"to": SOQUICK_GROUP_ID, "messages": [{"type": "text", "text": text}]}
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload, timeout=10)

        if resp.status_code == 200:
            log.info(f"[SQ Weekly] Pushed {len(ids)} box IDs to SQ group. force={force}")
            if not force:
                r.set(guard_key, "1", ex=48 * 3600)
        else:
            log.error(f"[SQ Weekly] Push failed: {resp.status_code} {resp.text}")

    except Exception as e:
        log.error(f"[SQ Weekly] Error: {e}", exc_info=True)

def sq_weekly_cron_tick():
    """
    被 Heroku Scheduler【每小時】呼叫一次。
    僅在「週六 09:00（America/Vancouver）」時，才真正呼叫 push_sq_weekly_shipments(force=False)。
    借助 push_sq_weekly_shipments 內建的 48h guard，確保只發一次、不重複。
    """
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)

    # 只在『週六』且『09:00』這一小時內做事；其他時間直接略過
    if now.weekday() != 5 or now.hour != 9:
        log.info(f"[SQ Weekly TICK] skip at {now.isoformat()}")
        return

    # 這裡不直接操作 guard，統一交給 push_sq_weekly_shipments() 內部處理
    log.info("[SQ Weekly TICK] due window hit; calling push_sq_weekly_shipments(force=False)")
    try:
        push_sq_weekly_shipments(force=False)
    except Exception as e:
        log.error(f"[SQ Weekly TICK] invoke failed: {e}", exc_info=True)