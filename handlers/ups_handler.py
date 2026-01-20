import re
import requests
import os
import logging
from log import log
from config import MONDAY_API_TOKEN, MONDAY_AIR_BOARD_ID
from typing import Optional
from sheets import get_gspread_client

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

# 從 main.py 搬過來的正則表達式
MULTI_UPS_PAT = re.compile(
    r'(\d{4})\s+([\d.]+)kg\s+(\d+)(?:[×x*\s]+)(\d+)(?:[×x*\s]+)(\d+)(?:cm)?',
    re.IGNORECASE
)

def handle_ups_logic(event, text, group_id, redis_client):
    """
    整合批量 UPS 處理與單筆尺寸錄入邏輯
    """
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
        return True

    # 2. 處理單筆錄入 (接續上一則訊息的狀態) pending_key 單筆 size/weight parser
    pending_key = f"last_subitem_for_{group_id}"
    sub_id = redis_client.get(pending_key)
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
        redis_client.delete(pending_key)
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

        redis_client.delete(pending_key)
        
        return True

    return False

def _process_monday_update(full_no, dims, weight):
    """封裝 Monday.com 的 Query 與 Mutation 邏輯"""
    # 這裡放入原本 main.py 裡面的 requests.post 程式碼
    pass