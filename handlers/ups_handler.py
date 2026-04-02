import re
import requests
import os
import logging
from log import log
from config import MONDAY_API_TOKEN, MONDAY_AIR_BOARD_ID
from typing import Optional
from sheets import get_gspread_client
from datetime import datetime

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

# ─── log_to_packing_sheet 定義 ───────────────────
def log_to_packing_sheet(tracking_no: str, dimensions: str, weight_kg: float):
    """
    記錄到打包資料表 Google Spreadsheet 的 Form Responses 1 頁籤
    
    Args:
        tracking_no: 追蹤碼
        dimensions: 尺寸 (格式: 長*寬*高cm)
        weight_kg: 重量 (kg)
    """
    try:
        # 1) Query Monday to get subitem + parent info
        query = """
        query ($boardId: ID!, $trackingNo: String!) {
            items_page_by_column_values(
                board_id: $boardId,
                limit: 1,
                columns: [{column_id: "name", column_values: [$trackingNo]}]
            ) {
                items {
                    id
                    name
                    parent_item {
                        id
                        name
                        column_values {
                            id
                            text
                            column { title }
                        }
                    }
                }
            }
        }
        """
        
        headers = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
        resp = requests.post(
            "https://api.monday.com/v2",
            headers=headers,
            json={
                "query": query,
                "variables": {
                    "boardId": os.getenv("AIR_BOARD_ID"),
                    "trackingNo": tracking_no
                }
            },
            timeout=10
        )
        
        data = resp.json()
        items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        
        if not items:
            log.warning(f"[SHEET_LOG] No Monday item found for tracking: {tracking_no}")
            return
            
        item = items[0]
        parent = item.get("parent_item")
        
        if not parent:
            log.warning(f"[SHEET_LOG] No parent item found for tracking: {tracking_no}")
            return
        
        # 2) Extract data
        parent_name = parent.get("name", "")
        
        # Extract Box ID (format: YLXXX or similar from parent name)
        # Parent name format might be: "20260325 Client - Name YL123" or just "YL123"
        box_id_match = re.search(r'\b(YL\d{2,3})\b', parent_name, re.IGNORECASE)
        box_id = box_id_match.group(1).upper() if box_id_match else ""
        
        # Extract sender/client name from parent name
        # Try to get everything after date and before box ID, or just use parent name
        sender_match = re.search(r'^\d{8}\s+(.+?)(?:\s+YL\d{2,3})?$', parent_name)
        sender_name = sender_match.group(1).strip() if sender_match else parent_name
        
        # If sender name is too long or contains date, try to clean it
        if not sender_name or len(sender_name) > 50:
            # Fallback: try to get from parent column values
            parent_cols = {col["column"]["title"]: col.get("text", "") 
                          for col in parent.get("column_values", []) 
                          if col.get("column")}
            sender_name = parent_cols.get("客戶姓名", "") or parent_cols.get("寄件人", "") or parent_name
        
        # Clean up sender name
        sender_name = re.sub(r'^\d{8}\s+', '', sender_name).strip()
        
        # Format timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Format dimensions (ensure it has "cm" suffix)
        dims_clean = dimensions.strip()
        if not dims_clean.endswith("cm"):
            dims_clean = f"{dims_clean}cm"
        
        # Format weight (ensure it has "kg" suffix)
        weight_str = f"{weight_kg:.2f}kg"
        
        # 3) Write to Google Sheet "打包資料表"
        SHEET_URL = "https://docs.google.com/spreadsheets/d/1vn_LSZlMGNlhId1N8hBjX-r3sptlw5liPd3nGpdAhsY"
        SHEET_ID = "1vn_LSZlMGNlhId1N8hBjX-r3sptlw5liPd3nGpdAhsY"
        
        gs = get_gspread_client()
        ss = gs.open_by_key(SHEET_ID)
        ws = ss.worksheet("Form Responses 1")
        
        # Append row with data in columns A, B, E, H, I, K
        # We need to fill other columns with empty strings
        row_data = [
            timestamp,      # A: timestamp
            box_id,         # B: Box ID
            "",             # C: empty
            "",             # D: empty
            sender_name,    # E: Sender Name/Client ID
            "",             # F: empty
            "",             # G: empty
            tracking_no,    # H: Tracking ID
            dims_clean,     # I: Dimension
            "",             # J: empty
            weight_str      # K: Weight
        ]
        
        ws.append_row(row_data, value_input_option='USER_ENTERED')
        log.info(f"[SHEET_LOG] Logged to 打包資料表: {tracking_no} | {box_id} | {sender_name} | {dims_clean} | {weight_str}")
        
    except Exception as e:
        log.error(f"[SHEET_LOG] Failed to log to sheet for {tracking_no}: {e}", exc_info=True)

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
            
            # —(8) 記錄到打包資料表 Google Sheet
            log_to_packing_sheet(full_no, dims_norm, weight_kg)
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
                # Get tracking number and log to Google Sheet
                tracking_query = f'''
                query {{
                    items(ids: [{sub_id}]) {{
                        name
                    }}
                }}'''
                tracking_resp = requests.post(
                    "https://api.monday.com/v2",
                    headers={ "Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json" },
                    json={ "query": tracking_query }
                )
                if tracking_resp.status_code == 200:
                    items = tracking_resp.json().get("data", {}).get("items", [])
                    if items:
                        tracking_no = items[0].get("name", "")
                        if tracking_no:
                            log_to_packing_sheet(tracking_no, dims_norm, weight_kg)
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