"""
上傳資料處理器 - Upload Data Handler
────────────────────────────────────────────
Manages the multi-step upload data conversation via LINE Flex Messages.

State Machine (persisted in Redis with 10-min TTL):
  collecting        → 等待使用者輸入資料
  confirming        → 資料已解析，等待確認
  selecting_match   → 等待使用者選擇匹配記錄
  correcting_field  → 等待使用者選擇要更正的欄位
  correcting_value  → 等待使用者輸入新值
  uploading         → 正在上傳資料
"""

import json
import logging
import re
import os
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from sheets import get_gspread_client
from config import MONDAY_API_TOKEN
from services.line_service import line_reply, line_push, line_reply_flex, line_push_flex
from handlers.upload_data_config import can_use_upload_data
from handlers.upload_data_flex import build_data_confirm_flex, build_match_selection_flex, build_field_selection_flex

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
UPLOAD_TTL = 600  # 10 minutes

# Google Sheet IDs
AIR_FORM_SHEET_ID = "1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q"
PACKING_SHEET_ID = "1vn_LSZlMGNlhId1N8hBjX-r3sptlw5liPd3nGpdAhsY"
OCEAN_FORM_SHEET_ID = "1ziOWeUxNHkGaX4hfQ-lQkTXULBk2Lbdxitsh0fHniaE"


# ─── Redis Key Helpers ────────────────────────────────────────────────────────

def _key(user_id: str, suffix: str) -> str:
    return f"upload:{user_id}:{suffix}"


def _get_state(r, uid):
    return r.get(_key(uid, "state"))


def _set_state(r, uid, state):
    r.set(_key(uid, "state"), state, ex=UPLOAD_TTL)


def _get_data(r, uid):
    raw = r.get(_key(uid, "data"))
    return json.loads(raw) if raw else {}


def _set_data(r, uid, data):
    r.set(_key(uid, "data"), json.dumps(data, ensure_ascii=False), ex=UPLOAD_TTL)


def _get_matches(r, uid):
    raw = r.get(_key(uid, "matches"))
    return json.loads(raw) if raw else []


def _set_matches(r, uid, matches):
    r.set(_key(uid, "matches"), json.dumps(matches, ensure_ascii=False), ex=UPLOAD_TTL)


def _clear_session(r, uid):
    for suffix in ("state", "data", "matches", "reply_token", "correcting_field"):
        r.delete(_key(uid, suffix))


def _get_reply_token(r, uid):
    return r.get(_key(uid, "reply_token"))


def _set_reply_token(r, uid, token):
    r.set(_key(uid, "reply_token"), token, ex=UPLOAD_TTL)


def _get_correcting_field(r, uid):
    return r.get(_key(uid, "correcting_field"))


def _set_correcting_field(r, uid, field):
    r.set(_key(uid, "correcting_field"), field, ex=UPLOAD_TTL)


# ─── Data Parsers ─────────────────────────────────────────────────────────────

def parse_box_id(text: str) -> Optional[str]:
    """Parse Box ID in format: 2 letters followed by 2-4 digits, e.g. YL123, SP22, SQ10."""
    match = re.search(r'\b([A-Z]{2}\d{2,4})\b', text, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def parse_dimension(text: str) -> Optional[str]:
    """
    Parse dimension in format: 62*42*38cm or 62*42*38 or 62 42 38
    Converts inches to cm if specified.
    Returns format: "62*42*38cm"

    Two patterns are tried in order:
    1. Explicit separators (*, ×, x)  — always unambiguous, no digit-count limit.
    2. Space-separated             — each value must be EXACTLY 1–3 digits (token-bounded).
       This prevents false matches on:
         • FedEx 4-4-4 tracking  e.g. "8704 3041 4731"
         • UPS segment numbers   e.g. "545 20 2469 1579"
    """
    # Pattern 1: explicit separator (*, ×, x) — no digit-count restriction needed
    explicit_sep = r'(?<![A-Za-z])(\d+(?:\.\d+)?)[×x*]+(\d+(?:\.\d+)?)[×x*]+(\d+(?:\.\d+)?)(?:\s*)(cm|公分|in|inch|吋|")?'
    match = re.search(explicit_sep, text, re.IGNORECASE)
    if match:
        l, w, h = map(float, match.group(1, 2, 3))
        unit = (match.group(4) or "cm").lower()
        if unit.startswith(("in", "吋", '"')):
            l, w, h = l * 2.54, w * 2.54, h * 2.54
        return f"{int(l)}*{int(w)}*{int(h)}cm"

    # Pattern 2: space-separated — each number must be exactly 1–3 digits
    # (?<![A-Za-z0-9]) and (?!\d) enforce token boundaries so "8704" won't match
    space_sep = r'(?<![A-Za-z0-9])(\d{1,3})(?!\d)\s+(\d{1,3})(?!\d)\s+(\d{1,3})(?!\d)(?:\s*)(cm|公分|in|inch|吋|")?'
    match = re.search(space_sep, text, re.IGNORECASE)
    if match:
        l, w, h = float(match.group(1)), float(match.group(2)), float(match.group(3))
        unit = (match.group(4) or "cm").lower()
        if unit.startswith(("in", "吋", '"')):
            l, w, h = l * 2.54, w * 2.54, h * 2.54
        return f"{int(l)}*{int(w)}*{int(h)}cm"

    return None


def parse_weight(text: str) -> Optional[str]:
    """
    Parse weight in format: 17.3kg or 17.3 or 17.3 kg
    Converts lbs to kg if specified.
    Returns format: "17.30kg"
    """
    # First try to match weight with explicit unit
    pattern_with_unit = r'(\d+(?:\.\d+)?)\s*(kg|公斤|lbs?|磅)'
    match = re.search(pattern_with_unit, text, re.IGNORECASE)
    
    if match:
        weight = float(match.group(1))
        unit = match.group(2).lower()
        
        # Convert lbs to kg
        if unit.startswith(("lb", "磅")):
            weight *= 0.453592
        
        return f"{weight:.2f}kg"
    
    # If no unit found, look for a standalone number (not part of dimensions)
    # Avoid matching numbers that are part of dimension pattern (X*X*X)
    pattern_standalone = r'(?<!\*)(?<!\d)(\d+(?:\.\d+)?)(?!\*|\d)'
    matches = re.findall(pattern_standalone, text)
    
    # Get the last standalone number as weight (dimensions usually come first)
    if matches:
        try:
            weight = float(matches[-1])
            # Only accept reasonable weight values (0.1 to 999 kg)
            if 0.1 <= weight <= 999:
                return f"{weight:.2f}kg"
        except ValueError:
            pass
    
    return None


def parse_tracking(text: str) -> Optional[str]:
    """
    Parse tracking number:
    - UPS: starts with 1Z followed by 16 more characters
      Also handles spaced/OCR formats, e.g.:
        "1Z HFO 545 20 2469 1579"  (spaces + O→0 substitution)
        "TRK  4USUIVI: 1Z HFO 545 20 2469 1579"  (messy prefix)
    - FedEx: 12 digits (continuous or spaced as 4-4-4, e.g. "8898 6250 8870")
    """
    # UPS compact format: 1Z + 16 characters (no spaces)
    ups_match = re.search(r'\b(1Z[A-Z0-9]{16})\b', text, re.IGNORECASE)
    if ups_match:
        return ups_match.group(1).upper()

    # UPS spaced/OCR format: find "1Z" (possibly preceded by garbage) then
    # collect up to 16 alphanumeric tokens, strip spaces, fix O→0
    ups_spaced = re.search(r'1Z\s*([A-Z0-9O\s]{10,40})', text, re.IGNORECASE)
    if ups_spaced:
        # Take everything after '1Z', collapse spaces, substitute letter-O with zero
        raw = ups_spaced.group(1)
        raw = re.sub(r'\s+', '', raw)        # remove all spaces
        raw = re.sub(r'(?<=[A-Z])O(?=[A-Z0-9])|(?<=[0-9])O|O(?=[0-9])', '0', raw, flags=re.IGNORECASE)
        candidate = ("1Z" + raw)[:18].upper()  # 1Z + 16 chars = 18 total
        if re.fullmatch(r'1Z[A-Z0-9]{16}', candidate, re.IGNORECASE):
            return candidate

    # FedEx format: 12 digits continuous
    fedex_match = re.search(r'\b(\d{12})\b', text)
    if fedex_match:
        return fedex_match.group(1)

    # FedEx format: 4-4-4 spaced (e.g. "8898 6250 8870")
    spaced_match = re.search(r'\b(\d{4})\s+(\d{4})\s+(\d{4})\b', text)
    if spaced_match:
        return spaced_match.group(1) + spaced_match.group(2) + spaced_match.group(3)

    return None


def parse_name(text: str, existing_data: Dict[str, Any]) -> Optional[str]:
    """
    Parse sender name or client ID.
    This is trickier - we'll extract text that's not part of other fields.
    """
    # Remove other parsed fields from text
    cleaned = text

    # Remove box ID (2 letters + 2-4 digits, e.g. YL123, SP22)
    cleaned = re.sub(r'\b[A-Z]{2}\d{2,4}\b', '', cleaned, flags=re.IGNORECASE)

    # Remove spaced FedEx tracking (4-4-4) before dimension cleanup eats it
    cleaned = re.sub(r'\b\d{4}\s+\d{4}\s+\d{4}\b', '', cleaned)

    # Remove dimensions — explicit separators only (*, ×, x); NO space as separator
    # to prevent false-matching decimal digits like the "5" in "70.5" with the
    # next number separated by a space.
    cleaned = re.sub(r'\d+(?:\.\d+)?[×x*]+\d+(?:\.\d+)?[×x*]+\d+(?:\.\d+)?\s*(cm|in|inch|吋|公分|")?', '', cleaned, flags=re.IGNORECASE)

    # Remove tracking BEFORE weight so the leading '1' in '1ZHF...' isn't
    # stripped first (which would leave 'ZHF...' to be misread as a name)
    cleaned = re.sub(r'\b1Z[A-Z0-9]{16}\b', '', cleaned, flags=re.IGNORECASE)
    
    # Also remove spaced/prefixed UPS formats like '1Z HFO 545 20 ...' or 'TRK ...: 1Z HFO ...'
    cleaned = re.sub(r'(?:[A-Z0-9_:\s]{0,20})?\b1Z\s*[A-Z0-9O\s]{10,40}', '', cleaned, flags=re.IGNORECASE)
    
    cleaned = re.sub(r'\b\d{12}\b', '', cleaned)

    # Remove weight
    cleaned = re.sub(r'\d+(?:\.\d+)?\s*(kg|lbs?|公斤|磅)?', '', cleaned, flags=re.IGNORECASE)
    
    # Remove transport mode keywords so they don't pollute the name
    cleaned = re.sub(r'海[運运]|空[運运]', '', cleaned)
    
    # Clean up whitespace and punctuation
    cleaned = re.sub(r'[*×x\s\-_/\\]+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    # If we have something left and it's not too long, use it
    if cleaned and len(cleaned) <= 50:
        return cleaned
    
    return None


def parse_hai_yun(text: str) -> Optional[str]:
    """Detect 海運 (ocean freight) in the message (traditional or simplified Chinese)."""
    if re.search(r'海[運运]', text):
        return "海運"
    return None


def parse_kong_yun(text: str) -> Optional[str]:
    """Detect 空運 (air freight) in the message (traditional or simplified Chinese)."""
    if re.search(r'空[運运]', text):
        return "空運"
    return None


def parse_message(text: str, existing_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a message and extract all possible fields.
    Updates existing_data with newly found fields.
    """
    data = existing_data.copy()
    
    # Try to parse each field if not already present
    if not data.get("box_id"):
        box_id = parse_box_id(text)
        if box_id:
            data["box_id"] = box_id
    
    if not data.get("dimension"):
        dimension = parse_dimension(text)
        if dimension:
            data["dimension"] = dimension
    
    if not data.get("weight"):
        weight = parse_weight(text)
        if weight:
            data["weight"] = weight
    
    if not data.get("tracking"):
        tracking = parse_tracking(text)
        if tracking:
            data["tracking"] = tracking
    
    if not data.get("name"):
        name = parse_name(text, data)
        if name:
            data["name"] = name

    if not data.get("hai_yun"):
        hai_yun = parse_hai_yun(text)
        if hai_yun:
            data["hai_yun"] = hai_yun
    
    if not data.get("kong_yun"):
        kong_yun = parse_kong_yun(text)
        if kong_yun:
            data["kong_yun"] = kong_yun

    # Auto-lookup name and package content from tracking when missing
    if data.get("tracking") and (not data.get("name") or not data.get("package_content")):
        is_kong = bool(data.get("kong_yun"))
        is_hai = bool(data.get("hai_yun"))
        if is_kong or is_hai:
            found_name, found_content = lookup_name_by_tracking(data["tracking"], is_kong, is_hai)
            if found_name and not data.get("name"):
                data["name"] = found_name
            if found_content and not data.get("package_content"):
                data["package_content"] = found_content

    return data


def is_data_complete(data: Dict[str, Any]) -> bool:
    """Check if all required fields are present."""
    # 海運 packages don't require box_id or tracking number
    if data.get("hai_yun"):
        required = ["name", "dimension", "weight"]
    else:
        required = ["box_id", "name", "dimension", "weight"]
    return all(data.get(field) for field in required)


# ─── 空運資料表 Search Function ──────────────────────────────────────────────

def search_air_form_matches(name_or_id: str) -> List[Dict[str, str]]:
    """
    Search 空運資料表 Form Responses 1 for matches.
    
    Args:
        name_or_id: Name or Client ID to search for
        
    Returns:
        List of matching records with timestamp, chinese_name, english_name, client_id
    """
    try:
        gs = get_gspread_client()
        ss = gs.open_by_key(AIR_FORM_SHEET_ID)
        ws = ss.worksheet("Form Responses 1")
        
        # Get all data
        all_data = ws.get_all_values()
        
        if len(all_data) < 2:
            return []
        
        headers = all_data[0]
        rows = all_data[1:]
        
        # Calculate one month ago
        one_month_ago = datetime.now() - timedelta(days=30)
        
        matches = []
        search_lower = name_or_id.lower()
        
        for row in rows:
            if len(row) < 6:  # Need at least columns A-E
                continue
            
            # Column A: timestamp
            timestamp_str = row[0] if len(row) > 0 else ""
            # Column C: Chinese name
            chinese_name = row[2] if len(row) > 2 else ""
            # Column D: English name
            english_name = row[3] if len(row) > 3 else ""
            # Column E: Client ID
            client_id = row[4] if len(row) > 4 else ""
            
            # Check if timestamp is within one month
            if timestamp_str:
                try:
                    # Parse timestamp (format: 2026-03-25 22:45:44 or similar)
                    row_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    if row_time < one_month_ago:
                        continue
                except:
                    continue
            
            # Check for match in English name (col D) or Client ID (col E)
            if (search_lower in english_name.lower() or 
                search_lower in client_id.lower() or
                english_name.lower() in search_lower or
                client_id.lower() in search_lower):
                
                matches.append({
                    "timestamp": timestamp_str,
                    "chinese_name": chinese_name,
                    "english_name": english_name,
                    "client_id": client_id
                })
        
        return matches
    
    except Exception as e:
        log.error(f"[UPLOAD] Error searching air form: {e}", exc_info=True)
        return []


# ─── 海運資料表 Search Function ──────────────────────────────────────────────

def search_sea_form_matches(name_or_id: str) -> List[Dict[str, str]]:
    """
    Search 海運資料表 Form Responses 1 for matches.
    Uses a 90-day lookback window (vs 30 days for air freight).

    Columns:
        A: Timestamp
        C: 寄件人中文姓名
        D: 寄件人英文姓名或 Line 名稱
        E: Abowbow會員帳號

    Args:
        name_or_id: Name or Client ID to search for

    Returns:
        List of matching records with timestamp, chinese_name, english_name,
        client_id, and sheet_row (1-based, for Workspace writes).
    """
    try:
        gs = get_gspread_client()
        ss = gs.open_by_key(OCEAN_FORM_SHEET_ID)
        ws = ss.worksheet("Form Responses 1")

        all_data = ws.get_all_values()
        if len(all_data) < 2:
            return []

        rows = all_data[1:]
        ninety_days_ago = datetime.now() - timedelta(days=90)

        matches = []
        search_lower = name_or_id.lower()

        for i, row in enumerate(rows):
            if len(row) < 5:
                continue

            timestamp_str  = row[0] if len(row) > 0 else ""   # Col A
            chinese_name   = row[2] if len(row) > 2 else ""   # Col C
            english_name   = row[3] if len(row) > 3 else ""   # Col D
            client_id      = row[4] if len(row) > 4 else ""   # Col E

            if timestamp_str:
                try:
                    row_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    if row_time < ninety_days_ago:
                        continue
                except Exception:
                    continue

            if (search_lower in english_name.lower() or
                search_lower in client_id.lower() or
                english_name.lower() in search_lower or
                client_id.lower() in search_lower):

                matches.append({
                    "timestamp": timestamp_str,
                    "chinese_name": chinese_name,
                    "english_name": english_name,
                    "client_id": client_id,
                    "sheet_row": i + 2,  # 1-based row in Form Responses 1
                })

        return matches

    except Exception as e:
        log.error(f"[UPLOAD] Error searching sea form: {e}", exc_info=True)
        return []


# ─── Sea Freight Monday Item Creation ─────────────────────────────────────────

# Monday board IDs for sea freight (same as 加台海運資料表 config)
SEA_PARENT_BOARD_ID  = os.getenv("SEA_PARENT_BOARD_ID", "8783157722")
SEA_SUBITEM_BOARD_ID = os.getenv("SEA_SUBITEM_BOARD_ID", "8783157868")


def create_sea_monday_items(
    match: Dict[str, Any],
    dimension: str,
    weight: str,
) -> Dict[str, Any]:
    """
    Replicate the createPickupItem() logic from the 海運資料表 spreadsheet.

    1. Construct parent name:  YYYYMMDD - ABB帳號 - 中文名 英文名
    2. Find or create parent item in 加台海運 Monday board.
    3. Ensure no duplicate subitem → create subitem with timestamp as name.
    4. Set subitem columns (客人種類, ABB帳號, 尺寸, 重量, 收款狀態, etc.).
    5. Write tracking code (subitem name) to Workspace tab 追蹤碼1/2/3.

    Args:
        match:     Dict with timestamp, chinese_name, english_name, client_id, sheet_row.
        dimension: e.g. "40*62*32cm"
        weight:    e.g. "12.15kg"

    Returns:
        dict with keys: success (bool), parent_id, subitem_id, tracking (the subitem name),
        and optionally error.
    """
    try:
        headers_api = {
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json",
        }

        timestamp_str = match["timestamp"]        # e.g. "2026-04-11 14:30:00"
        chinese_name  = match["chinese_name"]
        english_name  = match["english_name"]
        client_id     = match["client_id"]        # Abowbow帳號
        sheet_row     = match["sheet_row"]         # 1-based row

        # --- Construct subitem name (same as spreadsheet logic) ---------------
        sub_name = timestamp_str  # e.g. "2026-04-11 14:30:00"

        # --- Construct parent name  YYYYMMDD - ABB帳號 - 中文名 英文名 --------
        date_ymd = sub_name[:10].replace("-", "")  # "20260411"
        parent_name = f"{date_ymd} - {client_id} - {chinese_name}"
        if english_name:
            parent_name += f" {english_name}"

        # --- Find or create parent item on SEA board --------------------------
        find_q = """
        query ($b: ID!, $v: String!) {
            items_page_by_column_values(
                board_id: $b, limit: 1,
                columns: [{column_id: "name", column_values: [$v]}]
            ) { items { id } }
        }
        """
        resp = requests.post(
            MONDAY_API_URL, headers=headers_api,
            json={"query": find_q, "variables": {"b": SEA_PARENT_BOARD_ID, "v": parent_name}},
            timeout=15,
        )
        items = resp.json().get("data", {}).get("items_page_by_column_values", {}).get("items", [])

        if items:
            parent_id = items[0]["id"]
            log.info(f"[SEA] Found existing parent '{parent_name}' → {parent_id}")
        else:
            create_q = """
            mutation ($b: ID!, $name: String!) {
                create_item(board_id: $b, item_name: $name) { id }
            }
            """
            resp = requests.post(
                MONDAY_API_URL, headers=headers_api,
                json={"query": create_q, "variables": {"b": SEA_PARENT_BOARD_ID, "name": parent_name}},
                timeout=15,
            )
            parent_id = resp.json()["data"]["create_item"]["id"]
            log.info(f"[SEA] Created parent '{parent_name}' → {parent_id}")

        # --- Fetch existing subitems to prevent duplicates --------------------
        subs_q = """
        query ($id: [ID!]) {
            items(ids: $id) { subitems { id name } }
        }
        """
        resp = requests.post(
            MONDAY_API_URL, headers=headers_api,
            json={"query": subs_q, "variables": {"id": [parent_id]}},
            timeout=15,
        )
        existing_subs = resp.json().get("data", {}).get("items", [{}])[0].get("subitems", [])
        existing_names = {s["name"]: s["id"] for s in existing_subs}

        # Determine subitem numbering (same as spreadsheet: #1 = no suffix, #2+ = suffix)
        start_index = len(existing_subs) + 1
        final_sub_name = sub_name if start_index == 1 else f"{sub_name} #{start_index}"

        # --- Create subitem if it doesn't already exist -----------------------
        if final_sub_name in existing_names:
            subitem_id = existing_names[final_sub_name]
            log.info(f"[SEA] Reusing existing subitem '{final_sub_name}' → {subitem_id}")
        else:
            create_sub_q = """
            mutation ($pid: ID!, $name: String!) {
                create_subitem(parent_item_id: $pid, item_name: $name) { id }
            }
            """
            resp = requests.post(
                MONDAY_API_URL, headers=headers_api,
                json={"query": create_sub_q, "variables": {"pid": parent_id, "name": final_sub_name}},
                timeout=15,
            )
            subitem_id = resp.json()["data"]["create_subitem"]["id"]
            log.info(f"[SEA] Created subitem '{final_sub_name}' → {subitem_id}")

        # --- Set subitem columns (mirror spreadsheet logic) -------------------
        set_col_q = """
        mutation ($item: ID!, $board: ID!, $col: String!, $val: String!) {
            change_simple_column_value(item_id: $item, board_id: $board,
                column_id: $col, value: $val) { id }
        }
        """
        set_col_json_q = """
        mutation ($item: ID!, $board: ID!, $col: String!, $val: JSON!) {
            change_column_value(item_id: $item, board_id: $board,
                column_id: $col, value: $val) { id }
        }
        """

        def _set_simple(col_id, value):
            requests.post(
                MONDAY_API_URL, headers=headers_api,
                json={"query": set_col_q, "variables": {
                    "item": subitem_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": col_id, "val": str(value),
                }},
                timeout=10,
            )

        def _set_json(col_id, label):
            requests.post(
                MONDAY_API_URL, headers=headers_api,
                json={"query": set_col_json_q, "variables": {
                    "item": subitem_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": col_id, "val": json.dumps({"label": label}),
                }},
                timeout=10,
            )

        # 客人種類 → 溫哥華散客
        _set_simple("color__1", "溫哥華散客")
        # ABB帳號
        if client_id:
            try:
                _set_simple("text_mkywx26t", client_id)
            except Exception:
                pass  # column may not exist
        # 尺寸 (__1__cm__1)
        dims_match = re.match(r'(\d+)\*(\d+)\*(\d+)', dimension)
        if dims_match:
            _set_simple("__1__cm__1", f"{dims_match.group(1)}*{dims_match.group(2)}*{dims_match.group(3)}")
        # 重量 (numeric__1)
        weight_match = re.match(r'([\d.]+)', weight)
        if weight_match:
            _set_simple("numeric__1", weight_match.group(1))
        # 收款狀態 → 溫哥華收款
        _set_json("status__1", "溫哥華收款")
        # 國際物流 → 海運
        _set_json("status_18__1", "海運")
        # 台灣物流 → 新竹物流
        _set_json("status_19__1", "新竹物流")
        # 地點 → Y/R/Simply
        _set_json("location__1", "Y/R/Simply")

        # --- Write tracking code to Workspace tab (追蹤碼1/2/3, cols S/T/U) ---
        try:
            gs = get_gspread_client()
            ss = gs.open_by_key(OCEAN_FORM_SHEET_ID)
            ws = ss.worksheet("Workspace")
            ws_headers = ws.row_values(1)
            ws_header_map = {h.strip(): idx for idx, h in enumerate(ws_headers)}

            # Find which 追蹤碼 slot is empty for this row
            written = False
            for track_col in ("追蹤碼1", "追蹤碼2", "追蹤碼3"):
                col_idx = ws_header_map.get(track_col)
                if col_idx is None:
                    continue
                cell_val = ws.cell(sheet_row, col_idx + 1).value
                if not cell_val or not str(cell_val).strip():
                    ws.update_cell(sheet_row, col_idx + 1, final_sub_name)
                    log.info(f"[SEA] Wrote tracking '{final_sub_name}' to Workspace {track_col} row {sheet_row}")
                    written = True
                    break
            if not written:
                log.warning(f"[SEA] All 追蹤碼 slots occupied for row {sheet_row}")
        except Exception as ws_err:
            log.error(f"[SEA] Failed to write tracking to Workspace: {ws_err}", exc_info=True)

        return {
            "success": True,
            "parent_id": parent_id,
            "subitem_id": subitem_id,
            "tracking": final_sub_name,
        }

    except Exception as e:
        log.error(f"[SEA] Error creating Monday items: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ─── Tracking-Based Name Lookup ───────────────────────────────────────────────

# Maps 追蹤碼N column name → corresponding content column name
_TRACKING_TO_CONTENT = {
    "追蹤碼1": "第一件包裹內容物清單",
    "追蹤碼2": "第二件包裹內容物清單",
    "追蹤碼3": "第三件包裹內容物清單",
}


def lookup_name_by_tracking(tracking: str, kong_yun: bool, hai_yun: bool):
    """
    Search 空運資料表 (Tracking tab) or 海運資料表 (Workspace tab) for the
    tracking number in cols 追蹤碼1/追蹤碼2/追蹤碼3.

    Returns:
        (abb_account, package_content) tuple, both may be None.
        package_content is read from the matching 第X件包裹內容物清單 column.
    """
    try:
        gs = get_gspread_client()
        if kong_yun:
            ss = gs.open_by_key(AIR_FORM_SHEET_ID)
            ws = ss.worksheet("Tracking")
        elif hai_yun:
            ss = gs.open_by_key(OCEAN_FORM_SHEET_ID)
            ws = ss.worksheet("Workspace")
        else:
            return None, None

        headers = ws.row_values(1)
        header_map = {h.strip(): i for i, h in enumerate(headers)}

        # Build ordered list of (tracking_col_name, tracking_col_idx, content_col_idx)
        tracking_col_specs = []
        for t_col_name in ("追蹤碼1", "追蹤碼2", "追蹤碼3"):
            if t_col_name not in header_map:
                continue
            c_col_name = _TRACKING_TO_CONTENT[t_col_name]
            tracking_col_specs.append((
                header_map[t_col_name],
                header_map.get(c_col_name),  # may be None if column absent
            ))

        abb_col = header_map.get("ABB會員帳號")

        if not tracking_col_specs or abb_col is None:
            log.warning("[UPLOAD] Sheet missing 追蹤碼1/2/3 or ABB會員帳號 columns")
            return None, None

        all_rows = ws.get_all_values()
        tracking_lower = tracking.lower()

        for row in all_rows[1:]:
            if len(row) <= abb_col:
                continue
            for t_idx, c_idx in tracking_col_specs:
                if t_idx < len(row) and row[t_idx].strip().lower() == tracking_lower:
                    abb = row[abb_col].strip() if len(row) > abb_col else ""
                    content = ""
                    if c_idx is not None and c_idx < len(row):
                        content = row[c_idx].strip()
                    log.info(f"[UPLOAD] Found ABB='{abb}' content='{content[:40]}' for tracking {tracking}")
                    return (abb or None), (content or None)

        log.info(f"[UPLOAD] No ABB account found for tracking {tracking}")
        return None, None

    except Exception as e:
        log.error(f"[UPLOAD] Error looking up name by tracking: {e}", exc_info=True)
        return None, None


# ─── Helper: Ensure Unique Timestamp ─────────────────────────────────────────

def ensure_unique_timestamp(tracking_no: str) -> str:
    """
    Check if subitem with tracking_no exists and has status 溫哥華收款.
    If it does, append #2, #3, etc. until finding a unique timestamp.
    
    Args:
        tracking_no: Original timestamp or tracking number
        
    Returns:
        Unique tracking number (may have #2, #3, etc. appended)
    """
    try:
        headers = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
        board_id = os.getenv("AIR_BOARD_ID")
        
        # Check base timestamp
        counter = 1
        current_tracking = tracking_no
        
        while counter < 100:  # Safety limit
            # Query for subitem with this name
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
                        column_values {
                            id
                            text
                        }
                    }
                }
            }
            """
            
            resp = requests.post(
                "https://api.monday.com/v2",
                headers=headers,
                json={
                    "query": query,
                    "variables": {
                        "boardId": board_id,
                        "trackingNo": current_tracking
                    }
                },
                timeout=10
            )
            
            data = resp.json()
            items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
            
            if not items:
                # No item found with this name - it's unique
                log.info(f"[UPLOAD] Timestamp '{current_tracking}' is unique")
                return current_tracking
            
            # Check if the found item has status 溫哥華收款
            status_column = next(
                (col for col in items[0].get("column_values", []) if col["id"] == "status__1"),
                None
            )
            
            if status_column and status_column.get("text") == "溫哥華收款":
                # This timestamp is taken, try next number
                counter += 1
                current_tracking = f"{tracking_no} #{counter}"
                log.info(f"[UPLOAD] Timestamp conflict, trying '{current_tracking}'")
            else:
                # Found item but status is not 溫哥華收款, can use it
                log.info(f"[UPLOAD] Using timestamp '{current_tracking}' (existing but different status)")
                return current_tracking
        
        # Fallback if we hit the limit
        log.warning(f"[UPLOAD] Hit counter limit for timestamp {tracking_no}")
        return current_tracking
        
    except Exception as e:
        log.error(f"[UPLOAD] Error checking timestamp uniqueness: {e}", exc_info=True)
        return tracking_no  # Return original if check fails


# ─── Monday Upload Function ───────────────────────────────────────────────────

def upload_to_monday(tracking_no: str, dimensions: str, weight: str, box_id: str = "") -> bool:
    """
    Upload dimension and weight data to Monday based on tracking number.
    
    Args:
        tracking_no: Tracking number or timestamp
        dimensions: Dimension string (e.g., "40*62*32cm")
        weight: Weight string (e.g., "12.15kg")
        box_id: Box ID (to check for YL prefix)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Extract numeric values
        dims_match = re.match(r'(\d+)\*(\d+)\*(\d+)', dimensions)
        if not dims_match:
            log.error(f"[UPLOAD] Invalid dimension format: {dimensions}")
            return False
        
        dims_clean = f"{dims_match.group(1)}*{dims_match.group(2)}*{dims_match.group(3)}"
        
        weight_match = re.match(r'([\d.]+)', weight)
        if not weight_match:
            log.error(f"[UPLOAD] Invalid weight format: {weight}")
            return False
        
        weight_kg = float(weight_match.group(1))
        
        # Search for item in Monday by tracking number
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
        
        data = resp. json()
        items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        
        if not items:
            log.warning(f"[UPLOAD] No Monday item found for tracking: {tracking_no}")
            return False
        
        item_id = items[0]["id"]
        
        # Update dimensions
        dim_mutation = f'''
        mutation {{
            change_simple_column_value(
                item_id: {item_id},
                board_id: {os.getenv("AIR_BOARD_ID")},
                column_id: "__1__cm__1",
                value: "{dims_clean}"
            ) {{ id }}
        }}'''
        
        requests.post("https://api.monday.com/v2", headers=headers, json={"query": dim_mutation}, timeout=10)
        
        # Update weight
        wt_mutation = f'''
        mutation {{
            change_simple_column_value(
                item_id: {item_id},
                board_id: {os.getenv("AIR_BOARD_ID")},
                column_id: "numeric__1",
                value: "{weight_kg:.2f}"
            ) {{ id }}
        }}'''
        
        requests.post("https://api.monday.com/v2", headers=headers, json={"query": wt_mutation}, timeout=10)
        
        # Update status to 溫哥華收款
        status_mutation = f'''
        mutation {{
            change_column_value(
                item_id: {item_id},
                board_id: {os.getenv("AIR_BOARD_ID")},
                column_id: "status__1",
                value: "{{\\"label\\":\\"溫哥華收款\\"}}"
            ) {{ id }}
        }}'''
        
        requests.post("https://api.monday.com/v2", headers=headers, json={"query": status_mutation}, timeout=10)
        
        # If Box ID starts with YL, set Location and 國際物流
        if box_id and box_id.upper().startswith("YL"):
            log.info(f"[UPLOAD] Box ID {box_id} starts with YL, setting Location and 國際物流")
            
            # Set Location to 溫哥華倉A
            location_mutation = f'''
            mutation {{
                change_column_value(
                    item_id: {item_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "location__1",
                    value: "{{\\"label\\":\\"溫哥華倉A\\"}}"
                ) {{ id }}
            }}'''
            
            requests.post("https://api.monday.com/v2", headers=headers, json={"query": location_mutation}, timeout=10)
            
            # Set 國際物流 to Ace
            logistics_mutation = f'''
            mutation {{
                change_column_value(
                    item_id: {item_id},
                    board_id: {os.getenv("AIR_BOARD_ID")},
                    column_id: "status_18__1",
                    value: "{{\\"label\\":\\"Ace\\"}}"
                ) {{ id }}
            }}'''
            
            requests.post("https://api.monday.com/v2", headers=headers, json={"query": logistics_mutation}, timeout=10)
            
            # Auto-fill 國際單價 (numeric5__1) if currently empty
            intl_check_query = f'''
            query {{
                items(ids: [{item_id}]) {{
                    column_values(ids: ["numeric5__1"]) {{
                        text
                    }}
                }}
            }}'''
            check_resp = requests.post("https://api.monday.com/v2", headers=headers, json={"query": intl_check_query}, timeout=10)
            check_data = check_resp.json()
            col_vals = check_data.get("data", {}).get("items", [{}])[0].get("column_values", [])
            current_intl_price = col_vals[0].get("text", "") if col_vals else ""
            if not current_intl_price or current_intl_price in ("0", "0.0"):
                price = 14 if weight_kg < 3 else (10 if weight_kg < 25 else 11)
                intl_mutation = f'''
                mutation {{
                    change_simple_column_value(
                        item_id: {item_id},
                        board_id: {os.getenv("AIR_BOARD_ID")},
                        column_id: "numeric5__1",
                        value: "{price}"
                    ) {{ id }}
                }}'''
                requests.post("https://api.monday.com/v2", headers=headers, json={"query": intl_mutation}, timeout=10)
                log.info(f"[UPLOAD] Auto-filled 國際單價 to {price} (weight={weight_kg}kg) for item {item_id}")
            else:
                log.debug(f"[UPLOAD] 國際單價 already set ({current_intl_price}), skipping auto-fill for item {item_id}")
        
        log.info(f"[UPLOAD] Successfully updated Monday item {item_id} for tracking {tracking_no}")
        return True
        
    except Exception as e:
        log.error(f"[UPLOAD] Error uploading to Monday: {e}", exc_info=True)
        return False


# ─── 打包資料表 Upload Function ───────────────────────────────────────────────

def upload_to_packing_sheet(box_id: str, name: str, tracking: str, dimension: str, weight: str, col_l_remark: str = "", package_content: str = "") -> bool:
    """
    Upload data to 打包資料表 Form Responses 1 if tracking not already present.
    
    Args:
        box_id: Box ID (e.g., YL123)
        name: Sender name or client ID
        tracking: Tracking number or timestamp (may be empty for 海運 packages)
        dimension: Dimension string (e.g., "40*62*32cm")
        weight: Weight string (e.g., "12.15kg")
        col_l_remark: Value for col L 其他備註（要拆）(e.g. "海運" or "空運")
        package_content: Package contents from 第X件包裹內容物清單, written to col J
        
    Returns:
        True if successful, False otherwise
    """
    try:
        gs = get_gspread_client()
        ss = gs.open_by_key(PACKING_SHEET_ID)
        ws = ss.worksheet("Form Responses 1")
        
        # Resolve column indices by header name
        headers = ws.row_values(1)
        header_map = {h.strip(): i for i, h in enumerate(headers)}
        col_b_idx = header_map.get("廠商編號", 1)       # fallback: column B (index 1)
        col_l_idx = header_map.get("其他備註（要拆）", 11)  # fallback: column L (index 11)
        
        # Check if tracking already exists in column H (skip when tracking is empty)
        if tracking:
            col_h_values = ws.col_values(8)  # Column H
            if tracking in col_h_values:
                log.info(f"[UPLOAD] Tracking {tracking} already exists in packing sheet")
                return True  # Already exists, consider it success
        
        # Prepare row data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Strip unit suffix from dimension (parse_dimension returns "43*14*34cm")
        dimension_clean = re.sub(r'\s*(cm|公分)\s*$', '', dimension, flags=re.IGNORECASE).strip()
        
        # Weight: strip unit and convert to float so Google Sheets stores as a number
        weight_str = re.sub(r'\s*(kg|公斤)\s*$', '', weight, flags=re.IGNORECASE).strip()
        try:
            weight_value = float(weight_str)
        except (ValueError, TypeError):
            weight_value = weight_str  # fallback: keep as string if unparseable
        
        # Build row with enough columns to cover col L
        num_cols = max(col_l_idx + 1, 12)
        row_data = [""] * num_cols
        row_data[0] = timestamp       # A: timestamp
        row_data[4] = name            # E: Sender Name/Client ID
        row_data[7] = tracking        # H: Tracking ID
        row_data[8] = dimension_clean  # I: Dimension (no unit)
        row_data[9] = package_content  # J: Package contents (第X件包裹內容物清單)
        row_data[10] = weight_value    # K: Weight as number
        
        # Determine col B (廠商編號) and col L (其他備註（要拆）) values
        if col_l_remark == "海運":
            if box_id:
                # Box ID present: col B = box_id, col L = 海運
                row_data[col_b_idx] = box_id
                row_data[col_l_idx] = col_l_remark
            else:
                # No box ID: col B = 海運, col L = 海運
                row_data[col_b_idx] = col_l_remark
                row_data[col_l_idx] = col_l_remark
        else:
            row_data[col_b_idx] = box_id  # Normal: col B = box_id
            if col_l_remark:              # e.g. 空運
                row_data[col_l_idx] = col_l_remark
        
        ws.append_row(row_data)
        log.info(f"[UPLOAD] Successfully added to packing sheet: {tracking}")
        return True
        
    except Exception as e:
        log.error(f"[UPLOAD] Error uploading to packing sheet: {e}", exc_info=True)
        return False


# ─── Main Handler Functions ───────────────────────────────────────────────────

def handle_upload_trigger(event: Dict[str, Any], redis_client) -> bool:
    """
    Handle "upload data" trigger.
    
    Returns:
        True if trigger was handled, False otherwise
    """
    user_id = event["source"].get("userId")
    group_id = event["source"].get("groupId")
    text = event["message"]["text"].strip()
    
    # Check trigger
    if not re.match(r'^upload\s+data$', text, re.IGNORECASE):
        return False
    
    # Check permissions
    if not can_use_upload_data(user_id, group_id):
        line_reply(event["replyToken"], "❌ 您沒有權限使用此功能")
        return False
    
    # Initialize session
    _clear_session(redis_client, user_id)
    _set_state(redis_client, user_id, "collecting")
    _set_data(redis_client, user_id, {})
    _set_reply_token(redis_client, user_id, event["replyToken"])
    
    line_reply(event["replyToken"],
              "📦 上傳資料模式已啟動\n\n"
              "請輸入包裹資料，需包含：\n"
              "• Box ID (YL123)\n"
              "• 寄件人/客戶名稱\n"
              "• 尺寸 (長*寬*高cm)\n"
              "• 重量 (kg)\n"
              "• 追蹤編號 (選填)\n"
              "• 運送方式：空運 / 海運（簡體：空运 / 海运）\n\n"
              "⚠️ 海運包裹：僅寫入打包資料表，不推送 Monday，請事後補充追蹤編號\n\n"
              "輸入 'end' 結束此模式")
    
    return True


def handle_upload_message(event: Dict[str, Any], redis_client) -> bool:
    """
    Handle messages during upload data session.
    
    Returns:
        True if message was handled in upload session, False otherwise
    """
    user_id = event["source"].get("userId")
    state = _get_state(redis_client, user_id)
    
    if not state:
        return False
    
    text = event["message"]["text"].strip()
    reply_token = event["replyToken"]
    
    # Handle end command
    if text.lower() == "end":
        _clear_session(redis_client, user_id)
        line_reply(reply_token, "✅ 上傳資料模式已結束")
        return True
    
    # Handle restart command
    if text in ["重新開始", "取消上傳"]:
        _clear_session(redis_client, user_id)
        _set_state(redis_client, user_id, "collecting")
        _set_data(redis_client, user_id, {})
        line_reply(reply_token, "🔄 已重新開始，請輸入資料")
        return True
    
    # State: collecting
    if state == "collecting":
        data = _get_data(redis_client, user_id)
        data = parse_message(text, data)
        _set_data(redis_client, user_id, data)
        
        # Show confirmation with parsed data
        flex = build_data_confirm_flex(data)
        line_reply_flex(reply_token, "📝 已識別資料", flex)
        
        if is_data_complete(data):
            _set_state(redis_client, user_id, "confirming")
        
        return True
    
    # State: confirming
    elif state == "confirming":
        if text == "更正資料":
            flex = build_field_selection_flex()
            line_reply_flex(reply_token, "✏️ 更正資料", flex)
            _set_state(redis_client, user_id, "correcting_field")
            return True

        if text == "返回確認":
            data = _get_data(redis_client, user_id)
            flex = build_data_confirm_flex(data)
            line_reply_flex(reply_token, "📦 包裹資料確認", flex)
            return True

        if text == "確認上傳資料":
            data = _get_data(redis_client, user_id)
            
            # 海運 path: search 海運資料表 for match, then create Monday items
            if data.get("hai_yun"):
                if not data.get("tracking"):
                    matches = search_sea_form_matches(data["name"])
                    valid_matches = [
                        m for m in (matches or [])
                        if isinstance(m, dict) and m.get("timestamp")
                    ]

                    if not valid_matches:
                        # No match found — proceed without Monday (same as before)
                        _process_upload(redis_client, user_id, reply_token, data)
                        return True
                    elif len(valid_matches) == 1:
                        data["_sea_match"] = valid_matches[0]
                        _set_data(redis_client, user_id, data)
                        _process_upload(redis_client, user_id, reply_token, data)
                        return True
                    else:
                        _set_matches(redis_client, user_id, valid_matches)
                        _set_state(redis_client, user_id, "selecting_match")
                        try:
                            flex = build_match_selection_flex(valid_matches)
                            line_reply_flex(reply_token, "🔍 海運：請選擇匹配項目", flex)
                        except Exception as e:
                            log.error(f"[UPLOAD] Error building sea match flex: {e}", exc_info=True)
                            match_text = "🔍 找到以下海運記錄，請回覆選項編號：\n\n"
                            for i, m in enumerate(valid_matches[:5]):
                                match_text += f"【{i+1}】\n"
                                match_text += f"時間: {m.get('timestamp', 'N/A')}\n"
                                match_text += f"中文: {m.get('chinese_name', 'N/A')}\n"
                                match_text += f"英文: {m.get('english_name', 'N/A')}\n"
                                match_text += f"客戶: {m.get('client_id', 'N/A')}\n\n"
                            match_text += "請輸入「選擇匹配1」、「選擇匹配2」等指令"
                            line_reply(reply_token, match_text)
                        return True
                else:
                    # Has tracking already (manually provided)
                    _process_upload(redis_client, user_id, reply_token, data)
                    return True
            
            # Normal / 空運 path: need tracking
            if not data.get("tracking"):
                matches = search_air_form_matches(data["name"])
                
                # Validate matches
                if not matches or not isinstance(matches, list):
                    line_reply(reply_token, 
                              "⚠️ 未找到匹配的空運表單記錄\n"
                              "請手動輸入追蹤編號，或選擇重新開始")
                    return True
                
                # Filter out invalid matches
                valid_matches = [
                    m for m in matches 
                    if isinstance(m, dict) and m.get("timestamp")
                ]
                
                if not valid_matches:
                    line_reply(reply_token,
                              "⚠️ 找到記錄但資料不完整\n"
                              "請手動輸入追蹤編號，或選擇重新開始，或輸入 'end' 結束")
                    return True
                
                elif len(valid_matches) == 1:
                    # Auto-select single match
                    original_timestamp = valid_matches[0]["timestamp"]
                    unique_timestamp = ensure_unique_timestamp(original_timestamp)
                    data["tracking"] = unique_timestamp
                    _set_data(redis_client, user_id, data)
                    _process_upload(redis_client, user_id, reply_token, data)
                    return True
                
                else:
                    # Multiple matches - show selection
                    _set_matches(redis_client, user_id, valid_matches)
                    _set_state(redis_client, user_id, "selecting_match")
                    
                    try:
                        flex = build_match_selection_flex(valid_matches)
                        line_reply_flex(reply_token, "🔍 請選擇匹配項目", flex)
                    except Exception as e:
                        log.error(f"[UPLOAD] Error building/sending match selection flex: {e}", exc_info=True)
                        # Fallback to text list
                        match_text = "🔍 找到以下匹配記錄，請回覆選項編號：\n\n"
                        for i, m in enumerate(valid_matches[:5]):
                            match_text += f"【{i+1}】\n"
                            match_text += f"時間: {m.get('timestamp', 'N/A')}\n"
                            match_text += f"中文: {m.get('chinese_name', 'N/A')}\n"
                            match_text += f"英文: {m.get('english_name', 'N/A')}\n"
                            match_text += f"客戶: {m.get('client_id', 'N/A')}\n\n"
                        match_text += "請輸入「選擇匹配1」、「選擇匹配2」等指令"
                        line_reply(reply_token, match_text)
                    
                    return True
            else:
                # Has tracking, proceed with upload
                _process_upload(redis_client, user_id, reply_token, data)
                return True
        
        return True
    
    # State: selecting_match
    elif state == "selecting_match":
        match_pattern = re.match(r'選擇匹配(\d+)', text)
        if match_pattern:
            idx = int(match_pattern.group(1)) - 1
            matches = _get_matches(redis_client, user_id)
            
            if 0 <= idx < len(matches):
                data = _get_data(redis_client, user_id)

                if data.get("hai_yun"):
                    # Sea freight: store selected match for Monday creation
                    data["_sea_match"] = matches[idx]
                    _set_data(redis_client, user_id, data)
                    _process_upload(redis_client, user_id, reply_token, data)
                    return True
                else:
                    # Air freight: use timestamp as tracking
                    original_timestamp = matches[idx]["timestamp"]
                    unique_timestamp = ensure_unique_timestamp(original_timestamp)
                    data["tracking"] = unique_timestamp
                    _set_data(redis_client, user_id, data)
                    _process_upload(redis_client, user_id, reply_token, data)
                    return True
        
        return True

    # State: correcting_field — user picks which field to update
    elif state == "correcting_field":
        _field_map = {
            "更正_box_id":    "box_id",
            "更正_name":      "name",
            "更正_dimension": "dimension",
            "更正_weight":    "weight",
            "更正_tracking":  "tracking",
            "更正_transport": "transport",
        }
        if text == "返回確認":
            data = _get_data(redis_client, user_id)
            flex = build_data_confirm_flex(data)
            line_reply_flex(reply_token, "📦 包裹資料確認", flex)
            _set_state(redis_client, user_id, "confirming")
            return True

        field = _field_map.get(text)
        if field:
            _set_correcting_field(redis_client, user_id, field)
            _set_state(redis_client, user_id, "correcting_value")
            _field_prompts = {
                "box_id":    "Box ID （例：YL123）",
                "name":      "寄件人/客戶姓名",
                "dimension": "尺寸 （例：40*30*20）",
                "weight":    "重量 （例：12.5kg）",
                "tracking":  "追蹤編號",
                "transport": "運送方式 （空運 / 海運）",
            }
            line_reply(reply_token, f"✏️ 請輸入新的 {_field_prompts[field]}：")
        else:
            # Unrecognised text — re-show the field selection
            flex = build_field_selection_flex()
            line_reply_flex(reply_token, "✏️ 請選擇要更正的欄位", flex)
        return True

    # State: correcting_value — user types the new value for the chosen field
    elif state == "correcting_value":
        field = _get_correcting_field(redis_client, user_id)
        data = _get_data(redis_client, user_id)

        if not field:
            # Safety fallback: lost state, go back to confirming
            flex = build_data_confirm_flex(data)
            line_reply_flex(reply_token, "📦 包裹資料確認", flex)
            _set_state(redis_client, user_id, "confirming")
            return True

        if field == "box_id":
            val = parse_box_id(text) or text.strip().upper()
            data["box_id"] = val
        elif field == "name":
            data["name"] = text.strip()
        elif field == "dimension":
            val = parse_dimension(text)
            if not val:
                line_reply(reply_token, "⚠️ 無法識別尺寸格式，請重新輸入 （例：40*30*20）")
                return True
            data["dimension"] = val
        elif field == "weight":
            val = parse_weight(text)
            if not val:
                line_reply(reply_token, "⚠️ 無法識別重量格式，請重新輸入 （例：12.5kg）")
                return True
            data["weight"] = val
        elif field == "tracking":
            val = parse_tracking(text) or text.strip()
            data["tracking"] = val
        elif field == "transport":
            if re.search(r'海[運运]', text):
                data["hai_yun"] = "海運"
                data.pop("kong_yun", None)
            elif re.search(r'空[運运]', text):
                data["kong_yun"] = "空運"
                data.pop("hai_yun", None)
            else:
                line_reply(reply_token, "⚠️ 請輸入「空運」或「海運」")
                return True

        _set_data(redis_client, user_id, data)
        redis_client.delete(_key(user_id, "correcting_field"))
        _set_state(redis_client, user_id, "confirming")
        flex = build_data_confirm_flex(data)
        line_reply_flex(reply_token, "📦 包裹資料確認", flex)
        return True

    return False


def _process_upload(redis_client, user_id: str, reply_token: str, data: Dict[str, Any]):
    """Process the actual upload to Monday and packing sheet."""
    try:
        hai_yun = data.get("hai_yun", "")
        kong_yun = data.get("kong_yun", "")
        col_l_remark = hai_yun if hai_yun else (kong_yun if kong_yun else "")
        
        if hai_yun:
            # ── 海運 path: create Monday items if match found, then packing sheet ──
            sea_match = data.get("_sea_match")
            monday_success = False
            monday_tracking = ""

            if sea_match:
                result = create_sea_monday_items(
                    sea_match, data["dimension"], data["weight"],
                )
                if result.get("success"):
                    monday_success = True
                    monday_tracking = result.get("tracking", "")
                    data["tracking"] = monday_tracking  # use as packing sheet tracking
                else:
                    log.warning(f"[UPLOAD] Sea Monday creation failed: {result.get('error')}")

            sheet_success = upload_to_packing_sheet(
                data.get("box_id", ""),
                data["name"],
                data.get("tracking", ""),
                data["dimension"],
                data["weight"],
                col_l_remark,
                data.get("package_content", ""),
            )

            box_display = data.get("box_id") or "未提供"
            if monday_success and sheet_success:
                msg = (f"✅ 海運資料上傳成功！\n\n"
                      f"📦 Box ID: {box_display}\n"
                      f"👤 寄件人: {data['name']}\n"
                      f"🔢 追蹤編號: {monday_tracking}\n"
                      f"📏 尺寸: {data['dimension']}\n"
                      f"⚖️ 重量: {data['weight']}\n\n"
                      f"✓ Monday 海運板塊 已建立\n"
                      f"✓ 打包資料表 已記錄\n\n"
                      f"繼續輸入資料，或輸入 'end' 結束")
            elif monday_success:
                msg = (f"⚠️ 部分成功\n\n"
                      f"✓ Monday 海運板塊 已建立\n"
                      f"✗ 打包資料表 記錄失敗\n\n"
                      f"繼續輸入資料，或輸入 'end' 結束")
            elif sheet_success:
                if sea_match:
                    msg = (f"⚠️ 部分成功\n\n"
                          f"📦 Box ID: {box_display}\n"
                          f"👤 寄件人: {data['name']}\n"
                          f"📏 尺寸: {data['dimension']}\n"
                          f"⚖️ 重量: {data['weight']}\n\n"
                          f"✗ Monday 海運板塊 建立失敗\n"
                          f"✓ 打包資料表 已記錄\n\n"
                          f"繼續輸入資料，或輸入 'end' 結束")
                else:
                    msg = (f"✅ 海運記錄已寫入打包資料表！\n\n"
                          f"📦 Box ID: {box_display}\n"
                          f"👤 寄件人: {data['name']}\n"
                          f"📏 尺寸: {data['dimension']}\n"
                          f"⚖️ 重量: {data['weight']}\n\n"
                          f"⚠️ 未找到海運資料表匹配，Monday 項目未建立\n"
                          f"請至打包資料表補充追蹤編號，\n"
                          f"並自行推送資料至 Monday\n\n"
                          f"繼續輸入資料，或輸入 'end' 結束")
            else:
                msg = "❌ 上傳失敗，請重試\n\n輸入重新開始或 'end' 結束"

            line_reply(reply_token, msg)
            _set_state(redis_client, user_id, "collecting")
            _set_data(redis_client, user_id, {})
            return
        
        # ── Normal / 空運 path: Monday + packing sheet ──────────────────────
        # Upload to Monday
        monday_success = upload_to_monday(
            data["tracking"],
            data["dimension"],
            data["weight"],
            data.get("box_id", "")
        )
        
        # Upload to packing sheet
        sheet_success = upload_to_packing_sheet(
            data.get("box_id", ""),
            data["name"],
            data["tracking"],
            data["dimension"],
            data["weight"],
            col_l_remark,
            data.get("package_content", ""),
        )
        
        # Send result
        if monday_success and sheet_success:
            msg = (f"✅ 資料上傳成功！\n\n"
                  f"📦 Box ID: {data.get('box_id', '未提供')}\n"
                  f"👤 寄件人: {data['name']}\n"
                  f"🔢 追蹤編號: {data['tracking']}\n"
                  f"📏 尺寸: {data['dimension']}\n"
                  f"⚖️ 重量: {data['weight']}\n\n"
                  f"✓ Monday 已更新\n"
                  f"✓ 打包資料表 已記錄\n\n"
                  f"繼續輸入資料，或輸入 'end' 結束")
        elif monday_success:
            msg = (f"⚠️ 部分成功\n\n"
                  f"✓ Monday 已更新\n"
                  f"✗ 打包資料表 記錄失敗\n\n"
                  f"繼續輸入資料，或輸入 'end' 結束")
        elif sheet_success:
            msg = (f"⚠️ 部分成功\n\n"
                  f"✗ Monday 更新失敗\n"
                  f"✓ 打包資料表 已記錄\n\n"
                  f"繼續輸入資料，或輸入 'end' 結束")
        else:
            msg = "❌ 上傳失敗，請檢查數據後重試\n\n輸入重新開始或 'end' 結束"
        
        line_reply(reply_token, msg)
        
        # Reset to collecting state for next entry
        _set_state(redis_client, user_id, "collecting")
        _set_data(redis_client, user_id, {})
        
    except Exception as e:
        log.error(f"[UPLOAD] Error in upload process: {e}", exc_info=True)
        line_reply(reply_token, "❌ 上傳過程發生錯誤")


def is_in_upload_session(redis_client, user_id: str) -> bool:
    """Check if user is in an active upload session."""
    return _get_state(redis_client, user_id) is not None
