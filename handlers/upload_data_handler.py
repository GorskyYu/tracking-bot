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
    for suffix in ("state", "data", "matches", "reply_token", "correcting_field", "sea_trackings"):
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


def parse_dimension(text: str, weight_explicitly_given: bool = False) -> Optional[str]:
    """
    Parse dimension from user input.
    Converts inches to cm if specified.
    Returns format: "62*42*38cm"

    Accepted delimiter patterns (in order):
    1. Explicit delimiters: *, ×, x, -, /, ;, ,
       e.g. 51-51-56  51/51/56  51;51;56  51,51,56  51*51*56
    2. Space-separated — ONLY when weight_explicitly_given=True (weight has a
       unit like 'kg' or 'lbs'), so the standalone weight number can't be
       confused with a dimension.
       e.g. "25.7kg 51 51 56"  → space dims allowed
            "25.7 51 51 56"    → ambiguous, space dims NOT allowed
    """
    # Pattern 1: explicit delimiters  (* × x - / ; ,)
    explicit_sep = r'(?<![A-Za-z])(\d+(?:\.\d+)?)[×x*\-/;,]+(\d+(?:\.\d+)?)[×x*\-/;,]+(\d+(?:\.\d+)?)(?:\s*)(cm|公分|in|inch|吋|")?'
    match = re.search(explicit_sep, text, re.IGNORECASE)
    if match:
        l, w, h = map(float, match.group(1, 2, 3))
        unit = (match.group(4) or "cm").lower()
        if unit.startswith(("in", "吋", '"')):
            l, w, h = l * 2.54, w * 2.54, h * 2.54
        return f"{int(l)}*{int(w)}*{int(h)}cm"

    # Pattern 2: space-separated — only when weight is unambiguously known
    # Remove the explicit weight token before searching so it can't become a dim
    if weight_explicitly_given:
        # Strip weight token (e.g. "25.7kg", "25.7 kg") then look for 3 numbers
        text_no_weight = re.sub(r'\d+(?:\.\d+)?\s*(?:kg|公斤|lbs?|磅)', '', text, flags=re.IGNORECASE)
        space_sep = r'(?<![A-Za-z0-9.])(\d{1,3}(?:\.\d+)?)(?!\d)\s+(\d{1,3}(?:\.\d+)?)(?!\d)\s+(\d{1,3}(?:\.\d+)?)(?!\d)(?:\s*)(cm|公分|in|inch|吋|")?'
        match = re.search(space_sep, text_no_weight, re.IGNORECASE)
        if match:
            l, w, h = float(match.group(1)), float(match.group(2)), float(match.group(3))
            unit = (match.group(4) or "cm").lower()
            if unit.startswith(("in", "吋", '"')):
                l, w, h = l * 2.54, w * 2.54, h * 2.54
            return f"{int(l)}*{int(w)}*{int(h)}cm"

    return None


def has_explicit_weight_unit(text: str) -> bool:
    """Return True if text contains a weight with an explicit unit (kg/lbs/etc.)."""
    return bool(re.search(r'\d+(?:\.\d+)?\s*(?:kg|公斤|lbs?|磅)', text, re.IGNORECASE))


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
        if unit.startswith(("lb", "磅")):
            weight *= 0.453592
        return f"{weight:.2f}kg"

    # No explicit unit — remove box IDs and explicit-delimiter dim groups first
    cleaned = re.sub(r'\b[A-Z]{2}\d{2,4}\b', '', text, flags=re.IGNORECASE)
    cleaned = re.sub(r'\d+(?:\.\d+)?[×x*\-/;,]+\d+(?:\.\d+)?[×x*\-/;,]+\d+(?:\.\d+)?', '', cleaned, flags=re.IGNORECASE)
    pattern_standalone = r'(?<![×x*\-/;,\d])(\d+(?:\.\d+)?)(?![×x*\-/;,\d])'
    matches = re.findall(pattern_standalone, cleaned)

    if matches:
        # Prefer the first number that has a decimal fraction (e.g. 25.7 over 51 51 56)
        # Fall back to the first number if none have decimals
        decimal_candidates = [m for m in matches if '.' in m]
        candidate = decimal_candidates[0] if decimal_candidates else matches[0]
        try:
            weight = float(candidate)
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
    # Route box IDs by prefix: AB → vendor_box_id (col D); others → box_id (col B)
    all_box_ids = re.findall(r'\b([A-Z]{2}\d{2,4})\b', text, re.IGNORECASE)
    for bid in all_box_ids:
        bid_upper = bid.upper()
        if bid_upper.startswith('AB'):
            if not data.get("vendor_box_id"):
                data["vendor_box_id"] = bid_upper
                # Auto-set 海運 when ABxx is detected (廠商箱號 implies sea freight)
                data["hai_yun"] = "海運"
                data.pop("kong_yun", None)
        else:
            if not data.get("box_id"):
                data["box_id"] = bid_upper
    
    if not data.get("dimension"):
        dimension = parse_dimension(text, weight_explicitly_given=has_explicit_weight_unit(text))
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
        M: 包裹數 (number of packages)

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

        # Content columns are in Form Responses 1 (same row): Q, Y, AI
        content_col_idxs = [16, 24, 34]  # 0-indexed: Q=16, Y=24, AI=34

        for i, row in enumerate(rows):
            if len(row) < 5:
                continue

            assigned_to = row[1] if len(row) > 1 else ""  # Col B
            if assigned_to.strip().lower() == "cancelled":
                continue

            timestamp_str  = row[0]  if len(row) > 0  else ""  # Col A
            chinese_name   = row[2]  if len(row) > 2  else ""  # Col C
            english_name   = row[3]  if len(row) > 3  else ""  # Col D
            client_id      = row[4]  if len(row) > 4  else ""  # Col E
            num_pkg_str    = row[12] if len(row) > 12 else ""  # Col M 包裹數

            if timestamp_str:
                try:
                    row_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    if row_time < ninety_days_ago:
                        continue
                except Exception:
                    continue

            if (search_lower in chinese_name.lower() or
                search_lower in english_name.lower() or
                search_lower in client_id.lower() or
                chinese_name.lower() in search_lower or
                english_name.lower() in search_lower or
                client_id.lower() in search_lower):

                # Read package contents from this row (cols Q, Y, AI)
                package_contents = []
                for col_idx in content_col_idxs:
                    if col_idx < len(row):
                        val = row[col_idx].strip()
                        if val:
                            package_contents.append(val)

                try:
                    num_packages_declared = int(float(num_pkg_str.strip())) if num_pkg_str.strip() else 0
                except (ValueError, TypeError):
                    num_packages_declared = 0

                matches.append({
                    "timestamp": timestamp_str,
                    "chinese_name": chinese_name,
                    "english_name": english_name,
                    "client_id": client_id,
                    "sheet_row": i + 2,  # 1-based row in Form Responses 1
                    "package_contents": package_contents,
                    "num_packages": num_packages_declared,
                })

        return matches

    except Exception as e:
        log.error(f"[UPLOAD] Error searching sea form: {e}", exc_info=True)
        return []


def _build_combined_sea_options(valid_matches: list) -> list:
    """
    Given multiple sea form matches, return a flat list of all tracking options
    across all form rows for the user to pick from.

    Strategy (in order):
      1. Read Workspace 追蹤碼1/2/3 for each form row.
      2. For any row where Workspace gives fewer trackings than declared, fall
         back to querying the Monday parent item and reading its subitems.
         Subitems are sliced by offset (sum of num_packages of earlier matches
         that share the same Monday parent).
      3. If neither source has data, emit a "[待建立]" placeholder.

    Each option dict has:
        tracking    – existing tracking string (or "[待建立] <timestamp>")
        content     – package content text for that slot
        subitem_id  – Monday subitem ID if available, else ""
        _sea_match  – the full form-row match dict
        _create_new – True only when a brand-new subitem must be created
    """
    # ── 1. Read Workspace tab ──────────────────────────────────────────────────
    try:
        gs = get_gspread_client()
        ws_ss = gs.open_by_key(OCEAN_FORM_SHEET_ID)
        ws_tab = ws_ss.worksheet("Workspace")
        ws_headers = ws_tab.row_values(1)
        ws_header_map = {h.strip(): i for i, h in enumerate(ws_headers)}
        ws_all = ws_tab.get_all_values()
    except Exception as e:
        log.error(f"[SEA] Failed to read Workspace for combined tracking options: {e}", exc_info=True)
        ws_header_map = {}
        ws_all = []

    track_col_names = ("追蹤碼1", "追蹤碼2", "追蹤碼3")

    # ── 2. Read Workspace trackings for each match ────────────────────────────
    ws_per_match = []
    for m in valid_matches:
        row_idx = m["sheet_row"]  # 1-based, mirrors Form Responses 1 row numbers
        ws_row = ws_all[row_idx - 1] if 0 < row_idx <= len(ws_all) else []
        num_packages = m.get("num_packages", 0) or max(1, len(m.get("package_contents", [])))
        slot_trackings = []
        for col_name in track_col_names:
            col_i = ws_header_map.get(col_name)
            if col_i is not None and col_i < len(ws_row):
                val = ws_row[col_i].strip()
                if val:
                    slot_trackings.append(val)
        log.info(f"[SEA] combined_options: sheet_row={row_idx}, ts={m['timestamp']}, "
                 f"num_packages={num_packages}, ws_trackings={slot_trackings}, total_ws={len(ws_all)}")
        ws_per_match.append((m, num_packages, slot_trackings))

    # ── 3. Monday fallback: query parent subitems when Workspace is incomplete ─
    # Cache parent → listing so we only query Monday once per unique parent.
    _headers_api = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
    parent_subs_cache: dict = {}   # parent_name -> [{"id": ..., "name": ...}]

    def _get_parent_subs(m: dict) -> list:
        cn, en, cid = m["chinese_name"], m.get("english_name", ""), m.get("client_id", "")
        date_ymd = m["timestamp"][:10].replace("-", "")
        pname = f"{date_ymd} - {cid} - {cn}" + (f" {en}" if en else "")
        if pname in parent_subs_cache:
            return parent_subs_cache[pname]
        try:
            q = """
            query ($b: ID!, $v: String!) {
                items_page_by_column_values(
                    board_id: $b, limit: 1,
                    columns: [{column_id: "name", column_values: [$v]}]
                ) { items { subitems { id name } } }
            }
            """
            resp = requests.post(
                MONDAY_API_URL, headers=_headers_api,
                json={"query": q, "variables": {"b": SEA_PARENT_BOARD_ID, "v": pname}},
                timeout=15,
            )
            items = resp.json().get("data", {}).get("items_page_by_column_values", {}).get("items", [])
            subs = items[0].get("subitems", []) if items else []
            log.info(f"[SEA] combined_options Monday fallback: '{pname}' → {[s['name'] for s in subs]}")
        except Exception as e:
            log.error(f"[SEA] combined_options Monday query error: {e}", exc_info=True)
            subs = []
        parent_subs_cache[pname] = subs
        return subs

    # Track how many subitems each match consumes from the shared Monday parent
    parent_offset: dict = {}  # parent_name -> int offset

    def _parent_name(m: dict) -> str:
        cn, en, cid = m["chinese_name"], m.get("english_name", ""), m.get("client_id", "")
        date_ymd = m["timestamp"][:10].replace("-", "")
        return f"{date_ymd} - {cid} - {cn}" + (f" {en}" if en else "")

    # ── 4. Build combined list ─────────────────────────────────────────────────
    combined = []
    for m, num_packages, slot_trackings in ws_per_match:
        pkg_contents = m.get("package_contents", [])
        pname = _parent_name(m)
        offset = parent_offset.get(pname, 0)
        parent_offset[pname] = offset + num_packages

        if len(slot_trackings) >= num_packages:
            # Workspace has all slots for this row → use them
            for i, trk in enumerate(slot_trackings):
                combined.append({
                    "tracking": trk,
                    "content": pkg_contents[i] if i < len(pkg_contents) else "",
                    "subitem_id": "",
                    "_sea_match": m,
                })
        else:
            # Workspace is missing some or all → try Monday
            all_subs = _get_parent_subs(m)
            my_subs = all_subs[offset:offset + num_packages]
            # Items already in Workspace (avoid duplicates)
            ws_set = set(slot_trackings)
            added = 0
            for i, sub in enumerate(my_subs):
                if sub["name"] not in ws_set:
                    combined.append({
                        "tracking": sub["name"],
                        "content": pkg_contents[i] if i < len(pkg_contents) else "",
                        "subitem_id": sub["id"],
                        "_sea_match": m,
                    })
                    added += 1
            # Also keep any Workspace ones not already covered
            for i, trk in enumerate(slot_trackings):
                if trk not in {opt["tracking"] for opt in combined}:
                    combined.append({
                        "tracking": trk,
                        "content": pkg_contents[i] if i < len(pkg_contents) else "",
                        "subitem_id": "",
                        "_sea_match": m,
                    })
            if added == 0 and not slot_trackings:
                # Nothing anywhere yet — placeholder
                combined.append({
                    "tracking": f"[待建立] {m['timestamp']}",
                    "content": pkg_contents[0] if pkg_contents else "",
                    "subitem_id": "",
                    "_sea_match": m,
                    "_create_new": True,
                })

    log.info(f"[SEA] combined_options result: {[o['tracking'] for o in combined]}")
    return combined


# ─── Sea Freight Monday Item Creation ─────────────────────────────────────────

# Monday board IDs for sea freight (same as 加台海運資料表 config)
SEA_PARENT_BOARD_ID  = os.getenv("SEA_PARENT_BOARD_ID", "8783157722")
SEA_SUBITEM_BOARD_ID = os.getenv("SEA_SUBITEM_BOARD_ID", "8783157868")
MONDAY_API_URL = "https://api.monday.com/v2"


def create_sea_monday_items(
    match: Dict[str, Any],
    dimension: str,
    weight: str,
    vendor_box_id: str = "",
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

        # --- Determine how many subitems to create (1 per declared package) --
        package_contents = match.get("package_contents", [])
        # Prefer the explicit 包裹數 column; fall back to content list length, then 1
        num_packages_declared = match.get("num_packages", 0)
        num_packages = num_packages_declared if num_packages_declared >= 1 else max(1, len(package_contents))
        log.info(f"[SEA] 包裹數 declared={num_packages_declared}, package_contents={package_contents}, using num_packages={num_packages}")

        existing_count = len(existing_subs)

        # --- If all subitems already exist, just return them for selection ----
        # (e.g. second physical box of a multi-package shipment)
        if existing_count >= num_packages:
            log.info(f"[SEA] All {num_packages} subitems already exist (existing={existing_count}), skipping creation")
            created_subitems = []
            for i, s in enumerate(existing_subs[:num_packages]):
                pkg_content = package_contents[i] if i < len(package_contents) else ""
                created_subitems.append({
                    "tracking": s["name"],
                    "content": pkg_content,
                    "subitem_id": s["id"],
                })
            return {
                "success": True,
                "parent_id": parent_id,
                "subitems": created_subitems,
                "tracking": created_subitems[0]["tracking"] if created_subitems else "",
            }

        # --- Shared GraphQL templates -----------------------------------------
        create_sub_q = """
        mutation ($pid: ID!, $name: String!) {
            create_subitem(parent_item_id: $pid, item_name: $name) { id }
        }
        """
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

        def _set_simple(s_id, col_id, value):
            requests.post(
                MONDAY_API_URL, headers=headers_api,
                json={"query": set_col_q, "variables": {
                    "item": s_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": col_id, "val": str(value),
                }},
                timeout=10,
            )

        def _set_json(s_id, col_id, label):
            requests.post(
                MONDAY_API_URL, headers=headers_api,
                json={"query": set_col_json_q, "variables": {
                    "item": s_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": col_id, "val": json.dumps({"label": label}),
                }},
                timeout=10,
            )

        # Open Workspace once for all tracking writes
        gs = get_gspread_client()
        ws_ss = gs.open_by_key(OCEAN_FORM_SHEET_ID)
        ws_tab = ws_ss.worksheet("Workspace")
        ws_headers = ws_tab.row_values(1)
        ws_header_map = {h.strip(): idx for idx, h in enumerate(ws_headers)}
        track_col_names = ("追蹤碼1", "追蹤碼2", "追蹤碼3")

        dims_m = re.match(r'(\d+)\*(\d+)\*(\d+)', dimension)
        weight_m = re.match(r'([\d.]+)', weight)

        # Only create subitems that are still missing
        # (existing_count < num_packages at this point)
        created_subitems = []
        for pkg_idx in range(num_packages):
            # Name: no suffix for index 0, #N for subsequent
            overall_idx = pkg_idx  # absolute index (0-based) within this customer's subitems
            pkg_sub_name = sub_name if overall_idx == 0 else f"{sub_name} #{overall_idx + 1}"
            pkg_content = package_contents[pkg_idx] if pkg_idx < len(package_contents) else ""

            # Reuse if it already exists, else create
            if pkg_sub_name in existing_names:
                s_id = existing_names[pkg_sub_name]
                log.info(f"[SEA] Reusing existing subitem '{pkg_sub_name}' → {s_id}")
                created_subitems.append({
                    "tracking": pkg_sub_name,
                    "content": pkg_content,
                    "subitem_id": s_id,
                })
                continue
            else:
                resp = requests.post(
                    MONDAY_API_URL, headers=headers_api,
                    json={"query": create_sub_q, "variables": {"pid": parent_id, "name": pkg_sub_name}},
                    timeout=15,
                )
                s_id = resp.json()["data"]["create_subitem"]["id"]
                log.info(f"[SEA] Created subitem '{pkg_sub_name}' → {s_id}")

            # Set columns on this subitem
            _set_simple(s_id, "color__1", "溫哥華散客")
            if client_id:
                try:
                    _set_simple(s_id, "text_mkywx26t", client_id)
                except Exception:
                    pass
            # NOTE: dimension / weight are NOT set here.
            # They are set later via update_sea_subitem_data() when the user
            # selects which subitem this physical box belongs to.
            _set_json(s_id, "status__1", "溫哥華收款")
            _set_json(s_id, "status_18__1", "海運")
            _set_json(s_id, "status_19__1", "新竹物流")
            _set_json(s_id, "location__1", "Y/R/Simply")
            if vendor_box_id:
                try:
                    _set_simple(s_id, "text57__1", vendor_box_id)
                except Exception:
                    pass

            # Write tracking to Workspace 追蹤碼N at the correct slot
            if pkg_idx < len(track_col_names):
                track_col = track_col_names[pkg_idx]
                col_idx = ws_header_map.get(track_col)
                if col_idx is not None:
                    try:
                        ws_tab.update_cell(sheet_row, col_idx + 1, pkg_sub_name)
                        log.info(f"[SEA] Wrote tracking '{pkg_sub_name}' to Workspace {track_col} row {sheet_row}")
                    except Exception as ws_err:
                        log.error(f"[SEA] Failed to write tracking to Workspace: {ws_err}", exc_info=True)

            created_subitems.append({
                "tracking": pkg_sub_name,
                "content": pkg_content,
                "subitem_id": s_id,
            })

        log.info(f"[SEA] Created {len(created_subitems)} subitems: {[s['tracking'] for s in created_subitems]}")
        return {
            "success": True,
            "parent_id": parent_id,
            "subitems": created_subitems,
            "tracking": created_subitems[0]["tracking"] if created_subitems else "",
        }

    except Exception as e:
        log.error(f"[SEA] Error creating Monday items: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


def update_sea_subitem_data(subitem_id: str, dimension: str, weight: str, vendor_box_id: str = "") -> list:
    """Write dimension / weight / vendor_box_id to a Monday sea-freight subitem.
    Returns list of updated field descriptions for caller to include in reply."""
    updated = []
    try:
        _headers = {
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json",
        }
        set_col_q = """
        mutation ($item: ID!, $board: ID!, $col: String!, $val: String!) {
            change_simple_column_value(item_id: $item, board_id: $board,
                column_id: $col, value: $val) { id }
        }
        """
        dims_m = re.match(r'(\d+)\*(\d+)\*(\d+)', dimension)
        weight_m = re.match(r'([\d.]+)', weight)
        if dims_m:
            dim_val = f"{dims_m.group(1)}*{dims_m.group(2)}*{dims_m.group(3)}"
            requests.post(
                MONDAY_API_URL, headers=_headers,
                json={"query": set_col_q, "variables": {
                    "item": subitem_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": "__1__cm__1", "val": dim_val,
                }}, timeout=10,
            )
            updated.append(f"\u5c3a\u5bf8={dim_val}")
        if weight_m:
            requests.post(
                MONDAY_API_URL, headers=_headers,
                json={"query": set_col_q, "variables": {
                    "item": subitem_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": "numeric__1", "val": weight_m.group(1),
                }}, timeout=10,
            )
            updated.append(f"\u91cd\u91cf={weight_m.group(1)}")
        if vendor_box_id:
            requests.post(
                MONDAY_API_URL, headers=_headers,
                json={"query": set_col_q, "variables": {
                    "item": subitem_id, "board": SEA_SUBITEM_BOARD_ID,
                    "col": "text57__1", "val": vendor_box_id,
                }}, timeout=10,
            )
            updated.append(f"\u5ee0\u5546\u7bb1\u865f={vendor_box_id}")
        log.info(f"[SEA] Updated subitem {subitem_id}: {updated}")
    except Exception as e:
        log.error(f"[SEA] Failed to update subitem data: {e}", exc_info=True)
    return updated


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

def upload_to_packing_sheet(box_id: str, name: str, tracking: str, dimension: str, weight: str, col_l_remark: str = "", package_content: str = "", vendor_box_id: str = "") -> dict:
    """
    Upload data to 打包資料表 Form Responses 1 if not a duplicate.
    
    Args:
        box_id: Box ID (e.g., YL123)
        name: Sender name or client ID
        tracking: Tracking number or timestamp (may be empty for 海運 packages)
        dimension: Dimension string (e.g., "40*62*32cm")
        weight: Weight string (e.g., "12.15kg")
        col_l_remark: Value for col L 其他備註（要拆）(e.g. "海運" or "空運")
        package_content: Package contents from 第X件包裹內容物清單, written to col J
        vendor_box_id: AB box ID written to col D
        
    Returns:
        Dict with keys:
        - "success": bool (True if inserted or duplicate/ok, False on error)
        - "duplicate": bool (True if duplicate found, not inserted)
        - "message": str (Human-readable status message)
    """
    try:
        gs = get_gspread_client()
        ss = gs.open_by_key(PACKING_SHEET_ID)
        ws = ss.worksheet("Form Responses 1")
        
        # Resolve column indices by header name (for flexible column ordering)
        headers = ws.row_values(1)
        header_map = {h.strip(): i for i, h in enumerate(headers)}
        col_b_idx = header_map.get("廠商編號", 1)           # fallback: B
        col_d_idx = header_map.get("箱號", 3)               # fallback: D
        col_h_idx = header_map.get("追蹤編號", 7)           # fallback: H
        col_i_idx = header_map.get("尺寸", 8)               # fallback: I
        col_j_idx = header_map.get("內容物", 9)             # fallback: J
        col_k_idx = header_map.get("重量", 10)              # fallback: K
        col_l_idx = header_map.get("其他備註（要拆）", 11)    # fallback: L
        
        # Pre-compute cleaned dimension / weight (needed for both dup-check and insert)
        # Strip unit suffix from dimension (parse_dimension returns "43*14*34cm")
        dimension_clean = re.sub(r'\s*(cm|公分)\s*$', '', dimension, flags=re.IGNORECASE).strip()

        # Weight: strip unit and convert to float so Google Sheets stores as a number
        weight_str = re.sub(r'\s*(kg|公斤)\s*$', '', weight, flags=re.IGNORECASE).strip()
        try:
            weight_value = float(weight_str)
        except (ValueError, TypeError):
            weight_value = weight_str  # fallback: keep as string if unparseable

        def _norm_dim(d: str):
            """Sort dimension parts so 43*68*43 == 43*43*68."""
            parts = re.findall(r'\d+', d)
            return tuple(sorted(int(p) for p in parts)) if len(parts) == 3 else None

        def _norm_weight(w) -> float | None:
            try:
                return float(re.sub(r'[^\d.]', '', str(w)))
            except (ValueError, TypeError):
                return None

        new_dim_norm = _norm_dim(dimension_clean)
        new_weight_norm = _norm_weight(weight_value)

        # Compute new col B value (same logic as the insert path)
        if col_l_remark == "海運":
            new_b_val = box_id if box_id else col_l_remark
        else:
            new_b_val = box_id

        # Check for duplicates:
        #   Primary:  same tracking + same name
        #   Fallback: same name + same normalised dimensions + same weight
        # When a duplicate is found, update col B (廠商編號) and col D (箱號) on the
        # existing row so corrections are applied without creating a second entry.
        all_rows = ws.get_all_values()[1:]  # Skip header
        dup_row_idx = None  # 0-based index in all_rows
        for i, row in enumerate(all_rows):
            row_name = row[4].strip() if len(row) > 4 else ""
            if row_name != name:
                continue
            row_tracking = row[col_h_idx].strip() if len(row) > col_h_idx else ""
            # Primary match: tracking
            if tracking and row_tracking == tracking:
                dup_row_idx = i
                break
            # Fallback match: normalised dimensions + weight
            row_dim_norm = _norm_dim(row[col_i_idx].strip() if len(row) > col_i_idx else "")
            row_weight_norm = _norm_weight(row[col_k_idx].strip() if len(row) > col_k_idx else "")
            if (new_dim_norm and row_dim_norm == new_dim_norm
                    and new_weight_norm is not None and row_weight_norm == new_weight_norm):
                dup_row_idx = i
                break

        if dup_row_idx is not None:
            sheet_row = dup_row_idx + 2  # +1 for skipped header, +1 for 1-based
            existing_row = all_rows[dup_row_idx]
            existing_tracking = existing_row[col_h_idx].strip() if len(existing_row) > col_h_idx else ""
            updated_fields = []
            if new_b_val:
                ws.update_cell(sheet_row, col_b_idx + 1, new_b_val)
                updated_fields.append(f"廠商編號={new_b_val}")
            if vendor_box_id:
                ws.update_cell(sheet_row, col_d_idx + 1, vendor_box_id)
                updated_fields.append(f"箱號={vendor_box_id}")
            log.info(f"[UPLOAD] Duplicate for {name} (row {sheet_row}), updated: {updated_fields}, existing_tracking={existing_tracking}")
            return {
                "success": True,
                "duplicate": True,
                "existing_tracking": existing_tracking,
                "message": (f"✓ 重複記錄，已更新 {', '.join(updated_fields)}"
                            if updated_fields else f"✓ 重複記錄已略過（{name}）"),
            }

        # Prepare row data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Build row with enough columns to cover col L
        num_cols = max(col_l_idx + 1, 12)
        row_data = [""] * num_cols
        row_data[0] = timestamp                              # A: timestamp
        row_data[4] = name                                   # E: Sender Name/Client ID (default index 4)
        row_data[col_h_idx] = tracking                       # H: Tracking ID
        row_data[col_i_idx] = dimension_clean                # I: Dimension (no unit)
        row_data[col_j_idx] = package_content                # J: Package contents (內容物)
        row_data[col_k_idx] = weight_value                   # K: Weight as number
        
        # Determine col B (廠商編號), col D (箱號 / 廠商箱號), col L (其他備註（要拆）) values
        if col_l_remark == "海運":
            if box_id:
                # YL/SP box ID present: col B = box_id, col L = 海運
                row_data[col_b_idx] = box_id
                row_data[col_l_idx] = col_l_remark
            else:
                # No box ID: col B = 海運, col L = 海運
                row_data[col_b_idx] = col_l_remark
                row_data[col_l_idx] = col_l_remark
            # AB vendor box ID → col D (箱號)
            if vendor_box_id:
                row_data[col_d_idx] = vendor_box_id
        else:
            row_data[col_b_idx] = box_id  # Normal: col B = box_id
            if col_l_remark:              # e.g. 空運
                row_data[col_l_idx] = col_l_remark
            if vendor_box_id:
                row_data[col_d_idx] = vendor_box_id
        
        ws.append_row(row_data)
        log.info(f"[UPLOAD] Successfully added to packing sheet: {tracking} (content: {package_content})")
        return {
            "success": True,
            "duplicate": False,
            "message": f"✓ 打包資料表已記錄"
        }
        
    except Exception as e:
        log.error(f"[UPLOAD] Error uploading to packing sheet: {e}", exc_info=True)
        return {
            "success": False,
            "duplicate": False,
            "message": f"✗ 打包資料表寫入失敗: {str(e)}"
        }
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
              "• Box ID（如 YL123、SP22）\n"
              "• 寄件人 / 客戶名稱\n"
              "• 尺寸（長、寬、高，單位 cm）\n"
              "  ✅ 請用 - / ; , * 連接三個數字\n"
              "  例：51-51-56 或 51/51/56 或 51*51*56\n"
              "  ⚠️ 若尺寸用空格隔開，請在重量加上 kg\n"
              "  例：25.7kg 51 51 56（需有 kg 才能區分）\n"
              "• 重量（kg）\n"
              "  例：25.7 或 25.7kg\n"
              "• 追蹤編號（選填）\n"
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
                    # Fallback: search by box_id (廠商編號 often = ABB帳號 in col E)
                    if not matches and data.get("box_id"):
                        matches = search_sea_form_matches(data["box_id"])
                    valid_matches = [
                        m for m in (matches or [])
                        if isinstance(m, dict) and m.get("timestamp")
                    ]

                    if not valid_matches:
                        # No match found — proceed without Monday (same as before)
                        _process_upload(redis_client, user_id, reply_token, data)
                        return True
                    else:
                        # One or multiple form rows — always show tracking selection
                        # so the user can confirm the matched client info and pick
                        # the correct tracking number before upload proceeds.
                        combined_options = _build_combined_sea_options(valid_matches)
                        if combined_options:
                            redis_client.set(
                                _key(user_id, "sea_trackings"),
                                json.dumps(combined_options, ensure_ascii=False),
                                ex=UPLOAD_TTL,
                            )
                            _set_state(redis_client, user_id, "selecting_sea_tracking")
                            _set_data(redis_client, user_id, data)
                            try:
                                from handlers.upload_data_flex import build_sea_tracking_selection_flex
                                _box_display = data.get("box_id") or ""
                                flex = build_sea_tracking_selection_flex(combined_options, box_id=_box_display)
                                line_reply_flex(reply_token, "📦 找到多筆海運記錄，請選擇追蹤碼", flex)
                            except Exception as fx_err:
                                log.error(f"[UPLOAD] Error building combined tracking flex: {fx_err}", exc_info=True)
                                msg = "📦 找到多筆海運記錄，請選擇追蹤碼：\n\n"
                                for i, opt in enumerate(combined_options):
                                    msg += f"【{i+1}】 {opt['tracking']}\n"
                                    if opt.get("content"):
                                        msg += f"   內容: {opt['content'][:60]}\n"
                                    msg += "\n"
                                msg += "請輸入「選擇追蹤1」、「選擇追蹤2」等指令"
                                line_reply(reply_token, msg)
                        else:
                            # Workspace read failed — fall back to form-row selection
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

    # State: selecting_sea_tracking — user picks which package this box corresponds to
    elif state == "selecting_sea_tracking":
        match_pattern = re.match(r'\u9078\u64c7\u8ffd\u8e64(\d+)', text)
        if match_pattern:
            idx = int(match_pattern.group(1)) - 1
            raw = redis_client.get(_key(user_id, "sea_trackings"))
            sea_trackings = json.loads(raw) if raw else []

            if 0 <= idx < len(sea_trackings):
                data = _get_data(redis_client, user_id)
                selected = sea_trackings[idx]

                sel_subitem_id = selected.get("subitem_id", "")
                monday_updates = []
                data["tracking"] = selected.get("tracking", "")

                if not sel_subitem_id and selected.get("_sea_match"):
                    # subitem_id wasn't pre-fetched — look it up by tracking name
                    # on the Monday subitem board directly.
                    target_trk = data["tracking"]
                    _headers_api = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
                    try:
                        find_sub_q = """
                        query ($b: ID!, $v: String!) {
                            items_page_by_column_values(
                                board_id: $b, limit: 1,
                                columns: [{column_id: "name", column_values: [$v]}]
                            ) { items { id name } }
                        }
                        """
                        resp = requests.post(
                            MONDAY_API_URL, headers=_headers_api,
                            json={"query": find_sub_q, "variables": {"b": SEA_SUBITEM_BOARD_ID, "v": target_trk}},
                            timeout=15,
                        )
                        found_items = resp.json().get("data", {}).get("items_page_by_column_values", {}).get("items", [])
                        if found_items:
                            sel_subitem_id = found_items[0]["id"]
                            log.info(f"[SEA] Resolved subitem by name '{target_trk}' → {sel_subitem_id}")
                        else:
                            log.warning(f"[SEA] No Monday subitem found for tracking '{target_trk}'")
                    except Exception as e:
                        log.error(f"[SEA] Error resolving subitem by name: {e}", exc_info=True)

                _set_data(redis_client, user_id, data)

                # Update dim / weight / vendor_box_id on the resolved subitem
                if sel_subitem_id:
                    monday_updates = update_sea_subitem_data(
                        sel_subitem_id, data["dimension"], data["weight"],
                        data.get("vendor_box_id", ""),
                    )

                pkg_content = sea_trackings[idx].get("content", "")
                sheet_result = upload_to_packing_sheet(
                    data.get("box_id", ""),
                    data["name"],
                    data["tracking"],
                    data["dimension"],
                    data["weight"],
                    "海運",
                    pkg_content,
                    data.get("vendor_box_id", ""),
                )
                box_display = data.get("box_id") or "未提供"
                _monday_line = ("✓ Monday 海運子項目 已更新"
                                + (f"：{', '.join(monday_updates)}" if monday_updates else "")
                                + "\n")
                if sheet_result["success"]:
                    msg = (f"\u2705 \u6d77\u904b\u8cc7\u6599\u4e0a\u50b3\u6210\u529f\uff01\n\n"
                           f"\ud83d\udce6 Box ID: {box_display}\n"
                           f"\ud83d\udc64 \u5bc4\u4ef6\u4eba: {data['name']}\n"
                           f"\ud83d\udd22 \u8ffd\u8e64\u7de8\u865f: {data['tracking']}\n"
                           f"\ud83d\udcaf \u5c3a\u5bf8: {data['dimension']}\n"
                           f"\u2696\ufe0f \u91cd\u91cf: {data['weight']}\n\n"
                           + _monday_line)
                    if sheet_result["duplicate"]:
                        msg += f"\u26a0\ufe0f {sheet_result['message']}\n\n"
                    else:
                        msg += f"\u2713 \u6253\u5305\u8cc7\u6599\u8868 \u5df2\u8a18\u9304\n\n"
                    msg += f"\u7e7c\u7e8c\u8f38\u5165\u8cc7\u6599\uff0c\u6216\u8f38\u5165 'end' \u7d50\u675f"
                else:
                    msg = (f"\u26a0\ufe0f \u90e8\u5206\u6210\u529f\n\n"
                           + _monday_line
                           + f"\u2717 \u6253\u5305\u8cc7\u6599\u8868 \u8a18\u9304\u5931\u6557\n\u7531: {sheet_result['message']}\n\n"
                           f"\u7e7c\u7e8c\u8f38\u5165\u8cc7\u6599\uff0c\u6216\u8f38\u5165 'end' \u7d50\u675f")
                redis_client.delete(_key(user_id, "sea_trackings"))
                _set_state(redis_client, user_id, "collecting")
                _set_data(redis_client, user_id, {})
                line_reply(reply_token, msg)
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
            "更正_vendor_box_id": "vendor_box_id",
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
                "vendor_box_id": "廠商箱號 （例：AB12）",
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
            val = parse_dimension(text, weight_explicitly_given=has_explicit_weight_unit(text))
            if not val:
                line_reply(reply_token, "⚠️ 無法識別尺寸格式，請重新輸入 （例：51-51-56 或 51*51*56）")
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
        elif field == "vendor_box_id":
            val = parse_box_id(text) or text.strip().upper()
            if val:
                data["vendor_box_id"] = val
            else:
                data.pop("vendor_box_id", None)
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
            monday_updates = []

            if sea_match:
                result = create_sea_monday_items(
                    sea_match, data["dimension"], data["weight"],
                    vendor_box_id=data.get("vendor_box_id", ""),
                )
                if result.get("success"):
                    subitems = result.get("subitems", [])
                    log.info(f"[UPLOAD] Received {len(subitems)} subitems from create_sea_monday_items: {[s.get('tracking') for s in subitems]}")
                    if len(subitems) > 1:
                        # Multiple packages: ask user to select which one this box is
                        redis_client.set(
                            _key(user_id, "sea_trackings"),
                            json.dumps(subitems, ensure_ascii=False),
                            ex=UPLOAD_TTL,
                        )
                        _set_state(redis_client, user_id, "selecting_sea_tracking")
                        try:
                            from handlers.upload_data_flex import build_sea_tracking_selection_flex
                            _box_display = data.get("box_id") or ""
                            flex = build_sea_tracking_selection_flex(subitems, box_id=_box_display)
                            line_reply_flex(reply_token, "\ud83d\udce6 \u9019\u500b\u7b71\u5c6c\u65bc\u54ea\u4ef6\u5305\u88f9\uff1f", flex)
                        except Exception as fx_err:
                            log.error(f"[UPLOAD] Error building sea tracking selection flex: {fx_err}", exc_info=True)
                            msg = "\ud83d\udce6 \u8acb\u9078\u64c7\u6b64\u7b71\u5c6c\u65bc\u54ea\u4ef6\u5305\u88f9\uff1a\n\n"
                            for i, s in enumerate(subitems[:3]):
                                msg += f"\u300a{i+1}\u300b \u8ffd\u8e64\u78bc: {s['tracking']}\n"
                                if s.get("content"):
                                    msg += f"\u5167\u5bb9: {s['content'][:60]}\n"
                                msg += "\n"
                            msg += "\u8acb\u8f38\u5165\u300c\u9078\u64c7\u8ffd\u8e641\u300d\u3001\u300c\u9078\u64c7\u8ffd\u8e642\u300d\u7b49\u6307\u4ee4"
                            line_reply(reply_token, msg)
                        return
                    monday_success = True
                    monday_tracking = result.get("tracking", "")
                    data["tracking"] = monday_tracking
                    # Single subitem: set dim/weight/vendor_box_id now
                    single_sub_id = subitems[0].get("subitem_id", "")
                    if single_sub_id:
                        monday_updates = update_sea_subitem_data(
                            single_sub_id, data["dimension"], data["weight"],
                            data.get("vendor_box_id", ""),
                        )
                else:
                    log.warning(f"[UPLOAD] Sea Monday creation failed: {result.get('error')}")

            sheet_result = upload_to_packing_sheet(
                data.get("box_id", ""),
                data["name"],
                data.get("tracking", ""),
                data["dimension"],
                data["weight"],
                col_l_remark,
                data.get("package_content", ""),
                data.get("vendor_box_id", ""),
            )

            box_display = data.get("box_id") or "未提供"
            _m_line = ("✓ Monday 海運板塊 已建立"
                       + (f"：{', '.join(monday_updates)}" if monday_updates else "")
                       + "\n")
            if monday_success and sheet_result["success"]:
                msg = (f"✅ 海運資料上傳成功！\n\n"
                      f"📦 Box ID: {box_display}\n"
                      f"👤 寄件人: {data['name']}\n"
                      f"🔢 追蹤編號: {monday_tracking}\n"
                      f"📏 尺寸: {data['dimension']}\n"
                      f"⚖️ 重量: {data['weight']}\n\n"
                      + _m_line)
                if sheet_result["duplicate"]:
                    msg += f"⚠️ {sheet_result['message']}\n\n"
                else:
                    msg += f"✓ 打包資料表 已記錄\n\n"
                msg += f"繼續輸入資料，或輸入 'end' 結束"
            elif monday_success:
                msg = (f"⚠️ 部分成功\n\n"
                      + _m_line
                      + f"✗ 打包資料表 記錄失敗: {sheet_result['message']}\n\n"
                      f"繼續輸入資料，或輸入 'end' 結束")
            elif sheet_result["success"]:
                if sea_match:
                    msg = (f"⚠️ 部分成功\n\n"
                          f"📦 Box ID: {box_display}\n"
                          f"👤 寄件人: {data['name']}\n"
                          f"📏 尺寸: {data['dimension']}\n"
                          f"⚖️ 重量: {data['weight']}\n\n"
                          f"✗ Monday 海運板塊 建立失敗\n")
                    if sheet_result["duplicate"]:
                        msg += f"⚠️ {sheet_result['message']}\n\n"
                    else:
                        msg += f"✓ 打包資料表 已記錄\n\n"
                    msg += f"繼續輸入資料，或輸入 'end' 結束"
                else:
                    _existing_tkn = sheet_result.get("existing_tracking", "")
                    if sheet_result["duplicate"]:
                        _sheet_status = f"⚠️ 打包資料表現有記錄已更新：{sheet_result['message']}\n"
                        if _existing_tkn:
                            _sheet_status += f"📬 現有追蹤編號: {_existing_tkn}\n"
                    else:
                        _sheet_status = "✓ 打包資料表 已記錄\n"
                    msg = (f"{'📦 找到打包資料表現有記錄' if sheet_result['duplicate'] else '✅ 海運記錄已寫入打包資料表！'}\n\n"
                          f"📦 Box ID: {box_display}\n"
                          f"👤 寄件人: {data['name']}\n"
                          f"📏 尺寸: {data['dimension']}\n"
                          f"⚖️ 重量: {data['weight']}\n\n"
                          + _sheet_status
                          + f"\n⚠️ 未找到海運資料表匹配，Monday 項目未建立\n"
                          f"請至打包資料表補充追蹤編號，\n"
                          f"並自行推送資料至 Monday\n\n"
                          f"繼續輸入資料，或輸入 'end' 結束")
            else:
                msg = f"❌ 上傳失敗: {sheet_result['message']}\n\n輸入重新開始或 'end' 結束"

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
        sheet_result = upload_to_packing_sheet(
            data.get("box_id", ""),
            data["name"],
            data["tracking"],
            data["dimension"],
            data["weight"],
            col_l_remark,
            data.get("package_content", ""),
        )
        
        # Send result
        if monday_success and sheet_result["success"]:
            msg = (f"✅ 資料上傳成功！\n\n"
                  f"📦 Box ID: {data.get('box_id', '未提供')}\n"
                  f"👤 寄件人: {data['name']}\n"
                  f"🔢 追蹤編號: {data['tracking']}\n"
                  f"📏 尺寸: {data['dimension']}\n"
                  f"⚖️ 重量: {data['weight']}\n\n"
                  f"✓ Monday 已更新\n")
            if sheet_result["duplicate"]:
                msg += f"⚠️ {sheet_result['message']}\n\n"
            else:
                msg += f"✓ 打包資料表 已記錄\n\n"
            msg += f"繼續輸入資料，或輸入 'end' 結束"
        elif monday_success:
            msg = (f"⚠️ 部分成功\n\n"
                  f"✓ Monday 已更新\n"
                  f"✗ 打包資料表 記錄失敗: {sheet_result['message']}\n\n"
                  f"繼續輸入資料，或輸入 'end' 結束")
        elif sheet_result["success"]:
            msg = (f"⚠️ 部分成功\n\n"
                  f"✗ Monday 更新失敗\n")
            if sheet_result["duplicate"]:
                msg += f"⚠️ {sheet_result['message']}\n\n"
            else:
                msg += f"✓ 打包資料表 已記錄\n\n"
            msg += f"繼續輸入資料，或輸入 'end' 結束"
        else:
            msg = f"❌ 上傳失敗: {sheet_result['message']}\n\n輸入重新開始或 'end' 結束"
        
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
