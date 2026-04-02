"""
上傳資料處理器 - Upload Data Handler
────────────────────────────────────────────
Manages the multi-step upload data conversation via LINE Flex Messages.

State Machine (persisted in Redis with 10-min TTL):
  collecting      → 等待使用者輸入資料
  confirming      → 資料已解析，等待確認
  selecting_match → 等待使用者選擇匹配記錄
  uploading       → 正在上傳資料
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
from handlers.upload_data_flex import build_data_confirm_flex, build_match_selection_flex

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
UPLOAD_TTL = 600  # 10 minutes

# Google Sheet IDs
AIR_FORM_SHEET_ID = "1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q"
PACKING_SHEET_ID = "1vn_LSZlMGNlhId1N8hBjX-r3sptlw5liPd3nGpdAhsY"


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
    for suffix in ("state", "data", "matches", "reply_token"):
        r.delete(_key(uid, suffix))


def _get_reply_token(r, uid):
    return r.get(_key(uid, "reply_token"))


def _set_reply_token(r, uid, token):
    r.set(_key(uid, "reply_token"), token, ex=UPLOAD_TTL)


# ─── Data Parsers ─────────────────────────────────────────────────────────────

def parse_box_id(text: str) -> Optional[str]:
    """Parse Box ID in format YL followed by 2-3 digits."""
    match = re.search(r'\bYL(\d{2,3})\b', text, re.IGNORECASE)
    if match:
        return f"YL{match.group(1)}"
    return None


def parse_dimension(text: str) -> Optional[str]:
    """
    Parse dimension in format: 62*42*38cm or 62*42*38 or 62 42 38
    Converts inches to cm if specified.
    Returns format: "62*42*38cm"
    """
    # Match dimension patterns (support ×, x, *, or space as separator)
    pattern = r'(\d+(?:\.\d+)?)[×x*\s]+(\d+(?:\.\d+)?)[×x*\s]+(\d+(?:\.\d+)?)(?:\s*)(cm|公分|in|inch|吋|")?'
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        l, w, h = map(float, match.group(1, 2, 3))
        unit = (match.group(4) or "cm").lower()
        
        # Convert inches to cm
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
    - FedEx: 12 digits
    """
    # UPS format: 1Z + 16 characters
    ups_match = re.search(r'\b(1Z[A-Z0-9]{16})\b', text, re.IGNORECASE)
    if ups_match:
        return ups_match.group(1).upper()
    
    # FedEx format: 12 digits
    fedex_match = re.search(r'\b(\d{12})\b', text)
    if fedex_match:
        return fedex_match.group(1)
    
    return None


def parse_name(text: str, existing_data: Dict[str, Any]) -> Optional[str]:
    """
    Parse sender name or client ID.
    This is trickier - we'll extract text that's not part of other fields.
    """
    # Remove other parsed fields from text
    cleaned = text
    
    # Remove box ID
    cleaned = re.sub(r'\bYL\d{2,3}\b', '', cleaned, flags=re.IGNORECASE)
    
    # Remove dimensions
    cleaned = re.sub(r'\d+[×x*\s]+\d+[×x*\s]+\d+\s*(cm|in|inch|吋|公分|")?', '', cleaned, flags=re.IGNORECASE)
    
    # Remove weight
    cleaned = re.sub(r'\d+(?:\.\d+)?\s*(kg|lbs?|公斤|磅)?', '', cleaned, flags=re.IGNORECASE)
    
    # Remove tracking
    cleaned = re.sub(r'\b1Z[A-Z0-9]{16}\b', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\b\d{12}\b', '', cleaned)
    
    # Clean up whitespace and punctuation
    cleaned = re.sub(r'[*×x\s\-_/\\]+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    # If we have something left and it's not too long, use it
    if cleaned and len(cleaned) <= 50:
        return cleaned
    
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
    
    return data


def is_data_complete(data: Dict[str, Any]) -> bool:
    """Check if all required fields are present."""
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


# ─── Monday Upload Function ───────────────────────────────────────────────────

def upload_to_monday(tracking_no: str, dimensions: str, weight: str) -> bool:
    """
    Upload dimension and weight data to Monday based on tracking number.
    
    Args:
        tracking_no: Tracking number or timestamp
        dimensions: Dimension string (e.g., "40*62*32cm")
        weight: Weight string (e.g., "12.15kg")
        
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
        
        log.info(f"[UPLOAD] Successfully updated Monday item {item_id} for tracking {tracking_no}")
        return True
        
    except Exception as e:
        log.error(f"[UPLOAD] Error uploading to Monday: {e}", exc_info=True)
        return False


# ─── 打包資料表 Upload Function ───────────────────────────────────────────────

def upload_to_packing_sheet(box_id: str, name: str, tracking: str, dimension: str, weight: str) -> bool:
    """
    Upload data to 打包資料表 Form Responses 1 if tracking not already present.
    
    Args:
        box_id: Box ID (e.g., YL123)
        name: Sender name or client ID
        tracking: Tracking number or timestamp
        dimension: Dimension string (e.g., "40*62*32cm")
        weight: Weight string (e.g., "12.15kg")
        
    Returns:
        True if successful, False otherwise
    """
    try:
        gs = get_gspread_client()
        ss = gs.open_by_key(PACKING_SHEET_ID)
        ws = ss.worksheet("Form Responses 1")
        
        # Check if tracking already exists in column H
        col_h_values = ws.col_values(8)  # Column H
        
        if tracking in col_h_values:
            log.info(f"[UPLOAD] Tracking {tracking} already exists in packing sheet")
            return True  # Already exists, consider it success
        
        # Prepare row data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Remove units from dimension and weight for sheet storage
        # Dimension: "52*52*41cm" → "52*52*41"
        dimension_clean = dimension.replace("cm", "").replace("CM", "").strip()
        
        # Weight: "18.50kg" → "18.50"
        weight_clean = weight.replace("kg", "").replace("KG", "").strip()
        
        row_data = [
            timestamp,         # A: timestamp
            box_id,            # B: Box ID
            "",                # C: empty
            "",                # D: empty
            name,              # E: Sender Name/Client ID
            "",                # F: empty
            "",                # G: empty
            tracking,          # H: Tracking ID
            dimension_clean,   # I: Dimension (without cm)
            "",                # J: empty
            weight_clean       # K: Weight (without kg)
        ]
        
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
        return True
    
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
              "• 追蹤編號 (選填)\n\n"
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
        if text == "確認上傳資料":
            data = _get_data(redis_client, user_id)
            
            # Check if we need to search for tracking
            if not data.get("tracking"):
                matches = search_air_form_matches(data["name"])
                
                if not matches:
                    line_reply(reply_token, 
                              "⚠️ 未找到匹配的空運表單記錄\n"
                              "請手動輸入追蹤編號，或選擇重新開始")
                    return True
                
                elif len(matches) == 1:
                    # Auto-select single match
                    data["tracking"] = matches[0]["timestamp"]
                    _set_data(redis_client, user_id, data)
                    _process_upload(redis_client, user_id, reply_token, data)
                    return True
                
                else:
                    # Multiple matches - show selection
                    _set_matches(redis_client, user_id, matches)
                    _set_state(redis_client, user_id, "selecting_match")
                    flex = build_match_selection_flex(matches)
                    line_reply_flex(reply_token, "🔍 請選擇匹配項目", flex)
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
                data["tracking"] = matches[idx]["timestamp"]
                _set_data(redis_client, user_id, data)
                _process_upload(redis_client, user_id, reply_token, data)
                return True
        
        return True
    
    return False


def _process_upload(redis_client, user_id: str, reply_token: str, data: Dict[str, Any]):
    """Process the actual upload to Monday and packing sheet."""
    try:
        # Upload to Monday
        monday_success = upload_to_monday(
            data["tracking"],
            data["dimension"],
            data["weight"]
        )
        
        # Upload to packing sheet
        sheet_success = upload_to_packing_sheet(
            data["box_id"],
            data["name"],
            data["tracking"],
            data["dimension"],
            data["weight"]
        )
        
        # Send result
        if monday_success and sheet_success:
            msg = (f"✅ 資料上傳成功！\n\n"
                  f"📦 Box ID: {data['box_id']}\n"
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
