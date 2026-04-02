"""
上傳資料流程 - Flex Message 建構模組
────────────────────────────────────────
Flex Message JSON builders for upload data feature.
"""

from typing import List, Dict, Any, Optional


def _kv_row(label: str, value: str, value_color: str = "#000000") -> dict:
    """Create a key-value row for flex message."""
    return {
        "type": "box", "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label,
             "size": "sm", "color": "#888888", "flex": 2},
            {"type": "text", "text": value,
             "size": "sm", "flex": 3, "align": "end", "weight": "bold",
             "color": value_color, "wrap": True},
        ],
    }


def build_data_confirm_flex(data: Dict[str, Any]) -> dict:
    """
    Build confirmation flex message showing parsed upload data.
    
    Args:
        data: Dict with keys: box_id, name, dimension, weight, tracking (optional)
    """
    body = [
        {"type": "text", "text": "📦 包裹資料確認",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
        {"type": "separator", "margin": "md"},
        {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm",
         "contents": []},
    ]
    
    # Add parsed fields
    content_box = body[2]["contents"]
    
    if data.get("box_id"):
        content_box.append(_kv_row("📦 Box ID", data["box_id"], "#0066cc"))
    else:
        content_box.append(_kv_row("📦 Box ID", "❌ 未提供", "#dc3545"))
    
    if data.get("name"):
        content_box.append(_kv_row("👤 寄件人/客戶", data["name"], "#0066cc"))
    else:
        content_box.append(_kv_row("👤 寄件人/客戶", "❌ 未提供", "#dc3545"))
    
    if data.get("dimension"):
        content_box.append(_kv_row("📏 尺寸", data["dimension"], "#0066cc"))
    else:
        content_box.append(_kv_row("📏 尺寸", "❌ 未提供", "#dc3545"))
    
    if data.get("weight"):
        content_box.append(_kv_row("⚖️ 重量", data["weight"], "#0066cc"))
    else:
        content_box.append(_kv_row("⚖️ 重量", "❌ 未提供", "#dc3545"))
    
    if data.get("tracking"):
        content_box.append(_kv_row("🔢 追蹤編號", data["tracking"], "#0066cc"))
    else:
        content_box.append(_kv_row("🔢 追蹤編號", "⚠️ 未提供(將搜尋)", "#ffc107"))
    
    # Add instruction text
    body.append({"type": "separator", "margin": "lg"})
    
    missing_fields = [k for k in ["box_id", "name", "dimension", "weight"] 
                     if not data.get(k)]
    
    if missing_fields:
        body.append({
            "type": "text",
            "text": "⚠️ 請補充缺少的資料，或選擇重新開始",
            "size": "sm", "color": "#dc3545", "margin": "lg", "wrap": True
        })
    else:
        body.append({
            "type": "text",
            "text": "✅ 資料已完整，請確認",
            "size": "sm", "color": "#28a745", "margin": "lg"
        })
    
    # Footer with action buttons
    footer_contents = []
    
    if not missing_fields:
        # All required fields present - show confirm button
        footer_contents.append({
            "type": "button", "height": "sm", "style": "primary",
            "color": "#28a745",
            "action": {"type": "message", "label": "✅ 確認上傳", "text": "確認上傳資料"}
        })
    
    footer_contents.extend([
        {"type": "button", "height": "sm", "style": "secondary",
         "action": {"type": "message", "label": "🔄 重新開始", "text": "重新開始"}},
        {"type": "button", "height": "sm", "style": "secondary",
         "action": {"type": "message", "label": "❌ 取消", "text": "取消上傳"}}
    ])
    
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "contents": body},
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": footer_contents
        }
    }


def build_match_selection_flex(matches: List[Dict[str, str]]) -> dict:
    """
    Build flex message for selecting a matching record from 空運資料表.
    
    Args:
        matches: List of dicts with keys: timestamp, chinese_name, english_name, client_id
    """
    body = [
        {"type": "text", "text": "🔍 找到以下匹配記錄",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
        {"type": "text", "text": "請選擇正確的匹配項目：",
         "size": "sm", "color": "#888888", "margin": "md"},
        {"type": "separator", "margin": "md"},
    ]
    
    for i, match in enumerate(matches[:5]):  # Limit to 5 matches
        match_box = {
            "type": "box", "layout": "vertical",
            "margin": "md", "spacing": "sm",
            "backgroundColor": "#f0f0f0",
            "cornerRadius": "md",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": f"選項 {i+1}",
                 "weight": "bold", "size": "md", "color": "#0066cc"},
                _kv_row("📅 時間", match.get("timestamp", "N/A")),
                _kv_row("🇨🇳 中文姓名", match.get("chinese_name", "N/A")),
                _kv_row("🇬🇧 英文姓名", match.get("english_name", "N/A")),
                _kv_row("🆔 客戶ID", match.get("client_id", "N/A")),
                {"type": "button", "height": "sm", "style": "primary",
                 "margin": "md",
                 "action": {"type": "message",
                           "label": f"選擇此項目",
                           "text": f"選擇匹配{i+1}"}}
            ]
        }
        body.append(match_box)
    
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "contents": body},
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": [
                {"type": "button", "height": "sm", "style": "secondary",
                 "action": {"type": "message", "label": "取消並重新開始", "text": "重新開始"}}
            ]
        }
    }
