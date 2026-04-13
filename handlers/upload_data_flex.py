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
    elif data.get("hai_yun"):
        content_box.append(_kv_row("📦 Box ID", "⬜ 選填（海運）", "#888888"))
    else:
        content_box.append(_kv_row("📦 Box ID", "❌ 未提供", "#dc3545"))
    
    # 廠商箱號 (AB-prefix): show for sea freight or when present
    if data.get("vendor_box_id"):
        content_box.append(_kv_row("🏢 廠商箱號", data["vendor_box_id"], "#0066cc"))
    elif data.get("hai_yun"):
        content_box.append(_kv_row("🏢 廠商箱號", "⬜ 選填（ABxx）", "#888888"))
    
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
    elif data.get("hai_yun"):
        content_box.append(_kv_row("🔢 追蹤編號", "⬜ 海運免填", "#888888"))
    else:
        content_box.append(_kv_row("🔢 追蹤編號", "⚠️ 未提供(將搜尋)", "#ffc107"))

    if data.get("hai_yun"):
        content_box.append(_kv_row("🚢 運送方式", data["hai_yun"], "#0066cc"))
    elif data.get("kong_yun"):
        content_box.append(_kv_row("✈️ 運送方式", data["kong_yun"], "#0066cc"))
    
    # Add instruction text
    body.append({"type": "separator", "margin": "lg"})
    
    missing_fields = (
        [k for k in ["name", "dimension", "weight"] if not data.get(k)]
        if data.get("hai_yun")
        else [k for k in ["box_id", "name", "dimension", "weight"] if not data.get(k)]
    )
    
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
         "color": "#ff8c00",
         "action": {"type": "message", "label": "✏️ 更正資料", "text": "更正資料"}},
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
    Build flex message for selecting a matching record from 空運資料表 or
    as sea-freight fallback when combined-options are unavailable.

    Args:
        matches: List of dicts with keys: timestamp, chinese_name, english_name, client_id
    """
    # ── Merge client info shown once in header ─────────────────────────────────
    cn_set  = {m.get("chinese_name", "") for m in matches} - {""}
    en_set  = {m.get("english_name", "") for m in matches} - {""}
    cid_set = {m.get("client_id", "")    for m in matches} - {""}

    header_parts = []
    if cn_set:
        header_parts.append(f"中文名: {cn_set.pop()[:30]}")
    if en_set:
        header_parts.append(f"英文名: {en_set.pop()[:30]}")
    if cid_set:
        header_parts.append(f"Abowbow: {cid_set.pop()[:30]}")

    count = len(matches)
    body = [
        {"type": "text",
         "text": f"🔍 找到 {count} 筆匹配記錄",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
    ]
    if header_parts:
        body.append({
            "type": "text",
            "text": " ｜ ".join(header_parts),
            "size": "sm", "color": "#333333", "margin": "md", "wrap": True,
        })
    body.append({"type": "text", "text": "請選擇正確的匹配項目：",
                 "size": "sm", "color": "#888888", "margin": "md"})
    body.append({"type": "separator", "margin": "md"})

    for i, match in enumerate(matches[:5]):  # Limit to 5 matches
        timestamp = (match.get("timestamp") or "N/A")[:50]
        match_box = {
            "type": "box", "layout": "vertical",
            "margin": "md", "spacing": "sm",
            "backgroundColor": "#f0f0f0",
            "cornerRadius": "md",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": f"選項 {i+1}",
                 "weight": "bold", "size": "md", "color": "#0066cc"},
                _kv_row("時間戳記", timestamp),
                {"type": "button", "height": "sm", "style": "primary",
                 "margin": "md",
                 "action": {"type": "message",
                            "label": "選擇此項目",
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


def build_field_selection_flex() -> dict:
    """
    Build flex message prompting the user to pick which field to correct.
    Each button sends a message like "更正_box_id" which the handler intercepts.
    """
    fields = [
        ("📦 Box ID",     "更正_box_id"),
        ("🏢 廠商箱號",  "更正_vendor_box_id"),
        ("👤 寄件人/客戶", "更正_name"),
        ("📏 尺寸",       "更正_dimension"),
        ("⚖️ 重量",       "更正_weight"),
        ("🔢 追蹤編號",   "更正_tracking"),
        ("🚢 運送方式",   "更正_transport"),
    ]

    buttons = [
        {
            "type": "button", "height": "sm", "style": "secondary",
            "action": {"type": "message", "label": label, "text": text},
        }
        for label, text in fields
    ]
    buttons.append({
        "type": "button", "height": "sm", "style": "secondary",
        "action": {"type": "message", "label": "← 返回確認", "text": "返回確認"},
    })

    return {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical",
            "contents": [
                {"type": "text", "text": "✏️ 更正哪個欄位？",
                 "weight": "bold", "size": "xl", "color": "#1a1a1a"},
                {"type": "text", "text": "請選擇要更正的欄位：",
                 "size": "sm", "color": "#888888", "margin": "md"},
            ],
        },
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": buttons,
        },
    }


def _build_sea_selection_bubble(chunk: list, global_offset: int, total: int,
                                box_id: str = "", merged_info: dict = None) -> dict:
    """Build one bubble for a page of sea-tracking-selection options.

    Args:
        chunk:         Slice of the full subitems list for this bubble.
        global_offset: Index of chunk[0] in the full list (for 1-based button labels).
        total:         Total number of options across all bubbles (for header text).
        box_id:        Current box ID to display in the header.
        merged_info:   If all options share the same chinese_name/english_name/client_id,
                       these are merged into one dict shown in the header.
                       Keys: chinese_name, english_name, client_id (each str or None).
    """
    _box_label = f"({box_id})" if box_id else ""
    body_contents = [
        {
            "type": "text",
            "text": "🚢 請選擇對應追蹤號碼",
            "weight": "bold", "size": "xl", "color": "#1a1a1a",
        },
        {
            "type": "text",
            "text": (f"這個箱子{_box_label}的追蹤號碼是哪一個？"
                     + (f"（共 {total} 個選項）" if total > 3 else "")),
            "size": "sm", "color": "#888888", "margin": "md", "wrap": True,
        },
    ]

    # ── Merged customer info (shown once in header if identical) ──
    if merged_info:
        info_parts = []
        cn = merged_info.get("chinese_name")
        en = merged_info.get("english_name")
        cid = merged_info.get("client_id")
        if cn:
            info_parts.append(f"中文名: {cn}")
        if en:
            info_parts.append(f"英文名: {en}")
        if cid:
            info_parts.append(f"Abowbow: {cid}")
        if info_parts:
            body_contents.append({
                "type": "text",
                "text": " ｜ ".join(info_parts),
                "size": "xs", "color": "#333333", "margin": "md", "wrap": True,
            })
            body_contents.append({"type": "separator", "margin": "md"})

    buttons = []
    for local_i, item in enumerate(chunk):
        global_i = global_offset + local_i          # 0-based global index
        option_no = global_i + 1                    # 1-based for button text
        tracking = item.get("tracking", "")
        content = item.get("content", "")
        preview = (content[:60] + "…") if len(content) > 60 else (content or "(無包裹內容)")

        option_contents = [
            {
                "type": "text",
                "text": f"選項{option_no}：{tracking}",
                "weight": "bold", "size": "sm", "color": "#0057b8",
            },
            {
                "type": "text",
                "text": preview,
                "size": "xs", "color": "#555555", "wrap": True,
            },
        ]

        # Per-option customer info (only when NOT merged)
        if not merged_info:
            sea_match = item.get("_sea_match", {})
            per_parts = []
            cn = sea_match.get("chinese_name", "")
            en = sea_match.get("english_name", "")
            cid = sea_match.get("client_id", "")
            if cn:
                per_parts.append(f"中文: {cn}")
            if en:
                per_parts.append(f"英文: {en}")
            if cid:
                per_parts.append(f"ABB: {cid}")
            if per_parts:
                option_contents.append({
                    "type": "text",
                    "text": " ｜ ".join(per_parts),
                    "size": "xxs", "color": "#999999", "wrap": True,
                })

        body_contents.append({
            "type": "box", "layout": "vertical",
            "margin": "lg",
            "contents": option_contents,
        })
        buttons.append({
            "type": "button",
            "height": "sm",
            "style": "primary" if local_i == 0 else "secondary",
            "action": {
                "type": "message",
                "label": tracking[:40],
                "text": f"選擇追蹤{option_no}",
            },
        })

    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "contents": body_contents},
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": buttons},
    }


def build_sea_tracking_selection_flex(subitems: list, box_id: str = "") -> dict:
    """
    Build a flex message asking the user which sea-freight tracking number
    belongs to the current physical box.  Each entry shows the tracking label
    and a short package-content preview.

    `subitems` is a list of dicts:
        {"tracking": str, "content": str, "subitem_id": str,
         "_sea_match": {"chinese_name": ..., "english_name": ..., "client_id": ...}}

    The button text is "選擇追蹤N" (1-indexed), which the handler reads.
    Supports up to 9 options via a carousel (3 options per bubble page).
    """
    total = len(subitems)
    page_size = 3

    # ── Compute merged customer info (merge if all options share the same) ──
    merged_info = None
    if total > 0:
        first_m = (subitems[0].get("_sea_match") or {})
        cn_set = set()
        en_set = set()
        cid_set = set()
        for item in subitems:
            m = item.get("_sea_match") or {}
            cn_set.add(m.get("chinese_name", ""))
            en_set.add(m.get("english_name", ""))
            cid_set.add(m.get("client_id", ""))
        # Remove empties for comparison
        cn_vals = cn_set - {""}
        en_vals = en_set - {""}
        cid_vals = cid_set - {""}
        if len(cn_vals) <= 1 and len(en_vals) <= 1 and len(cid_vals) <= 1:
            merged_info = {
                "chinese_name": cn_vals.pop() if cn_vals else None,
                "english_name": en_vals.pop() if en_vals else None,
                "client_id": cid_vals.pop() if cid_vals else None,
            }

    if total <= page_size:
        return _build_sea_selection_bubble(subitems, global_offset=0, total=total,
                                           box_id=box_id, merged_info=merged_info)

    # Multiple pages → carousel
    bubbles = []
    for start in range(0, total, page_size):
        chunk = subitems[start:start + page_size]
        bubbles.append(_build_sea_selection_bubble(chunk, global_offset=start, total=total,
                                                    box_id=box_id, merged_info=merged_info))
    return {"type": "carousel", "contents": bubbles}
