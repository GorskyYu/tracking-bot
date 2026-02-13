"""
報價互動處理器 - Quote Flow Handler
────────────────────────────────────
Manages the multi-step quote conversation via LINE Flex Messages.

State Machine (persisted in Redis with 10-min TTL):
  collecting   → 等待使用者貼上客人訊息
  parsed       → 資料已解析，等待「正確/錯誤」確認
  correcting   → 使用者按了「錯誤」, 等待手動輸入
  choosing_mode→ 等待選擇「空運/海運」
"""

import json
import logging
import threading
from typing import Optional, List

from services.quote_service import (
    ParsedInput, Package, ServiceQuote, BoxWeights,
    parse_package_input, try_parse_structured,
    get_te_quotes, get_cp_quotes,
    calculate_box_weights, build_quote_text,
    WAREHOUSE_POSTAL, _fmt_postal,
)
from services.line_service import (
    line_push, line_reply, line_push_flex,
    line_reply_flex, line_push_messages,
)

log = logging.getLogger(__name__)

# ─── Redis Key Helpers ────────────────────────────────────────────────────────
QUOTE_TTL = 600  # 10 minutes


def _key(user_id: str, suffix: str) -> str:
    return f"quote:{user_id}:{suffix}"


def _get_state(r, uid):
    return r.get(_key(uid, "state"))


def _set_state(r, uid, state):
    r.set(_key(uid, "state"), state, ex=QUOTE_TTL)


def _get_data(r, uid):
    raw = r.get(_key(uid, "data"))
    return json.loads(raw) if raw else None


def _set_data(r, uid, data):
    r.set(_key(uid, "data"), json.dumps(data, ensure_ascii=False), ex=QUOTE_TTL)


def _get_buffer(r, uid):
    return r.get(_key(uid, "buffer")) or ""


def _append_buffer(r, uid, text):
    buf = _get_buffer(r, uid)
    new_buf = buf + "\n" + text if buf else text
    r.set(_key(uid, "buffer"), new_buf, ex=QUOTE_TTL)


def _clear_session(r, uid):
    for suffix in ("state", "data", "buffer", "target"):
        r.delete(_key(uid, suffix))


def _get_target(r, uid):
    return r.get(_key(uid, "target")) or uid


def _set_target(r, uid, target_id):
    r.set(_key(uid, "target"), target_id, ex=QUOTE_TTL)


# ─── Public API ───────────────────────────────────────────────────────────────

def is_in_quote_session(r, user_id: str) -> bool:
    """Check whether a user currently has an active quote session."""
    return _get_state(r, user_id) is not None


def handle_quote_trigger(event: dict, user_id: str,
                         group_id: Optional[str], r) -> bool:
    """Handle '開始報價' trigger.  Returns True if consumed."""
    reply_token = event.get("replyToken")
    target_id = group_id or user_id

    _clear_session(r, user_id)
    _set_state(r, user_id, "collecting")
    _set_target(r, user_id, target_id)

    line_reply(
        reply_token,
        "📝 報價模式已啟動！\n\n"
        "請貼上客人的訊息（包含包裹尺寸、重量、郵遞區號）。\n"
        "可以一次貼上或分多次貼上，我會自動讀取資料。\n\n"
        "💡 輸入「取消報價」可隨時退出。"
    )
    return True


def handle_quote_message(event: dict, user_id: str,
                         group_id: Optional[str], text: str, r) -> bool:
    """Route a message through the active quote session.  Returns True if consumed."""
    state = _get_state(r, user_id)
    if not state:
        return False

    target_id = _get_target(r, user_id)

    # ── universal cancel ──────────────────────────────────────────────────
    if text == "取消報價":
        _clear_session(r, user_id)
        line_push(target_id, "已取消報價。")
        return True

    # ── state dispatch ────────────────────────────────────────────────────
    if state == "collecting":
        return _on_collecting(r, user_id, target_id, text)

    if state == "parsed":
        if text == "報價確認正確":
            return _on_confirmed(r, user_id, target_id)
        if text == "報價確認錯誤":
            return _on_rejected(r, user_id, target_id)
        if text == "報價重新輸入":
            _clear_session(r, user_id)
            _set_state(r, user_id, "collecting")
            _set_target(r, user_id, target_id)
            line_push(target_id, "已清除資料，請重新輸入包裹資訊。")
            return True
        # Any other text → treat as additional input, re-parse
        return _on_collecting(r, user_id, target_id, text)

    if state == "correcting":
        return _on_correcting(r, user_id, target_id, text)

    if state == "choosing_mode":
        if text == "報價選擇空運":
            return _on_mode_selected(r, user_id, target_id, "加台空運")
        if text == "報價選擇海運":
            return _on_mode_selected(r, user_id, target_id, "加台海運")
        line_push(target_id, "請點選「✈️ 空運」或「🚢 海運」按鈕選擇運送方式。")
        return True

    return False


# ─── Private State Handlers ──────────────────────────────────────────────────

def _on_collecting(r, uid, target, text):
    """Parse message text and show confirm flex."""
    _append_buffer(r, uid, text)
    full_text = _get_buffer(r, uid)

    parsed = parse_package_input(full_text)

    if not parsed or not parsed.packages:
        line_push(
            target,
            "🔍 尚未偵測到完整的包裹資料。\n"
            "請確認訊息包含：\n"
            "• 包裹尺寸（長×寬×高，公分）\n"
            "• 重量（公斤）\n"
            "• 加拿大郵遞區號（如 V6X 1Z7）\n\n"
            "可繼續貼上更多訊息，或輸入「取消報價」退出。"
        )
        return True

    _save_parsed(r, uid, parsed)
    _set_state(r, uid, "parsed")

    flex = _build_confirm_flex(parsed)
    line_push_flex(target, "📦 包裹資料確認", flex)
    return True


def _on_confirmed(r, uid, target):
    """User confirmed → decide next step based on postal code count."""
    data = _get_data(r, uid)
    if not data:
        line_push(target, "❌ 資料遺失，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    postal_codes = data.get("postal_codes", [])

    if len(postal_codes) >= 2:
        # Two postal codes → 加境內, skip mode selection
        return _on_mode_selected(r, uid, target, "加境內")
    elif len(postal_codes) == 1:
        _set_state(r, uid, "choosing_mode")
        flex = _build_mode_select_flex()
        line_push_flex(target, "請選擇運送方式", flex)
        return True
    else:
        line_push(
            target,
            "⚠️ 未偵測到郵遞區號。\n"
            "請補充加拿大郵遞區號（如 V6X 1Z7），或輸入「取消報價」退出。"
        )
        _set_state(r, uid, "collecting")
        return True


def _on_rejected(r, uid, target):
    """User said data is wrong → switch to manual entry mode."""
    _set_state(r, uid, "correcting")
    r.delete(_key(uid, "buffer"))

    line_push(
        target,
        "📝 請重新輸入正確的包裹資訊。\n\n"
        "格式範例（每行一個包裹）：\n"
        "─────────────\n"
        "113*50*20 7\n"
        "80*40*30 5\n"
        "B2V1R9\n"
        "─────────────\n\n"
        "📮 郵遞區號單獨一行\n"
        "📮 如為境內運送，請提供兩組郵遞區號\n"
        "💡 也可以直接貼上客人訊息，系統會再次嘗試自動解析"
    )
    return True


def _on_correcting(r, uid, target, text):
    """Process text during correction mode."""
    # Try structured first, then OpenAI
    parsed = try_parse_structured(text)
    if not parsed or not parsed.packages:
        parsed = parse_package_input(text)

    if not parsed or not parsed.packages:
        line_push(
            target,
            "❌ 無法解析輸入，格式不正確。\n\n"
            "正確格式（每行一個包裹）：\n"
            "長*寬*高 重量\n\n"
            "範例：\n"
            "113*50*20 7\n"
            "80*40*30 5\n"
            "B2V1R9\n\n"
            "💡 尺寸單位：公分，重量單位：公斤"
        )
        return True

    # Preserve postal codes from previous data if not re-provided
    old_data = _get_data(r, uid)
    if old_data and not parsed.postal_codes and old_data.get("postal_codes"):
        parsed.postal_codes = old_data["postal_codes"]

    _save_parsed(r, uid, parsed)
    _set_state(r, uid, "parsed")
    r.set(_key(uid, "buffer"), text, ex=QUOTE_TTL)

    flex = _build_confirm_flex(parsed)
    line_push_flex(target, "📦 包裹資料確認", flex)
    return True


def _on_mode_selected(r, uid, target, mode):
    """Mode determined → call APIs and deliver results (in background thread)."""
    data = _get_data(r, uid)
    if not data:
        line_push(target, "❌ 資料遺失，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    packages = [
        Package(p["length"], p["width"], p["height"], p["weight"])
        for p in data["packages"]
    ]
    postal_codes = data.get("postal_codes", [])

    if mode == "加境內":
        from_postal = postal_codes[0] if len(postal_codes) >= 1 else ""
        to_postal   = postal_codes[1] if len(postal_codes) >= 2 else ""
    else:
        from_postal = postal_codes[0] if postal_codes else ""
        to_postal   = WAREHOUSE_POSTAL

    if not from_postal or not to_postal:
        line_push(target, "❌ 郵遞區號不足，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    # Clear session immediately so user can start a new one
    _clear_session(r, uid)

    line_push(target, f"📡 正在查詢{mode}運費，請稍候…")

    # Run API calls in a background thread to avoid webhook timeout
    threading.Thread(
        target=_fetch_and_send_quote,
        args=(target, mode, from_postal, to_postal, packages),
        daemon=True,
    ).start()

    return True


def _fetch_and_send_quote(target, mode, from_postal, to_postal, packages):
    """Background: call TE + CP APIs, build messages, and push results."""
    try:
        te_quotes = get_te_quotes(from_postal, to_postal, packages)
        cp_quotes = get_cp_quotes(from_postal, to_postal, packages)

        all_quotes = sorted(te_quotes + cp_quotes, key=lambda q: q.total)

        if not all_quotes:
            line_push(target, "❌ 無法取得運費報價，請稍後再試或手動使用報價計算器。")
            return

        cheapest = all_quotes[0]
        box_weights = calculate_box_weights(packages, mode)

        # Build canned text message
        quote_text = build_quote_text(
            mode, from_postal, to_postal,
            packages, box_weights, cheapest, all_quotes,
        )

        # Build flex table
        flex = _build_result_flex(all_quotes, mode)

        # Push both messages
        line_push_messages(target, [
            {"type": "text", "text": quote_text},
            {"type": "flex", "altText": f"📊 {mode}運費比較表", "contents": flex},
        ])

    except Exception as e:
        log.error(f"[QuoteHandler] Background quote error: {e}", exc_info=True)
        line_push(target, f"❌ 報價過程發生錯誤: {e}")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _save_parsed(r, uid, parsed: ParsedInput):
    data = {
        "packages": [
            {"length": p.length, "width": p.width, "height": p.height, "weight": p.weight}
            for p in parsed.packages
        ],
        "postal_codes": parsed.postal_codes,
    }
    _set_data(r, uid, data)


# ─── Flex Message Builders ────────────────────────────────────────────────────

def _build_confirm_flex(parsed: ParsedInput) -> dict:
    """Data-confirmation bubble with 正確 / 錯誤 / 重新輸入 buttons."""
    body = [
        {"type": "text", "text": "📦 包裹資料確認",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
        {"type": "separator", "margin": "md"},
    ]

    for i, pkg in enumerate(parsed.packages):
        body.append({
            "type": "box", "layout": "vertical",
            "margin": "lg", "spacing": "sm",
            "contents": [
                {"type": "text", "text": f"Box {i+1}",
                 "weight": "bold", "size": "md", "color": "#333333"},
                _kv_row("尺寸", f"{pkg.length:.0f} × {pkg.width:.0f} × {pkg.height:.0f} cm"),
                _kv_row("重量", f"{pkg.weight:.1f} kg"),
                _kv_row("材積重", f"{pkg.vol_weight:.2f} kg"),
            ],
        })
        body.append({"type": "separator", "margin": "md"})

    # Postal codes
    if len(parsed.postal_codes) >= 2:
        pc_text = f"{_fmt_postal(parsed.postal_codes[0])} → {_fmt_postal(parsed.postal_codes[1])}"
    elif parsed.postal_codes:
        pc_text = _fmt_postal(parsed.postal_codes[0])
    else:
        pc_text = "未偵測到"

    body.extend([
        {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm",
         "contents": [
             {"type": "text", "text": "📮 郵遞區號",
              "weight": "bold", "size": "md", "color": "#333333"},
             {"type": "text", "text": pc_text, "size": "sm", "weight": "bold"},
         ]},
        {"type": "separator", "margin": "md"},
        {"type": "text", "text": "請選擇您的操作：",
         "size": "sm", "color": "#888888", "margin": "lg"},
    ])

    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "contents": body},
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": [
                {
                    "type": "box", "layout": "horizontal", "spacing": "sm",
                    "contents": [
                        {"type": "button", "height": "sm", "style": "primary",
                         "color": "#28a745",
                         "action": {"type": "message",
                                    "label": "正確",
                                    "text": "報價確認正確"}},
                        {"type": "button", "height": "sm", "style": "primary",
                         "color": "#dc3545",
                         "action": {"type": "message",
                                    "label": "錯誤",
                                    "text": "報價確認錯誤"}},
                    ],
                },
                {"type": "button", "height": "sm", "style": "secondary",
                 "action": {"type": "message",
                            "label": "重新輸入",
                            "text": "報價重新輸入"}},
            ],
        },
    }


def _build_mode_select_flex() -> dict:
    """Bubble asking user to pick ✈️ 空運 or 🚢 海運."""
    return {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📦 請選擇運送方式",
                 "weight": "bold", "size": "xl"},
                {"type": "text", "wrap": True,
                 "text": "偵測到一組郵遞區號，請選擇從加拿大寄往台灣的運送方式：",
                 "size": "sm", "color": "#888888", "margin": "md"},
            ],
        },
        "footer": {
            "type": "box", "layout": "horizontal", "spacing": "sm",
            "contents": [
                {"type": "button", "height": "sm", "style": "primary",
                 "color": "#007bff",
                 "action": {"type": "message",
                            "label": "✈️ 空運",
                            "text": "報價選擇空運"}},
                {"type": "button", "height": "sm", "style": "primary",
                 "color": "#17a2b8",
                 "action": {"type": "message",
                            "label": "🚢 海運",
                            "text": "報價選擇海運"}},
            ],
        },
    }


def _build_result_flex(services: List[ServiceQuote], mode: str) -> dict:
    """Results-comparison bubble listing up to 8 services."""
    body = [
        {"type": "text", "text": f"📊 {mode}運費比較",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
        {"type": "separator", "margin": "md"},
    ]

    for idx, svc in enumerate(services[:8]):
        is_best = (idx == 0)
        rows: list = []

        # Badge for cheapest
        if is_best:
            rows.append({
                "type": "box", "layout": "vertical",
                "backgroundColor": "#28a745", "cornerRadius": "sm",
                "paddingAll": "xs",
                "contents": [
                    {"type": "text", "text": "⭐ 最低價", "size": "xxs",
                     "color": "#ffffff", "weight": "bold", "align": "center"},
                ],
            })

        # Service name + total
        rows.append({
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{svc.carrier} - {svc.name}",
                 "size": "sm", "weight": "bold", "flex": 5, "wrap": True},
                {"type": "text", "text": f"${svc.total:.2f}",
                 "size": "sm", "weight": "bold", "flex": 2, "align": "end",
                 "color": "#28a745" if is_best else "#333333"},
            ],
        })

        # Breakdown rows
        rows.append(_detail_row("運費基價", f"${svc.freight:.2f}"))
        if svc.surcharges > 0:
            rows.append(_detail_row("附加費", f"${svc.surcharges:.2f}"))
        if svc.tax > 0:
            rows.append(_detail_row("稅金", f"${svc.tax:.2f}"))
        rows.append(_detail_row("ETA", str(svc.eta)))

        svc_box = {
            "type": "box", "layout": "vertical",
            "margin": "lg", "spacing": "xs",
            "contents": rows,
        }
        if is_best:
            svc_box["backgroundColor"] = "#f0fff0"
            svc_box["cornerRadius"] = "md"
            svc_box["paddingAll"] = "sm"

        body.append(svc_box)

        if idx < min(len(services), 8) - 1:
            body.append({"type": "separator", "margin": "sm"})

    return {
        "type": "bubble", "size": "mega",
        "body": {"type": "box", "layout": "vertical", "contents": body},
    }


# ─── Tiny Flex Helpers ────────────────────────────────────────────────────────

def _kv_row(label: str, value: str) -> dict:
    return {
        "type": "box", "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label,
             "size": "sm", "color": "#888888", "flex": 2},
            {"type": "text", "text": value,
             "size": "sm", "flex": 5, "align": "end", "weight": "bold"},
        ],
    }


def _detail_row(label: str, value: str) -> dict:
    return {
        "type": "box", "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label,
             "size": "xs", "color": "#888888", "flex": 3},
            {"type": "text", "text": value,
             "size": "xs", "flex": 2, "align": "end"},
        ],
    }
