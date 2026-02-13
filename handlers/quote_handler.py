"""
報價互動處理器 - Quote Flow Handler
────────────────────────────────────
Manages the multi-step quote conversation via LINE Flex Messages.

State Machine (persisted in Redis with 10-min TTL):
  collecting      → 等待使用者貼上客人訊息
  parsed          → 資料已解析，等待「正確/錯誤」確認
  correcting      → 使用者按了「錯誤」, 等待手動輸入
  choosing_service→ API 已查詢，等待選擇境內運送服務
  choosing_mode   → 服務已選，等待選擇「空運/海運」
  post_quote      → 報價已顯示，等待後續操作
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
    for suffix in ("state", "data", "buffer", "target",
                    "services", "selected_svc", "selected_mode"):
        r.delete(_key(uid, suffix))


def _get_target(r, uid):
    return r.get(_key(uid, "target")) or uid


def _set_target(r, uid, target_id):
    r.set(_key(uid, "target"), target_id, ex=QUOTE_TTL)


# ─── Services Serialization ──────────────────────────────────────────────────

def _set_services(r, uid, services: List[ServiceQuote]):
    data = [
        {"carrier": s.carrier, "name": s.name, "freight": s.freight,
         "surcharges": s.surcharges, "tax": s.tax, "total": s.total,
         "eta": s.eta, "surcharge_details": s.surcharge_details,
         "source": s.source}
        for s in services
    ]
    r.set(_key(uid, "services"), json.dumps(data, ensure_ascii=False), ex=QUOTE_TTL)


def _get_services(r, uid) -> Optional[List[ServiceQuote]]:
    raw = r.get(_key(uid, "services"))
    if not raw:
        return None
    data = json.loads(raw)
    return [
        ServiceQuote(
            carrier=d["carrier"], name=d["name"], freight=d["freight"],
            surcharges=d["surcharges"], tax=d["tax"], total=d["total"],
            eta=d["eta"], surcharge_details=d.get("surcharge_details", ""),
            source=d.get("source", "TE"),
        )
        for d in data
    ]


def _set_selected_svc(r, uid, idx: int):
    r.set(_key(uid, "selected_svc"), str(idx), ex=QUOTE_TTL)


def _get_selected_svc(r, uid) -> Optional[int]:
    raw = r.get(_key(uid, "selected_svc"))
    return int(raw) if raw is not None else None


def _set_selected_mode(r, uid, mode: str):
    r.set(_key(uid, "selected_mode"), mode, ex=QUOTE_TTL)


def _get_selected_mode(r, uid) -> Optional[str]:
    return r.get(_key(uid, "selected_mode"))


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

    if state == "choosing_service":
        if text.startswith("報價選擇服務_"):
            try:
                idx = int(text.split("_")[-1])
            except ValueError:
                line_push(target_id, "❌ 無效的選擇，請重新點選服務按鈕。")
                return True
            return _on_service_selected(r, user_id, target_id, idx)
        line_push(target_id, "請從上方列表點選一個境內運送服務。")
        return True

    if state == "choosing_mode":
        if text == "報價選擇空運":
            return _on_mode_selected(r, user_id, target_id, "加台空運")
        if text == "報價選擇海運":
            return _on_mode_selected(r, user_id, target_id, "加台海運")
        line_push(target_id, "請點選「✈️ 空運」或「🚢 海運」按鈕選擇運送方式。")
        return True

    if state == "post_quote":
        if text == "報價切換空運":
            return _on_mode_selected(r, user_id, target_id, "加台空運")
        if text == "報價切換海運":
            return _on_mode_selected(r, user_id, target_id, "加台海運")
        if text == "報價選擇其他服務":
            return _on_reselect_service(r, user_id, target_id)
        if text == "報價處理新報價":
            return _on_new_quote(r, user_id, target_id)
        if text == "報價完成":
            _clear_session(r, user_id)
            line_push(target_id, "✅ 報價完成，感謝使用！")
            return True
        line_push(target_id, "請點選下方按鈕選擇操作。")
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
    """User confirmed → call APIs for domestic quotes, show service selection."""
    data = _get_data(r, uid)
    if not data:
        line_push(target, "❌ 資料遺失，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    postal_codes = data.get("postal_codes", [])
    packages = [
        Package(p["length"], p["width"], p["height"], p["weight"])
        for p in data["packages"]
    ]

    if not postal_codes:
        line_push(
            target,
            "⚠️ 未偵測到郵遞區號。\n"
            "請補充加拿大郵遞區號（如 V6X 1Z7），或輸入「取消報價」退出。"
        )
        _set_state(r, uid, "collecting")
        return True

    from_postal = postal_codes[0]

    if len(postal_codes) >= 2:
        # 加境內: ship between two Canadian addresses
        to_postal = postal_codes[1]
    else:
        # 加台空運/海運: ship to warehouse
        to_postal = WAREHOUSE_POSTAL

    line_push(target, "📡 正在查詢境內段運費，請稍候…")

    # Call APIs in background to avoid webhook timeout
    threading.Thread(
        target=_fetch_services_and_show,
        args=(r, uid, target, from_postal, to_postal, packages, postal_codes),
        daemon=True,
    ).start()

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


def _on_service_selected(r, uid, target, idx):
    """User picked a domestic service → decide next step."""
    services = _get_services(r, uid)
    if not services or idx < 0 or idx >= len(services):
        line_push(target, "❌ 無效的服務選擇，請重新點選。")
        return True

    _set_selected_svc(r, uid, idx)

    data = _get_data(r, uid)
    postal_codes = data.get("postal_codes", []) if data else []

    if len(postal_codes) >= 2:
        # 加境內 → skip mode selection, go directly to results
        return _on_mode_selected(r, uid, target, "加境內")

    # 1 postal code → ask air/sea
    _set_state(r, uid, "choosing_mode")
    flex = _build_mode_select_flex()
    line_push_flex(target, "請選擇運送方式", flex)
    return True


def _on_mode_selected(r, uid, target, mode):
    """Mode determined → calculate and deliver results."""
    data = _get_data(r, uid)
    services = _get_services(r, uid)
    selected_idx = _get_selected_svc(r, uid)

    if not data or not services or selected_idx is None:
        line_push(target, "❌ 資料遺失，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    selected_svc = services[selected_idx] if selected_idx < len(services) else services[0]

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

    _set_selected_mode(r, uid, mode)
    _set_state(r, uid, "post_quote")

    line_push(target, f"📡 正在計算{mode}報價…")

    # Run in background to avoid blocking webhook
    threading.Thread(
        target=_calculate_and_send_quote,
        args=(r, uid, target, mode, from_postal, to_postal,
              packages, selected_svc, services),
        daemon=True,
    ).start()

    return True


def _on_reselect_service(r, uid, target):
    """Post-quote: go back to service selection."""
    services = _get_services(r, uid)
    if not services:
        line_push(target, "❌ 運送服務資料遺失，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    _set_state(r, uid, "choosing_service")
    flex = _build_service_select_flex(services)
    line_push_flex(target, "🚚 請選擇境內運送服務", flex)
    return True


def _on_new_quote(r, uid, target):
    """Post-quote: start fresh quote (keep session alive)."""
    target_id = _get_target(r, uid)
    _clear_session(r, uid)
    _set_state(r, uid, "collecting")
    _set_target(r, uid, target_id)
    line_push(
        target,
        "📝 新報價模式已啟動！\n\n"
        "請貼上客人的訊息（包含包裹尺寸、重量、郵遞區號）。\n"
        "可以一次貼上或分多次貼上，我會自動讀取資料。\n\n"
        "💡 輸入「取消報價」可隨時退出。"
    )
    return True


# ─── Background Workers ──────────────────────────────────────────────────────

def _fetch_services_and_show(r, uid, target, from_postal, to_postal,
                             packages, postal_codes):
    """Background: call TE + CP APIs, store results, show service selection."""
    try:
        te_quotes = get_te_quotes(from_postal, to_postal, packages)
        cp_quotes = get_cp_quotes(from_postal, to_postal, packages)

        all_quotes = sorted(te_quotes + cp_quotes, key=lambda q: q.total)

        if not all_quotes:
            line_push(target, "❌ 無法取得運費報價，請稍後再試或手動使用報價計算器。")
            _clear_session(r, uid)
            return

        # Store all quotes
        _set_services(r, uid, all_quotes)
        _set_state(r, uid, "choosing_service")

        # Build service selection flex (UPS/FedEx only from TE)
        flex = _build_service_select_flex(all_quotes)
        line_push_flex(target, "🚚 請選擇境內運送服務", flex)

    except Exception as e:
        log.error(f"[QuoteHandler] Service fetch error: {e}", exc_info=True)
        line_push(target, f"❌ 查詢運費過程發生錯誤: {e}")
        _clear_session(r, uid)


def _calculate_and_send_quote(r, uid, target, mode, from_postal, to_postal,
                              packages, selected_svc, all_services):
    """Background: calculate full quote with selected service, push results."""
    try:
        box_weights = calculate_box_weights(packages, mode)

        # Build canned text using the selected service
        quote_text = build_quote_text(
            mode, from_postal, to_postal,
            packages, box_weights, selected_svc, all_services,
        )

        # Build comparison flex (titled "境內段運費比較")
        result_flex = _build_result_flex(all_services, "境內段", selected_svc)

        # Build post-quote action flex
        action_flex = _build_post_quote_flex(mode)

        # Push all messages (text + 2 flex)
        line_push_messages(target, [
            {"type": "text", "text": quote_text},
            {"type": "flex", "altText": "📊 境內段運費比較表", "contents": result_flex},
            {"type": "flex", "altText": "接下來要做什麼？", "contents": action_flex},
        ])

    except Exception as e:
        log.error(f"[QuoteHandler] Quote calculation error: {e}", exc_info=True)
        line_push(target, f"❌ 報價計算過程發生錯誤: {e}")


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


def _build_service_select_flex(all_services: List[ServiceQuote]) -> dict:
    """Bubble listing UPS/FedEx services with Service | Cost | ETA | 選擇 button."""
    body: list = [
        {"type": "text", "text": "🚚 境內段運送服務",
         "weight": "bold", "size": "lg", "color": "#1a1a1a"},
        {"type": "text", "text": "以下為 UPS / FedEx 境內運送報價，請選擇一項",
         "size": "xs", "color": "#888888", "margin": "sm", "wrap": True},
        {"type": "separator", "margin": "md"},
        # Header row
        {
            "type": "box", "layout": "horizontal", "margin": "md",
            "paddingStart": "sm", "paddingEnd": "sm",
            "contents": [
                {"type": "text", "text": "Service", "size": "xxs",
                 "color": "#888888", "flex": 4, "weight": "bold"},
                {"type": "text", "text": "支出", "size": "xxs",
                 "color": "#888888", "flex": 3, "align": "end", "weight": "bold"},
                {"type": "text", "text": "ETA", "size": "xxs",
                 "color": "#888888", "flex": 2, "align": "end", "weight": "bold"},
                {"type": "filler", "flex": 3},
            ],
        },
        {"type": "separator", "margin": "xs"},
    ]

    count = 0
    for idx, svc in enumerate(all_services):
        # Only show UPS / FedEx (TE source)
        if svc.source != "TE":
            continue
        count += 1

        is_cheapest = (count == 1)  # first TE service (sorted by total)

        row_contents: list = [
            {
                "type": "box", "layout": "vertical", "flex": 4,
                "contents": [
                    {"type": "text",
                     "text": f"{svc.carrier} - {svc.name}",
                     "size": "xxs", "weight": "bold", "wrap": True},
                ],
            },
            {"type": "text", "text": f"${svc.total:.2f}", "size": "xxs",
             "flex": 3, "align": "end", "gravity": "center",
             "wrap": False,
             "color": "#28a745" if is_cheapest else "#333333",
             "weight": "bold" if is_cheapest else "regular"},
            {"type": "text", "text": _short_eta(svc.eta), "size": "xxs",
             "flex": 2, "align": "end", "gravity": "center",
             "color": "#888888"},
            {"type": "button", "style": "primary", "height": "sm", "flex": 3,
             "color": "#28a745" if is_cheapest else "#007bff",
             "action": {"type": "message",
                        "label": "繼續",
                        "text": f"報價選擇服務_{idx}"}},
        ]

        row = {
            "type": "box", "layout": "horizontal",
            "margin": "md", "spacing": "sm",
            "alignItems": "center",
            "contents": row_contents,
        }

        if is_cheapest:
            row["backgroundColor"] = "#f0fff0"
            row["cornerRadius"] = "md"
            row["paddingAll"] = "sm"

        body.append(row)

        if count < 8:
            body.append({"type": "separator", "margin": "xs"})

        if count >= 8:
            break

    # Remove trailing separator
    if body and body[-1].get("type") == "separator":
        body.pop()

    return {
        "type": "bubble", "size": "mega",
        "body": {"type": "box", "layout": "vertical", "contents": body},
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


def _build_result_flex(services: List[ServiceQuote], mode: str, selected_svc: Optional[ServiceQuote] = None) -> dict:
    """Results-comparison bubble listing up to 8 services, highlighting best & selected."""
    body = [
        {"type": "text", "text": f"📊 {mode}運費比較",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
        {"type": "separator", "margin": "md"},
    ]

    for idx, svc in enumerate(services[:8]):
        is_best = (idx == 0)
        is_selected = False
        if selected_svc:
            # Match by name and carrier (assuming distinct enough for short list)
            if svc.carrier == selected_svc.carrier and svc.name == selected_svc.name:
                is_selected = True

        rows: list = []
        
        # Badges row (if any)
        badges = []
        if is_best:
            badges.append({
                "type": "box", "layout": "vertical",
                "backgroundColor": "#28a745", "cornerRadius": "sm",
                "paddingAll": "xs", "margin": "sm",
                "width": "60px",
                "contents": [
                    {"type": "text", "text": "⭐ 最低價", "size": "xxs",
                     "color": "#ffffff", "weight": "bold", "align": "center"},
                ],
            })
        
        if is_selected:
            badges.append({
                "type": "box", "layout": "vertical",
                "backgroundColor": "#dc3545", "cornerRadius": "sm",
                "paddingAll": "xs", "margin": "sm",
                "width": "60px",
                "contents": [
                    {"type": "text", "text": "✅ 已選擇", "size": "xxs",
                     "color": "#ffffff", "weight": "bold", "align": "center"},
                ],
            })

        if badges:
            rows.append({
                "type": "box", "layout": "horizontal",
                "contents": badges
            })

        # Determine colors based on priority: Selected (Red) > Best (Green) > Normal
        text_color = "#333333"
        bg_color = None
        
        if is_selected:
            text_color = "#dc3545"
            bg_color = "#fff5f5"  # Light red background
        elif is_best:
            text_color = "#28a745"
            bg_color = "#f0fff0"  # Light green background

        # Service name + total
        rows.append({
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{svc.carrier} - {svc.name}",
                 "size": "sm", "weight": "bold", "flex": 5, "wrap": True,
                 "color": text_color},
                {"type": "text", "text": f"${svc.total:.2f}",
                 "size": "sm", "weight": "bold", "flex": 2, "align": "end",
                 "color": text_color},
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
        
        if bg_color:
            svc_box["backgroundColor"] = bg_color
            svc_box["cornerRadius"] = "md"
            svc_box["paddingAll"] = "sm"

        body.append(svc_box)

        if idx < min(len(services), 8) - 1:
            body.append({"type": "separator", "margin": "sm"})

    return {
        "type": "bubble", "size": "mega",
        "body": {"type": "box", "layout": "vertical", "contents": body},
    }


def _build_post_quote_flex(current_mode: str) -> dict:
    """Post-quote action buttons: switch mode / reselect service / new quote / done."""
    buttons: list = []

    # Switch mode button (only for non-加境內)
    if current_mode == "加台空運":
        buttons.append({
            "type": "button", "height": "sm", "style": "primary",
            "color": "#17a2b8",
            "action": {"type": "message",
                       "label": "🚢 海運報價",
                       "text": "報價切換海運"},
        })
    elif current_mode == "加台海運":
        buttons.append({
            "type": "button", "height": "sm", "style": "primary",
            "color": "#007bff",
            "action": {"type": "message",
                       "label": "✈️ 空運報價",
                       "text": "報價切換空運"},
        })

    buttons.extend([
        {
            "type": "button", "height": "sm", "style": "secondary",
            "action": {"type": "message",
                       "label": "🔄 選擇其他境內服務",
                       "text": "報價選擇其他服務"},
        },
        {
            "type": "button", "height": "sm", "style": "secondary",
            "action": {"type": "message",
                       "label": "📝 處理新報價",
                       "text": "報價處理新報價"},
        },
        {
            "type": "button", "height": "sm", "style": "primary",
            "color": "#6c757d",
            "action": {"type": "message",
                       "label": "✅ 報價完成",
                       "text": "報價完成"},
        },
    ])

    return {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📋 接下來要做什麼？",
                 "weight": "bold", "size": "lg"},
                {"type": "text", "text": "請選擇後續操作",
                 "size": "xs", "color": "#888888", "margin": "sm"},
            ],
        },
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": buttons,
        },
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


def _short_eta(eta: str) -> str:
    """Shorten ETA for compact display in service table."""
    if not eta or eta == "N/A":
        return "N/A"
    # If it's a date like "2026-02-24", show "02-24"
    if len(eta) == 10 and eta[4] == "-":
        return eta[5:]
    # Truncate long text
    return eta[:12] if len(eta) > 12 else eta
