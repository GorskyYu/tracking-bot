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
    WAREHOUSE_POSTAL, _fmt_postal, is_greater_vancouver,
)
from services.line_service import (
    line_push, line_reply, line_push_flex,
    line_reply_flex, line_push_messages,
)
from handlers.quote_config import (
    QuoteProfile, DEFAULT_PROFILE, IRIS_PROFILE,
    get_profile, is_warn_service, WARN_DISCLAIMER,
)
from handlers.quote_flex import (
    build_confirm_flex, build_service_select_flex,
    build_mode_select_flex, build_result_flex,
    build_post_quote_flex,
)

log = logging.getLogger(__name__)

# Profile lookup by name (for Redis serialization)
_PROFILE_BY_NAME = {
    "default": DEFAULT_PROFILE,
    "iris": IRIS_PROFILE,
}

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
                    "services", "selected_svc", "selected_mode", "profile",
                    "gv_delivery", "pickup_fee"):
        r.delete(_key(uid, suffix))


def _get_target(r, uid):
    return r.get(_key(uid, "target")) or uid


def _set_target(r, uid, target_id):
    r.set(_key(uid, "target"), target_id, ex=QUOTE_TTL)


# ─── Profile Serialization ───────────────────────────────────────────────────

def _set_profile_name(r, uid, name: str):
    r.set(_key(uid, "profile"), name, ex=QUOTE_TTL)


def _resolve_profile(r, uid) -> QuoteProfile:
    name = r.get(_key(uid, "profile")) or "default"
    return _PROFILE_BY_NAME.get(name, DEFAULT_PROFILE)


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


def _set_gv_delivery(r, uid, delivery_type: str):
    """Store GV local delivery type: 'pickup' or 'dropoff'."""
    r.set(_key(uid, "gv_delivery"), delivery_type, ex=QUOTE_TTL)


def _get_gv_delivery(r, uid) -> Optional[str]:
    return r.get(_key(uid, "gv_delivery"))


def _set_pickup_fee(r, uid, fee: float):
    r.set(_key(uid, "pickup_fee"), str(fee), ex=QUOTE_TTL)


def _get_pickup_fee(r, uid) -> Optional[float]:
    raw = r.get(_key(uid, "pickup_fee"))
    return float(raw) if raw is not None else None


# ─── Public API ───────────────────────────────────────────────────────────────

def is_in_quote_session(r, user_id: str) -> bool:
    """Check whether a user currently has an active quote session."""
    return _get_state(r, user_id) is not None


def handle_quote_trigger(event: dict, user_id: str,
                         group_id: Optional[str], r,
                         profile: QuoteProfile = DEFAULT_PROFILE) -> bool:
    """Handle '開始報價' trigger.  Returns True if consumed."""
    reply_token = event.get("replyToken")
    target_id = group_id or user_id

    _clear_session(r, user_id)
    _set_state(r, user_id, "collecting")
    _set_target(r, user_id, target_id)
    _set_profile_name(r, user_id, profile.name)

    line_reply(
        reply_token,
        "📝 報價模式已啟動！\n\n"
        "請貼上客人的訊息（包含包裹尺寸、重量、郵遞區號）。\n"
        "可以一次貼上或分多次貼上，我會自動讀取資料。\n"
        "💡 若有錯誤，可隨時輸入「更正重量 5kg」或「修改郵遞區號」來更新資料。\n\n"
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
    profile = _resolve_profile(r, user_id)

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
            return _on_confirmed(r, user_id, target_id, profile)
        if text == "報價錯誤":
            return _on_rejected(r, user_id, target_id)
        if text == "報價重新輸入":
            _clear_session(r, user_id)
            _set_state(r, user_id, "collecting")
            _set_target(r, user_id, target_id)
            _set_profile_name(r, user_id, profile.name)
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
            return _on_service_selected(r, user_id, target_id, idx, profile)
        if text == "報價選擇GV取件":
            return _on_gv_pickup(r, user_id, target_id, profile)
        if text == "報價選擇GV_DROPOFF":
            return _on_gv_dropoff(r, user_id, target_id, profile)
        line_push(target_id, "請從上方列表點選一個境內運送服務。")
        return True

    if state == "choosing_mode":
        if text == "報價選擇空運":
            return _on_mode_selected(r, user_id, target_id, "加台空運", profile)
        if text == "報價選擇海運":
            return _on_mode_selected(r, user_id, target_id, "加台海運", profile)
        line_push(target_id, "請點選「✈️ 空運」或「🚢 海運」按鈕選擇運送方式。")
        return True

    if state == "entering_pickup_fee":
        return _on_pickup_fee_entered(r, user_id, target_id, text, profile)

    if state == "post_quote":
        if text == "報價切換空運":
            return _on_mode_selected(r, user_id, target_id, "加台空運", profile)
        if text == "報價切換海運":
            return _on_mode_selected(r, user_id, target_id, "加台海運", profile)
        if text == "報價選擇其他服務":
            return _on_reselect_service(r, user_id, target_id, profile)
        if text in ("處理新報價", "報價處理新報價"):
            return _on_new_quote(r, user_id, target_id, profile)
        if text == "報價完成":
            _clear_session(r, user_id)
            line_push(target_id, "✅ 報價完成，感謝使用！")
            return True
        line_push(target_id, "請點選下方按鈕選擇操作。")
        return True

    return False


# ─── Private State Handlers ──────────────────────────────────────────────────

def _on_collecting(r, uid, target, text):
    """Parse message text and show confirm flex or partial status."""
    # First: try parsing ONLY the new text to detect complete re-entry.
    # This prevents duplicate boxes when user re-enters everything.
    new_only_parsed = parse_package_input(text)
    if (new_only_parsed
        and new_only_parsed.packages
        and new_only_parsed.postal_codes
        and all(p.length > 0 and p.width > 0 and p.height > 0 and p.weight > 0
                for p in new_only_parsed.packages)):
        # New text alone is a complete valid set → replace buffer entirely
        r.set(_key(uid, "buffer"), text, ex=QUOTE_TTL)
        parsed = new_only_parsed
    else:
        _append_buffer(r, uid, text)
        full_text = _get_buffer(r, uid)
        parsed = parse_package_input(full_text)

    # 1. Nothing found at all (or parse error)
    if not parsed or (not parsed.packages and not parsed.postal_codes):
        line_push(
            target,
            "🔍 尚未偵測到任何包裹資料。\n"
            "請確認訊息包含：\n"
            "• 包裹尺寸（長×寬×高，公分）\n"
            "• 重量（公斤）\n"
            "• 加拿大郵遞區號（如 V6X 1Z7）\n\n"
            "可繼續貼上更多訊息，或輸入「取消報價」退出。"
        )
        return True

    # 2. Check for completeness
    pkgs = parsed.packages
    postal_codes = parsed.postal_codes
    
    # Check if ALL packages are valid (L>0, W>0, H>0, Wt>0)
    all_pkgs_valid = True
    for p in pkgs:
        if not (p.length > 0 and p.width > 0 and p.height > 0 and p.weight > 0):
            all_pkgs_valid = False
            break
            
    has_pkgs = len(pkgs) > 0
    has_postal = len(postal_codes) > 0

    # 3. If everything is complete -> Proceed to Confirmation
    if has_pkgs and all_pkgs_valid and has_postal:
        _save_parsed(r, uid, parsed)
        _set_state(r, uid, "parsed")

        flex = build_confirm_flex(parsed)
        line_push_flex(target, "📦 包裹資料確認", flex)
        return True

    # 4. Partial data detected -> Show status update
    # Construct a helpful message listing what we have and what's missing
    lines = ["🔍 已讀取部分資料：", ""]
    
    if has_pkgs:
        lines.append(f"📦 包裹：{len(pkgs)} 件")
        for i, p in enumerate(pkgs):
            dims = f"{p.length:.0f}x{p.width:.0f}x{p.height:.0f}"
            wt = f"{p.weight:.1f}kg"
            
            # Check what's missing for this package
            missing = []
            if not (p.length > 0 and p.width > 0 and p.height > 0):
                missing.append("尺寸")
            if not (p.weight > 0):
                missing.append("重量")
            
            if missing:
                status = f"❌ 缺{'、'.join(missing)}"
            else:
                status = "✅ 完整"
                
            lines.append(f"  • Box {i+1}: {dims}, {wt} ({status})")
    else:
        lines.append("❌ 尚未偵測到包裹尺寸/重量")

    lines.append("")
    
    if has_postal:
        pc_str = ", ".join([_fmt_postal(pc) for pc in postal_codes])
        lines.append(f"📮 郵遞區號：{pc_str} (✅)")
    else:
        lines.append("❌ 尚未偵測到加拿大郵遞區號")

    lines.append("")
    lines.append("請繼續輸入缺少的資訊，或輸入「更正」來修改。")
    
    line_push(target, "\n".join(lines))
    return True


def _on_confirmed(r, uid, target, profile):
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
        args=(r, uid, target, from_postal, to_postal, packages, postal_codes,
              profile),
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

    flex = build_confirm_flex(parsed)
    line_push_flex(target, "📦 包裹資料確認", flex)
    return True


def _on_service_selected(r, uid, target, idx, profile):
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
        return _on_mode_selected(r, uid, target, "加境內", profile)

    # Profile forces mode? (e.g. Iris → always 加台空運)
    if not profile.allow_mode_select and profile.forced_mode:
        return _on_mode_selected(r, uid, target, profile.forced_mode, profile)

    # 1 postal code → ask air/sea
    _set_state(r, uid, "choosing_mode")
    flex = build_mode_select_flex()
    line_push_flex(target, "請選擇運送方式", flex)
    return True


def _on_mode_selected(r, uid, target, mode, profile):
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

    gv_delivery = _get_gv_delivery(r, uid)
    pickup_fee = _get_pickup_fee(r, uid) or 0

    line_push(target, f"📡 正在計算{mode}報價…")

    # Run in background to avoid blocking webhook
    threading.Thread(
        target=_calculate_and_send_quote,
        args=(r, uid, target, mode, from_postal, to_postal,
              packages, selected_svc, services, profile,
              gv_delivery, pickup_fee),
        daemon=True,
    ).start()

    return True


def _auto_select_cheapest_te(r, uid):
    """Auto-select cheapest TE service for GV local delivery."""
    services = _get_services(r, uid)
    if not services:
        return
    for idx, svc in enumerate(services):
        if svc.source == "TE":
            _set_selected_svc(r, uid, idx)
            return
    # Fallback to cheapest overall
    _set_selected_svc(r, uid, 0)


def _on_gv_pickup(r, uid, target, profile):
    """User chose 大溫地區上門取件 → ask for pickup fee."""
    _set_gv_delivery(r, uid, "pickup")
    _auto_select_cheapest_te(r, uid)
    _set_state(r, uid, "entering_pickup_fee")
    line_push(
        target,
        "🚗 您選擇了「大溫地區上門取件」\n\n"
        "請輸入取件費用（CAD），例如：15（也可直接輸入「另計」）\n\n"
        "💡 輸入「取消報價」可隨時退出。"
    )
    return True


def _on_gv_dropoff(r, uid, target, profile):
    """User chose 大溫地區 drop off → go straight to mode selection."""
    _set_gv_delivery(r, uid, "dropoff")
    _set_pickup_fee(r, uid, 0)
    _auto_select_cheapest_te(r, uid)

    # Skip mode select for GV local → always 加台空運
    if not profile.allow_mode_select and profile.forced_mode:
        return _on_mode_selected(r, uid, target, profile.forced_mode, profile)

    _set_state(r, uid, "choosing_mode")
    flex = build_mode_select_flex()
    line_push_flex(target, "請選擇運送方式", flex)
    return True


def _on_pickup_fee_entered(r, uid, target, text, profile):
    """User entered pickup fee amount → validate and proceed."""
    import re as _re
    val = text.strip()
    
    if val == "另計":
        _set_pickup_fee(r, uid, -1.0)
    else:
        fee_match = _re.match(r'^[\d.]+$', val)
        if not fee_match:
            line_push(
                target,
                "❌ 請輸入正確的數字金額（例如：15），或輸入「另計」，或輸入「取消報價」退出。"
            )
            return True
    
        fee = float(val)
        _set_pickup_fee(r, uid, fee)

    # Skip mode select for GV local → default behaviour
    if not profile.allow_mode_select and profile.forced_mode:
        return _on_mode_selected(r, uid, target, profile.forced_mode, profile)

    _set_state(r, uid, "choosing_mode")
    flex = build_mode_select_flex()
    line_push_flex(target, "請選擇運送方式", flex)
    return True


def _on_reselect_service(r, uid, target, profile):
    """Post-quote: go back to service selection."""
    services = _get_services(r, uid)
    if not services:
        line_push(target, "❌ 運送服務資料遺失，請重新輸入「開始報價」。")
        _clear_session(r, uid)
        return True

    data = _get_data(r, uid)
    postal_codes = data.get("postal_codes", []) if data else []
    from_postal = postal_codes[0] if postal_codes else ""
    if len(postal_codes) >= 2:
        to_postal = postal_codes[1]
    else:
        to_postal = WAREHOUSE_POSTAL

    _set_state(r, uid, "choosing_service")
    flex = build_service_select_flex(services, profile,
                                     from_postal=from_postal,
                                     to_postal=to_postal)
    line_push_flex(target, "🚚 請選擇境內運送服務", flex)
    return True


def _on_new_quote(r, uid, target, profile):
    """Post-quote: start fresh quote (keep session alive)."""
    target_id = _get_target(r, uid)
    _clear_session(r, uid)
    _set_state(r, uid, "collecting")
    _set_target(r, uid, target_id)
    _set_profile_name(r, uid, profile.name)
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
                             packages, postal_codes, profile):
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

        # ── Profile: auto-select forced service (e.g. Iris → FEDEX_GROUND) ──
        if not profile.allow_service_select and profile.forced_service:
            forced_idx = None
            target_svc = profile.forced_service.upper().replace("_", " ").replace("-", " ")
            
            for idx, svc in enumerate(all_quotes):
                # Try to fuzzy match (check name OR carrier+name)
                # e.g. "FedEx Ground" should match "FEDEX_GROUND"
                c1 = svc.name.upper()
                c2 = f"{svc.carrier} {svc.name}".upper()
                if target_svc in c1 or target_svc in c2:
                    forced_idx = idx
                    break

            if forced_idx is None:
                # Forced service not found ─ fall back to cheapest TE
                for idx, svc in enumerate(all_quotes):
                    if svc.source == "TE":
                        forced_idx = idx
                        break
                if forced_idx is None:
                    forced_idx = 0

            _set_selected_svc(r, uid, forced_idx)

            # Determine mode
            if len(postal_codes) >= 2:
                mode = "加境內"
            elif not profile.allow_mode_select and profile.forced_mode:
                mode = profile.forced_mode
            else:
                # Shouldn't happen for Iris, but fallback
                _set_state(r, uid, "choosing_mode")
                flex = build_mode_select_flex()
                line_push_flex(target, "請選擇運送方式", flex)
                return

            selected_svc = all_quotes[forced_idx]
            data = _get_data(r, uid)
            pkgs = [Package(p["length"], p["width"], p["height"], p["weight"])
                    for p in data["packages"]]

            if mode == "加境內":
                fp = postal_codes[0] if len(postal_codes) >= 1 else ""
                tp = postal_codes[1] if len(postal_codes) >= 2 else ""
            else:
                fp = postal_codes[0] if postal_codes else ""
                tp = WAREHOUSE_POSTAL

            _set_selected_mode(r, uid, mode)
            _set_state(r, uid, "post_quote")

            gv_delivery = _get_gv_delivery(r, uid)
            pickup_fee = _get_pickup_fee(r, uid) or 0

            _calculate_and_send_quote(
                r, uid, target, mode, fp, tp,
                pkgs, selected_svc, all_quotes, profile,
                gv_delivery, pickup_fee,
            )
            return

        # ── Normal flow: show service selection ──
        _set_state(r, uid, "choosing_service")
        flex = build_service_select_flex(all_quotes, profile,
                                         from_postal=from_postal,
                                         to_postal=to_postal)
        line_push_flex(target, "🚚 請選擇境內運送服務", flex)

    except Exception as e:
        log.error(f"[QuoteHandler] Service fetch error: {e}", exc_info=True)
        line_push(target, f"❌ 查詢運費過程發生錯誤: {e}")
        _clear_session(r, uid)


def _calculate_and_send_quote(r, uid, target, mode, from_postal, to_postal,
                              packages, selected_svc, all_services, profile,
                              gv_delivery=None, pickup_fee=0):
    """Background: calculate full quote with selected service, push results."""
    try:
        box_weights = calculate_box_weights(packages, mode)

        # ── Handle GV Local Delivery Injection ──────────────────────────────
        gv_to_warehouse = (
            from_postal and to_postal
            and is_greater_vancouver(from_postal)
            and to_postal.upper().replace(" ", "") == WAREHOUSE_POSTAL.upper().replace(" ", "")
        )

        if gv_to_warehouse:
            # Create GV Dropoff service (Free)
            gv_dropoff = ServiceQuote(
                carrier="大溫地區",
                name="Drop Off",
                freight=0, surcharges=0, tax=0, total=0,
                eta="自行送至指定地點",
                source="GV"
            )
            all_services.append(gv_dropoff)

            # Create GV Pickup service (only if fee > 0 or user chose it)
            if pickup_fee != 0 or gv_delivery == "pickup":
                display_fee = pickup_fee if pickup_fee > 0 else 0
                gv_pickup = ServiceQuote(
                    carrier="大溫地區",
                    name="上門取件",
                    freight=display_fee, surcharges=0, tax=0, total=display_fee,
                    eta="預約取件",
                    source="GV"
                )
                all_services.append(gv_pickup)

            # Re-sort services by price
            all_services.sort(key=lambda s: s.total)

            # Update selected_svc to point to our new object if GV mode was chosen
            if gv_delivery == "pickup":
                for s in all_services:
                    if s.source == "GV" and s.name == "上門取件":
                        selected_svc = s
                        break
            elif gv_delivery == "dropoff":
                for s in all_services:
                    if s.source == "GV" and s.name == "Drop Off":
                        selected_svc = s
                        break

        # Build canned text using the selected service
        quote_text = build_quote_text(
            mode, from_postal, to_postal,
            packages, box_weights, selected_svc, all_services,
            gv_delivery=gv_delivery, pickup_fee=pickup_fee,
        )

        # Build comparison flex (titled "境內段運費比較")
        result_flex = build_result_flex(all_services, "境內段", selected_svc)

        # Build post-quote action flex (filtered by profile)
        action_flex = build_post_quote_flex(mode, profile)

        # ── Route messages based on profile visibility ──────────────────
        group_msgs: list = []   # messages for the main chat
        private_msgs: dict = {} # target_id -> list[msg]

        def add_private(uid, msg):
            if uid:
                if uid not in private_msgs:
                    private_msgs[uid] = []
                private_msgs[uid].append(msg)

        # 1. Canned text
        if profile.show_cost_in_group:
            group_msgs.append({"type": "text", "text": quote_text})
        else:
            # Cost goes to profile.cost_push_target privately
            if profile.cost_push_target:
                add_private(profile.cost_push_target, {"type": "text", "text": quote_text})
            # Group gets a simplified acknowledgement
            group_msgs.append({
                "type": "text",
                "text": f"✅ {mode}報價已完成，報價資料已傳送給管理員。",
            })

        # 2. Warning for discrepant service
        if is_warn_service(selected_svc.name):
            warn_text = (
                f"⚠️ 注意：您選擇的服務 {selected_svc.carrier} - {selected_svc.name} "
                f"的系統報價可能與 TE 網站顯示不同，請務必進入 TE 網站確認金額。"
            )
            if profile.show_cost_in_group:
                group_msgs.append({"type": "text", "text": warn_text})
            else:
                if profile.cost_push_target:
                    add_private(profile.cost_push_target, {"type": "text", "text": warn_text})

        # 3. Result comparison flex
        if profile.show_result_flex_in_group:
            group_msgs.append({
                "type": "flex", "altText": "📊 境內段運費比較表",
                "contents": result_flex,
            })
        else:
            if profile.result_flex_push_target:
                add_private(profile.result_flex_push_target, {
                    "type": "flex", "altText": "📊 境內段運費比較表",
                    "contents": result_flex,
                })

        # 4. Post-quote action flex (always in group)
        group_msgs.append({
            "type": "flex", "altText": "接下來要做什麼？",
            "contents": action_flex,
        })

        # Push to group
        if group_msgs:
            line_push_messages(target, group_msgs)

        # Push private messages
        for uid, msgs in private_msgs.items():
            line_push_messages(uid, msgs)

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

