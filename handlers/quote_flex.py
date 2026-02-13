"""
報價流程 - Flex Message 建構模組 (Single Responsibility)
──────────────────────────────────────────────────────────
All LINE Flex Message JSON builders live here, separated from
business logic in quote_handler.py.
"""

from typing import List, Optional

from services.quote_service import (
    ParsedInput, ServiceQuote, _fmt_postal,
)
from handlers.quote_config import (
    QuoteProfile, is_warn_service, WARN_DISCLAIMER,
)


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
             "size": "xs", "flex": 2, "align": "end", "wrap": True},
        ],
    }


def _short_eta(eta: str) -> str:
    """Shorten ETA for compact display in service table."""
    if not eta or eta == "N/A":
        return "N/A"
    if len(eta) == 10 and eta[4] == "-":
        return eta[5:]
    # Allow slightly longer text like "Not Guaranteed" (14 chars)
    return eta[:16] if len(eta) > 16 else eta


# ─── Confirm Flex ─────────────────────────────────────────────────────────────

def build_confirm_flex(parsed: ParsedInput) -> dict:
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
                                    "text": "報價錯誤"}},
                    ],
                },
                {"type": "button", "height": "sm", "style": "secondary",
                 "action": {"type": "message",
                            "label": "重新輸入",
                            "text": "報價重新輸入"}},
            ],
        },
    }


# ─── Service Selection Flex ───────────────────────────────────────────────────

def build_service_select_flex(all_services: List[ServiceQuote],
                              profile: QuoteProfile) -> dict:
    """Bubble listing UPS/FedEx services with Service | Cost | ETA | button."""
    show_cost = profile.show_cost_in_group

    body: list = [
        {"type": "text", "text": "🚚 境內段運送服務",
         "weight": "bold", "size": "lg", "color": "#1a1a1a"},
        {"type": "text", "text": "以下為 UPS / FedEx 境內運送報價，請選擇一項",
         "size": "xs", "color": "#888888", "margin": "sm", "wrap": True},
        {"type": "separator", "margin": "md"},
    ]

    # Header row
    header_contents = [
        {"type": "text", "text": "Service", "size": "xxs",
         "color": "#888888", "flex": 3, "weight": "bold"},
    ]
    if show_cost:
        header_contents.append(
            {"type": "text", "text": "支出", "size": "xxs",
             "color": "#888888", "flex": 3, "align": "end", "weight": "bold"})
    header_contents.extend([
        {"type": "text", "text": "ETA", "size": "xxs",
         "color": "#888888", "flex": 2, "align": "end", "weight": "bold"},
        {"type": "filler", "flex": 4},
    ])
    body.append({
        "type": "box", "layout": "horizontal", "margin": "md",
        "paddingStart": "sm", "paddingEnd": "sm",
        "contents": header_contents,
    })
    body.append({"type": "separator", "margin": "xs"})

    count = 0
    for idx, svc in enumerate(all_services):
        if svc.source != "TE":
            continue
        count += 1
        is_cheapest = (count == 1)
        has_warning = is_warn_service(svc.name)

        # Service name column
        svc_name_contents = [
            {"type": "text", "text": f"{svc.carrier} - {svc.name}",
             "size": "xxs", "weight": "bold", "wrap": True},
        ]
        if has_warning:
            svc_name_contents.append({
                "type": "text", "text": "⚠️ 報價僅供參考",
                "size": "xxs", "color": "#ffc107", "weight": "bold",
                "wrap": True, "margin": "xs",
            })

        row_contents: list = [
            {"type": "box", "layout": "vertical", "flex": 3,
             "contents": svc_name_contents},
        ]

        if show_cost:
            row_contents.append(
                {"type": "text", "text": f"${svc.total:.2f}", "size": "xxs",
                 "flex": 3, "align": "end", "gravity": "center",
                 "wrap": False,
                 "color": "#28a745" if is_cheapest else "#333333",
                 "weight": "bold" if is_cheapest else "regular"})

        row_contents.extend([
            {"type": "text", "text": _short_eta(svc.eta), "size": "xxs",
             "flex": 2, "align": "end", "gravity": "center",
             "wrap": True,
             "color": "#888888"},
            {"type": "button", "style": "primary", "height": "sm", "flex": 4,
             "color": "#28a745" if is_cheapest else "#007bff",
             "action": {"type": "message",
                        "label": "繼續",
                        "text": f"報價選擇服務_{idx}"}},
        ])

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

    # Warning disclaimer
    body.append({"type": "separator", "margin": "md"})
    body.append({
        "type": "text", "text": WARN_DISCLAIMER,
        "size": "xxs", "color": "#ff9800", "wrap": True, "margin": "md",
    })

    return {
        "type": "bubble", "size": "mega",
        "body": {"type": "box", "layout": "vertical", "contents": body},
    }


# ─── Mode Selection Flex ─────────────────────────────────────────────────────

def build_mode_select_flex() -> dict:
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


# ─── Result Comparison Flex ───────────────────────────────────────────────────

def build_result_flex(services: List[ServiceQuote], mode: str,
                      selected_svc: Optional[ServiceQuote] = None) -> dict:
    """Results-comparison bubble listing up to 8 services,
    highlighting best, selected, and warning services."""
    body = [
        {"type": "text", "text": f"📊 {mode}運費比較",
         "weight": "bold", "size": "xl", "color": "#1a1a1a"},
        {"type": "separator", "margin": "md"},
    ]

    for idx, svc in enumerate(services[:8]):
        is_best = (idx == 0)
        is_selected = False
        if selected_svc:
            if svc.carrier == selected_svc.carrier and svc.name == selected_svc.name:
                is_selected = True

        has_warning = is_warn_service(svc.name)
        rows: list = []

        # Badges row
        badges = []
        if is_best:
            badges.append({
                "type": "box", "layout": "vertical",
                "backgroundColor": "#28a745", "cornerRadius": "sm",
                "paddingAll": "xs", "margin": "sm", "width": "60px",
                "contents": [
                    {"type": "text", "text": "⭐ 最低價", "size": "xxs",
                     "color": "#ffffff", "weight": "bold", "align": "center"},
                ],
            })
        if is_selected:
            badges.append({
                "type": "box", "layout": "vertical",
                "backgroundColor": "#dc3545", "cornerRadius": "sm",
                "paddingAll": "xs", "margin": "sm", "width": "60px",
                "contents": [
                    {"type": "text", "text": "✅ 已選擇", "size": "xxs",
                     "color": "#ffffff", "weight": "bold", "align": "center"},
                ],
            })
        if badges:
            rows.append({"type": "box", "layout": "horizontal", "contents": badges})

        # Colors
        text_color = "#333333"
        bg_color = None
        if is_selected:
            text_color = "#dc3545"
            bg_color = "#fff5f5"
        elif is_best:
            text_color = "#28a745"
            bg_color = "#f0fff0"

        # Service heading (add ⚠️ for warning services)
        svc_heading = f"{svc.carrier} - {svc.name}"
        if has_warning:
            svc_heading = f"⚠️ {svc_heading}"

        rows.append({
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": svc_heading,
                 "size": "sm", "weight": "bold", "flex": 5, "wrap": True,
                 "color": text_color},
                {"type": "text", "text": f"${svc.total:.2f}",
                 "size": "sm", "weight": "bold", "flex": 2, "align": "end",
                 "color": text_color},
            ],
        })

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


# ─── Post-Quote Action Flex ──────────────────────────────────────────────────

def build_post_quote_flex(current_mode: str, profile: QuoteProfile) -> dict:
    """Post-quote action buttons, filtered by profile.post_quote_actions."""
    allowed = profile.post_quote_actions
    buttons: list = []

    if "switch_mode" in allowed:
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

    if "reselect_service" in allowed:
        buttons.append({
            "type": "button", "height": "sm", "style": "secondary",
            "action": {"type": "message",
                       "label": "🔄 選擇其他境內服務",
                       "text": "報價選擇其他服務"},
        })

    if "new_quote" in allowed:
        buttons.append({
            "type": "button", "height": "sm", "style": "secondary",
            "action": {"type": "message",
                       "label": "📝 處理新報價",
                       "text": "報價處理新報價"},
        })

    if "done" in allowed:
        buttons.append({
            "type": "button", "height": "sm", "style": "primary",
            "color": "#6c757d",
            "action": {"type": "message",
                       "label": "✅ 報價完成",
                       "text": "報價完成"},
        })

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
