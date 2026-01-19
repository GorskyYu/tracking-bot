import hmac
import hashlib
import base64
import requests
import logging
import pytz
from datetime import datetime
from urllib.parse import quote

# 從你已經重構好的 config.py 匯入設定項
from config import APP_ID, APP_SECRET, LINE_HEADERS, TIMEZONE

# 初始化日誌紀錄器
log = logging.getLogger(__name__)

# 包裹狀態翻譯字典 (建議也可以移到 config.py，如果還沒移請保留在這裡)
TRANSLATIONS = {
    "out for delivery today":         "今日派送中",
    "out for delivery":               "派送中",
    "processing at ups facility":     "UPS處理中",
    "arrived at facility":            "已到達派送中心",
    "departed from facility":         "已離開派送中心",
    "pickup scan":                    "取件掃描",
    "your package is currently at the ups access point™ and is scheduled to be tendered to ups.": 
                                      "貨件目前在 UPS 取貨點，稍後將交予 UPS",
    "drop-off":                       "已寄件",
    "order created at triple eagle":  "已在系統建立訂單",
    "shipper created a label, ups has not received the package yet.": 
                                      "已建立運單，UPS 尚未收件",
    "delivered":                      "已送達",
}

# ─── Signature Generator ──────────────────────────────────────────────────────
def generate_sign(params: dict, secret: str) -> str:
    # Build encodeURIComponent-style querystring
    parts = []
    for k in sorted(params.keys()):
        v = params[k]
        parts.append(f"{k}={quote(str(v), safe='~')}")
    qs = "&".join(parts)

    # HMAC-SHA256 and Base64-encode
    sig_bytes = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig_bytes).decode('utf-8')

# ─── TripleEagle API Caller ───────────────────────────────────────────────────
def call_api(action: str, payload: dict = None) -> dict:
    ts = str(int(datetime.now().timestamp()))
    params = {"id": APP_ID, "timestamp": ts, "format": "json", "action": action}
    params["sign"] = generate_sign(params, APP_SECRET)
    url = "https://eship.tripleeaglelogistics.com/api?" + "&".join(
        f"{k}={quote(str(params[k]), safe='~')}" for k in params
    )
    headers = {"Content-Type": "application/json"}
    if payload:
        r = requests.post(url, json=payload, headers=headers)
    else:
        r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# Helper for sending single final LINE message for uploading PDF
def _line_push(to: str, text: str):
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers=LINE_HEADERS,
            json={"to": to, "messages": [{"type": "text", "text": text}]},
            timeout=10,
        )
    except Exception as e:
        log.error(f"[LINE PUSH] failed: {e}")


# ─── Business Logic ───────────────────────────────────────────────────────────
def get_statuses_for(keywords: list[str]) -> list[str]:
    # 1) list all active orders
    resp = call_api("shipment/list")
    lst  = resp.get("response", {}).get("list") or resp.get("response") or []
    order_ids = [o["id"] for o in lst if "id" in o]
    # 2) filter by these keywords
    cust_ids = []
    for oid in order_ids:
        det = call_api("shipment/detail", {"id": oid}).get("response", {})
        if isinstance(det, list): det = det[0]
        init = det.get("initiation", {})
        loc  = next(iter(init), None)
        name = init.get(loc,{}).get("name","").lower() if loc else ""
        if any(kw in name for kw in keywords):
            cust_ids.append(oid)
    if not cust_ids:
        return ["📦 沒有此客戶的有效訂單"]
    # 3) fetch tracking updates
    td = call_api("shipment/tracking", {
        "keyword": ",".join(cust_ids),
        "rsync":   0,
        "timezone": TIMEZONE
    })
    # 4) format reply using each event’s own timestamp
    lines: list[str] = []
    for item in td.get("response", []):
        oid = item.get("id"); num = item.get("number","")
        events = item.get("list") or []
        if not events:
            lines.append(f"📦 {oid} ({num}) – 尚無追蹤紀錄")
            continue
        # pick the most recent event
        ev = max(events, key=lambda e: int(e["timestamp"]))
        loc_raw    = ev.get("location","")
        loc        = f"[{loc_raw.replace(',',', ')}] " if loc_raw else ""
        ctx_lc     = ev.get("context","").strip().lower()
        translated = TRANSLATIONS.get(ctx_lc, ev.get("context","").replace("Triple Eagle","system"))

        # derive the *real* event time from its epoch timestamp
        # 1) parse the numeric timestamp
        event_ts = int(ev["timestamp"])
        # 2) convert to a timezone‐aware datetime
        #    (make sure you have `import pytz` and `from datetime import datetime` at the top)
        tzobj = pytz.timezone(TIMEZONE)
        dt = datetime.fromtimestamp(event_ts, tz=tzobj)
        # 3) format it exactly like "Wed, 11 Jun 2025 15:05:46 -0700"
        tme = dt.strftime('%a, %d %b %Y %H:%M:%S %z')

        lines.append(f"📦 {oid} ({num}) → {loc}{translated}  @ {tme}")
    return lines
