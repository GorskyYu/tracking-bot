"""
報價計算服務 - Quick Quote Engine
──────────────────────────────────
Replicates the calculation logic from the 報價計算器 Google Sheet Apps Script,
porting TE API / CP API calls + weight rounding + cost summary generation.
"""

import re
import math
import json
import logging
import base64
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from dataclasses import dataclass, field

import openai

from config import OPENAI_API_KEY, OPENAI_MODEL
from services.te_api_service import call_api as te_call_api

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
WAREHOUSE_POSTAL = "V6X1Z7"

# International surcharge rates (I15 equivalent from setI15ByMode_)
INTL_RATE_AIR_LIGHT = 14    # total intl weight < 3 kg
INTL_RATE_AIR_HEAVY = 10    # total intl weight >= 3 kg
INTL_RATE_SEA = 5
INTL_RATE_DOMESTIC = 0.5

# Taiwan Domestic Fee (per-box, calculated from cuft weight)
EXCHANGE_RATE = 24.0

# Eastern Canada postal prefixes (for Q3 rate markup formula)
# NL(A), NS(B), PEI(C), NB(E), QC(G,H,J), ON(K,L,M,N,P)
EASTERN_POSTAL_PREFIXES = frozenset("ABCEGHIJKLMNP")

# Canada Post API (credentials from CP.js)
CP_ENDPOINT = "https://soa-gw.canadapost.ca"
CP_USERNAME = "e09db0d137a26ee9"
CP_PASSWORD = "4c4ef3a6933301766f7813"

# Canadian postal code regex
POSTAL_RE = re.compile(r'[A-Za-z]\d[A-Za-z]\d[A-Za-z]\d')


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class Package:
    length: float   # cm
    width: float    # cm
    height: float   # cm
    weight: float   # kg (actual)

    @property
    def vol_weight(self) -> float:
        return (self.length * self.width * self.height) / 5000

    @property
    def dim_text(self) -> str:
        # 去除不必要的小數 (.0)
        def _fmt(v):
            return f"{v:.0f}" if v == int(v) else f"{v:.1f}"
        return f"{_fmt(self.length)}*{_fmt(self.width)}*{_fmt(self.height)}"


@dataclass
class ParsedInput:
    packages: List[Package] = field(default_factory=list)
    postal_codes: List[str] = field(default_factory=list)
    raw_text: str = ""


@dataclass
class ServiceQuote:
    carrier: str
    name: str
    freight: float        # base cost
    surcharges: float
    tax: float
    total: float
    eta: str
    surcharge_details: str = ""
    source: str = "TE"    # "TE" or "CP"


@dataclass
class BoxWeights:
    index: int
    pkg: Package
    r_vol: float        # rounded vol weight
    r_act: float        # rounded actual weight
    dom_weight: float   # domestic billable weight
    intl_weight: float  # international billable weight
    min_bill: float


# ─── Weight Rounding (ported from Apps Script roundSpecial_) ─────────────────

def round_special(val: float) -> float:
    """
    Port of roundSpecial_ from Helper.js.
    3-5 kg  : 0.00-0.04 → floor, 0.05-0.99 → next int
    ≥5 kg   : 0.00-0.04 → floor, 0.05-0.50 → +0.5, 0.51-0.99 → next int
    """
    EPS = 1e-6
    f = math.floor(val + EPS)
    d = val - f

    if 3 <= val < 5:
        return f if d < 0.05 - EPS else f + 1

    if d < 0.05 - EPS:
        return f
    if d <= 0.50 + EPS:
        return f + 0.5
    return f + 1


def min_billable_weight(act_kg: float, vol_kg: float) -> float:
    """Port of minBillableWeight_ from Helper.js."""
    EPS = 1e-6
    max_raw = max(act_kg, vol_kg)
    if max_raw < 1 + EPS:
        return 1
    if max_raw < 2 + EPS:
        return 2
    return 0


# ─── Box Weight Calculation ──────────────────────────────────────────────────

def calculate_box_weights(packages: List[Package], mode: str) -> List[BoxWeights]:
    """Calculate per-box weights with rounding (processBoxWeights_ equivalent)."""
    is_sea = (mode == "加台海運")
    is_domestic = (mode == "加境內")

    results = []
    for i, pkg in enumerate(packages):
        vol = pkg.vol_weight
        r_vol = round_special(vol)
        r_act = round_special(pkg.weight)

        min_bill = min_billable_weight(r_act, r_vol)
        if is_sea and max(pkg.weight, vol) < 15:
            min_bill = 15

        base_dom = max(r_vol, r_act)

        if is_domestic:
            base_intl = base_dom
        elif is_sea:
            base_intl = base_dom
        else:  # 加台空運
            base_intl = r_act if r_vol < 2 * r_act else r_vol

        dom_weight = max(base_dom, min_bill)
        intl_weight = max(base_intl, min_bill)

        results.append(BoxWeights(
            index=i + 1,
            pkg=pkg,
            r_vol=r_vol,
            r_act=r_act,
            dom_weight=dom_weight,
            intl_weight=intl_weight,
            min_bill=min_bill,
        ))

    return results


# ─── OpenAI Parsing ──────────────────────────────────────────────────────────

PARSE_SYSTEM_PROMPT = """你是一個包裹資訊提取助手。從客戶訊息中精確提取包裹的尺寸、重量和加拿大郵遞區號。

規則：
1. 尺寸提取為 長、寬、高（公分 cm）。各種格式都要能辨識：
   - "113x50x20" "113*50*20" "113×50×20"
   - "長113 寬50 高20" "113公分x50公分x20公分"
   - 描述性的如 "大約113x50x20公分"
   - 若單位為英吋 (inch, in, "), 將其換算為公分 (cm)。換算邏輯：val * 2.54。
     **重要：換算結果必須無條件進位到小數點下第一位 (Example: 14in = 35.56cm -> 35.6cm)。**
   - **特別注意：若輸入為精確的小數 (如 45.72, 27.94)，通常為已換算過的公分，請勿再次換算，除非使用者明確標示單位為英吋。預設單位為公分。**
2. 重量提取為公斤(kg)。注意單位轉換：
   - "7公斤" "7kg" "7 kilos" → 7
   - "15磅" "15 lbs" → 6.9（1磅≈0.45359kg，15*0.45359=6.803...，請無條件進位到一位小數即6.9）
     **重要：重量換算也必須無條件進位到小數點下第一位。**
   - **如果數字單獨出現且沒有單位，若格式類似尺寸（三個數字連在一起）判斷為尺寸；若是單一數字（如 "8.8"）或 "8.8 5"，請判斷為重量 (kg)。**
3. 郵遞區號是加拿大格式：字母數字字母 數字字母數字（如 V6X1Z7, B2V1R9, T2P3G5）
4. 如果有多個包裹，分別列出每個的尺寸和重量
5. **支援更正/更新邏輯**：如果輸入的文本包含「更正」「修改」或與前文數字衝突的更新，請以最新的數值為準。例如：
   input: "10x10x10 5kg\n更正重量 8kg"
   output should be: 8kg for that package.
   input: "8.8" (followed by prev context if any) -> treat as 8.8 kg.
6. 忽略無關的聊天內容、問候語等
7. 如果訊息中提到尺寸但沒提到重量（或反之），仍然提取有的部分
8. 如果無法從文本中提取任何有用資訊，回傳空的 packages 和 postal_codes

回覆格式（嚴格 JSON）：
{"packages": [{"length": 113, "width": 50, "height": 20, "weight": 7}], "postal_codes": ["B2V1R9"]}"""


def parse_package_input(text: str) -> Optional[ParsedInput]:
    """Use OpenAI to extract package info from messy customer messages."""
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": PARSE_SYSTEM_PROMPT},
                {"role": "user", "content": f"客戶訊息：\n{text}"},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )

        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)

        packages = []
        for p in data.get("packages", []):
            pkg = Package(
                length=float(p.get("length", 0)),
                width=float(p.get("width", 0)),
                height=float(p.get("height", 0)),
                weight=float(p.get("weight", 0)),
            )
            # Allow partial packages if any dimension or weight is present
            if pkg.length > 0 or pkg.width > 0 or pkg.height > 0 or pkg.weight > 0:
                packages.append(pkg)

        postal_codes = []
        for pc in data.get("postal_codes", []):
            cleaned = re.sub(r'\s+', '', str(pc).upper())
            if POSTAL_RE.fullmatch(cleaned):
                postal_codes.append(cleaned)

        if not packages and not postal_codes:
            return None

        return ParsedInput(packages=packages, postal_codes=postal_codes, raw_text=text)

    except Exception as e:
        log.error(f"[QuoteService] OpenAI parse error: {e}")
        return None


def try_parse_structured(text: str) -> Optional[ParsedInput]:
    """
    Try to parse structured user input.
    Expected format (one box per line):
        L*W*H weight
        B2V1R9
    """
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    packages: List[Package] = []
    postal_codes: List[str] = []

    for line in lines:
        # Postal code only line
        pc_candidates = POSTAL_RE.findall(line.replace(' ', '').upper())
        if pc_candidates and not re.search(r'\d+\s*[*x×]\s*\d+', line, re.IGNORECASE):
            for pc in pc_candidates:
                if pc not in postal_codes:
                    postal_codes.append(pc)
            continue

        # Dimension + weight: L*W*H weight [postal]
        m = re.match(
            r'([\d.]+)\s*[*x×]\s*([\d.]+)\s*[*x×]\s*([\d.]+)\s+([\d.]+)\s*(.*)',
            line, re.IGNORECASE,
        )
        if m:
            packages.append(Package(
                length=float(m.group(1)),
                width=float(m.group(2)),
                height=float(m.group(3)),
                weight=float(m.group(4)),
            ))
            remainder = m.group(5)
            for pc in POSTAL_RE.findall(remainder.replace(' ', '').upper()):
                if pc not in postal_codes:
                    postal_codes.append(pc)
            continue

    if packages or postal_codes:
        return ParsedInput(packages=packages, postal_codes=postal_codes, raw_text=text)
    return None


# ─── TE API Quote ─────────────────────────────────────────────────────────────

def get_te_quotes(from_postal: str, to_postal: str,
                  packages: List[Package]) -> List[ServiceQuote]:
    """Call TripleEagle shipment/quote API."""
    to_inches = lambda cm: round(cm / 2.54, 2)

    api_packages = [
        {
            "weight": pkg.weight,
            "dimension": {
                "length": to_inches(pkg.length),
                "width": to_inches(pkg.width),
                "height": to_inches(pkg.height),
            },
            "insurance": 100,
        }
        for pkg in packages
    ]

    payload = {
        "initiation": {
            "region_id": "CA",
            "postalcode": from_postal.replace(" ", ""),
            "type": "commercial",
        },
        "destination": {
            "region_id": "CA",
            "postalcode": to_postal.replace(" ", ""),
            "type": "commercial",
        },
        "package": {"type": "parcel", "packages": api_packages},
        "option": {"memo": "Parcel"},
    }

    try:
        result = te_call_api("shipment/quote", payload)
        if not result or result.get("status") != 1:
            log.warning(f"[TE Quote] API error: {result}")
            return []

        quotes: List[ServiceQuote] = []
        for carrier in result.get("response", []):
            vendor = carrier.get("name", "Unknown")
            for svc in carrier.get("services", []):
                freight = float(svc.get("freight", 0) or 0)
                total = float(svc.get("charge", 0) or 0)
                tax = sum(float(t.get("price", 0)) for t in svc.get("tax_details", []))
                surcharge_sum = sum(float(d.get("price", 0)) for d in svc.get("charge_details", []))
                surcharge_details = "; ".join(
                    f"{d['name']}: ${float(d['price']):.2f}"
                    for d in svc.get("charge_details", [])
                )
                quotes.append(ServiceQuote(
                    carrier=vendor,
                    name=svc.get("name", "Unknown"),
                    freight=freight,
                    surcharges=surcharge_sum,
                    tax=tax,
                    total=total,
                    eta=svc.get("eta", "N/A"),
                    surcharge_details=surcharge_details,
                    source="TE",
                ))

        quotes.sort(key=lambda q: q.total)
        return quotes

    except Exception as e:
        log.error(f"[TE Quote] Error: {e}", exc_info=True)
        return []


# ─── CP API Quote ─────────────────────────────────────────────────────────────

def _build_cp_xml(origin_pc: str, dest_pc: str, weight_kg: float,
                  length_cm: float, width_cm: float, height_cm: float) -> str:
    """Build Canada Post mailing-scenario XML (ported from CPBuildParsXML.js)."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<mailing-scenario xmlns="http://www.canadapost.ca/ws/ship/rate-v4">
  <quote-type>counter</quote-type>
  <parcel-characteristics>
    <weight>{weight_kg:.3f}</weight>
    <dimensions>
      <length>{length_cm:.1f}</length>
      <width>{width_cm:.1f}</width>
      <height>{height_cm:.1f}</height>
    </dimensions>
  </parcel-characteristics>
  <origin-postal-code>{origin_pc.upper().replace(' ', '')}</origin-postal-code>
  <destination>
    <domestic>
      <postal-code>{dest_pc.upper().replace(' ', '')}</postal-code>
    </domestic>
  </destination>
</mailing-scenario>"""


def _parse_cp_response(xml_text: str) -> List[Dict]:
    """Parse Canada Post XML rate response (ported from CPBuildParsXML.js)."""
    try:
        root = ET.fromstring(xml_text)
        ns = {'cp': 'http://www.canadapost.ca/ws/ship/rate-v4'}

        if root.tag.endswith('messages'):
            errors = []
            for msg in root.findall('.//cp:message', ns):
                code = msg.findtext('cp:code', '', ns)
                desc = msg.findtext('cp:description', '', ns)
                errors.append(f"{code}: {desc}")
            log.warning(f"[CP] API errors: {'; '.join(errors)}")
            return []

        if not root.tag.endswith('price-quotes'):
            return []

        quotes = []
        for pq in root.findall('.//cp:price-quote', ns):
            service_code = pq.findtext('cp:service-code', '', ns)
            service_name = pq.findtext('cp:service-name', '', ns)

            pd = pq.find('cp:price-details', ns)
            base_val = float(pd.findtext('cp:base', '0', ns)) if pd is not None else 0

            taxes = 0.0
            taxes_el = pd.find('cp:taxes', ns) if pd is not None else None
            if taxes_el is not None:
                for tax_el in taxes_el.findall('cp:tax', ns):
                    taxes += float(tax_el.findtext('cp:amount', '0', ns))

            due = float(pd.findtext('cp:due', '0', ns)) if pd is not None else base_val + taxes

            ss = pq.find('cp:service-standard', ns)
            transit_days = ss.findtext('cp:expected-transit-time', '', ns) if ss is not None else ''
            expected_date = ss.findtext('cp:expected-delivery-date', '', ns) if ss is not None else ''
            eta = expected_date or (f"{transit_days} 個工作日" if transit_days else "N/A")

            quotes.append({
                'service_code': service_code,
                'service_name': service_name,
                'base': base_val,
                'taxes': taxes,
                'total': due,
                'eta': eta,
            })
        return quotes

    except ET.ParseError as e:
        log.error(f"[CP] XML parse error: {e}")
        return []


ALLOWED_CP_SERVICES = {'DOM.RP', 'DOM.EP', 'DOM.XP', 'DOM.PC'}


def get_cp_quotes(from_postal: str, to_postal: str,
                  packages: List[Package]) -> List[ServiceQuote]:
    """Call Canada Post API for domestic shipping quotes.
    Aggregates across all packages per service, returns sorted list."""
    auth_str = base64.b64encode(f"{CP_USERNAME}:{CP_PASSWORD}".encode()).decode()
    headers = {
        'Accept': 'application/vnd.cpc.ship.rate-v4+xml',
        'Content-Type': 'application/vnd.cpc.ship.rate-v4+xml',
        'Accept-language': 'en-CA',
        'Authorization': f'Basic {auth_str}',
    }

    service_totals: Dict[str, Dict] = {}   # service_code → aggregated totals

    for pkg in packages:
        xml = _build_cp_xml(from_postal, to_postal, pkg.weight,
                            pkg.length, pkg.width, pkg.height)
        try:
            resp = requests.post(
                f"{CP_ENDPOINT}/rs/ship/price",
                data=xml.encode('utf-8'),
                headers=headers,
                timeout=30,
            )
            if 200 <= resp.status_code < 300:
                box_quotes = _parse_cp_response(resp.text)
                for q in box_quotes:
                    sc = q['service_code']
                    if sc not in ALLOWED_CP_SERVICES:
                        continue
                    if sc not in service_totals:
                        service_totals[sc] = {
                            'service_name': q['service_name'],
                            'base': 0, 'taxes': 0, 'total': 0,
                            'eta': q['eta'],
                        }
                    service_totals[sc]['base'] += q['base']
                    service_totals[sc]['taxes'] += q['taxes']
                    service_totals[sc]['total'] += q['total']
            else:
                log.warning(f"[CP] HTTP {resp.status_code}: {resp.text[:300]}")

        except Exception as e:
            log.error(f"[CP] Request error: {e}", exc_info=True)

    results = [
        ServiceQuote(
            carrier="Canada Post",
            name=data['service_name'],
            freight=round(data['base'], 2),
            surcharges=0,
            tax=round(data['taxes'], 2),
            total=round(data['total'], 2),
            eta=data['eta'],
            source="CP",
        )
        for data in service_totals.values()
    ]
    results.sort(key=lambda q: q.total)
    return results


# ─── Rate Helpers ─────────────────────────────────────────────────────────────

def get_i15_rate(mode: str, total_intl_weight: float) -> float:
    """International surcharge rate (setI15ByMode_ equivalent)."""
    if mode == "加台空運":
        return INTL_RATE_AIR_LIGHT if total_intl_weight < 3 else INTL_RATE_AIR_HEAVY
    if mode == "加台海運":
        return INTL_RATE_SEA
    if mode == "加境內":
        return INTL_RATE_DOMESTIC
    return 0


def is_greater_vancouver(postal: str) -> bool:
    """Check if postal code is in Greater Vancouver (V3-V7)."""
    pc = postal.strip().upper().replace(" ", "")
    if len(pc) < 2:
        return False
    # V3, V4, V5, V6, V7 used as loose definition of GV
    return pc.startswith("V") and pc[1] in ("3", "4", "5", "6", "7")


def calc_q3_rate(from_postal: str, total_dom_weight: float,
                 cost_per_kg: float) -> float:
    """Replicate Q3 formula from Google Sheet 報價計算器.

    Eastern postcodes + light + cheap → floor of 3.0 CAD/kg.
    Otherwise → max(2.5, cost_per_kg + 1).
    """
    first = from_postal.strip().upper()[:1] if from_postal.strip() else ""
    is_eastern = first in EASTERN_POSTAL_PREFIXES
    if is_eastern and total_dom_weight < 15 and cost_per_kg < 3:
        return 3.0
    return max(2.5, cost_per_kg + 1)


def calc_tw_fee_ntd(pkg: Package) -> float:
    """Replicate M column formula from Google Sheet.

    cuft = L*W*H / 27000
    fee = 0 if cuft==0; 150 if cuft<2; else (ceil(cuft)-2)*30 + 150.
    """
    cuft = (pkg.length * pkg.width * pkg.height) / 27000
    if cuft < 1e-9:
        return 0.0
    if cuft < 2:
        return 150.0
    return round((math.ceil(cuft) - 2) * 30 + 150)


def _is_date_str(s: str) -> bool:
    """Check if string is a parseable YYYY-MM-DD date."""
    try:
        datetime.strptime(str(s).strip(), "%Y-%m-%d")
        return True
    except (ValueError, AttributeError):
        return False


# ─── Canned Message Builder ──────────────────────────────────────────────────

def build_quote_text(mode: str,
                     from_postal: str,
                     to_postal: str,
                     packages: List[Package],
                     box_weights: List[BoxWeights],
                     cheapest: ServiceQuote,
                     all_services: List[ServiceQuote]) -> str:
    """
    Build the '罐頭訊息' canned message, mimicking buildReportText_ in
    writeCostSummary.js as closely as possible.
    """
    lines = [f"👉{mode}初步報價：", ""]

    # ── 1. ETA ────────────────────────────────────────────────────────────
    eta = cheapest.eta
    eta_is_date = _is_date_str(eta)
    if mode == "加台空運":
        if eta_is_date:
            lines.append(f"🕒若今日寄件，預計 {eta} 抵達國際空運倉")
            eta_range = _calc_arrival_range(eta)
            if eta_range:
                lines.append(f"🕒台灣投遞 ETA：{eta_range[0]} ～ {eta_range[1]}")
            else:
                lines.append("🕒台灣投遞 ETA：約抵達空運倉後 3～8 個工作日")
        else:
            lines.append("🕒抵達國際空運倉 ETA：系統未提供")
            lines.append("🕒台灣投遞 ETA：約抵達空運倉後 3～8 個工作日")
        lines.append("🕒逢台加假日/颱風假/旺季延誤/店到店作業等則順延")
        lines.append("🕒實際投遞日期依物流狀況為準")
    elif mode == "加台海運":
        if eta_is_date:
            lines.append(f"🕒若今日寄件，預計 {eta} 抵達國際海運倉")
        else:
            lines.append("🕒抵達國際海運倉 ETA：系統未提供")
        lines.append("🕒海運包裹抵達海運倉後，運送時效約 1～3 個月")
        lines.append("🕒實際投遞日期依船期及港口狀況為準")
    elif mode == "加境內":
        if eta_is_date:
            lines.append(f"🕒若今日寄件，預計 {eta} 抵達指定地址")
        else:
            lines.append("🕒送達指定地址 ETA：系統未提供")
        lines.append("🕒實際投遞日期依物流狀況為準")
    lines.append("")

    # ── 2. Postal Codes ──────────────────────────────────────────────────
    fp = _fmt_postal(from_postal)
    tp = _fmt_postal(to_postal)
    if mode == "加境內":
        lines.append(f"📮{fp} → {tp}")
    else:
        lines.append(f"📮From: {fp}")
    lines.append("")

    # Compute derived values
    is_domestic = (mode == "加境內")
    total_dom_weight = sum(bw.dom_weight for bw in box_weights)
    total_intl_weight = sum(bw.intl_weight for bw in box_weights)
    i15 = get_i15_rate(mode, total_intl_weight)

    # Q3-equivalent markup rate (replaces raw API rate / total_dom_weight)
    cost_per_kg = cheapest.total / total_dom_weight if total_dom_weight else 0
    q3 = calc_q3_rate(from_postal, total_dom_weight, cost_per_kg)

    if is_domestic:
        dom_rate = q3 + i15       # Q5 = Q3 + I15 (combined rate for 加境內)
        display_rate = dom_rate
    else:
        dom_rate = q3              # H column = Q3
        display_rate = q3 + i15    # header rate = H + I15

    svc_label = f"{cheapest.carrier} - {cheapest.name}"
    lines.append(f"💻系統顯示 {display_rate:.3f} CAD/kg")
    lines.append(f"🚚使用境內服務: {svc_label}")
    lines.append("")

    # ── 4. Per-Box Details ───────────────────────────────────────────────
    grand_total = 0.0
    box_subtotals = []

    for bw in box_weights:
        cmp = (">>" if bw.r_vol > 2 * bw.r_act
               else (">" if bw.r_vol > bw.r_act
                     else ("<" if bw.r_vol < bw.r_act else "=")))

        if is_domestic:
            box_cost = dom_rate * bw.dom_weight
            expr = f"{dom_rate:.3f}*{bw.dom_weight:.1f}"
        else:
            dom_cost = dom_rate * bw.dom_weight
            intl_cost = i15 * bw.intl_weight
            box_cost = dom_cost + intl_cost
            expr = f"{dom_rate:.3f}*{bw.dom_weight:.1f} + {i15:.3f}*{bw.intl_weight:.1f}"

        # Taiwan domestic fee per box (加台空運 only, based on cuft weight)
        if mode == "加台空運":
            tw_fee = calc_tw_fee_ntd(bw.pkg)
            if tw_fee > 0:
                box_cost += tw_fee / EXCHANGE_RATE
                expr += f" + {tw_fee:.0f}/{EXCHANGE_RATE:.1f}"

        grand_total += box_cost
        box_subtotals.append(box_cost)

        lines.append(f"📦Box {bw.index}:")
        lines.append(
            f"{bw.pkg.dim_text}/5000 = {bw.pkg.vol_weight:.2f} → "
            f"{bw.r_vol:.1f}kg {cmp} {bw.pkg.weight:.2f}kg → {bw.r_act:.1f}kg"
        )
        lines.append(f"{expr} = {box_cost:.2f} CAD")

        if bw.min_bill == 15:
            lines.append("（海運最低計費 15 kg）")
        elif bw.min_bill == 1:
            lines.append("（不足 1 公斤，以 1 公斤計價）")
        elif bw.min_bill == 2:
            lines.append("（最大值介於 1–2 公斤，以 2 公斤計價）")
        lines.append("")

    # ── 5. Total ─────────────────────────────────────────────────────────
    if len(box_subtotals) > 1:
        parts = " + ".join(f"{s:.2f}" for s in box_subtotals)
        lines.append(f"💲Total Cost: {parts} = {grand_total:.2f} CAD")
    else:
        lines.append(f"💲Total Cost: {grand_total:.2f} CAD")
    lines.append("")

    # ── 6. Footer ────────────────────────────────────────────────────────
    lines.append("🚚使用 UPS / FedEx / Purolator 寄送時，可憑運單上的追蹤碼查詢配送進度。")
    lines.append("")

    if mode == "加境內":
        lines.extend([
            "✅若同意報價並已提交表單，可直接回覆確認。",
            "📌如尚未填寫寄件資料，請先填寫表單：https://bit.ly/4oH9Q8F",
            "💵完成 e-Transfer 後即可出單。",
            "💰如未於大溫地區面交寄件，需先支付押金。",
            "🔄包裹送達後約 1～2 週，若 UPS / FedEx / Purolator 有追加費用，"
            "將自押金中扣除後退款；若無則全額退還。詳情請見表單。",
        ])
    elif mode == "加台空運":
        lines.extend([
            "✅若同意報價並已提交表單，可直接回覆確認即可出單。",
            "📏最終請款金額以國際空運倉的復測數據為準。",
            "📌如尚未填寫寄件資料，請先填寫表單：https://bit.ly/49brblV",
        ])
    elif mode == "加台海運":
        lines.extend([
            "✅若同意報價並已提交表單，可直接回覆確認即可出單。",
            "📏最終請款金額以國際空運倉的復測數據為準。",
            "📌如尚未填寫寄件資料，請先填寫表單：https://bit.ly/4nETYCY",
        ])

    return "\n".join(lines)


# ─── ETA Business-Day Range Helper ───────────────────────────────────────────

def _calc_arrival_range(eta_str: str):
    """
    Calculate Taiwan arrival range: ETA date + 3~8 business days.
    Returns (from_str, to_str) or None if ETA can't be parsed.
    Mimics calcArrivalRange_ from Google Sheet.
    """
    try:
        eta_date = datetime.strptime(eta_str.strip(), "%Y-%m-%d")
    except (ValueError, AttributeError):
        return None

    def _add_biz_days(start: datetime, days: int) -> datetime:
        current = start
        added = 0
        while added < days:
            current += timedelta(days=1)
            # Skip weekends (5=Sat, 6=Sun)
            if current.weekday() >= 5:
                continue
            added += 1
        return current

    d3 = _add_biz_days(eta_date, 3)
    d8 = _add_biz_days(eta_date, 8)
    return (d3.strftime("%Y-%m-%d"), d8.strftime("%Y-%m-%d"))


def _fmt_postal(pc: str) -> str:
    pc = pc.upper().replace(" ", "")
    return f"{pc[:3]} {pc[3:]}" if len(pc) == 6 else pc
