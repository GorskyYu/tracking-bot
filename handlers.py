# tracking_bot/handlers.py
from __future__ import annotations
from typing import Dict, Any, Optional, List, Set

import re
import json
import requests
import pytz
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse as parse_date

from log import log
from sheets import get_gspread_client
from config import (
    # LINE & group ids
    ACE_GROUP_ID,
    SOQUICK_GROUP_ID,
    VICKY_GROUP_ID,
    YUMI_GROUP_ID,
    JOYCE_GROUP_ID,
    PDF_GROUP_ID,
    VICKY_USER_ID,
    YVES_USER_ID,

    # names/filters
    VICKY_NAMES,
    YUMI_NAMES,
    YVES_NAMES,
    EXCLUDED_SENDERS,

    # misc config
    TIMEZONE,
    SQ_SHEET_URL,

    # LINE API config
    LINE_TOKEN,
    LINE_HEADERS,
)
# Some constants are used directly with requests; keep URL literal here for clarity
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# Regex triggers originally defined in main.py
CODE_TRIGGER_RE = re.compile(r"\b(?:ACE|250N)\d+[A-Z0-9]*\b")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def strip_mention(line: str) -> str:
    """
    Remove an @mention at the very start of the line (e.g. "@Gorsky ").
    """
    return re.sub(r"^@\S+\s*", "", line)


# ─────────────────────────────────────────────────────────────────────────────
# Handlers (moved from main.py, logic preserved)
# ─────────────────────────────────────────────────────────────────────────────

def handle_soquick_and_ace_shipments(event: Dict[str, Any]) -> None:
    """
    Parse Soquick & Ace text containing "上周六出貨包裹的派件單號", "出貨單號", "宅配單號"
    split out lines of tracking+code+recipient, then push
    only the matching Vicky/Yumi lines + footer.
    """
    raw = event["message"]["text"]
    if "上周六出貨包裹的派件單號" not in raw and not ("出貨單號" in raw and "宅配單號" in raw):
        return

    vicky: List[str] = []
    yumi: List[str] = []

    # — Soquick flow —
    if "上周六出貨包裹的派件單號" in raw:
        # Split into non-empty lines
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        # Locate footer (starts with “您好”)
        footer_idx = next((i for i, l in enumerate(lines) if l.startswith("您好")), len(lines))
        header = lines[:footer_idx]
        footer = "\n".join(lines[footer_idx:])

        for line in header:
            parts = line.split()
            if len(parts) < 3:
                continue
            recipient = parts[-1]
            if recipient in VICKY_NAMES:
                vicky.append(line)
            elif recipient in YUMI_NAMES:
                yumi.append(line)

    # — Ace flow —
    else:
        # split into one block per “出貨單號:” line
        blocks = [b.strip().strip('"') for b in re.split(r'(?=出貨單號:)', raw) if b.strip()]

        for blk in blocks:
            # strip whitespace and any wrapping quotes
            block = blk.strip().strip('"')
            if not block:
                continue
            # must contain both 出貨單號 and 宅配單號
            if "出貨單號" not in block or "宅配單號" not in block:
                continue
            lines = block.splitlines()
            if len(lines) < 3:
                continue
            recipient = lines[2].split()[0]
            if recipient in VICKY_NAMES:
                vicky.append(block)
            elif recipient in YUMI_NAMES:
                yumi.append(block)

    def push(group: str, msgs: List[str]) -> None:
        if not msgs:
            return

        # choose formatting per flow
        if "上周六出貨包裹的派件單號" in raw:
            text = "\n".join(msgs) + "\n\n" + footer
        else:
            text = "\n\n".join(msgs)
        payload = {"to": group, "messages": [{"type": "text", "text": text}]}
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent {len(msgs)} Soquick blocks to {group}: {resp.status_code}")

    push(VICKY_GROUP_ID, vicky)
    push(YUMI_GROUP_ID, yumi)


def handle_soquick_full_notification(event: Dict[str, Any]) -> None:
    """
    1) Parse the incoming text for “您好，請…” + “按申報相符”
    2) Split off the footer and extract all recipient names
    3) Push Vicky/Yumi group messages with their names + footer
    4) Look up those same names in col M of your Soquick sheet
       to find the corresponding senders in col C, and privately
       notify Yves of any senders not already in Vicky/Yumi/Excluded.
    """
    log.info(f"[SOQ FULL] invoked on text={event['message']['text']!r}")
    text = event["message"]["text"]
    if not ("您好，請" in text and "按" in text and "申報相符" in text):
        return

    # 1) extract lines & footer
    # split into non-empty lines and strip any leading @mention
    lines = [
        strip_mention(l.strip())
        for l in text.splitlines()
        if l.strip()
    ]
    try:
        footer_idx = next(i for i, l in enumerate(lines) if "您好，請" in l)
    except StopIteration:
        footer_idx = len(lines)
    recipients = lines[:footer_idx]
    footer = "\n".join(lines[footer_idx:])

    # 2) split into Vicky / Yumi / “others” batches
    vicky_batch = [r for r in recipients if r in VICKY_NAMES]
    yumi_batch = [r for r in recipients if r in YUMI_NAMES]
    other_recipients = [
        r for r in recipients
        if r not in VICKY_NAMES
        and r not in YUMI_NAMES
        and r not in EXCLUDED_SENDERS
    ]

    # ===== DEBUG =====
    log.info(f"[SOQ FULL][DEBUG] other_recipients = {other_recipients!r}")

    # dedupe helper
    def dedupe(seq: List[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    vicky_batch = dedupe(vicky_batch)
    yumi_batch = dedupe(yumi_batch)
    other_recipients = dedupe(other_recipients)

    # 3) push the group notifications
    def push_group(group: str, batch: List[str]) -> None:
        if not batch:
            return
        standard_footer = "您好，請提醒以上認證人按申報相符"
        msg = "\n".join(batch) + "\n\n" + standard_footer
        requests.post(
            LINE_PUSH_URL,
            headers=LINE_HEADERS,
            json={"to": group, "messages": [{"type": "text", "text": msg}]}
        )

    push_group(VICKY_GROUP_ID, vicky_batch)
    push_group(YUMI_GROUP_ID, yumi_batch)

    # ── Private “other” pushes ─────────────────────
    other_recipients = dedupe([
        r for r in recipients
        if r not in VICKY_NAMES
        and r not in YUMI_NAMES
        and r not in EXCLUDED_SENDERS
    ])
    log.info(f"[SOQ FULL][DEBUG] other_recipients = {other_recipients!r}")

    if other_recipients:
        # 依照訊息日期動態選分頁：前3天到後2天（原始程式假定 +08:00，這裡沿用）
        import datetime as _dt
        ts = event["timestamp"]  # ms
        dt = _dt.datetime.fromtimestamp(ts / 1000,
                                        tz=_dt.timezone(_dt.timedelta(hours=8)))
        base = dt.date()
        candidates = [(base + _dt.timedelta(days=d)).strftime("%y%m%d")
                      for d in range(-3, 3)]

        # guard to ensure the sheet exist and don't crash
        if not SQ_SHEET_URL:
            log.error("[SOQ FULL] SQ_SHEET_URL not set")
            return

        gs = get_gspread_client()
        ss = gs.open_by_url(SQ_SHEET_URL)
        found = [ws.title for ws in ss.worksheets() if ws.title in candidates]
        if len(found) == 1:
            sheet = ss.worksheet(found[0])
            log.info(f"[SOQ FULL][DEBUG] 使用分頁 {found[0]}")
        else:
            log.error(f"[SOQ FULL] 分頁數量不唯一，expected=1 got={len(found)}; candidates={candidates}, found={found}")
            return
        rows = sheet.get_all_values()[1:]  # skip header
        senders: Set[str] = set()

        for idx, row in enumerate(rows, start=2):
            # 印每一列 E 欄
            name_in_sheet = row[4].strip() if len(row) > 4 else ""
            log.info(f"[SOQ FULL][DEBUG] row {idx} colE = {name_in_sheet!r}")

            if name_in_sheet in other_recipients:
                sender = row[2].strip() if len(row) > 2 else ""
                log.info(f"[SOQ FULL][DEBUG] matched recipient {name_in_sheet!r} → sender {sender!r}")
                if sender and sender not in (VICKY_NAMES | YUMI_NAMES | EXCLUDED_SENDERS):
                    senders.add(sender)

        if senders:
            # header notification
            requests.post(
                LINE_PUSH_URL, headers=LINE_HEADERS,
                json={
                    "to": YVES_USER_ID,
                    "messages": [{"type": "text", "text": "Soquick散客EZWay需提醒以下寄件人："}]
                }
            )
            for s in sorted(senders):
                requests.post(
                    LINE_PUSH_URL, headers=LINE_HEADERS,
                    json={"to": YVES_USER_ID, "messages": [{"type": "text", "text": s}]}
                )
            log.info(f"[SOQ FULL] Privately pushed {len(senders)} senders to Yves")


def handle_missing_confirm(event: Dict[str, Any]) -> None:
    """
    處理包含「申報相符」關鍵字的訊息：逐行抓 ACE/250N 單號，判斷姓名屬於 Vicky 或 Yumi，
    分別推播到對應群組。
    """
    text = event["message"]["text"]

    # 如果這是原始 EZ-Way 通知，就跳過
    if "收到EZ way通知後" in text:
        return

    # 如果訊息裡沒有「申報相符」，就跳過
    if "申報相符" not in text:
        return

    # 逐行找 ACE/250N 單號
    for l in text.splitlines():
        if CODE_TRIGGER_RE.search(l):
            parts = re.split(r"\s+", l.strip())
            # 確保至少有三段：單號、姓名、電話（原碼只用到姓名）
            if len(parts) < 2:
                continue
            name = parts[1]
            if name in VICKY_NAMES:
                target = VICKY_GROUP_ID
            elif name in YUMI_NAMES:
                target = YUMI_GROUP_ID
            else:
                # 不是 Vicky 也不是 Yumi 的人，直接跳過
                continue

            # 推播姓名（你可以改成更完整的訊息）
            requests.post(
                LINE_PUSH_URL,
                headers=LINE_HEADERS,
                json={"to": target, "messages": [{"type": "text", "text": f"{name} 尚未按申報相符"}]}
            )


def handle_ace_schedule(event: Dict[str, Any]) -> None:
    """
    Extracts the Ace message, filters lines for Yumi/Vicky,
    and pushes a cleaned summary into their groups with the names
    inserted between 麻煩請 and 收到EZ way通知後…
    """
    text = event["message"]["text"]
    lines = text.splitlines()

    # find the index of the “麻煩請” line
    try:
        idx_m = next(i for i, l in enumerate(lines) if "麻煩請" in l)
    except StopIteration:
        idx_m = 1  # fallback just after the first line

    # find the index of the “收到EZ way通知後” line
    try:
        idx_r = next(i for i, l in enumerate(lines) if l.startswith("收到EZ way通知後"))
    except StopIteration:
        idx_r = len(lines)

    # header before names: up through 麻煩請
    header = lines[: idx_m + 1]

    # footer after names: from 收到EZ way通知後 onward
    footer = lines[idx_r:]

    # collect only the code lines (ACE/250N+name)
    code_lines = [l for l in lines if CODE_TRIGGER_RE.search(l)]

    # strip the code prefix and any stray quotes
    cleaned = [
        CODE_TRIGGER_RE.sub("", l).strip().strip('"')
        for l in code_lines
    ]

    # now split into per-group lists
    vicky_batch = [c for c in cleaned if any(name in c for name in VICKY_NAMES)]
    yumi_batch = [c for c in cleaned if any(name in c for name in YUMI_NAMES)]

    # extract just the name token (first word) from each cleaned line
    names_only = [c.split()[0] for c in cleaned]

    # “others” = those whose name token isn’t in any of the three lists
    other_batch = [
        cleaned[i] for i, nm in enumerate(names_only)
        if nm not in VICKY_NAMES
        and nm not in YUMI_NAMES
        and nm not in YVES_NAMES
    ]

    def push_to(group: str, batch: List[str]) -> None:
        if not batch:
            return

        # Build the mini-message: header + blank + batch + blank + footer
        msg_lines = header + [""] + batch + [""] + footer
        text_msg = "\n".join(msg_lines)

        # Push to the group
        requests.post(
            LINE_PUSH_URL,
            headers=LINE_HEADERS,
            json={"to": group, "messages": [{"type": "text", "text": text_msg}]}
        )

    push_to(VICKY_GROUP_ID, vicky_batch)
    push_to(YUMI_GROUP_ID, yumi_batch)
    # also push any “other” entries to your personal chat
    push_to(YVES_USER_ID, other_batch)


def handle_ace_shipments(event: Dict[str, Any]) -> None:
    """
    Splits the text into blocks starting with '出貨單號:', then
    forwards each complete block to Yumi or Vicky based on the
    recipient name.
    """
    # 1) Grab & clean the raw text
    raw = event["message"]["text"]
    log.info(f"[ACE SHIP] raw incoming text: {repr(raw)}")  # DEBUG log
    text = raw.replace('"', '').strip()                     # strip stray quotes

    # split into shipment‐blocks
    parts = re.split(r'(?=出貨單號:)', text)
    log.info(f"[ACE SHIP] split into {len(parts)} parts")   # DEBUG log

    vicky: List[str] = []
    yumi: List[str] = []

    for blk in parts:
        if "出貨單號:" not in blk or "宅配單號:" not in blk:
            continue
        lines = [l.strip() for l in blk.strip().splitlines() if l.strip()]
        if len(lines) < 4:
            continue
        # recipient name is on line 3
        recipient = lines[2].split()[0]
        full_msg = "\n".join(lines)
        if recipient in VICKY_NAMES:
            vicky.append(full_msg)
        elif recipient in YUMI_NAMES:
            yumi.append(full_msg)

    def push(group: str, messages: List[str]) -> None:
        if not messages:
            return
        payload = {
            "to": group,
            "messages": [{"type": "text", "text": "\n\n".join(messages)}]
        }
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent {len(messages)} shipment blocks to {group}: {resp.status_code}")


    push(VICKY_GROUP_ID, vicky)
    push(YUMI_GROUP_ID, yumi)


def handle_ace_ezway_check_and_push_to_yves(event: Dict[str, Any]) -> None:
    """
    For any ACE message that contains “麻煩請” + “收到EZ way通知後” + (週四出貨 or 週日出貨),
    we will look up the *sheet* for the row whose date is closest to today, but ONLY
    for those “declaring persons” that actually appeared in the ACE text.  For each
    matching row, we pull the “sender” (column C) and push it privately if it's not in
    VICKY_NAMES or YUMI_NAMES or EXCLUDED_SENDERS.
    """
    text = event["message"]["text"]

    # Only trigger on the exact keywords
    if not (
        "麻煩請" in text
        and "收到EZ way通知後" in text
        and ("週四出貨" in text or "週日出貨" in text)
    ):
        return

    # ── 1) Extract declarer‐names from the ACE text ────────────────────────
    lines = text.splitlines()

    # find the line index that contains “麻煩請”
    try:
        idx_m = next(i for i, l in enumerate(lines) if "麻煩請" in l)
    except StopIteration:
        # If we can't find it, default to the top
        idx_m = 0

    # find the line index that starts with “收到EZ way通知後”
    try:
        idx_r = next(i for i, l in enumerate(lines) if l.startswith("收到EZ way通知後"))
    except StopIteration:
        idx_r = len(lines)

    # declarer lines are everything strictly between “麻煩請” and “收到EZ way通知後”
    raw_declarer_lines = lines[idx_m + 1: idx_r]
    declarer_names: Set[str] = set()

    for line in raw_declarer_lines:
        # Remove any ACE‐style code prefix (e.g. “ACE250605YL04 ”)
        cleaned = CODE_TRIGGER_RE.sub("", line).strip().strip('"')
        if not cleaned:
            continue

        # Take the first “token” as the actual name (before any phone or other columns)
        name_token = cleaned.split()[0]
        if name_token:
            declarer_names.add(name_token)

    if not declarer_names:
        # No valid declarers found in the message → nothing to do
        return

    # ── 2) Open the ACE sheet and find the “closest‐date” row ─────────────
    ACE_SHEET_URL = __import__("os").getenv("ACE_SHEET_URL")
    gs = get_gspread_client()
    sheet = gs.open_by_url(ACE_SHEET_URL).sheet1
    data = sheet.get_all_values()  # raw rows as lists of strings

    today = datetime.now(timezone.utc).date()
    closest_date = None
    closest_diff = timedelta(days=9999)

    # Assume column A is date; skip header row at index 0, so start at row 2 in the sheet
    for row_idx, row in enumerate(data[1:], start=2):
        date_str = (row[0] or "").strip()
        if not date_str:
            continue
        try:
            row_date = parse_date(date_str).date()
        except Exception:
            continue

        diff = abs(row_date - today)
        if diff < closest_diff:
            closest_diff = diff
            closest_date = row_date

    if closest_date is None:
        # No parseable dates in sheet → bail out
        return

    # ── 3) Scan only the rows on that closest_date, and only if column B (declarer)
    #         is in our declarer_names set.  Then we grab column C (sender) for private push.
    results: Set[str] = set()

    for row_idx, row in enumerate(data[1:], start=2):
        date_str = (row[0] or "").strip()
        if not date_str:
            continue
        try:
            row_date = parse_date(date_str).date()
        except Exception:
            continue

        if row_date != closest_date:
            continue

        # Column B is at index 1 in 'row'
        declarer = (row[1] if len(row) > 1 else "").strip()
        if not declarer or declarer not in declarer_names:
            continue

        # Column C is at index 2 in 'row' → this is the “sender” we want to notify
        sender = (row[2] if len(row) > 2 else "").strip()
        if not sender:
            continue

        # Skip anyone already in VICKY_NAMES, YUMI_NAMES, or EXCLUDED_SENDERS
        if sender in VICKY_NAMES or sender in YUMI_NAMES or sender in EXCLUDED_SENDERS:
            continue

        results.add(sender)

    # ── 4) Push to Yves privately if any senders remain ────────────────────
    if results:
        header_payload = {
            "to": YVES_USER_ID,
            "messages": [{"type": "text", "text": "Ace散客EZWay需提醒以下寄件人："}]
        }
        requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=header_payload)

        for sender in sorted(results):
            payload = {
                "to": YVES_USER_ID,
                "messages": [{"type": "text", "text": sender}]
            }
            requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)

        log.info(f"[ACE EZWay] Pushed {len(results)} sender(s) to Yves: {sorted(results)}")
    else:
        log.info("[ACE EZWay] No matching senders found for any declarer in the ACE message.")
