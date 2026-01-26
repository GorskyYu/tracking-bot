# tracking_bot/handlers.py
from __future__ import annotations
from typing import Dict, Any, Optional, List, Set
from linebot.models import TextSendMessage
from utils.permissions import ADMIN_USER_IDS

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
    IRIS_GROUP_ID,
    SOQUICK_GROUP_ID,
    VICKY_GROUP_ID,
    YUMI_GROUP_ID,
    JOYCE_GROUP_ID,
    PDF_GROUP_ID,
    DANNY_USER_ID,
    GORSKY_USER_ID,
    IRIS_USER_ID,
    VICKY_USER_ID,
    YVES_USER_ID,

    # names/filters
    IRIS_NAMES,
    VICKY_NAMES,
    YUMI_NAMES,
    YVES_NAMES,
    EXCLUDED_SENDERS,

    # misc config
    TIMEZONE,
    SQ_SHEET_URL,
    CODE_TRIGGER_RE,

    # LINE API config
    LINE_TOKEN,
    LINE_HEADERS,
    line_bot_api,
)

# Sky's user ID (treat like Danny for auto-confirmation)
SKY_USER_ID = "U0a92a6a032457ccfec9b4c5e76cd65cb"

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

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
    解析 Soquick & Ace 文字。若收件人不在名單內，則去 ACE 試算表反查寄件人，
    並將結果私訊給 Yves。
    """
    # 1. 獲取發送者暱稱 (為了 Fallback 標籤)
    user_id = event["source"]["userId"]
    try:
        profile = line_bot_api.get_profile(user_id)
        sender_name = profile.display_name
    except Exception:
        sender_name = "未知發送者"

    raw = event["message"]["text"]
    if "上周六出貨包裹的派件單號" not in raw and not ("出貨單號" in raw and "宅配單號" in raw):
        return

    vicky: List[str] = []
    yumi: List[str] = []
    iris: List[str] = []
    fallback_map: Dict[str, List[str]] = {}

    # --- 1. 執行分流邏輯 ---
    if "上周六出貨包裹的派件單號" in raw:
        # Soquick 流程
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
            full_msg = line

            if recipient in VICKY_NAMES:
                vicky.append(full_msg)
            elif recipient in YUMI_NAMES:
                yumi.append(full_msg)
            elif recipient in IRIS_NAMES:
                iris.append(full_msg)
            else:
                # 按收件人收集單號
                if recipient not in fallback_map: fallback_map[recipient] = []
                fallback_map[recipient].append(full_msg)
    else:
        # ACE 流程
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

            # 使用 "\n".join(lines[1:]) 來刪除第一行
            full_msg = "\n".join(lines[1:])

            if recipient in VICKY_NAMES:
                vicky.append(full_msg)
            elif recipient in YUMI_NAMES:
                yumi.append(full_msg)
            elif recipient in IRIS_NAMES:
                iris.append(full_msg)
            else:
                # 按收件人收集單號
                if recipient not in fallback_map: fallback_map[recipient] = []
                fallback_map[recipient].append(full_msg)

    # --- 2. 執行群組發送 ---
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
    push(IRIS_GROUP_ID, iris)

    #--- 3. ACE 試算表反查與打包分組 (名字一條、內容一條) ---
    sender_to_bundles: Dict[str, List[str]] = {} # 儲存 寄件人 -> [單號1, 單號2...]
    unmapped_blocks: List[str] = []

    if fallback_map:
        from utils.permissions import ADMIN_USER_IDS
        from config import ACE_SHEET_URL, EXCLUDED_SENDERS
        
        try:
            gs = get_gspread_client()
            ss = gs.open_by_url(ACE_SHEET_URL)
            target_date = datetime.fromtimestamp(event["timestamp"]/1000, tz=timezone(timedelta(hours=8))).strftime("%y%m%d")
            
            worksheets = ss.worksheets()
            ws = next((w for w in worksheets if w.title == target_date), worksheets[0]) 
            rows = ws.get_all_values()[1:] 

            matched_recipients = set()
            # 加入 matched_recipients 判定，避免同一收件人重複匹配多行
            for row in rows:
                sheet_recipient = row[6].strip() if len(row) > 6 else ""
                if sheet_recipient in fallback_map and sheet_recipient not in matched_recipients:
                    sender = row[2].strip() if len(row) > 2 else "未知寄件人"
                    if sender and sender not in (VICKY_NAMES | YUMI_NAMES | IRIS_NAMES | EXCLUDED_SENDERS):
                        if sender not in sender_to_bundles:
                            sender_to_bundles[sender] = []
                        sender_to_bundles[sender].extend(fallback_map[sheet_recipient])
                        matched_recipients.add(sheet_recipient) # 標記已處理
            
            # 處理查不到寄件人的項目
            for rec, msgs in fallback_map.items():
                if rec not in matched_recipients: unmapped_blocks.extend(msgs)
        except Exception as e:
            log.error(f"Fallback sheet lookup failed: {e}")
            for msgs in fallback_map.values(): unmapped_blocks.extend(msgs)

        # --- 4. 私訊發送給管理員 (分條發送邏輯) ---
        for admin_id in ADMIN_USER_IDS:
            # A. 發送開頭標題
            line_bot_api.push_message(admin_id, TextSendMessage(text=f"ACE Fallback收件人："))
            
            # B. 依寄件人分組發送
            for s_name, blocks in sender_to_bundles.items():
                # 訊息1：寄件人名字 (獨立一條)
                line_bot_api.push_message(admin_id, TextSendMessage(text=s_name))
                # 訊息2：該寄件人打包後的單號 (合併在一條)
                line_bot_api.push_message(admin_id, TextSendMessage(text="\n\n".join(blocks)))

            # C. 查無寄件人的剩餘資料
            if unmapped_blocks:
                line_bot_api.push_message(admin_id, TextSendMessage(text="以下為查無 ACE 試算表寄件人之單號："))
                line_bot_api.push_message(admin_id, TextSendMessage(text="\n\n".join(unmapped_blocks)))


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
            # header notification 改為迴圈發送給所有管理員
            for admin_id in ADMIN_USER_IDS:
                # A. 發送標題
                requests.post(
                    LINE_PUSH_URL, headers=LINE_HEADERS,
                    json={
                        "to": admin_id,
                        "messages": [{"type": "text", "text": "Soquick散客EZWay需提醒以下寄件人："}]
                    }
                )
                # B. 發送寄件人名單
                for s in sorted(senders):
                    requests.post(
                        LINE_PUSH_URL, headers=LINE_HEADERS,
                        json={"to": admin_id, "messages": [{"type": "text", "text": s}]}
                    )


def handle_missing_confirm(event: Dict[str, Any]) -> None:
    """
    處理包含「申報相符」關鍵字的訊息：逐行抓 ACE/250N 單號，判斷姓名屬於 Vicky 或 Yumi，
    分別推播到對應群組。
    """
    text = event["message"]["text"]

    # 如果這是原始 EZ-Way 通知，就跳過
    if "收到EZ way通知後" in text:
        return

    # 允許「申報相符」或「還沒按」作為觸發關鍵字
    if not ("申報相符" in text or "還沒按" in text):
        return

    # 準備名單收集器
    vicky_found = []
    yumi_found = []
    iris_found = []
    unknown_found = []  # 新增：收集不在任何名單的人

    # 逐行找 ACE/250N 單號
    for l in text.splitlines():
        if CODE_TRIGGER_RE.search(l):
            parts = re.split(r"\s+", l.strip())
            # 確保至少有三段：單號、姓名、電話（原碼只用到姓名）
            if len(parts) < 2:
                continue
            name = parts[1].strip()
            log.info(f"[Debug] Checking name: '{name}' against Vicky/Yumi lists") # 加入這一行

            # 分類收集名字
            if name in VICKY_NAMES:
                vicky_found.append(name)
            elif name in YUMI_NAMES:
                yumi_found.append(name)
            elif name in IRIS_NAMES:
                iris_found.append(name)
            else:
                # 不在任何名單中，記錄整行資訊
                unknown_found.append(l.strip())

    # 彙整發送函式
    def push_summary(group_id: str, names: List[str]):
        if not names:
            return
        # 將名字用換行串接，並加上結尾語
        # 修改後的版本
        summary_text = "您好，以下申報人尚未按申報相符，再麻煩通知：\n" + "\n".join(names)
        requests.post(
            LINE_PUSH_URL,
            headers=LINE_HEADERS,
            json={"to": group_id, "messages": [{"type": "text", "text": summary_text}]}
        )

    # 分別發送各群組推播
    push_summary(VICKY_GROUP_ID, vicky_found)
    push_summary(YUMI_GROUP_ID, yumi_found)
    push_summary(IRIS_GROUP_ID, iris_found)
    
    # 新增：查詢 ACE 試算表找出寄件人，並通知管理員
    if unknown_found:
        log.info(f"[Missing Confirm] Found unknown declarers: {unknown_found}")
        # 提取姓名（第二個欄位）
        declarer_names = set()
        for line in unknown_found:
            parts = re.split(r"\s+", line.strip())
            if len(parts) >= 2:
                declarer_names.add(parts[1].strip())
        
        log.info(f"[Missing Confirm] Extracted declarer names: {declarer_names}")
        
        if declarer_names:
            # 查詢 ACE 試算表
            ACE_SHEET_URL = __import__("os").getenv("ACE_SHEET_URL")
            if not ACE_SHEET_URL:
                log.error("[Missing Confirm] ACE_SHEET_URL not set")
                return
            
            gs = get_gspread_client()
            sheet = gs.open_by_url(ACE_SHEET_URL).sheet1
            data = sheet.get_all_values()
            
            log.info(f"[Missing Confirm] ACE sheet has {len(data)} rows")
            
            # 找出最接近今天的日期
            today = datetime.now(timezone.utc).date()
            closest_date = None
            closest_diff = timedelta(days=9999)
            
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
            
            if closest_date:
                log.info(f"[Missing Confirm] Closest date found: {closest_date}")
                # 查找對應的寄件人
                senders = set()
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
                    
                    # Column G (index 6) is declarer
                    declarer = (row[6] if len(row) > 6 else "").strip()
                    if declarer in declarer_names:
                        # Column C (index 2) is sender
                        sender = (row[2] if len(row) > 2 else "").strip()
                        log.info(f"[Missing Confirm] Row {row_idx}: declarer={declarer}, sender={sender}")
                        if sender and sender not in (VICKY_NAMES | YUMI_NAMES | IRIS_NAMES | EXCLUDED_SENDERS):
                            senders.add(sender)
                
                log.info(f"[Missing Confirm] Found senders to notify: {senders}")
                # 發送給所有管理員
                if senders:
                    for admin_id in ADMIN_USER_IDS:
                        requests.post(
                            LINE_PUSH_URL,
                            headers=LINE_HEADERS,
                            json={
                                "to": admin_id,
                                "messages": [{"type": "text", "text": "以下寄件人的收件人尚未按申報相符，再麻煩通知："}]
                            }
                        )
                        for sender in sorted(senders):
                            requests.post(
                                LINE_PUSH_URL,
                                headers=LINE_HEADERS,
                                json={"to": admin_id, "messages": [{"type": "text", "text": sender}]}
                            )
                else:
                    log.info("[Missing Confirm] No senders found or all senders are in excluded lists")


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
    iris_batch  = [c for c in cleaned if any(name in c for name in IRIS_NAMES)]

    # extract just the name token (first word) from each cleaned line
    names_only = [c.split()[0] for c in cleaned]

    # “others” = those whose name token isn’t in any of the three lists
    other_batch = [
        cleaned[i] for i, nm in enumerate(names_only)
        if nm not in VICKY_NAMES
        and nm not in YUMI_NAMES
        and nm not in IRIS_NAMES
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
    push_to(IRIS_GROUP_ID, iris_batch)
    # also push any “other” entries to your personal chat 改為迴圈發送給所有管理員
    if other_batch:
        for admin_id in ADMIN_USER_IDS:
            push_to(admin_id, other_batch)


def handle_ace_shipments(event: Dict[str, Any]) -> None:
    """
    解析 ACE 文字。若收件人不在名單內，則去 ACE 試算表反查寄件人，
    並將結果私訊給管理員。
    """
    # 獲取發送者 ID 並查詢暱稱 (新增)
    user_id = event["source"]["userId"]
    try:
        profile = line_bot_api.get_profile(user_id)
        sender_name = profile.display_name
    except Exception:
        sender_name = "未知發送者"

    # 原有的清理與分割邏輯
    raw = event["message"]["text"]
    text = raw.replace('"', '').strip()                     # strip stray quotes

    # split into shipment‐blocks
    parts = re.split(r'(?=出貨單號:)', text)

    vicky: List[str] = []
    yumi: List[str] = []
    iris: List[str] = []
    fallback_map: Dict[str, List[str]] = {}

    for blk in parts:
        if "出貨單號:" not in blk or "宅配單號:" not in blk:
            continue
        lines = [l.strip() for l in blk.strip().splitlines() if l.strip()]
        if len(lines) < 4:
            continue
        
        # recipient name is on line 3
        recipient = lines[2].split()[0]
        full_msg  = "\n".join(lines[1:])

        if recipient in VICKY_NAMES:
            vicky.append(full_msg)
        elif recipient in YUMI_NAMES:
            yumi.append(full_msg)
        elif recipient in IRIS_NAMES:
            iris.append(full_msg)
        else:
            # 收集需要反查的收件人與對應內容
            if recipient not in fallback_map:
                fallback_map[recipient] = []
            fallback_map[recipient].append(full_msg)

    # 定義群組發送函式
    def push(group: str, messages: List[str]) -> None:
        if not messages:
            return
        payload = {
            "to": group,
            "messages": [{"type": "text", "text": "\n\n".join(messages)}]
        }
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log.info(f"Sent {len(messages)} ACE blocks to {group}: {resp.status_code}")

    push(VICKY_GROUP_ID, vicky)
    push(YUMI_GROUP_ID, yumi)
    push(IRIS_GROUP_ID, iris)

    #--- 3. ACE 試算表反查與按寄件人分組發送 ---
    sender_to_bundles: Dict[str, List[str]] = {} # 儲存 寄件人 -> [單號1, 單號2...]
    unmapped_blocks: List[str] = []

    if fallback_map:
        from utils.permissions import ADMIN_USER_IDS
        from config import ACE_SHEET_URL, EXCLUDED_SENDERS
        
        try:
            gs = get_gspread_client()
            ss = gs.open_by_url(ACE_SHEET_URL)
            target_date = datetime.fromtimestamp(event["timestamp"]/1000, tz=timezone(timedelta(hours=8))).strftime("%y%m%d")
            
            worksheets = ss.worksheets()
            ws = next((w for w in worksheets if w.title == target_date), worksheets[0]) 
            rows = ws.get_all_values()[1:] 

            matched_recipients = set()
            for row in rows:
                sheet_recipient = row[6].strip() if len(row) > 6 else ""
                # 確保每個收件人只會被 extend 一次
                if sheet_recipient in fallback_map and sheet_recipient not in matched_recipients:
                    sender = row[2].strip() if len(row) > 2 else "未知寄件人"
                    if sender and sender not in (VICKY_NAMES | YUMI_NAMES | IRIS_NAMES | EXCLUDED_SENDERS):
                        if sender not in sender_to_bundles:
                            sender_to_bundles[sender] = []
                        sender_to_bundles[sender].extend(fallback_map[sheet_recipient])
                        matched_recipients.add(sheet_recipient)
            
            for rec, msgs in fallback_map.items():
                if rec not in matched_recipients:
                    unmapped_blocks.extend(msgs)
        except Exception as e:
            log.error(f"Fallback sheet lookup failed: {e}")
            for msgs in fallback_map.values(): unmapped_blocks.extend(msgs)

        # --- 4. 分條發送給所有管理員 ---
        for admin_id in ADMIN_USER_IDS:
            # 開頭標題
            line_bot_api.push_message(admin_id, TextSendMessage(text=f"ACE Fallback收件人："))
            
            for s_name, blocks in sender_to_bundles.items():
                # 訊息1：寄件人名字 (獨立一條)
                line_bot_api.push_message(admin_id, TextSendMessage(text=s_name))
                # 訊息2：打包後的該寄件人所有單號
                line_bot_api.push_message(admin_id, TextSendMessage(text="\n\n".join(blocks)))

            if unmapped_blocks:
                line_bot_api.push_message(admin_id, TextSendMessage(text="以下為查無 ACE 試算表寄件人之單號："))
                line_bot_api.push_message(admin_id, TextSendMessage(text="\n\n".join(unmapped_blocks)))


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
        declarer = (row[6] if len(row) > 6 else "").strip()
        if not declarer or declarer not in declarer_names:
            continue

        # Column C is at index 2 in 'row' → this is the “sender” we want to notify
        sender = (row[2] if len(row) > 2 else "").strip()
        if not sender:
            continue

        # Skip anyone already in VICKY_NAMES, YUMI_NAMES, or EXCLUDED_SENDERS
        if sender in VICKY_NAMES or sender in YUMI_NAMES or sender in IRIS_NAMES or sender in EXCLUDED_SENDERS:
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

def dispatch_confirmation_notification(event, text, user_id):
    """
    專門處理申報相符通知的分流邏輯
    """
    has_code = CODE_TRIGGER_RE.search(text)
    
    # 1. Danny 的判定：必須是 Danny 發送 + 包含「還沒按」 + 包含單號
    if user_id == DANNY_USER_ID and "還沒按" in text and has_code:
        log.info(f"[Danny Trigger] Auto-processing re-notification: {text[:20]}...")
        handle_missing_confirm(event) # 呼叫原本的處理邏輯
        return True

    # 2. 管理員 (Yves/Gorsky) 的判定：手動輸入「申報相符」或「還沒按」
    is_admin = user_id in ADMIN_USER_IDS
    if is_admin and ("申報相符" in text or "還沒按" in text) and has_code:
        log.info(f"[Admin Trigger] Processing confirmation request: {text[:20]}...")
        handle_missing_confirm(event)
        return True

    return False
