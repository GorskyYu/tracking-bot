import os
import re
import logging
import requests
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date

log = logging.getLogger(__name__)

class ShipmentParserService:
    def __init__(self, config, gspread_client_func, line_push_func):
        self.cfg = config
        self.get_gspread = gspread_client_func
        self.line_push = line_push_func
        self.line_push_url = "https://api.line.me/v2/bot/message/push"
        self.line_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.getenv('LINE_TOKEN')}"
        }

    def _safe_line_push(self, to, text):
        """內部的推送工具"""
        if not to or not text: return
        try:
            payload = {"to": to, "messages": [{"type": "text", "text": text}]}
            requests.post(self.line_push_url, headers=self.line_headers, json=payload, timeout=10)
        except Exception as e:
            log.error(f"[Parser Push] failed: {e}")

    def handle_missing_confirm(self, event):
        """處理「申報相符」提醒與散客 Fallback 邏輯"""
        text = event["message"]["text"]
        if "申報相符" not in text:
            return

        # 如果是排程訊息，則不發送群組訊息
        is_schedule = "週四出貨" in text or "週日出貨" in text

        bundled_names = {
            self.cfg['VICKY_GROUP_ID']: [],
            self.cfg['YUMI_GROUP_ID']: [],
            self.cfg['IRIS_GROUP_ID']: []
        }

        # 所有出現在訊息中的姓名都要查表單，以便找出寄件人
        all_extracted_items = []  # List of (box_id, name) tuples

        # 1. 掃描文字並分流
        for l in text.splitlines():
            if self.cfg['CODE_TRIGGER_RE'].search(l):
                parts = re.split(r"\s+", l.strip())
                if len(parts) < 2: continue
                box_id = parts[0]  # Box ID like ACE260122YL03
                name = parts[1]
                
                if name in self.cfg['VICKY_NAMES']:
                    bundled_names[self.cfg['VICKY_GROUP_ID']].append(name)
                elif name in self.cfg['YUMI_NAMES']:
                    bundled_names[self.cfg['YUMI_GROUP_ID']].append(name)
                elif name in self.cfg['IRIS_NAMES']:
                    bundled_names[self.cfg['IRIS_GROUP_ID']].append(name)
                else:
                    # 只有不在上述清單的人，才需要去表單查 Sender
                    all_extracted_items.append((box_id, name))

        # 2. 推送給各負責人群組
        for target_id, names in bundled_names.items():
            if not names or is_schedule: continue  # 如果是排程訊息，就跳過這裡的推送
            unique_names = sorted(list(set(names)))
            msg = "您好，以下申報人還沒有按申報相符：\n\n" + "\n".join(unique_names)
            self._safe_line_push(target_id, msg)

        # 3. 處理散客 Fallback 包含 Bundling 功能)
        if all_extracted_items:
            try:
                gs = self.get_gspread()
                ss = gs.open_by_url(self.cfg['ACE_SHEET_URL'])
                ws = ss.sheet1
                all_rows = ws.get_all_values()
                
                sender_groups = defaultdict(list)
                found_items = set()  # Set of (box_id, name) tuples

                for box_id, name in all_extracted_items:
                    for row in reversed(all_rows):
                        row_box_id = row[1].strip() if len(row) > 1 else ""
                        row_name = row[6].strip() if len(row) > 6 else ""
                        if row_box_id == box_id and row_name == name:
                            sender = row[2].strip()
                            phone = row[7].strip() if len(row) > 7 else ""
                            sender_groups[sender].append(f"{name} {phone}")
                            found_items.add((box_id, name)) # 標記為已找到
                            break
                
                # 處理時間邏輯，確保與排程訊息一致
                is_sunday = "週日出貨" in text
                ship_day = "週日出貨" if is_sunday else ("週四出貨" if "週四" in text else "近期出貨")
                timing_note = "週一" if is_sunday else "週五" # 週日出貨對應週一，週四對應週五

                # 發送已 Bundle 的寄件人通知
                for sender, declarants in sender_groups.items():
                    # 排除掉負責人自己，只轉發需要的通知
                    if sender in self.cfg.get('EXCLUDED_SENDERS', []): continue

                    declarant_list = "\n".join(declarants)
                    bundled_msg = (
                        f"{ship_day}\n\n麻煩請 \n\n{declarant_list}\n\n"
                        f"收到EZ way通知後 請按申報相符 海關才能受理清關\n\n"
                        f"**須按申報相符者 EZ Way 會提前提傳輸\n\n"
                        f"台灣時間周五 傍晚至晚上 就可以開始按申報相符**"
                    )
                    
                    # 推送給管理員
                    for admin_id in [self.cfg['YVES_USER_ID'], self.cfg['GORSKY_USER_ID']]:
                        if admin_id:
                            self.line_push(admin_id, sender)
                            self.line_push(admin_id, bundled_msg)
                            
                # 新增：若有姓名不在表單內，仍發送給 Yves 避免漏掉
                unfound = [name for box_id, name in all_extracted_items if (box_id, name) not in found_items]
                if unfound:
                    unfound_msg = f"{ship_day} (表單無資料)：\n\n" + "\n".join(unfound)
                    self._safe_line_push(self.cfg['YVES_USER_ID'], unfound_msg)

            except Exception as e:
                log.error(f"[FALLBACK ERROR] {e}", exc_info=True)


    def handle_ace_schedule(self, event):
        """處理 Ace 出貨排程通知"""
        text = event["message"]["text"]
        lines = text.splitlines()

        try:
            idx_m = next(i for i, l in enumerate(lines) if "麻煩請" in l)
            idx_r = next(i for i, l in enumerate(lines) if l.startswith("收到EZ way通知後"))
        except StopIteration:
            return

        header = lines[:idx_m+1]
        footer = lines[idx_r:]
        code_lines = [l for l in lines if self.cfg['CODE_TRIGGER_RE'].search(l)]
        cleaned = [self.cfg['CODE_TRIGGER_RE'].sub("", l).strip().strip('"') for l in code_lines]

        vicky_batch = [c for c in cleaned if any(name in c for name in self.cfg['VICKY_NAMES'])]
        yumi_batch  = [c for c in cleaned if any(name in c for name in self.cfg['YUMI_NAMES'])]
        iris_batch = [c for c in cleaned if any(name in c for name in self.cfg['IRIS_NAMES'])]
        
        names_only = [c.split()[0] for c in cleaned]
        other_batch = [cleaned[i] for i, nm in enumerate(names_only) 
                      if nm not in self.cfg['VICKY_NAMES'] 
                      and nm not in self.cfg['YUMI_NAMES'] 
                      and nm not in self.cfg['IRIS_NAMES']
                      and nm not in self.cfg.get('YVES_NAMES', [])]

        def push_to(group, batch):
            if not batch: return
            msg_lines = header + [""] + batch + [""] + footer
            self._safe_line_push(group, "\n".join(msg_lines))
        
        push_to(self.cfg['VICKY_GROUP_ID'], vicky_batch)
        push_to(self.cfg['YUMI_GROUP_ID'], yumi_batch)
        push_to(self.cfg['IRIS_GROUP_ID'], iris_batch)

    def handle_soquick_full_notification(self, event):
        """處理 Soquick 全體通知邏輯"""
        text = event["message"]["text"]
        if not ("您好，請" in text and "按" in text and "申報相符" in text):
            return

        lines = [l.strip() for l in text.splitlines() if l.strip()]
        try:
            footer_idx = next(i for i, l in enumerate(lines) if "您好，請" in l)
        except StopIteration:
            footer_idx = len(lines)
            
        recipients = lines[:footer_idx]
        vicky_batch = sorted(list(set([r for r in recipients if r in self.cfg['VICKY_NAMES']])))
        yumi_batch  = sorted(list(set([r for r in recipients if r in self.cfg['YUMI_NAMES']])))
        
        def push_group(group, batch):
            if not batch: return
            msg = "\n".join(batch) + "\n\n您好，請提醒以上認證人按申報相符"
            self._safe_line_push(group, msg)

        push_group(self.cfg['VICKY_GROUP_ID'], vicky_batch)
        push_group(self.cfg['YUMI_GROUP_ID'], yumi_batch)