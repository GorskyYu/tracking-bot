import os
import json
import re
import time
import random
import requests
import logging
from datetime import datetime

log = logging.getLogger(__name__)

class MondaySyncService:
    def __init__(self, api_token, gspread_client_func, line_push_func):
        """
        初始化 Monday 同步服務，傳入必要的 Token 與工具函式
        """
        self.api_url = "https://api.monday.com/v2"
        self.api_token = api_token
        self.headers = {"Authorization": api_token, "Content-Type": "application/json"}
        self.get_gspread = gspread_client_func
        self.line_push = line_push_func
        self.sheet_id = "1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q" [cite: 10]
        self.line_status_group = "C1f77f5ef1fe48f4782574df449eac0cf" [cite: 14]

    def _post_with_backoff(self, url, payload=None, headers=None, files=None, max_tries=5, timeout=12):
        """完全復刻原版的指數退避請求邏輯 [cite: 3, 4, 5, 6]"""
        t = 0.8
        last_exc = None
        current_headers = headers or self.headers
        for _ in range(max_tries):
            try:
                if files is not None:
                    # 檔案上傳時，requests 會自動處理 boundary，因此不應手動設定 JSON Content-Type [cite: 4]
                    return requests.post(url, headers=current_headers, data=payload, files=files, timeout=timeout)
                else:
                    return requests.post(url, headers=current_headers, json=payload, timeout=timeout)
            except requests.RequestException as e:
                last_exc = e
                time.sleep(t + random.uniform(0, 0.5)) [cite: 5]
                t = min(t * 2, 8) [cite: 5]
        if last_exc:
            raise last_exc [cite: 6]

    def _adjust_caps(self, s: str) -> str:
        """完全復刻原版的大小寫轉換邏輯 [cite: 6, 7]"""
        if not isinstance(s, str):
            return ""
        if s.isupper():
            parts = []
            for w in s.split():
                parts.append("-".join(p.capitalize() for p in w.split("-")))
            return " ".join(parts) [cite: 7]
        return s

    def _sync_to_google_sheet(self, ref_no, tracking_numbers):
        """完全復刻原版 Google Sheet 功能 (含高亮與報錯通知) [cite: 10-15]"""
        try:
            gs = self.get_gspread()
            ss = gs.open_by_key(self.sheet_id) [cite: 10]
            ws = ss.worksheet("Tracking")

            values = ws.col_values(1) [cite: 10]
            row_idx = next((i for i, v in enumerate(values, start=1) if (v or "").strip() == ref_no), None) [cite: 11]

            if not row_idx:
                log.warning(f"[GSHEET] '{ref_no}' not found in A:A. Skip sheet write.") [cite: 11, 12]
                return

            # 填入最多 3 筆追蹤碼到 S, T, U 欄 [cite: 12, 13]
            for i, tn in enumerate(tracking_numbers[:3], start=1):
                ws.update_cell(row_idx, 18 + i, tn) # 19=S, 20=T, 21=U [cite: 13]
            
            # 高亮 F 欄 (ABB 會員) [cite: 13, 14]
            cell_f = f"F{row_idx}"
            fmt = {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}
            ws.format(cell_f, fmt) [cite: 14]
            
            log.info(f"[GSHEET] Row {row_idx} updated & highlighted.")
            self.line_push(self.line_status_group, "[PDF→空運表單]已同步到Tracking Tab") [cite: 14]

        except Exception as sheet_err:
            log.error(f"[GSHEET] Sync error: {sheet_err}") [cite: 15]
            self.line_push(self.line_status_group, f"⚠️ Sheet 同步失敗: {str(sheet_err)}") [cite: 15]

    def run_sync(self, full_data, pdf_bytes, original_filename):
        """
        整合所有步驟的公開入口方法 [cite: 1]
        """
        try:
            # 1. 處理參考編號 [cite: 8, 9]
            ref_no = (full_data.get("reference_number") or "").strip()
            if ref_no and "-" in ref_no and len(ref_no) > 19:
                ref_no = ref_no.rsplit('-', 1)[0] [cite: 9]
            
            # 2. 同步 Google Sheet
            all_tracking_numbers = full_data.get("all_tracking_numbers", []) or []
            self._sync_to_google_sheet(ref_no, all_tracking_numbers)

            # 3. 處理名稱與代理人判定 [cite: 15, 16]
            sender = full_data.get("sender", {}) or {}
            receiver = full_data.get("receiver", {}) or {}
            name = (sender.get("name") or "").strip() [cite: 8]
            client_id = (sender.get("client_id") or "").strip() [cite: 8]
            
            temp = re.sub(r"\s*\((?:YUMI|VICKY)\)\s*", " ", name, flags=re.IGNORECASE) [cite: 15]
            raw_name = re.sub(r"\s+", " ", temp).strip() [cite: 15]
            adj_name = self._adjust_caps(raw_name)
            adj_client = self._adjust_caps(client_id)

            # 判定早期代購代理人 [cite: 16]
            if (("Yumi" in adj_name or "Shu-Yen" in adj_name) and "Liu" in adj_name):
                adj_name, adj_client = "Shu-Yen Liu", "Yumi"
            elif (("Vicky" in adj_name or "Chia-Chi" in adj_name) and "Ku" in adj_name):
                adj_name, adj_client = "Chia-Chi Ku", "Vicky"

            today = datetime.now().strftime("%Y%m%d") [cite: 8]
            parent_name = f"{today} {adj_client} - {adj_name}" [cite: 16]

            # 4. 尋找或建立 Monday 父項目 [cite: 17-20]
            find_parent_q = f"""
            query {{
              items_by_column_values(
                board_id: {os.getenv('AIR_PARENT_BOARD_ID')},
                column_id: "name",
                column_value: "{parent_name}"
              ) {{ id }}
            }}
            """
            r = self._post_with_backoff(self.api_url, {"query": find_parent_q})
            items = (r.json().get("data", {}) or {}).get("items_by_column_values", []) or []
            
            if items:
                parent_id = items[0]["id"] [cite: 18]
            else:
                create_parent_m = f"""
                mutation {{
                  create_item(
                    board_id: {os.getenv('AIR_PARENT_BOARD_ID')},
                    item_name: "{parent_name}"
                  ) {{ id }}
                }}
                """
                r2 = self._post_with_backoff(self.api_url, {"query": create_parent_m})
                parent_id = r2.json()["data"]["create_item"]["id"] [cite: 20]

            # 5. 建立更新並上傳 PDF [cite: 20-24]
            create_update_q = f'mutation {{ create_update(item_id: {parent_id}, body: "原始 PDF 檔案") {{ id }} }}'
            upd_resp = self._post_with_backoff(self.api_url, {"query": create_update_q})
            update_id = (upd_resp.json().get("data", {}) or {}).get("create_update", {}).get("id")

            if update_id:
                multipart_payload = {
                    "query": f'mutation ($file: File!) {{ add_file_to_update(update_id: {update_id}, file: $file) {{ id }} }}',
                    "map": json.dumps({"file": ["variables.file"]})
                }
                files = [("file", (original_filename, pdf_bytes, "application/pdf"))] [cite: 23]
                file_resp = self._post_with_backoff(f"{self.api_url}/file", payload=multipart_payload, 
                                              headers={"Authorization": self.api_token}, files=files)
                if file_resp.status_code != 200:
                    log.error(f"[PDF→Monday] attach PDF failed: {file_resp.status_code} {file_resp.text}") [cite: 24]

            # 6. 建立子項目與設定初始狀態 [cite: 24-35]
            postal = (receiver.get("postal_code") or "").replace(" ", "").upper() [cite: 28]
            for tn in all_tracking_numbers:
                create_sub_m = f'mutation {{ create_subitem(parent_item_id: {parent_id}, item_name: "{tn}") {{ id }} }}'
                resp_sub = self._post_with_backoff(self.api_url, {"query": create_sub_m})
                sub_id = resp_sub.json()["data"]["create_subitem"]["id"] [cite: 25]

                # 設定狀態為「收包裹」 [cite: 26, 27]
                mut_status = f"""
                mutation {{
                  change_column_value(
                    item_id: {sub_id},
                    board_id: {os.getenv('AIR_BOARD_ID')},
                    column_id: "status__1",
                    value: "{{\\"label\\":\\"收包裹\\"}}"
                  ) {{ id }}
                }}
                """
                self._post_with_backoff(self.api_url, {"query": mut_status})

                # 根據郵遞區號設定物流 [cite: 28-35]
                if postal.startswith("V6X1Z7"):
                    # 國際物流 → Ace & 台灣物流 → ACE大嘴鳥 [cite: 28-32]
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {os.getenv("AIR_BOARD_ID")}, column_id: "status_18__1", value: "{{\\"label\\":\\"Ace\\"}}") {{ id }} }}'})
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {os.getenv("AIR_BOARD_ID")}, column_id: "status_19__1", value: "{{\\"label\\":\\"ACE大嘴鳥\\"}}") {{ id }} }}'})
                elif postal.startswith("V6X0B9"):
                    # 國際物流 → SoQuick [cite: 33-35]
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {os.getenv("AIR_BOARD_ID")}, column_id: "status_18__1", value: "{{\\"label\\":\\"SoQuick\\"}}") {{ id }} }}'})

            # 7. 標記早期代購種類 [cite: 35-37]
            is_early = (("Yumi" in adj_name or "Shu-Yen" in adj_name) and "Liu" in adj_name) or (("Vicky" in adj_name or "Chia-Chi" in adj_name) and "Ku" in adj_name)
            if is_early:
                set_type_q = f"""
                mutation {{
                  change_column_value(
                    item_id: {parent_id},
                    board_id: {os.getenv('AIR_PARENT_BOARD_ID')},
                    column_id: "status_11__1",
                    value: "{{\\"label\\":\\"早期代購\\"}}"
                  ) {{ id }}
                }}
                """
                self._post_with_backoff(self.api_url, {"query": set_type_q}) [cite: 37]

            log.info(f"[PDF→Monday] Monday sync completed for {parent_name}")
            self.line_push(self.line_status_group, f"[PDF→Monday] Monday sync completed for {parent_name}")

        except Exception as e:
            log.error(f"[PDF→Monday] Monday sync failed: {e}", exc_info=True) [cite: 38]
            self.line_push(self.line_status_group, f"ERROR [PDF→Monday] {e}") [cite: 38]