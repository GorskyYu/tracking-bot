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
        self.sheet_id = "1BgmCA1DSotteYMZgAvYKiTRWEAfhoh7zK9oPaTTyt9Q"
        self.line_status_group = "C1f77f5ef1fe48f4782574df449eac0cf"
        self.domestic_expense_col = "numeric5__1" # <-- 請確認父板塊「加境內支出」的實際 ID

    def _post_with_backoff(self, url, payload=None, headers=None, files=None, max_tries=5, timeout=12):
        """完全復刻原版的指數退避請求邏輯"""
        t = 0.8
        last_exc = None
        current_headers = headers or self.headers
        for _ in range(max_tries):
            try:
                if files is not None:
                    # 檔案上傳時，requests 會自動處理 boundary，因此不應手動設定 JSON Content-Type
                    return requests.post(url, headers=current_headers, data=payload, files=files, timeout=timeout)
                else:
                    return requests.post(url, headers=current_headers, json=payload, timeout=timeout)
            except requests.RequestException as e:
                last_exc = e
                time.sleep(t + random.uniform(0, 0.5))
                t = min(t * 2, 8)
        if last_exc:
            raise last_exc

    def _adjust_caps(self, s: str) -> str:
        """完全復刻原版的大小寫轉換邏輯"""
        if not isinstance(s, str):
            return ""
        if s.isupper():
            parts = []
            for w in s.split():
                parts.append("-".join(p.capitalize() for p in w.split("-")))
            return " ".join(parts)
        return s

    def _sync_to_google_sheet(self, ref_no, tracking_numbers):
        """完全復刻原版 Google Sheet 功能 (含高亮與報錯通知)"""
        try:
            gs = self.get_gspread()
            ss = gs.open_by_key(self.sheet_id)
            ws = ss.worksheet("Tracking")

            values = ws.col_values(1)
            row_idx = next((i for i, v in enumerate(values, start=1) if (v or "").strip() == ref_no), None)

            if not row_idx:
                log.warning(f"[GSHEET] '{ref_no}' not found in A:A. Skip sheet write.")
                return

            # 填入最多 3 筆追蹤碼到 S, T, U 欄
            for i, tn in enumerate(tracking_numbers[:3], start=1):
                ws.update_cell(row_idx, 18 + i, tn) # 19=S, 20=T, 21=U
            
            # 高亮 F 欄 (ABB 會員)
            cell_f = f"F{row_idx}"
            fmt = {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}
            ws.format(cell_f, fmt)
            
            log.info(f"[GSHEET] Row {row_idx} updated & highlighted.")
            self.line_push(self.line_status_group, "[PDF→空運表單]已同步到Tracking Tab")

        except Exception as sheet_err:
            log.error(f"[GSHEET] Sync error: {sheet_err}")
            self.line_push(self.line_status_group, f"⚠️ Sheet 同步失敗: {str(sheet_err)}")

    def run_sync(self, full_data, pdf_bytes, original_filename, redis_client, group_id):
        """
        整合所有步驟的公開入口方法 - 已整合海運判定、加拿大散客標籤與環境變數
        """
        try:
            # 1. 處理參考編號
            ref_no = (full_data.get("reference_number") or "").strip()
            if ref_no and "-" in ref_no and len(ref_no) > 19:
                ref_no = ref_no.rsplit('-', 1)[0]
            
            # 2. 同步 Google Sheet
            all_tracking_numbers = full_data.get("all_tracking_numbers", []) or []
            self._sync_to_google_sheet(ref_no, all_tracking_numbers)

            # 3. 處理名稱與代理人判定
            sender = full_data.get("sender", {}) or {}
            receiver = full_data.get("receiver", {}) or {}
            name = (sender.get("name") or "").strip()
            client_id = (sender.get("client_id") or "").strip()
            
            temp = re.sub(r"\s*\((?:YUMI|VICKY)\)\s*", " ", name, flags=re.IGNORECASE)
            raw_name = re.sub(r"\s+", " ", temp).strip()
            adj_name = self._adjust_caps(raw_name)
            adj_client = self._adjust_caps(client_id)

            # 判定早期代購代理人
            if (("Yumi" in adj_name or "Shu-Yen" in adj_name) and "Liu" in adj_name):
                adj_name, adj_client = "Shu-Yen Liu", "Yumi"
            elif (("Vicky" in adj_name or "Chia-Chi" in adj_name) and "Ku" in adj_name):
                adj_name, adj_client = "Chia-Chi Ku", "Vicky"

            # --- 🟢 海運邏輯判定 (使用 Heroku 環境變數) ---
            is_sea = adj_client.lower().endswith(" sea")
            if is_sea:
                target_parent_board_id = os.getenv('SEA_PARENT_BOARD_ID')
                target_subitem_board_id = os.getenv('SEA_BOARD_ID')
            else:
                target_parent_board_id = os.getenv('AIR_PARENT_BOARD_ID')
                target_subitem_board_id = os.getenv('AIR_BOARD_ID')

            today = datetime.now().strftime("%Y%m%d")
            parent_name = f"{today} {adj_client} - {adj_name}"

            # 4. 尋找或建立 Monday 父項目
            find_parent_q = f"""
            query {{
              items_by_column_values(
                board_id: {target_parent_board_id},
                column_id: "name",
                column_value: "{parent_name}"
              ) {{ id }}
            }}
            """
            r = self._post_with_backoff(self.api_url, {"query": find_parent_q})
            items = (r.json().get("data", {}) or {}).get("items_by_column_values", []) or []
            
            if items:
                parent_id = items[0]["id"]
            else:
                create_parent_m = f"""
                mutation {{
                  create_item(
                    board_id: {target_parent_board_id},
                    item_name: "{parent_name}"
                  ) {{ id }}
                }}
                """
                r2 = self._post_with_backoff(self.api_url, {"query": create_parent_m})
                parent_id = r2.json()["data"]["create_item"]["id"]

            # 5. 建立更新並上傳 PDF
            create_update_q = f'mutation {{ create_update(item_id: {parent_id}, body: "原始 PDF 檔案") {{ id }} }}'
            upd_resp = self._post_with_backoff(self.api_url, {"query": create_update_q})
            update_id = (upd_resp.json().get("data", {}) or {}).get("create_update", {}).get("id")

            if update_id:
                multipart_payload = {
                    "query": f'mutation ($file: File!) {{ add_file_to_update(update_id: {update_id}, file: $file) {{ id }} }}',
                    "map": json.dumps({"file": ["variables.file"]})
                }
                files = [("file", (original_filename, pdf_bytes, "application/pdf"))]
                file_resp = self._post_with_backoff(f"{self.api_url}/file", payload=multipart_payload, 
                                              headers={"Authorization": self.api_token}, files=files)
                if file_resp.status_code != 200:
                    log.error(f"[PDF→Monday] attach PDF failed: {file_resp.status_code} {file_resp.text}")

            # 6. 建立子項目與設定初始狀態
            postal = (receiver.get("postal_code") or "").replace(" ", "").upper()
            for tn in all_tracking_numbers:
                create_sub_m = f'mutation {{ create_subitem(parent_item_id: {parent_id}, item_name: "{tn}") {{ id }} }}'
                resp_sub = self._post_with_backoff(self.api_url, {"query": create_sub_m})
                sub_id = resp_sub.json()["data"]["create_subitem"]["id"]

                # 設定狀態為「收包裹」
                mut_status = f"""
                mutation {{
                  change_column_value(
                    item_id: {sub_id},
                    board_id: {target_subitem_board_id},
                    column_id: "status__1",
                    value: "{{\\"label\\":\\"收包裹\\"}}"
                  ) {{ id }}
                }}
                """
                self._post_with_backoff(self.api_url, {"query": mut_status})

                # 根據郵遞區號設定物流
                if postal.startswith("V6X1Z7"):
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {target_subitem_board_id}, column_id: "status_18__1", value: "{{\\"label\\":\\"Ace\\"}}") {{ id }} }}'})
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {target_subitem_board_id}, column_id: "status_19__1", value: "{{\\"label\\":\\"ACE大嘴鳥\\"}}") {{ id }} }}'})
                elif postal.startswith("V6X0B9"):
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {target_subitem_board_id}, column_id: "status_18__1", value: "{{\\"label\\":\\"SoQuick\\"}}") {{ id }} }}'})

            # --- 7. 🟢 客人種類分類 (早期代購 vs 加拿大散客) ---
            is_early = (adj_name == "Shu-Yen Liu" and adj_client == "Yumi") or \
                       (adj_name == "Chia-Chi Ku" and adj_client == "Vicky")
            
            guest_label = "早期代購" if is_early else "加拿大散客"
            
            set_type_q = f"""
            mutation {{
              change_column_value(
                item_id: {parent_id},
                board_id: {target_parent_board_id},
                column_id: "status_11__1",
                value: "{{\\"label\\":\\"{guest_label}\\"}}"
              ) {{ id }}
            }}
            """
            self._post_with_backoff(self.api_url, {"query": set_type_q})

            log.info(f"[PDF→Monday] Monday sync completed for {parent_name}")
            # 同時存入項目 ID 與板塊 ID，用直線 | 隔開
            redis_client.set("global_last_pdf_parent", f"{parent_id}|{target_parent_board_id}", ex=600)
            self.line_push(self.line_status_group, f"[PDF→Monday] Monday sync completed for {parent_name}")

        except Exception as e:
            log.error(f"[PDF→Monday] Monday sync failed: {e}", exc_info=True)
            self.line_push(self.line_status_group, f"ERROR [PDF→Monday] {e}")
            
    # 修正：參數增加 board_id
    def update_domestic_expense(self, parent_id, amount, group_id, board_id):
        """檢查並錄入境內支出金額"""
        # 1. 查詢該項目的名稱與境內支出
        query = f'''
        query {{
          items (ids: [{parent_id}]) {{
            name
            column_values(ids: ["{self.domestic_expense_col}"]) {{
              text
            }}
          }}
        }}'''
        try:
            r = self._post_with_backoff(self.api_url, {"query": query})
            res = r.json().get("data", {}).get("items", [])
            if not res: return False, "找不到項目", ""

            item_name = res[0].get("name", "Unknown Item")
            
            # 安全檢查：確保 column_values 存在
            cols = res[0].get("column_values", [])
            current_val = cols[0].get("text", "") if cols else ""
            
            if current_val and current_val.strip():
                return False, f"欄位已有數值 ({current_val})", item_name

            # 2. 執行更新 (使用傳入的 board_id)
            mutation = f'''
            mutation {{
              change_simple_column_value(
                item_id: {parent_id},
                board_id: {board_id},
                column_id: "{self.domestic_expense_col}",
                value: "{amount}"
              ) {{ id }}
            }}'''
            self._post_with_backoff(self.api_url, {"query": mutation})
            return True, "成功", item_name # 回傳名稱
        except Exception as e:
            log.error(f"[EXPENSE] Update failed: {str(e)}")
            return False, str(e), ""

    def change_simple_column_value(self, board_id, item_id, column_id, value):
        query = """
        mutation ($board_id: ID!, $item_id: ID!, $column_id: String!, $value: String!) {
            change_simple_column_value (board_id: $board_id, item_id: $item_id, column_id: $column_id, value: $value) {
                id
            }
        }
        """
        variables = {
            "board_id": int(board_id),
            "item_id": int(item_id),
            "column_id": column_id,
            "value": str(value)
        }
        return self._post_with_backoff(self.api_url, {"query": query, "variables": variables})

    def change_multiple_column_values(self, board_id, item_id, column_values):
        query = """
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values (board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        """
        variables = {
            "board_id": int(board_id),
            "item_id": int(item_id),
            "column_values": json.dumps(column_values)
        }
        return self._post_with_backoff(self.api_url, {"query": query, "variables": variables})