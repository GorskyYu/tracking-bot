import os
import json
import re
import time
import random
import requests
import logging
from datetime import datetime

log = logging.getLogger(__name__)

# 定義倉庫的郵遞區號 (移除空白，統一格式)
WAREHOUSE_ZIPS = {
    "V6Y0E3", # Ruichao 702-6733 & 6733 Buswell
    "V6Y1K3", # 185-9040 Blundell
    "V6X1Z7", # 158-11782 River Rd
    "V6X0B9"  # 1025-2633 Simpson
}

# 定義倉庫收件人關鍵字 (Backup Logic)
WAREHOUSE_NAMES = {"yves", "richard", "tom gorsky", "y&g"}

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
            # Defensive strip: remove trailing -1/-2/etc. in case OCR didn't clean it
            ref_no = re.sub(r'-\d+$', '', ref_no).strip()

            gs = self.get_gspread()
            ss = gs.open_by_key(self.sheet_id)
            ws = ss.worksheet("Tracking")

            values = ws.col_values(1)
            row_idx = next((i for i, v in enumerate(values, start=1) if (v or "").strip() == ref_no), None)

            if not row_idx:
                log.warning(f"[GSHEET] '{ref_no}' not found in A:A. Skip sheet write.")
                tracking_str = ", ".join(tracking_numbers) if tracking_numbers else "無單號"
                self.line_push(
                    self.line_status_group,
                    f"⚠️ [PDF→空運表單] 未找到對應 REF: {ref_no}\n🏷 單號: {tracking_str}\n💡 請手動更新 Tracking tab"
                )
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

    def _route_by_timestamp(self, ref_no: str) -> str:
        """
        Given a ref_no that is a form-submission timestamp (e.g. "2026-04-05 07:15:40"),
        search col A of Form Responses 1 across three sheets to determine routing.

        Search order: 空運資料表 → 海運資料表 → 境內資料表

        Returns: "air", "sea", "domestic", or "not_found"
        """
        if not ref_no:
            return "not_found"

        from handlers.upload_data_handler import (
            AIR_FORM_SHEET_ID,
            OCEAN_FORM_SHEET_ID,
            DOMESTIC_FORM_SHEET_ID,
        )

        def _check_sheet(sheet_id: str) -> bool:
            try:
                gs = self.get_gspread()
                ss = gs.open_by_key(sheet_id)
                ws = ss.worksheet("Form Responses 1")
                col_a = ws.col_values(1)
                return ref_no in col_a
            except Exception as e:
                log.error(f"[RouteByTS] Error checking sheet {sheet_id}: {e}")
                return False

        if _check_sheet(AIR_FORM_SHEET_ID):
            return "air"
        if _check_sheet(OCEAN_FORM_SHEET_ID):
            return "sea"
        if _check_sheet(DOMESTIC_FORM_SHEET_ID):
            return "domestic"
        return "not_found"

    def run_sync(self, full_data, pdf_bytes, original_filename, redis_client, group_id):
        """
        整合所有步驟的公開入口方法 - 已整合海運判定、加拿大散客標籤、加境內直寄與環境變數
        """
        try:
            # 1. 處理參考編號
            ref_no = (full_data.get("reference_number") or "").strip()
            if ref_no and "-" in ref_no and len(ref_no) > 19:
                ref_no = ref_no.rsplit('-', 1)[0]
            
            # 2. 提取追蹤號碼 (Google Sheet 同步移至 Monday 建立後)
            all_tracking_numbers = full_data.get("all_tracking_numbers", []) or []

            # 3. 處理名稱與代理人判定 (含 混合式邏輯判定)
            _is_karl_lagerfeld = False  # 追蹤是否為 Karl Lagerfeld 來源
            sender = full_data.get("sender", {}) or {}
            receiver = full_data.get("receiver", {}) or {}
            
            # --- 🟢 自動費率判定 (Auto-Rate Logic) ---
            # 檢查寄件人與收件人是否符合特定規則，若符合則後續只需輸入成本即可
            s_name = (sender.get("name") or "").upper().replace(" ", "")
            s_addr = (sender.get("address") or "").upper().replace(" ", "")
            r_name = (receiver.get("name") or "").upper().replace(" ", "")
            r_addr = (receiver.get("address") or "").upper().replace(" ", "")
            r_zip = (receiver.get("postal_code") or "").upper().replace(" ", "")

            is_vicky_sender = "VICKY" in s_name and "T1W0L4" in s_addr
            is_yumi_sender = "YUMI" in s_name and "L6B1R2" in s_addr
            valid_sender = is_vicky_sender or is_yumi_sender

            is_yves_recv = "YVES" in r_name and "V6X1Z7" in r_zip
            is_richard_recv_1 = "RICHARD" in r_name and "V6Y0E3" in r_zip  # Matches both Buzz 1813 addresses
            is_richard_recv_2 = "RICHARD" in r_name and "V6Y1K3" in r_zip
            valid_receiver = is_yves_recv or is_richard_recv_1 or is_richard_recv_2

            is_auto_rate = valid_sender and valid_receiver
            log.info(f"[AutoRate] SenderMatch={valid_sender} RecvMatch={valid_receiver} => Auto={is_auto_rate}")

            name = (sender.get("name") or "").strip()
            client_id = (sender.get("client_id") or "").strip()
            
            # 清理代理人名稱
            temp = re.sub(r"\s*\((?:YUMI|VICKY)\)\s*", " ", name, flags=re.IGNORECASE)
            raw_name = re.sub(r"\s+", " ", temp).strip()
            adj_name = self._adjust_caps(raw_name)
            adj_client = self._adjust_caps(client_id)

            # 判定早期代購代理人
            if (("Yumi" in adj_name or "Shu-Yen" in adj_name) and "Liu" in adj_name):
                adj_name, adj_client = "Shu-Yen Liu", "Yumi"
            elif (("Vicky" in adj_name or "Chia-Chi" in adj_name) and "Ku" in adj_name):
                adj_name, adj_client = "Chia-Chi Ku", "Vicky"
            # 🟢 Karl Lagerfeld → Yumi (自動歸類)
            elif re.search(r"karl\s*lagerfeld", name, re.IGNORECASE):
                adj_name, adj_client = "Shu-Yen Liu", "Yumi"
                _is_karl_lagerfeld = True

            # ------------------------------------------------------------------
            # 🟢 時間戳路由判定：依 REF 時間戳在三個資料表中比對
            # ------------------------------------------------------------------
            # Keep clean_zip for Ace/SoQuick carrier tagging in step 6
            raw_zip = (receiver.get("postal_code") or "")
            clean_zip = re.sub(r"\s+", "", raw_zip).upper()  # Normalize to V6X1Z7

            route = self._route_by_timestamp(ref_no)
            decision_reason = ""
            board_display_name = ""

            if route == "air":
                target_parent_board_id = os.getenv('AIR_PARENT_BOARD_ID')
                target_subitem_board_id = os.getenv('AIR_BOARD_ID')
                is_domestic = False
                board_display_name = "🇹🇼 空運 Air"
                decision_reason = f"✅ 時間戳吻合 空運資料表 ({ref_no})"
            elif route == "sea":
                target_parent_board_id = os.getenv('SEA_PARENT_BOARD_ID')
                target_subitem_board_id = os.getenv('SEA_BOARD_ID')
                is_domestic = False
                board_display_name = "🇹🇼 海運 Sea"
                decision_reason = f"✅ 時間戳吻合 海運資料表 ({ref_no})"
            elif route == "domestic":
                target_parent_board_id = 8082569538
                target_subitem_board_id = 8082569581
                is_domestic = True
                board_display_name = "🇨🇦 境內配送 (Domestic)"
                decision_reason = f"✅ 時間戳吻合 境內資料表 ({ref_no})"
            else:  # not_found — warn but default to 空運
                target_parent_board_id = os.getenv('AIR_PARENT_BOARD_ID')
                target_subitem_board_id = os.getenv('AIR_BOARD_ID')
                is_domestic = False
                board_display_name = "🇹🇼 空運 Air"
                decision_reason = f"⚠️ 未找到對應時間戳 ({ref_no})，預設空運"
                self.line_push(
                    self.line_status_group,
                    f"⚠️ [PDF路由] 在三個資料表中均未找到時間戳：{ref_no}\n"
                    f"已預設路由至空運 Board，請手動確認是否正確"
                )

            log.info(f"[PDF→Monday] Routing: {board_display_name} | Reason: {decision_reason}")

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

                # 根據郵遞區號設定物流 (針對倉庫進貨自動貼標 Ace/SoQuick)
                # 使用 clean_zip 判斷
                if clean_zip.startswith("V6X1Z7"):
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {target_subitem_board_id}, column_id: "status_18__1", value: "{{\\"label\\":\\"Ace\\"}}") {{ id }} }}'})
                    self._post_with_backoff(self.api_url, {"query": f'mutation {{ change_column_value(item_id: {sub_id}, board_id: {target_subitem_board_id}, column_id: "status_19__1", value: "{{\\"label\\":\\"ACE大嘴鳥\\"}}") {{ id }} }}'})
                elif clean_zip.startswith("V6X0B9") or clean_zip.startswith("V6Y1K3") or clean_zip.startswith("V6Y0E3"): 
                    # 擴展：把其他倉庫地址也納入 SoQuick 或依據舊邏輯
                    # 舊邏輯只有 V6X0B9 -> SoQuick
                    if clean_zip.startswith("V6X0B9"):
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

            # --- 8. 🟢 加境內：根據 PDF 內容自動設定境內物流 (Fedex / UPS) ---
            if is_domestic:
                carrier = (full_data.get("carrier") or "").strip()
                carrier_label = ""
                if carrier.upper() == "UPS":
                    carrier_label = "UPS"
                elif carrier.upper() == "FEDEX":
                    carrier_label = "Fedex"
                
                if carrier_label:
                    set_carrier_q = f"""
                    mutation {{
                      change_column_value(
                        item_id: {parent_id},
                        board_id: {target_parent_board_id},
                        column_id: "status_1_mkkc5pa0",
                        value: "{{\\"label\\":\\"{carrier_label}\\"}}"
                      ) {{ id }}
                    }}
                    """
                    self._post_with_backoff(self.api_url, {"query": set_carrier_q})
                    log.info(f"[PDF→Monday] Domestic carrier set to: {carrier_label}")

            log.info(f"[PDF→Monday] Monday sync completed for {parent_name}")

            # --- 8.5 🟢 Google Sheet 同步 (在 Monday 建立後執行) ---
            self._sync_to_google_sheet(ref_no, all_tracking_numbers)

            # --- 9. 🟢 發送詳細通知到狀態群組 ---
            tracking_str = ", ".join(all_tracking_numbers) if all_tracking_numbers else "無單號"
            
            # --- 判斷自動匯率標記 ---
            auto_rate_flag = "1" if is_auto_rate else "0"
            pdf_type = "domestic" if is_domestic else "air"
            redis_client.set(
                "global_last_pdf_parent",
                f"{parent_id}|{target_parent_board_id}|{target_subitem_board_id}|{pdf_type}|{auto_rate_flag}",
                ex=1800
            )

            # --- 顯示提示 ---
            extra_hint = ""
            if is_auto_rate:
                extra_hint = "\n⚡ ***自動單價模式***：請僅輸入【加境內成本】即可 (加拿大單價各為 2.5 / 國際由系統自動補 10)"
            elif is_domestic:
                extra_hint = "\n請輸入：[加境內支出] [加拿大單價]"
            else:
                extra_hint = "\n請輸入：[加境內支出] [合計單價]  或  [加境內支出] [加拿大單價] [國際單價]"

            msg = (
                f"📄 PDF 處理完成{extra_hint}\n"
                f"單號: {tracking_str}\n"
                f"去向: {board_display_name}\n"
                f"邏輯: {decision_reason}"
            )
            self.line_push(self.line_status_group, msg)

            # --- 10. 🟢 發送錄入提示到 PDF 群組 ---
            pdf_group_id = os.getenv("LINE_GROUP_ID_PDF")
            if pdf_group_id:
                if is_domestic:
                    prompt_msg = (
                        f"📄 PDF 處理完成 ─ {parent_name}\n"
                        f"🏷 單號: {tracking_str}\n"
                        f"📍 去向: {board_display_name}\n\n"
                        f"💡 請在此群組輸入以下格式完成錄入：\n"
                        f"[加境內支出] [加拿大單價]\n"
                        f"例如：43.10 2.5\n"
                        f"⚠️ 如某欄為 0 請輸入 0"
                    )
                else:
                    prompt_msg = (
                        f"📄 PDF 處理完成 ─ {parent_name}\n"
                        f"🏷 單號: {tracking_str}\n"
                        f"📍 去向: {board_display_name}\n\n"
                        f"💡 請在此群組輸入以下格式完成錄入：\n"
                        f"2 個數值（推薦）：[加境內支出] [合計單價]\n"
                        f"例如：43.10 12.5（空運）或 32.31 7.5（海運）\n"
                        f"系統自動拆分：CA=2.5，國際=合計-2.5\n"
                        f"3 個數值（手動）：[加境內支出] [加拿大單價] [國際單價]\n"
                        f"例如：43.10 2.5 10\n"
                        f"⚠️ 如某欄為 0 請輸入 0"
                    )
                self.line_push(pdf_group_id, prompt_msg)

        except Exception as e:
            log.error(f"[PDF→Monday] Monday sync failed: {e}", exc_info=True)
            self.line_push(self.line_status_group, f"ERROR [PDF→Monday] {e}")
            
    # 修正：參數增加 board_id
    def update_domestic_expense(self, parent_id, amount, group_id, board_id):
        """檢查並錄入境內支出金額 (舊版，保留向下相容)"""
        ok, msg, item_name = self.update_expense_and_rates(
            parent_id, amount, None, None, board_id, None, True
        )
        return ok, msg, item_name

    def update_expense_and_rates(self, parent_id, expense_amount, canada_price, intl_price, board_id, subitem_board_id, is_domestic):
        """更新境內支出金額及子項目的加拿大單價 / 國際單價"""
        # 1. 查詢該項目的名稱、境內支出、以及子項目清單
        query = f'''
        query {{
          items (ids: [{parent_id}]) {{
            name
            column_values(ids: ["{self.domestic_expense_col}"]) {{
              text
            }}
            subitems {{
              id
            }}
          }}
        }}'''
        try:
            r = self._post_with_backoff(self.api_url, {"query": query})
            res = r.json().get("data", {}).get("items", [])
            if not res:
                return False, "找不到項目", ""

            item = res[0]
            item_name = item.get("name", "Unknown Item")

            # 安全檢查：確保 column_values 存在
            cols = item.get("column_values", [])
            current_val = cols[0].get("text", "") if cols else ""

            if current_val and current_val.strip():
                return False, f"加境內支出欄位已有數值 ({current_val})", item_name

            # 2. 更新父項目的「加境內支出」
            mutation = f'''
            mutation {{
              change_simple_column_value(
                item_id: {parent_id},
                board_id: {board_id},
                column_id: "{self.domestic_expense_col}",
                value: "{expense_amount}"
              ) {{ id }}
            }}'''
            self._post_with_backoff(self.api_url, {"query": mutation})
            log.info(f"[EXPENSE] Parent {parent_id} expense updated to {expense_amount}")

            # 3. 更新所有子項目的單價
            subitems = item.get("subitems", []) or []
            if subitem_board_id and canada_price is not None:
                for sub in subitems:
                    sub_id = sub["id"]
                    # 加拿大單價 (numeric9__1)
                    self.change_simple_column_value(subitem_board_id, sub_id, "numeric9__1", str(canada_price))
                    log.info(f"[EXPENSE] Subitem {sub_id} CA price → {canada_price}")

                    # 國際單價 (numeric5__1) — 僅空運/海運
                    if not is_domestic and intl_price is not None:
                        self.change_simple_column_value(subitem_board_id, sub_id, "numeric5__1", str(intl_price))
                        log.info(f"[EXPENSE] Subitem {sub_id} intl price → {intl_price}")

            return True, "成功", item_name
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