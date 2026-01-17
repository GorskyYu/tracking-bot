import requests
import os
import logging

log = logging.getLogger(__name__)

def get_twws_value_by_name(subitem_name):
    """
    根據名稱搜尋並讀取子項目的應付金額 (formula28__1)
    """
    api_url = "https://api.monday.com/v2"
    api_token = os.getenv("MONDAY_API_TOKEN")
    
    # 定義要搜尋的板塊列表：1. 環境變數的 Air Board, 2. 你提供的 Vicky Board
    # 這樣無論在哪一箱都能查到
    board_ids = [os.getenv("AIR_BOARD_ID"), "4815120249"]
    column_id = "formula28__1"
    search_value = str(subitem_name).strip()

    headers = {
        "Authorization": api_token,
        "Content-Type": "application/json"
    }

    for b_id in board_ids:
        if not b_id: continue
        
        # 使用新版 items_page_by_column_values 語法
        query = """
        query ($boardId: ID!, $colId: String!, $val: String!) {
          items_page_by_column_values (board_id: $boardId, columns: [{column_id: $colId, column_values: [$val]}]) {
            items {
              id
              name
              column_values (ids: ["formula28__1"]) {
                ... on FormulaValue { display_value }
                text
              }
            }
          }
        }
        """
        variables = {"boardId": b_id, "colId": "name", "val": search_value}

        try:
            resp = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers, timeout=10)
            data = resp.json()
            items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])

            if items:
                col_vals = items[0].get("column_values", [])
                if col_vals:
                    val = col_vals[0].get("display_value") or col_vals[0].get("text")
                    return val if (val and val.strip()) else "0"
                return "項目存在，但金額欄位為空"
        except Exception as e:
            log.error(f"[TWWS] Board {b_id} query failed: {e}")
            continue

    return f"❌ 在所有板塊中都找不到名稱為 '{search_value}' 的項目"