import requests
import os
import logging

log = logging.getLogger(__name__)

def get_twws_value_by_name(subitem_name):
    """
    根據子項目名稱搜尋並讀取 formula28__1 數值
    """
    api_url = "https://api.monday.com/v2"
    api_token = os.getenv("MONDAY_API_TOKEN")
    board_id = "4814336467"  # 您指定的子項目板塊 ID
    column_id = "formula28__1"

    # GraphQL：搜尋名稱匹配的項目並取得特定欄位
    query = f"""
    query {{
      items_by_column_values (board_id: {board_id}, column_id: "name", column_value: "{subitem_name}") {{
        id
        name
        column_values (ids: ["{column_id}"]) {{
          ... on FormulaValue {{
            display_value
          }}
          text
        }}
      }}
    }}
    """

    headers = {
        "Authorization": api_token,
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(api_url, json={'query': query}, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        items = data.get("data", {}).get("items_by_column_values", [])
        if not items:
            return f"找不到名稱為 '{subitem_name}' 的項目"

        # 取第一個匹配的項目
        col_vals = items[0].get("column_values", [])
        if col_vals:
            # 優先取公式顯示值
            val = col_vals[0].get("display_value") or col_vals[0].get("text")
            return val if (val and val.strip()) else "0"
        return "項目存在，但該欄位無資料"

    except Exception as e:
        log.error(f"Monday API Error: {e}")
        return f"查詢出錯: {str(e)}"