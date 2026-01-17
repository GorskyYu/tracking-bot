import requests
import os
import logging

log = logging.getLogger(__name__)

def get_twws_value():
    """
    讀取特定子項目的 formula28__1 數值
    """
    api_url = "https://api.monday.com/v2"
    api_token = os.getenv("MONDAY_API_TOKEN")
    
    # 你指定的子項目 ID 與 欄位 ID
    subitem_id = "10679525016"
    column_id = "formula28__1"

    # GraphQL 查詢：針對 Formula 欄位使用 display_value
    query = f"""
    query {{
      items (ids: [{subitem_id}]) {{
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
        
        items = data.get("data", {}).get("items", [])
        if not items:
            return "❌ 找不到該項目"

        col_vals = items[0].get("column_values", [])
        if col_vals:
            # 優先取 display_value (公式計算結果)，若無則取 text
            val = col_vals[0].get("display_value") or col_vals[0].get("text")
            return val if val else "0"
        return "⚠️ 欄位無資料"

    except Exception as e:
        log.error(f"Monday API Error: {e}")
        return f"🔥 讀取失敗: {str(e)}"