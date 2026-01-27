import pytz
import traceback
from datetime import datetime

from services.monday import _monday_request, get_subitem_board_id, SUBITEM_BOARD_MAPPING
from utils.permissions import is_authorized_for_event, ADMIN_USER_IDS
from utils.line_reply import reply_text
from config import line_bot_api
from linebot.models import TextSendMessage, QuickReply, QuickReplyButton, MessageAction, FlexSendMessage, BubbleContainer, BoxComponent, TextComponent, SeparatorComponent
from threading import Thread
from redis_client import r
import logging
import json
import re

# 從 config 匯入所有相關群組 ID
from config import (
    line_bot_api, 
    IRIS_GROUP_ID, 
    VICKY_GROUP_ID, 
    YUMI_GROUP_ID,
    ANGELA_GROUP_ID
)

# 建立對應表：只要指令來自這個群組，就自動查詢對應的名稱
GROUP_TO_CLIENT_MAP = {
    IRIS_GROUP_ID: "Lammond",
    VICKY_GROUP_ID: "Vicky",
    YUMI_GROUP_ID: "Yumi",
    ANGELA_GROUP_ID: "Jeth",
    # 未來想加新的群組，直接在下面加一行即可
    # os.getenv("LINE_GROUP_ID_ABC"): "ABC_Client",
}

CLIENT_ALIASES = {
    "Yumi - Shu-Yen Liu": "Yumi - Shu-Yen (Yumi) Liu"
}

TARGET_BOARD_IDS = [4814336467, 8783157722]
TARGET_STATUSES = ["溫哥華收款", "未收款出貨", "台中收款"]
PAID_STATUSES = ["已收款出貨", "核實訂單", "已完成"]

# Column Keys (Titles) for Dynamic Mapping
COL_STATUS = "Status"
COL_DIMENSION = "箱子尺寸cm"
COL_WEIGHT = "箱子重量"
COL_PRICE = "加幣應收"
COL_CAD_PRICE = "加拿大單價"
COL_INTL_PRICE = "國際單價"
COL_ADDT_CAD = "追加加幣支出"
COL_ADDT_TWD = "追加台幣支出"
COL_CAD_PAID ="加幣實收"
COL_TWD_PAID ="台幣實收"
COL_COLLECTOR = "收款人"
COL_EXCHANGE = "匯率"
COL_BILL_DATE = "出帳日"

def _get_column_value(col_name, sources):
    """Helper to find value in a list of column data sources (priority order)."""
    for source in sources:
        if source and col_name in source:
             val = source[col_name]
             # Explicitly check for None to allow "0", 0, or ""
             if val is not None:
                 return str(val)
    return None

def _extract_float(text):
    """Safe float extraction from string."""
    if not text:
        return 0.0
    try:
        # Remove currency symbols and comma
        clean = text.replace("$", "").replace(",", "").strip()
        # Remove potential (Est) or other suffix notes in parentheses
        clean = clean.split('(')[0].strip()
        return float(clean)
    except ValueError:
        return 0.0

def _map_column_values(column_values_list):
    """Maps list of column values to a dictionary {Title: Text/DisplayValue}."""
    mapped = {}
    if not column_values_list:
        return mapped
    for cv in column_values_list:
        if cv.get("column") and cv["column"].get("title"):
             title = cv["column"]["title"].strip()
             # Prioritize display_value for Formula columns
             val = cv.get("display_value")
             
             if val is None:
                 val = cv.get("text")
                 
             if val is None:
                 val = ""
                 
             mapped[title] = str(val)
    return mapped

def _fetch_col_id_by_title(board_id, title):
    """Fetches the Status column ID for a board."""
    query = """
    query ($board_id: [ID!]) {
        boards (ids: $board_id) {
            columns { id title }
        }
    }
    """
    res = _monday_request(query, {"board_id": [int(board_id)]})
    logging.info(f"Fetched columns for board {board_id}: {json.dumps(res)}")
    if res and "data" in res and res["data"]["boards"]:
         for col in res["data"]["boards"][0]["columns"]:
             if col["title"].strip() == title:
                 return col["id"]
    return None

def fetch_unpaid_items_globally():
    """
    Searches across target boards (Bottom-Up Strategy).
    1. Resolve Subitem Board.
    2. Filter by Status on Server.
    3. Fetch Item + Parent Columns together.
    4. Map Columns by Title.
    """
    items_found = []
    
    for parent_board_id in TARGET_BOARD_IDS:
        # 1. Resolve Subitem Board
        subitem_board_id = SUBITEM_BOARD_MAPPING.get(parent_board_id)
        if not subitem_board_id:
             subitem_board_id = get_subitem_board_id(parent_board_id)
        
        if not subitem_board_id:
            logging.warning(f"Skipping parent {parent_board_id}: No subitem board found.")
            continue
            
        logging.info(f"Scanning Subitem Board {subitem_board_id} (Parent: {parent_board_id})")
        
        # 2. Get Status Column ID for Filtering
        status_col_id = _fetch_col_id_by_title(subitem_board_id, COL_STATUS)
        if not status_col_id:
             logging.warning(f"Status column not found on {subitem_board_id}") 
            #  1
             continue
             
        # 3. Search Query (Fetching Columns for Item AND Parent)
        cursor = None
        while True:
            query = """
            query ($board_id: ID!, $col_id: String!, $vals: [String]!, $cursor: String) {
                items_page_by_column_values (
                    board_id: $board_id, 
                    columns: [{column_id: $col_id, column_values: $vals}],
                    limit: 100,
                    cursor: $cursor
                ) {
                    cursor
                    items {
                        id
                        name
                        column_values {
                            ... on FormulaValue { display_value }
                            text
                            column { title }
                        }
                        parent_item {
                            id
                            name
                            column_values {
                                ... on FormulaValue { display_value }
                                text
                                column { title }
                            }
                        }
                    }
                }
            }
            """
            
            variables = {
                "board_id": int(subitem_board_id),
                "col_id": status_col_id,
                "vals": TARGET_STATUSES,
                "cursor": cursor
            }
            
            res = _monday_request(query, variables)
            if not res or "data" not in res or not res["data"].get("items_page_by_column_values"):
                 break
                 
            page_data = res["data"]["items_page_by_column_values"]
            items = page_data.get("items", [])
            cursor = page_data.get("cursor")
            
            logging.info(f"Fetched {len(items)} items from board {subitem_board_id}. Next Cursor: {bool(cursor)}")

            for item in items:
                # ✅ 直接調用獨立出的處理函式
                processed = _process_monday_item(item, subitem_board_id, parent_board_id)
                
                # 如果符合條件 (折讓案或尺寸重量齊全)，就加入列表
                if processed:
                    items_found.append(processed)

            # Exit loop if no cursor returned
            if not cursor:
                break
                 
    return items_found

def _resolve_client_name(name):
    """Resolve client name using manual alias mapping."""
    clean = name.strip()
    return CLIENT_ALIASES.get(clean, clean)

def _group_items_by_client(items, filter_name=None, filter_date=None):
    """
    Groups items by Client -> Bill Date -> Parent Date.
    Returns: { canonical_name: { display, total, data: { bill_date: { parent_dates: { parent_date: { items:[], subtotal, paid_amount } } } } } }
    filter_date: Optional YYYYMMDD string to filter by specific date
    """
    raw_clients = {} 

    for item in items:
        # Parse Parent Name to extract date and client name
        raw_parent = item["parent_name"]
        match = re.match(r'^(\d+)\s+(.*)$', raw_parent.strip())
        if match:
            parent_date = match.group(1)  # Date from parent name
            client_name = match.group(2)
        else:
            parent_date = ""
            client_name = raw_parent
        
        # Get bill_date for grouping
        item_bill_date = item.get("bill_date", "").strip()
        
        # Filter by date using bill_date column
        if filter_date:
            # bill_date format from Monday: "2026-01-20"
            # filter_date format: "20260120" (YYYYMMDD)
            # Convert filter_date to ISO format for comparison
            if len(filter_date) == 8:  # YYYYMMDD
                filter_date_iso = f"{filter_date[:4]}-{filter_date[4:6]}-{filter_date[6:8]}"
            else:
                filter_date_iso = filter_date
            
            if item_bill_date != filter_date_iso:
                continue
        
        # 只要名稱中包含關鍵字，就統一歸類到該客戶下
        found_canonical = False
        for main_name in ["Vicky", "Yumi", "Lammond"]:
            if main_name.lower() in client_name.lower():
                canonical_name = main_name
                found_canonical = True
                break
        
        if not found_canonical:
            # 不在名單的客人，只取第一個單詞或橫槓前的文字作為 Abowbow ID (Key)
            canonical_name = client_name.split(" - ")[0].split()[0]
        
        # Filter Logic (case-insensitive)
        if filter_name and filter_name != "All":
             if filter_name.lower() not in canonical_name.lower(): 
                 continue

        if canonical_name not in raw_clients:
            raw_clients[canonical_name] = {
                "display_name": canonical_name,
                "data": {},
                "total": 0.0
            }
             
        client_data = raw_clients[canonical_name]
        
        # Use bill_date as first level grouping key (or "未出帳" for empty bill_date)
        bill_date_key = item_bill_date if item_bill_date else "未出帳"
        
        if bill_date_key not in client_data["data"]:
            client_data["data"][bill_date_key] = {
                "parent_dates": {}
            }
        
        bill_date_group = client_data["data"][bill_date_key]
        
        # Use parent_date as second level grouping key
        if parent_date not in bill_date_group["parent_dates"]:
            # Calculate paid amount from parent item (only once per unique parent)
            rate = item.get("parent_rate", 1.0)
            if rate <= 0: rate = 1.0
            total_paid_cad = item.get("parent_cad_paid", 0) + (item.get("parent_twd_paid", 0) / rate)
            
            bill_date_group["parent_dates"][parent_date] = {
                "items": [], 
                "subtotal": 0.0,
                "paid_amount": total_paid_cad # 該筆貨物已付總額 (CAD)
            }
            # 預扣除實收
            bill_date_group["parent_dates"][parent_date]["subtotal"] -= total_paid_cad
            client_data["total"] -= total_paid_cad
        
        parent_group = bill_date_group["parent_dates"][parent_date]
        parent_group["items"].append(item)
        parent_group["subtotal"] += item["price_val"]
        client_data["total"] += item["price_val"]
        
    return raw_clients

def _create_item_row(item):
    """Creates a vertical box component for a single item row."""
    # 折讓案顯示母項目全名，其餘顯示子項目名
    parent_name = item.get("parent_name", "N/A")
    sub_name = item.get("sub_name", "").strip()

    # 判斷是否為「折讓」特例路徑
    is_discount_path = "折讓" in parent_name

    # 不論原始文字為何，統一由 price_val 轉為兩位小數
    price_val = item.get("price_val", 0.0)
    formatted_price = f"${price_val:.2f}"

    # 準備顯示內容容器
    row_contents = []
    
    if is_discount_path:
        # ─── 折讓特例路徑 ───
        # 1. 第一行：顯示母項目全名 + 金額
        row_contents.append(
            BoxComponent(
                layout='horizontal',
                contents=[
                    TextComponent(text=parent_name, flex=4, size='sm', wrap=True, weight='bold'),
                    TextComponent(text=formatted_price, flex=2, size='sm', align='end', weight='bold')
                ]
            )
        )
        # 2. 第二行：若子項目名稱存在且不等於母項目名，則顯示子項目名 (不顯示分隔符)
        if sub_name and sub_name != parent_name:
            row_contents.append(
                BoxComponent(
                    layout='horizontal',
                    contents=[
                        TextComponent(text=sub_name, size='xs', color='#aaaaaa', flex=1)
                    ]
                )
            )
    else:    
        # ─── 原本路徑 (一般包裹) ───
        # 1. 第一行：顯示原本的子項目名 (單號) + 金額
        row_contents.append(
            BoxComponent(
                layout='horizontal',
                contents=[
                    TextComponent(text=sub_name if sub_name else "N/A", flex=4, size='sm', wrap=True),
                    TextComponent(text=formatted_price, flex=2, size='sm', align='end', weight='bold')
                ]
            )
        )
        # 2. 第二行：運費單價 | 規格 (CAD rate | INTL rate | 尺寸 | 重量) 
        cad_rate = item.get("cad_domestic_rate", 0)
        intl_rate = item.get("intl_shipping_rate", 0)
        dims = item.get("dimensions", "").strip()
        weight = item.get("weight", "").strip()
        
        # Build the specs text with shipping rates
        cad_rate_display = f"{cad_rate} CAD/kg"
        intl_rate_display = f"{intl_rate} CAD/kg"
        dims_display = f"{dims} cm" if dims and not dims.lower().endswith("cm") else dims
        weight_display = f"{weight} kg" if weight and not weight.lower().endswith("kg") else weight
        
        # Build specs as a single joined string for proper wrapping on mobile
        specs_parts = [cad_rate_display, intl_rate_display]
        if dims:
            specs_parts.append(dims_display)
        if weight:
            specs_parts.append(weight_display)
        
        specs_text = " | ".join(specs_parts)
        
        row_contents.append(
            BoxComponent(
                layout='horizontal',
                contents=[
                    TextComponent(text=specs_text, size='xs', color='#aaaaaa', wrap=True)
                ]
            )
        )
    
    return BoxComponent(layout='vertical', margin='md', contents=row_contents)

def _create_client_flex_message(client_obj, is_paid_bill=False):
    """Builds a Flex Bubble for a single client.
    is_paid_bill: If True, displays total in green; if False, displays total in red
    """
    display_name = client_obj["display_name"]
    total = client_obj["total"]
    dates_data = client_obj["data"]
    
    # Header
    header = BoxComponent(
        layout='vertical',
        contents=[
            TextComponent(text=display_name, size='xl', weight='bold')
        ]
    )
    
    # Body
    body_contents = []
    
    sorted_bill_dates = sorted(dates_data.keys())
    for bill_idx, bill_date_key in enumerate(sorted_bill_dates):
        bill_date_group = dates_data[bill_date_key]
        
        # Add Separator between bill dates
        if bill_idx > 0:
            body_contents.append(SeparatorComponent(margin='lg'))

        # Bill Date Subheader
        if bill_date_key:
            # Format bill_date: "2026-01-20" -> "出帳日：260120" or show "未出帳" as is
            if bill_date_key == "未出帳":
                display_date = "未出帳"
            elif len(bill_date_key) == 10 and bill_date_key[4] == '-' and bill_date_key[7] == '-':  # ISO format YYYY-MM-DD
                # Convert to YYMMDD format: "2026-01-20" -> "260120"
                display_date = f"出帳日：{bill_date_key[2:4]}{bill_date_key[5:7]}{bill_date_key[8:10]}"
            else:
                display_date = f"出帳日：{bill_date_key}"
            
            body_contents.append(
                TextComponent(text=display_date, weight='bold', margin='lg', size='md', color='#1DB446')
            )
        
        # Loop through parent dates within this bill date
        sorted_parent_dates = sorted(bill_date_group["parent_dates"].keys())
        for parent_idx, parent_date in enumerate(sorted_parent_dates):
            parent_group = bill_date_group["parent_dates"][parent_date]
            
            # Parent Date Section Header
            if parent_date:
                body_contents.append(
                    TextComponent(text=parent_date, weight='bold', margin='md', size='sm', color='#555555')
                )
            
            # Items under this parent date
            for item in parent_group["items"]:
                body_contents.append(_create_item_row(item))
                
            # Paid amount for this parent section (if any)
            if parent_group["paid_amount"] != 0:
                # 檢查該組包裹中是否包含「折讓」母項目
                is_discount = any("折讓" in item.get("parent_name", "") for item in parent_group["items"])
                label_text = "Discount" if is_discount else "Paid (Already Received)"

                body_contents.append(
                    BoxComponent(
                        layout='horizontal',
                        margin='md',
                        contents=[
                            TextComponent(text=label_text, flex=4, size='sm', color='#1DB446'),
                            TextComponent(text=f"-${parent_group['paid_amount']:.2f}", flex=2, align='end', size='sm', color='#1DB446', weight='bold')
                        ]
                    )
                )
                
            # Parent Date Subtotal
            body_contents.append(SeparatorComponent(margin='sm'))
            body_contents.append(
                BoxComponent(
                    layout='horizontal',
                    margin='sm',
                    contents=[
                        TextComponent(text="Subtotal", flex=4, size='sm', color='#555555'),
                        TextComponent(text=f"${parent_group['subtotal']:.2f}", flex=2, align='end', size='sm', weight='bold')
                    ]
                )
            )

    # Footer (Total)
    # Use green color for paid bills, red for unpaid bills
    total_color = '#1DB446' if is_paid_bill else '#FF4B4B'
    footer = BoxComponent(
        layout='vertical',
        spacing='sm',
        contents=[
             SeparatorComponent(),
             BoxComponent(
                layout='horizontal',
                margin='md',
                contents=[
                    TextComponent(text="Total Amount", flex=4, size='lg', weight='bold'),
                    TextComponent(text=f"${total:.2f}", flex=3, align='end', size='lg', weight='bold', color=total_color)
                ]
            )
        ]
    )
    
    bubble = BubbleContainer(
        header=header,
        body=BoxComponent(layout='vertical', contents=body_contents),
        footer=footer
    )
    
    return FlexSendMessage(alt_text=f"Bill for {display_name}", contents=bubble)

def _unpaid_worker(destination_id, filter_name=None, today_client_filter=None, filter_date=None):
    """Background thread worker.
    filter_date: Optional YYMMDD string to filter by specific date
    """
    try:
        # ✅ 判斷是否為 today 模式
        is_today_mode = (filter_name == "today")
        
        if is_today_mode:
            results, date_display = fetch_and_tag_unpaid_today()
            # 發送第一條訊息：YYMMDD出帳：
            line_bot_api.push_message(destination_id, TextSendMessage(text=f"{date_display}出帳："))
            # today 模式下，如果有指定客戶，則過濾該客戶；否則顯示所有客戶
            final_filter = today_client_filter
        else:
            results = fetch_unpaid_items_globally()
            final_filter = filter_name
        
        if not results:
             line_bot_api.push_message(destination_id, TextSendMessage(text="沒有發現符合條件的項目（箱子尺寸與重量皆不為空，且狀態符合作業需求）。"))
             return

        # Group Data 這裡要改用 final_filter，因為在 today 模式下 final_filter 會被設為 None
        grouped_clients = _group_items_by_client(results, final_filter, filter_date)
        
        if not grouped_clients:
             line_bot_api.push_message(destination_id, TextSendMessage(text=f"在 '{filter_name}' 條件下未搜尋到符合結果。"))
             return
             
        # Send one Flex Message per client
        for canonical_name, client_data in grouped_clients.items():
            try:
                flex_message = _create_client_flex_message(client_data)
                line_bot_api.push_message(destination_id, flex_message)
            except Exception as e:
                logging.error(f"Error sending flex for {canonical_name}: {e}")
                line_bot_api.push_message(destination_id, TextSendMessage(text=f"❌ 發送 {canonical_name} 帳單時發生錯誤 (可能是內容過長)。"))

    except Exception as e:
        logging.error(f"Unpaid worker failed: {e}")
        try:
             line_bot_api.push_message(destination_id, TextSendMessage(text="❌ 系統發生錯誤，請稍後再試。"))
        except:
             pass

def handle_unpaid_event(sender_id, message_text, reply_token, user_id=None, group_id=None):
    # 🔍 先抓取管理員狀態與自動對應名稱
    is_admin = user_id in ADMIN_USER_IDS
    auto_target_name = GROUP_TO_CLIENT_MAP.get(group_id)
    
    parts = message_text.strip().split()
    text_lower = message_text.strip().lower()
    
    # 處理目前功能指令 (僅限管理員私訊)
    if message_text.strip() == "目前功能" and is_admin and not group_id:
        help_text = """📋 目前可用指令：

【未付款相關】
• unpaid - 查詢所有未付款項目
• unpaid [客戶ID] - 查詢特定客戶未付款項目
  例如：unpaid Lorant
• unpaid [日期] - 查詢特定日期的未付款項目（在群組自動偵測客戶）
  例如：在 Vicky 群組輸入 unpaid 260125
• unpaid [日期] [客戶ID] - 查詢特定日期的未付款項目（指定客戶）
  例如：unpaid 260125 Lorant
• unpaid today - 標記今日出帳並顯示
• unpaid today [客戶ID] - 標記今日出帳並顯示特定客戶
  例如：unpaid today Lorant

【已付款相關】
• paid [日期] - 查看特定日期已付款帳單（在指定群組）
  例如：paid 260125
• paid [日期] [客戶ID] - 查看特定日期已付款帳單
  例如：paid 260125 Lorant
• paid [金額] - 錄入實收金額（在群組自動偵測客戶，或使用上次查詢）
  例如：在 Iris 群組輸入 paid 152.99
• paid [金額] ntd/twd - 錄入台幣實收（在群組自動偵測客戶）
  例如：paid 1500 ntd
• paid [金額] ntd/twd [客戶ID] - 錄入實收金額（指定客戶）
  例如：paid 1500 ntd Iris

【查看帳單】
• 查看帳單 [日期] - 查看特定日期帳單（在指定群組）
  例如：查看帳單 260125
• 查看帳單 [客戶] [日期] - 查看特定客戶特定日期帳單（任何群組）
  例如：查看帳單 Vicky 260125

💡 提示：
- 日期格式為 YYMMDD（例如：260125 代表 2026/01/25）
- 客戶ID不區分大小寫
- 在 Iris/Vicky/Yumi 群組中，unpaid 和 paid 指令會自動偵測客戶
- 帳單顯示按「出帳日」分組，每組再按母項目日期細分
- paid 指令支援多項目按比例分配付款，從最舊日期開始分配
- 所有指令僅限管理員使用（除非在指定群組）"""
        reply_text(reply_token, help_text)
        return

    # 處理 unpaid today [client_code] (帶客戶代號的 today 指令)
    if len(parts) >= 3 and parts[1].lower() == "today":
        if not is_admin:
            reply_text(reply_token, "⛔ 此指令僅限管理員使用。")
            return
        
        # 提取客戶代號 (第三個詞之後的所有內容)
        client_code = " ".join(parts[2:])
        r.set(f"last_unpaid_client_{sender_id}", client_code, ex=3600)
        
        reply_text(reply_token, f"📅 正在掃描 {client_code} 的未出帳項目並標記日期，請稍候...")
        Thread(target=_unpaid_worker, args=(group_id if group_id else sender_id, "today", client_code)).start()
        return

    # 如果輸入 unpaid [名稱]，記錄到 Redis
    if len(parts) > 1 and parts[1].lower() != "today":
        target_name = " ".join(parts[1:])
        r.set(f"last_unpaid_client_{sender_id}", target_name, ex=3600) # 紀錄 1 小時

    # 處理 unpaid today (不帶客戶代號)
    if text_lower == "unpaid today":
        if not is_admin:
            reply_text(reply_token, "⛔ 此指令僅限管理員使用。")
            return
        
        # 如果在非指定群組，提示需要輸入客戶ID
        if not auto_target_name:
            reply_text(reply_token, "需要輸入Abowbow客戶ID\n例如：unpaid today Kit")
            return
        
        reply_text(reply_token, "📅 正在掃描未出帳項目並標記日期，請稍候...")
        # 啟動 Thread 執行，傳入 "today" 作為 filter_name，並傳入 auto_target_name 進行過濾
        Thread(target=_unpaid_worker, args=(group_id if group_id else sender_id, "today", auto_target_name)).start()
        return
    
    # 1. 如果是一般成員 (非管理員)
    if not is_admin:
        # 僅限在有對應表的群組中輸入單純的 "unpaid"
        if len(parts) == 1 and auto_target_name:
            # 允許執行自動查詢
            reply_text(reply_token, f"🔍 正在搜尋 {auto_target_name} 的未付款項目，請稍候...")
            t = Thread(target=_unpaid_worker, args=(group_id, auto_target_name))
            t.start()
            return
        else:
            # 企圖查別人 (例如 unpaid All) 或在私訊使用，直接拒絕
            reply_text(reply_token, "⛔ 您僅限在指定群組查詢該群組的帳單。")
            return

    # 2. 如果是管理員，維持原有的完整權限 (包含私訊選單、手動查所有人)
    cmd = parts[0].lower()
    
    auto_target_name = GROUP_TO_CLIENT_MAP.get(group_id)
 
    # 如果在特定群組發送且沒有帶參數 (例如只打 unpaid)
    if len(parts) == 1 and auto_target_name:
        reply_text(reply_token, f"🔍 正在搜尋 {auto_target_name} 的未付款項目，請稍候...")
        target_id = group_id if group_id else sender_id
        t = Thread(target=_unpaid_worker, args=(target_id, auto_target_name))
        t.start()
        return
 
    # If user used the Quick Reply, it might send "unpaid All" etc.
    if len(parts) > 1:
        # Check if parts[1] is a date (YYMMDD format)
        if re.match(r'^\d{6}$', parts[1]):
            # Normalize YYMMDD to YYYYMMDD format for comparison
            filter_date = "20" + parts[1]
            
            # If client name is provided explicitly (3+ parts)
            if len(parts) >= 3:
                target_name = " ".join(parts[2:])
            # If in a mapped group, auto-detect client
            elif auto_target_name:
                target_name = auto_target_name
            # Otherwise, require explicit client name
            else:
                reply_text(reply_token, f"❌ 請指定客戶名稱：unpaid {parts[1]} [客戶名稱]")
                return
            
            r.set(f"last_unpaid_client_{sender_id}", target_name, ex=3600)
            reply_text(reply_token, f"🔍 正在搜尋 {target_name} 在 {filter_date} 的未付款項目，請稍候...")
            target_id = group_id if group_id else sender_id
            t = Thread(target=_unpaid_worker, args=(target_id, target_name, None, filter_date))
            t.start()
            return
        else:
            # Format: unpaid ClientID (original logic)
            target_name = " ".join(parts[1:]) 
            reply_text(reply_token, f"🔍 正在搜尋未付款項目 ({target_name})，請稍候...")
            target_id = group_id if group_id else sender_id
            t = Thread(target=_unpaid_worker, args=(target_id, target_name))
            t.start()
            return

    # 管理員點選 unpaid 時，動態顯示有欠款的客戶
    if is_admin and len(parts) == 1:
        reply_text(reply_token, "🔍 正在掃描所有欠款客戶...")
        def _send_dynamic_buttons():
            all_items = fetch_unpaid_items_globally()
            clients = _group_items_by_client(all_items) 
            
            buttons = [QuickReplyButton(action=MessageAction(label="All", text="unpaid All"))]
            # 取前 12 個有欠款的客人 (LINE 限制總數 13)
            for name in list(clients.keys())[:12]:
                display_name = (name[:17] + "...") if len(name) > 20 else name
                buttons.append(QuickReplyButton(action=MessageAction(label=name, text=f"unpaid {name}")))
            
            line_bot_api.push_message(sender_id, TextSendMessage(text="請選擇或直接輸入客戶 ID：", quick_reply=QuickReply(items=buttons)))
        
        Thread(target=_send_dynamic_buttons).start()
        return

def fetch_items_by_bill_date(target_date_yyyymmdd):
    """
    依照「出帳日」搜尋所有板塊的項目
    """
    items_found = []
    # 轉換日期格式：260120 -> 2026-01-20 (以匹配 Monday Date 格式)
    formatted_date = f"20{target_date_yyyymmdd[:2]}-{target_date_yyyymmdd[2:4]}-{target_date_yyyymmdd[4:]}"
    
    for parent_board_id in TARGET_BOARD_IDS:
        subitem_board_id = SUBITEM_BOARD_MAPPING.get(parent_board_id) or get_subitem_board_id(parent_board_id)
        if not subitem_board_id: continue

        # 這裡沿用你的 _fetch_col_id_by_title 邏輯，但改為抓取「出帳日」的 ID
        bill_date_col_id = _fetch_col_id_by_title(subitem_board_id, COL_BILL_DATE)
        if not bill_date_col_id: continue

        # 搜尋 Query：篩選出帳日等於目標日期的項目
        query = """
        query ($board_id: ID!, $col_id: String!, $val: String!) {
            items_page_by_column_values (
                board_id: $board_id, 
                columns: [{column_id: $col_id, column_values: [$val]}],
                limit: 100
            ) {
                items {
                    id name
                    column_values { ... on FormulaValue { display_value } text column { title } }
                    parent_item {
                        id
                        name
                        column_values { ... on FormulaValue { display_value } text column { title } }
                    }
                }
            }
        }
        """
        res = _monday_request(query, {"board_id": int(subitem_board_id), "col_id": bill_date_col_id, "val": formatted_date})
        
        if res and "data" in res and res["data"].get("items_page_by_column_values"):
            items_data = res["data"]["items_page_by_column_values"].get("items", [])
            for item in items_data:
                # ✅ 直接調用共用的處理邏輯
                processed = _process_monday_item(item, subitem_board_id, parent_board_id)
                if processed: items_found.append(processed)
                
    return items_found

def _process_monday_item(item, subitem_board_id, parent_board_id):
    """
    通用處理邏輯：將 Monday 的 Item 物件轉化為帳單資料格式
    """
    sub_name = item["name"]
    subitem_cols = _map_column_values(item.get("column_values", []))
    parent_item = item.get("parent_item")
    if not parent_item: return None

    parent_name = parent_item["name"]
    parent_cols = _map_column_values(parent_item.get("column_values", []))
    sources = [subitem_cols, parent_cols]

    dim_val = _get_column_value(COL_DIMENSION, sources)
    weight_val = _get_column_value(COL_WEIGHT, sources)

    # ✅ 沿用折讓特例或尺寸齊全邏輯
    if ("折讓" in parent_name) or (dim_val and dim_val.strip() and weight_val and weight_val.strip()):
        rate = _extract_float(parent_cols.get(COL_EXCHANGE, "1"))
        if rate <= 0: rate = 1.0
        
        # Extract shipping rates from subitem columns
        cad_price = _extract_float(subitem_cols.get(COL_CAD_PRICE, "0"))
        intl_price = _extract_float(subitem_cols.get(COL_INTL_PRICE, "0"))
        
        # Extract bill_date from subitem columns
        bill_date = subitem_cols.get(COL_BILL_DATE, "").strip()
        
        return {
            "id": item["id"],
            "parent_id": parent_item["id"],
            "board_id": subitem_board_id,
            "parent_board_id": parent_board_id,
            "parent_name": parent_name,
            "sub_name": sub_name,
            "bill_date": bill_date,
            "price_text": subitem_cols.get(COL_PRICE, "0"),
            "price_val": _extract_float(subitem_cols.get(COL_PRICE, "0")),
            "dimensions": dim_val,
            "weight": weight_val,
            "cad_domestic_rate": cad_price,
            "intl_shipping_rate": intl_price,
            "parent_cad_paid": _extract_float(parent_cols.get(COL_CAD_PAID, "0")),
            "parent_twd_paid": _extract_float(parent_cols.get(COL_TWD_PAID, "0")),
            "parent_rate": rate
        }
    return None

def _bill_worker(destination_id, client_filter, date_val):
    """查看帳單的背景執行程序"""
    try:
        results = fetch_items_by_bill_date(date_val)
        if not results:
            line_bot_api.push_message(destination_id, TextSendMessage(text=f"📅 {date_val} 沒有找到任何出帳項目。"))
            return

        # ✅ 直接複用原本的群組邏輯 (會自動按 Parent 分類並計算實收)
        grouped = _group_items_by_client(results, client_filter)
        if not grouped:
            line_bot_api.push_message(destination_id, TextSendMessage(text=f"🔍 在 {date_val} 找不到屬於 {client_filter} 的項目。"))
            return

        for client_name, client_data in grouped.items():
            flex = _create_client_flex_message(client_data)
            line_bot_api.push_message(destination_id, flex)
            
    except Exception as e:
        logging.error(f"Bill worker failed: {e}")

def handle_bill_event(sender_id, message_text, reply_token, user_id, group_id=None):
    text = message_text.strip()
    is_admin = user_id in ADMIN_USER_IDS
    auto_client = GROUP_TO_CLIENT_MAP.get(group_id)

    # Parse the command to check for explicit client name and date
    # Format: 查看帳單 [客戶] [日期] or 查看帳單 [日期]
    parts = text.split()
    
    # Check if there's a date (YYMMDD format) in the command
    date_val = None
    client_name = None
    
    for part in parts[1:]:  # Skip "查看帳單"
        if re.match(r'^\d{6}$', part):
            date_val = part
        elif not date_val:  # Client name comes before date
            client_name = part
    
    # If explicit client name and date provided (works anywhere)
    if client_name and date_val:
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"🔍 正在抓取 {client_name} 的 {date_val} 帳單..."))
        Thread(target=_bill_worker, args=(group_id if group_id else user_id, client_name, date_val)).start()
        return
    
    # 1. 群組模式：查看帳單YYMMDD (only date, use auto_client)
    if group_id and date_val and not client_name:
        if not auto_client:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="⛔ 此群組未對應客戶。"))
            return
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"🔍 正在抓取 {auto_client} 的 {date_val} 帳單..."))
        Thread(target=_bill_worker, args=(group_id, auto_client, date_val)).start()
        return

    # 2. 私訊模式 (僅限管理員)
    if not group_id and is_admin:
        if text == "查看帳單":
            buttons = [
                QuickReplyButton(action=MessageAction(label="Vicky", text="查看帳單 Vicky")),
                QuickReplyButton(action=MessageAction(label="Yumi", text="查看帳單 Yumi")),
                QuickReplyButton(action=MessageAction(label="Iris", text="查看帳單 Lammond"))
            ]
            line_bot_api.reply_message(reply_token, TextSendMessage(text="請選擇客戶：", quick_reply=QuickReply(items=buttons)))
        elif client_name and not date_val:  # 點選了客戶，提示輸入日期
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"請補上日期 (YYMMDD)\n例如：查看帳單 {client_name} 260120"))

def fetch_paid_items_by_bill_date(target_date_yyyymmdd):
    """
    依照「出帳日」和「已付款狀態」搜尋所有板塊的項目
    """
    items_found = []
    # 轉換日期格式：260120 -> 2026-01-20 (以匹配 Monday Date 格式)
    formatted_date = f"20{target_date_yyyymmdd[:2]}-{target_date_yyyymmdd[2:4]}-{target_date_yyyymmdd[4:]}"
    
    for parent_board_id in TARGET_BOARD_IDS:
        subitem_board_id = SUBITEM_BOARD_MAPPING.get(parent_board_id) or get_subitem_board_id(parent_board_id)
        if not subitem_board_id: continue

        bill_date_col_id = _fetch_col_id_by_title(subitem_board_id, COL_BILL_DATE)
        status_col_id = _fetch_col_id_by_title(subitem_board_id, COL_STATUS)
        if not bill_date_col_id or not status_col_id: continue

        # 先按日期搜尋
        query = """
        query ($board_id: ID!, $col_id: String!, $val: String!) {
            items_page_by_column_values (
                board_id: $board_id, 
                columns: [{column_id: $col_id, column_values: [$val]}],
                limit: 100
            ) {
                items {
                    id name
                    column_values { ... on FormulaValue { display_value } text column { title } }
                    parent_item {
                        id
                        name
                        column_values { ... on FormulaValue { display_value } text column { title } }
                    }
                }
            }
        }
        """
        res = _monday_request(query, {"board_id": int(subitem_board_id), "col_id": bill_date_col_id, "val": formatted_date})
        
        if res and "data" in res and res["data"].get("items_page_by_column_values"):
            items_data = res["data"]["items_page_by_column_values"].get("items", [])
            for item in items_data:
                # 檢查狀態是否為已付款狀態
                cols = _map_column_values(item.get("column_values", []))
                item_status = cols.get(COL_STATUS, "")
                
                if item_status in PAID_STATUSES:
                    processed = _process_monday_item(item, subitem_board_id, parent_board_id)
                    if processed: 
                        items_found.append(processed)
                
    return items_found

def _paid_worker(destination_id, client_filter, date_val):
    """查看已付款帳單的背景執行程序"""
    try:
        results = fetch_paid_items_by_bill_date(date_val)
        if not results:
            line_bot_api.push_message(destination_id, TextSendMessage(text="未找到帳單，請檢查日期、所在群組或Abowbow ID。"))
            return

        # 使用相同的分組和顯示邏輯（與 unpaid 一致）
        grouped = _group_items_by_client(results, client_filter)
        if not grouped:
            line_bot_api.push_message(destination_id, TextSendMessage(text="未找到帳單，請檢查日期、所在群組或Abowbow ID。"))
            return

        for client_name, client_data in grouped.items():
            # 使用相同的 Flex Message 格式，但標記為 paid bill (總額顯示綠色)
            flex = _create_client_flex_message(client_data, is_paid_bill=True)
            line_bot_api.push_message(destination_id, flex)
            
    except Exception as e:
        logging.error(f"Paid worker failed: {e}\n{traceback.format_exc()}")

def handle_paid_bill_event(sender_id, message_text, reply_token, user_id, group_id=None):
    """處理查看已付款帳單指令 (paid YYMMDD [AbowbowID])"""
    if user_id not in ADMIN_USER_IDS:
        reply_text(reply_token, "⛔ 此指令僅限管理員使用。")
        return

    text = message_text.strip()
    parts = text.split()
    
    # 檢查是否符合 paid 指令格式
    if len(parts) < 2:
        return
    
    # 解析日期 (第二個參數應該是 YYMMDD)
    date_match = re.match(r"^\d{6}$", parts[1])
    if not date_match:
        return
    
    date_val = parts[1]
    auto_client = GROUP_TO_CLIENT_MAP.get(group_id)
    
    # 情況 1: 在指定群組 (Vicky/Yumi/Iris)，格式：paid YYMMDD
    if auto_client and len(parts) == 2:
        reply_text(reply_token, f"🔍 正在抓取 {auto_client} 的 {date_val} 已付款帳單...")
        Thread(target=_paid_worker, args=(group_id if group_id else sender_id, auto_client, date_val)).start()
        return
    
    # 情況 2: 在非指定群組，格式：paid YYMMDD AbowbowID
    if len(parts) >= 3:
        client_code = " ".join(parts[2:])
        reply_text(reply_token, f"🔍 正在抓取 {client_code} 的 {date_val} 已付款帳單...")
        Thread(target=_paid_worker, args=(group_id if group_id else sender_id, client_code, date_val)).start()
        return
    
    # 情況 3: 在非指定群組但沒有提供客戶ID
    if not auto_client and len(parts) == 2:
        reply_text(reply_token, "需要輸入Abowbow客戶ID\n例如：paid 260120 Kit")
        return

def fetch_and_tag_unpaid_today():
    """
    抓取未出帳項目，並自動填入今日日期 (溫哥華時間)
    """
    items_found = []
    
    # 1. 取得溫哥華今日日期
    tz = pytz.timezone('America/Vancouver')
    now_van = datetime.now(tz)
    today_iso = now_van.strftime("%Y-%m-%d") # 用於寫入 Monday (YYYY-MM-DD)
    today_display = now_van.strftime("%y%m%d") # 用於訊息標題 (YYMMDD)
    
    for parent_board_id in TARGET_BOARD_IDS:
        subitem_board_id = SUBITEM_BOARD_MAPPING.get(parent_board_id) or get_subitem_board_id(parent_board_id)
        if not subitem_board_id: continue

        status_col_id = _fetch_col_id_by_title(subitem_board_id, COL_STATUS)
        date_col_id = _fetch_col_id_by_title(subitem_board_id, COL_BILL_DATE)
        if not status_col_id or not date_col_id: continue

        # 搜尋符合狀態的項目
        query = """
        query ($board_id: ID!, $col_id: String!, $vals: [String]!) {
            items_page_by_column_values (
                board_id: $board_id, 
                columns: [{column_id: $col_id, column_values: $vals}],
                limit: 100
            ) {
                items {
                    id name
                    column_values { ... on FormulaValue { display_value } text column { title } }
                    parent_item {
                        id
                        name
                        column_values { ... on FormulaValue { display_value } text column { title } }
                    }
                }
            }
        }
        """
        res = _monday_request(query, {"board_id": int(subitem_board_id), "col_id": status_col_id, "vals": TARGET_STATUSES})
        
        if res and "data" in res and res["data"].get("items_page_by_column_values"):
            items_data = res["data"]["items_page_by_column_values"].get("items", [])
            for item in items_data:
                cols = _map_column_values(item.get("column_values", []))
                bill_date_value = cols.get(COL_BILL_DATE, "").strip()
                
                # 處理兩種情況：1) 出帳日為空 2) 出帳日已經是今天
                if not bill_date_value:
                    # 🚀 A. 在 Monday.com 寫上今日日期
                    mutation = """
                    mutation ($board_id: ID!, $item_id: ID!, $col_id: String!, $val: String!) {
                        change_simple_column_value (board_id: $board_id, item_id: $item_id, column_id: $col_id, value: $val) { id }
                    }
                    """
                    _monday_request(mutation, {
                        "board_id": int(subitem_board_id),
                        "item_id": int(item["id"]),
                        "col_id": date_col_id,
                        "val": today_iso
                    })
                    
                    # 🚀 B. 處理資料格式以便後續發送
                    processed = _process_monday_item(item, subitem_board_id, parent_board_id)
                    if processed:
                        items_found.append(processed)
                elif bill_date_value == today_iso:
                    # 🚀 C. 如果已經是今天的日期，也加入結果（不需要寫入）
                    processed = _process_monday_item(item, subitem_board_id, parent_board_id)
                    if processed:
                        items_found.append(processed)
                
    return items_found, today_display

def handle_paid_event(sender_id, message_text, reply_token, user_id, group_id=None):
    """處理實收金額錄入與狀態自動轉換邏輯"""
    if user_id not in ADMIN_USER_IDS:
        return reply_text(reply_token, "⛔ 此指令僅限管理員使用。")

    # 1. 解析指令 (例如：paid 42.41 ntd 或 paid 42.41 ntd Iris)
    match = re.match(r"^(paid|Paid)\s*(\d+(?:\.\d+)?)\s*(ntd|twd|cad)?\s*(.+)?$", message_text.strip(), re.IGNORECASE)
    if not match:
        return
    
    amount = float(match.group(2))
    currency = (match.group(3) or "cad").lower()
    client_name_from_cmd = match.group(4).strip() if match.group(4) else None

    # 2. 決定客戶名稱 (優先級：命令中的名稱 > 群組映射 > Redis緩存)
    target_client = None
    
    # 優先使用命令中的客戶名稱
    if client_name_from_cmd:
        target_client = client_name_from_cmd
    # 如果在特定群組中，自動映射客戶
    elif group_id and group_id in GROUP_TO_CLIENT_MAP:
        target_client = GROUP_TO_CLIENT_MAP[group_id]
    # 最後才從 Redis 讀取
    else:
        target_client = r.get(f"last_unpaid_client_{sender_id}")
    
    if not target_client:
        return reply_text(reply_token, "❌ 請指定客戶名稱：paid [金額] [ntd|twd] [客戶名稱]")

    reply_text(reply_token, f"💰 正在為 {target_client} 錄入 {currency.upper()} ${amount}，請稍候...")

    def _paid_worker():
        try:
            # 抓取該客戶所有未付項目
            items = fetch_unpaid_items_globally()
            grouped = _group_items_by_client(items, target_client)
            if not grouped:
                return line_bot_api.push_message(sender_id, TextSendMessage(text="查無該客戶的未付項目。"))

            # Find the client in grouped dict (case-insensitive key match)
            client_key = None
            for key in grouped.keys():
                if key.lower() == target_client.lower():
                    client_key = key
                    break
            
            if not client_key:
                return line_bot_api.push_message(sender_id, TextSendMessage(text="查無該客戶的未付項目。"))

            client_data = grouped[client_key]
            
            # Sort parent items by date (oldest first)
            sorted_dates = sorted(client_data["data"].items(), key=lambda x: x[0])
            
            remaining_amount = amount  # Amount left to distribute
            collector_name = "CY" if currency in ["ntd", "twd"] else "YL"
            
            # Track distribution results
            distribution_log = []
            
            mutation = """
            mutation ($board_id: ID!, $item_id: ID!, $col_id: String!, $val: String!) {
                change_simple_column_value (board_id: $board_id, item_id: $item_id, column_id: $col_id, value: $val) { id }
            }
            """
            
            # Distribute payment across parent items (oldest to newest)
            # Flatten the nested structure: bill_date -> parent_dates -> items
            for bill_date_str, bill_data in sorted_dates:
                if remaining_amount <= 0:
                    break
                
                # Iterate through parent_dates within each bill_date
                for parent_date_str, parent_group in sorted(bill_data.get("parent_dates", {}).items()):
                    if remaining_amount <= 0:
                        break
                    
                    items_list = parent_group.get("items", [])
                    if not items_list:
                        continue
                        
                    sample_item = items_list[0]
                    parent_id = sample_item.get("parent_id")
                    parent_board_id = sample_item.get("parent_board_id")
                    subitem_board_id = sample_item.get("board_id")
                    rate = sample_item.get("parent_rate", 1.0)
                    
                    # Calculate remaining balance for this parent item
                    remaining_balance_cad = parent_group.get("subtotal", 0)
                    
                    # Get existing paid amounts
                    existing_cad_paid = sample_item.get("parent_cad_paid", 0)
                    existing_twd_paid = sample_item.get("parent_twd_paid", 0)
                    
                    # Check if already fully paid
                    if remaining_balance_cad <= 0:
                        distribution_log.append(f"⏭️ {bill_date_str}/{parent_date_str}: 已全額付清，跳過")
                        continue
                    
                    # Calculate how much to apply to this parent item (in CAD)
                    if currency in ["ntd", "twd"]:
                        amount_to_apply_cad = remaining_amount / rate
                    else:
                        amount_to_apply_cad = remaining_amount
                    
                    # Determine actual amount to apply (cannot exceed remaining balance)
                    actual_applied_cad = min(amount_to_apply_cad, remaining_balance_cad)
                    actual_applied_original = actual_applied_cad * rate if currency in ["ntd", "twd"] else actual_applied_cad
                    
                    # Calculate new total paid amount
                    target_col = COL_TWD_PAID if currency in ["ntd", "twd"] else COL_CAD_PAID
                    col_id = _fetch_col_id_by_title(parent_board_id, target_col)
                    
                    if currency in ["ntd", "twd"]:
                        new_paid_amount = existing_twd_paid + actual_applied_original
                    else:
                        new_paid_amount = existing_cad_paid + actual_applied_original
                    
                    # Write paid amount to Monday
                    _monday_request(mutation, {
                        "board_id": int(parent_board_id), 
                        "item_id": int(parent_id), 
                        "col_id": col_id, 
                        "val": str(new_paid_amount)
                    })
                    
                    # Write collector name to 收款人 column
                    collector_col_id = _fetch_col_id_by_title(parent_board_id, COL_COLLECTOR)
                    _monday_request(mutation, {
                        "board_id": int(parent_board_id), 
                        "item_id": int(parent_id), 
                        "col_id": collector_col_id, 
                        "val": collector_name
                    })
                    
                    # Update remaining amount
                    remaining_amount -= actual_applied_original
                    
                    # Check if fully paid and update status
                    if actual_applied_cad >= remaining_balance_cad:
                        status_col_id = _fetch_col_id_by_title(subitem_board_id, COL_STATUS)
                        for sub in items_list:
                            _monday_request(mutation, {
                                "board_id": int(subitem_board_id), 
                                "item_id": int(sub["id"]), 
                                "col_id": status_col_id, 
                                "val": "已收款出貨"
                            })
                        distribution_log.append(f"✅ {bill_date_str}/{parent_date_str}: {currency.upper()} ${actual_applied_original:.2f} → 已全額收訖")
                    else:
                        new_remaining = remaining_balance_cad - actual_applied_cad
                        distribution_log.append(f"📝 {bill_date_str}/{parent_date_str}: {currency.upper()} ${actual_applied_original:.2f} → 仍欠 CAD ${new_remaining:.2f}")
            
            # Send summary message
            summary = f"💰 {target_client} 付款分配完成 (收款人: {collector_name})：\n\n" + "\n".join(distribution_log)
            if remaining_amount > 0.01:  # Small threshold for floating point comparison
                summary += f"\n\n⚠️ 尚有 {currency.upper()} ${remaining_amount:.2f} 未分配（所有項目已付清）"
            line_bot_api.push_message(sender_id, TextSendMessage(text=summary))

        except Exception as e:
            logging.error(f"Paid worker failed: {e}\n{traceback.format_exc()}")

    Thread(target=_paid_worker).start()