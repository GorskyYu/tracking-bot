from services.monday import _monday_request, get_subitem_board_id, SUBITEM_BOARD_MAPPING
from utils.permissions import is_authorized_for_event
from utils.line_reply import reply_text
from config import line_bot_api
from linebot.models import TextSendMessage, QuickReply, QuickReplyButton, MessageAction, FlexSendMessage, BubbleContainer, BoxComponent, TextComponent, SeparatorComponent
from threading import Thread
import logging
import json
import re

# 從 config 匯入所有相關群組 ID
from config import (
    line_bot_api, 
    IRIS_GROUP_ID, 
    VICKY_GROUP_ID, 
    YUMI_GROUP_ID,
    YVES_USER_ID,
    GORSKY_USER_ID
)

# 建立對應表：只要指令來自這個群組，就自動查詢對應的名稱
GROUP_TO_CLIENT_MAP = {
    IRIS_GROUP_ID: "Lammond",
    VICKY_GROUP_ID: "Vicky",
    YUMI_GROUP_ID: "Yumi",
    # 未來想加新的群組，直接在下面加一行即可
    # os.getenv("LINE_GROUP_ID_ABC"): "ABC_Client",
}

CLIENT_ALIASES = {
    "Yumi - Shu-Yen Liu": "Yumi - Shu-Yen (Yumi) Liu"
}

TARGET_BOARD_IDS = [4815120249, 8783157722]
TARGET_STATUSES = ["溫哥華收款", "未收款出貨", "台中收款"]

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
COL_EXCHANGE = "匯率"
COL_BILL_DATE = "出賬日"

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
            if not res or "data" not in res or not res["data"]["items_page_by_column_values"]:
                 break
                 
            page_data = res["data"]["items_page_by_column_values"]
            items = page_data["items"]
            cursor = page_data.get("cursor")
            
            logging.info(f"Fetched {len(items)} items from board {subitem_board_id}. Next Cursor: {bool(cursor)}")

            for item in items:
                # ✅ 直接調用獨立出的處理函式
                processed = _process_monday_item(item)
                
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

def _group_items_by_client(items, filter_name=None):
    """
    Groups items by Client -> Date.
    Returns: { canonical_name: { display, total, dates: { date: { items:[], subtotal } } } }
    """
    raw_clients = {} 

    for item in items:
        # Parse Parent Name
        raw_parent = item["parent_name"]
        match = re.match(r'^(\d+)\s+(.*)$', raw_parent.strip())
        if match:
            date_str = match.group(1)
            client_name = match.group(2)
        else:
            date_str = ""
            client_name = raw_parent
        
        # 只要名稱中包含關鍵字，就統一歸類到該客戶下
        found_canonical = False
        for main_name in ["Vicky", "Yumi", "Lammond"]:
            if main_name in client_name:
                canonical_name = main_name
                found_canonical = True
                break
        
        if not found_canonical:
            canonical_name = _resolve_client_name(client_name)
        
        # Filter Logic
        if filter_name and filter_name != "All":
             if filter_name not in canonical_name: 
                 continue

        if canonical_name not in raw_clients:
            raw_clients[canonical_name] = {
                "display_name": canonical_name,
                "data": {},
                "total": 0.0
            }
             
        client_data = raw_clients[canonical_name]
        
        if date_str not in client_data["data"]:
            # 紀錄母項目的實收總額
            rate = item.get("parent_rate", 1.0)
            if rate <= 0: rate = 1.0
            total_paid_cad = item.get("parent_cad_paid", 0) + (item.get("parent_twd_paid", 0) / rate)
            
            client_data["data"][date_str] = {
                "items": [], 
                "subtotal": 0.0,
                "paid_amount": total_paid_cad # 該筆貨物已付總額 (CAD)
            }
            # 預扣除實收
            client_data["data"][date_str]["subtotal"] -= total_paid_cad
            client_data["total"] -= total_paid_cad
            
        client_data["data"][date_str]["items"].append(item)
        client_data["data"][date_str]["subtotal"] += item["price_val"]
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
        # 2. 第二行：規格 (尺寸 | 重量) 
        dims = item.get("dimensions", "").strip()
        weight = item.get("weight", "").strip()
        if dims or weight:
            dims_display = f"{dims} cm" if dims and not dims.lower().endswith("cm") else dims
            weight_display = f"{weight} kg" if weight and not weight.lower().endswith("kg") else weight
            # 使用分隔符結合，如果只有一個則不顯示符號
            specs_text = " | ".join(filter(None, [dims_display, weight_display]))
            row_contents.append(
                BoxComponent(
                    layout='horizontal',
                    contents=[
                        TextComponent(text=specs_text, size='xs', color='#aaaaaa', flex=1)
                    ]
                )
            )
    
    return BoxComponent(layout='vertical', margin='md', contents=row_contents)

def _create_client_flex_message(client_obj):
    """Builds a Flex Bubble for a single client."""
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
    
    sorted_dates = sorted(dates_data.keys())
    for i, date_key in enumerate(sorted_dates):
        group_data = dates_data[date_key]
        
        # Add Separator between dates
        if i > 0:
            body_contents.append(SeparatorComponent(margin='lg'))

        # Date Header
        if date_key:
            body_contents.append(
                TextComponent(text=date_key, weight='bold', margin='lg', size='md', color='#555555')
            )
            
        # Items
        for item in group_data["items"]:
            body_contents.append(_create_item_row(item))
            
        # 只要實收不為 0 就顯示，並根據母項目名稱判定是否顯示 "Discount"
        if group_data["paid_amount"] != 0:
            # 檢查該組包裹中是否包含「折讓」母項目
            is_discount = any("折讓" in item.get("parent_name", "") for item in group_data["items"])
            label_text = "Discount" if is_discount else "Paid (Already Received)"

            body_contents.append(
                BoxComponent(
                    layout='horizontal',
                    margin='md',
                    contents=[
                        TextComponent(text=label_text, flex=4, size='sm', color='#1DB446'),
                        TextComponent(text=f"-${group_data['paid_amount']:.2f}", flex=2, align='end', size='sm', color='#1DB446', weight='bold')
                    ]
                )
            )
            
        # Date Subtotal
        body_contents.append(SeparatorComponent(margin='sm'))
        body_contents.append(
            BoxComponent(
                layout='horizontal',
                margin='sm',
                contents=[
                    TextComponent(text="Subtotal", flex=4, size='sm', color='#555555'),
                    TextComponent(text=f"${group_data['subtotal']:.2f}", flex=2, align='end', size='sm', weight='bold')
                ]
            )
        )

    # Footer (Total)
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
                    TextComponent(text=f"${total:.2f}", flex=3, align='end', size='lg', weight='bold', color='#FF4B4B')
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

def _unpaid_worker(destination_id, filter_name=None):
    """Background thread worker."""
    try:
        results = fetch_unpaid_items_globally()
        
        if not results:
             line_bot_api.push_message(destination_id, TextSendMessage(text="沒有發現符合條件的項目（箱子尺寸與重量皆不為空，且狀態符合作業需求）。"))
             return

        # Group Data
        grouped_clients = _group_items_by_client(results, filter_name)
        
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
    is_admin = (user_id == YVES_USER_ID or user_id == GORSKY_USER_ID) # 這裡需確保有匯入變數
    auto_target_name = GROUP_TO_CLIENT_MAP.get(group_id)
    
    parts = message_text.strip().split()
    
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
        target_name = " ".join(parts[1:]) 
        reply_text(reply_token, f"🔍 正在搜尋未付款項目 ({target_name})，請稍候...")
        target_id = group_id if group_id else sender_id
        t = Thread(target=_unpaid_worker, args=(target_id, target_name))
        t.start()
        return

    # If no args, Ask Question with Quick Reply, 如果都不符合 (例如私訊且沒帶參數)，才顯示 Quick Reply 選單
    buttons = [
        QuickReplyButton(action=MessageAction(label="All", text=f"{cmd} All")),
        QuickReplyButton(action=MessageAction(label="Vicky", text=f"{cmd} Vicky")),
        QuickReplyButton(action=MessageAction(label="Yumi", text=f"{cmd} Yumi")),
        QuickReplyButton(action=MessageAction(label="Iris", text=f"{cmd} Lammond"))
    ]
    
    text_message = TextSendMessage(
        text="請輸入要查詢的名稱",
        quick_reply=QuickReply(items=buttons)
    )
    
    line_bot_api.reply_message(reply_token, text_message)

def fetch_items_by_bill_date(target_date_yyyymmdd):
    """
    依照「出賬日」搜尋所有板塊的項目
    """
    items_found = []
    # 轉換日期格式：260120 -> 2026-01-20 (以匹配 Monday Date 格式)
    formatted_date = f"20{target_date_yyyymmdd[:2]}-{target_date_yyyymmdd[2:4]}-{target_date_yyyymmdd[4:]}"
    
    for parent_board_id in TARGET_BOARD_IDS:
        subitem_board_id = SUBITEM_BOARD_MAPPING.get(parent_board_id) or get_subitem_board_id(parent_board_id)
        if not subitem_board_id: continue

        # 這裡沿用你的 _fetch_col_id_by_title 邏輯，但改為抓取「出賬日」的 ID
        bill_date_col_id = _fetch_col_id_by_title(subitem_board_id, COL_BILL_DATE)
        if not bill_date_col_id: continue

        # 搜尋 Query：篩選出賬日等於目標日期的項目
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
                        name
                        column_values { ... on FormulaValue { display_value } text column { title } }
                    }
                }
            }
        }
        """
        res = _monday_request(query, {"board_id": int(subitem_board_id), "col_id": bill_date_col_id, "val": formatted_date})
        
        if res and "data" in res and res["data"]["items_page_by_column_values"]:
            for item in res["data"]["items_page_by_column_values"]["items"]:
                # ✅ 直接調用共用的處理邏輯
                processed = _process_monday_item(item)
                if processed: items_found.append(processed)
                
    return items_found

def _process_monday_item(item):
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
        
        return {
            "parent_name": parent_name,
            "sub_name": sub_name,
            "price_text": subitem_cols.get(COL_PRICE, "0"),
            "price_val": _extract_float(subitem_cols.get(COL_PRICE, "0")),
            "dimensions": dim_val,
            "weight": weight_val,
            "parent_cad_paid": _extract_float(parent_cols.get(COL_CAD_PAID, "0")),
            "parent_twd_paid": _extract_float(parent_cols.get(COL_TWD_PAID, "0")),
            "parent_rate": rate
        }
    return None

def _bill_worker(destination_id, client_filter, date_val):
    """查看賬單的背景執行程序"""
    try:
        results = fetch_items_by_bill_date(date_val)
        if not results:
            line_bot_api.push_message(destination_id, TextSendMessage(text=f"📅 {date_val} 沒有找到任何出賬項目。"))
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
    is_admin = (user_id == YVES_USER_ID or user_id == GORSKY_USER_ID)
    auto_client = GROUP_TO_CLIENT_MAP.get(group_id)

    # 1. 群組模式：查看賬單YYMMDD
    group_match = re.match(r"查看賬單(\d{6})", text)
    if group_id and group_match:
        date_val = group_match.group(1)
        if not auto_client:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="⛔ 此群組未對應客戶。"))
            return
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"🔍 正在抓取 {auto_client} 的 {date_val} 賬單..."))
        Thread(target=_bill_worker, args=(group_id, auto_client, date_val)).start()
        return

    # 2. 私訊模式 (僅限管理員)
    if not group_id and is_admin:
        if text == "查看賬單":
            buttons = [
                QuickReplyButton(action=MessageAction(label="Vicky", text="查看賬單 Vicky")),
                QuickReplyButton(action=MessageAction(label="Yumi", text="查看賬單 Yumi")),
                QuickReplyButton(action=MessageAction(label="Iris", text="查看賬單 Lammond"))
            ]
            line_bot_api.reply_message(reply_token, TextSendMessage(text="請選擇客戶：", quick_reply=QuickReply(items=buttons)))
        elif text.startswith("查看賬單 "):
            parts = text.split()
            if len(parts) == 2: # 點選了客戶，提示輸入日期
                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"請補上日期 (YYMMDD)\n例如：查看賬單 {parts[1]} 260120"))
            elif len(parts) == 3: # 完整指令
                client, date_val = parts[1], parts[2]
                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"🔍 正在抓取 {client} 的 {date_val} 賬單..."))
                Thread(target=_bill_worker, args=(user_id, client, date_val)).start()
