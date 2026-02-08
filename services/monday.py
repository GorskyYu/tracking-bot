import requests
import json
import logging
import os
from config import MONDAY_API_TOKEN

logger = logging.getLogger(__name__)

from typing import Optional, Dict, List, Any

MONDAY_API_URL = "https://api.monday.com/v2"

# Central Board Registry
# Defines the boards that are confirmed data boards and should be searchable.
BOARD_REGISTRY = [
    {"id": 8082685182, "name": "台加直寄", "flow_type": "AIR"},
    {"id": 8783157722, "name": "加台海運", "flow_type": "SEA"},
    {"id": 9359342674, "name": "ABB採購加台集運", "flow_type": "PROCUREMENT"},
    {"id": 8082569538, "name": "加境內直寄", "flow_type": "DOMESTIC"},  
    {"id": 4815120249, "name": "加台空運和直寄", "flow_type": "AIR_BOADCAST"},

]

# Static mapping to avoid API lookups
SUBITEM_BOARD_MAPPING = {
    8783157722: 8783157868,
    4815120249: 4815120249, # Placeholder 4815120355
    8082685182: 8082685182, # Placeholder 8082685244
    8082569538: 8082569581, # Canadian Domestic Shipping
}

def infer_flow_by_tracking(tracking_number: str) -> Optional[str]:
    """
    Infers the flow type based on the tracking number prefix/format.
    Returns a flow_type string (e.g. 'PROCUREMENT', 'AIR') or None if unknown.
    This function only narrows down candidate boards, not decide final ownership.
    """
    tn = tracking_number.upper().strip()
    
    # Simple heuristics
    if tn.startswith("ABB"):
        return "PROCUREMENT"
    
    # Add more heuristics here as needed
    if tn.startswith("1Z"):
        return "AIR" 
        
    return None

def _monday_request(query, variables=None):
    headers = {
        "Authorization": MONDAY_API_TOKEN,
        "API-Version": "2025-01"
    }
    data = {"query": query, "variables": variables}
    response = requests.post(MONDAY_API_URL, json=data, headers=headers)
    if response.status_code != 200:
        logger.error(f"Monday API Error: {response.status_code} {response.text}")
        return None
    try:
        result = response.json()
        logger.debug(f"Monday API Response: {result}")
        return result
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Monday API response: {e}, text: {response.text}")
        return None

def get_column_id_by_title(board_id, title):
    query = """
    query ($board_id: [ID!]) {
        boards (ids: $board_id) {
            columns {
                id
                title
            }
        }
    }
    """
    variables = {"board_id": [int(board_id)]}
    result = _monday_request(query, variables)
    if not result or "data" not in result:
        return None
    
    boards = result["data"]["boards"]
    if not boards:
        return None
        
    for col in boards[0]["columns"]:
        if col["title"].strip() == title.strip():
            return col["id"]
    return None

SUBITEM_BOARD_CACHE = {}

def get_subitem_board_id(parent_board_id):
    parent_board_id = int(parent_board_id)
    if parent_board_id in SUBITEM_BOARD_CACHE:
        return SUBITEM_BOARD_CACHE[parent_board_id]

    query = """
    query ($board_id: [ID!]) {
        boards (ids: $board_id) {
            columns {
                id
                type
                settings_str
            }
        }
    }
    """
    variables = {"board_id": [parent_board_id]}
    result = _monday_request(query, variables)

    if not result or "data" not in result or not result["data"]["boards"]:
        return None
    
    columns = result["data"]["boards"][0]["columns"]
    for col in columns:
        if col["type"] == "subtasks":
            try:
                settings = json.loads(col["settings_str"])
                if "boardIds" in settings and settings["boardIds"]:
                    subitem_board_id = settings["boardIds"][0]
                    SUBITEM_BOARD_CACHE[parent_board_id] = subitem_board_id
                    return subitem_board_id
            except Exception as e:
                logger.error(f"Error parsing subtasks settings: {e}")
                pass
    return None

def search_subitem_by_name(board_id, tracking_number):
    logger.info(f"[Subitem Search] Searching subitems in board {board_id} for {tracking_number}")
    
    subitem_board_id = get_subitem_board_id(board_id)
    if not subitem_board_id:
        logger.warning(f"[Subitem Search] Could not find subitem board for parent board {board_id}")
        return None

    target = tracking_number.strip()
    
    # Helper to search by column
    def search_by_col(col_id, val):
        query = """
        query ($board_id: ID!, $col_id: String!, $val: String!) {
            items_page_by_column_values (board_id: $board_id, limit: 1, columns: [{column_id: $col_id, column_values: [$val]}]) {
                items {
                    id
                    name
                    parent_item {
                        id
                        name
                    }
                }
            }
        }
        """
        variables = {
            "board_id": int(subitem_board_id),
            "col_id": col_id,
            "val": val
        }
        res = _monday_request(query, variables)
        if res and "data" in res and "items_page_by_column_values" in res["data"]:
            items = res["data"]["items_page_by_column_values"]["items"]
            if items:
                return items[0]
        return None

    # 1. Search by Name
    item = search_by_col("name", target)
    if item:
        parent = item.get("parent_item")
        if parent:
            logger.info(f"[Subitem Search] Found subitem by Name '{target}'")
            return {
                "id": item["id"],
                "name": item["name"],
                "parent_id": parent["id"],
                "parent_name": parent["name"],
            }
    
    # 2. Search by "廠商箱號"
    # We need to find the column ID for "廠商箱號" on the subitem board
    vendor_col_id = get_column_id_by_title(subitem_board_id, "廠商箱號")
    if vendor_col_id:
        item = search_by_col(vendor_col_id, target)
        if item:
            parent = item.get("parent_item")
            if parent:
                logger.info(f"[Subitem Search] Found subitem by 廠商箱號 '{target}'")
                return {
                    "id": item["id"],
                    "name": item["name"],
                    "parent_id": parent["id"],
                    "parent_name": parent["name"],
                }

    logger.info(f"[Subitem Search] Not found in subitem board {subitem_board_id}")
    return None

def search_item_by_tracking_number(board_id, tracking_number):
    logger.info(f"Searching board {board_id} for tracking number {tracking_number}")
    # First try to find a column named "Tracking Number" or "Tracking No"
    # If not found, assume it's the Name
    
    # For simplicity, let's try to search by column value if we can find the column
    # But searching by name is also common.
    
    # Let's try to find the item by name first (if tracking number is the name)
    # items_page_by_column_values is efficient.
    
    # Strategy:
    # 1. Get all columns to find "Tracking Number" column ID.
    # 2. If found, search by that column.
    # 3. If not found, search by Name (which is column "name").
    # 4. Also search subitems by name.
    
    col_id = get_column_id_by_title(board_id, "Tracking Number")
    if not col_id:
        col_id = get_column_id_by_title(board_id, "Tracking No")
        
    if col_id:
        logger.info(f"Searching by column {col_id} for {tracking_number}")
        query = """
        query ($board_id: ID!, $col_id: String!, $value: String!) {
            items_page_by_column_values (board_id: $board_id, columns: [{column_id: $col_id, column_values: [$value]}]) {
                items {
                    id
                    name
                }
            }
        }
        """
        variables = {
            "board_id": int(board_id),
            "col_id": col_id,
            "value": tracking_number
        }
        result = _monday_request(query, variables)
        if result and "data" in result and result["data"]["items_page_by_column_values"]["items"]:
            logger.info(f"Found item by column {col_id}: {result['data']['items_page_by_column_values']['items'][0]['name']}")
            return result["data"]["items_page_by_column_values"]["items"][0]
        else:
            logger.info(f"No items found by column {col_id}")
            
    # Fallback: Search by Name (using items_page_by_column_values with column_id "name")
    logger.info(f"Searching by Name for {tracking_number}")
    query = """
    query ($board_id: ID!, $value: String!) {
        items_page_by_column_values (board_id: $board_id, columns: [{column_id: "name", column_values: [$value]}]) {
            items {
                id
                name
            }
        }
    }
    """
    variables = {
        "board_id": int(board_id),
        "value": tracking_number
    }
    result = _monday_request(query, variables)
    if result and "data" in result and result["data"]["items_page_by_column_values"]["items"]:
        logger.info(f"Found item by Name: {result['data']['items_page_by_column_values']['items'][0]['name']}")
        return result["data"]["items_page_by_column_values"]["items"][0]
    else:
        logger.info(f"No items found by Name")

    # Final fallback: search subitems in the CURRENT board
    logger.info(f"Falling back to subitem search in board {board_id}")

    subitem_result = search_subitem_by_name(
        board_id,
        tracking_number
    )

    if subitem_result:
        return subitem_result

    return None

def find_tracking_across_boards(tracking_number: str) -> Optional[Dict[str, Any]]:
    """
    Searches for a tracking number across all registered boards.
    First narrows down by flow type if possible.
    Returns structured info including board_id, board_name, and matched item.
    """
    # 1. Infer flow type to narrow down search
    flow_type = infer_flow_by_tracking(tracking_number)
    
    candidate_boards = BOARD_REGISTRY
    if flow_type:
        # Filter boards by flow type
        candidate_boards = [b for b in BOARD_REGISTRY if b["flow_type"] == flow_type]
        logger.info(f"Narrowed search to {len(candidate_boards)} boards for flow {flow_type}")
    
    # If narrowing resulted in no boards (e.g. config error), fallback to all
    if not candidate_boards:
        candidate_boards = BOARD_REGISTRY
        
    # 2. Iterate through candidate boards
    for board in candidate_boards:
        board_id = board["id"]
        board_name = board["name"]
        
        logger.info(f"Checking board: {board_name} ({board_id})")
        # 3. Search in the specific board
        match = search_item_by_tracking_number(board_id, tracking_number)
        
        if match:
            logger.info(f"Found match in board {board_name} ({board_id})")
            # 4. Return structured info immediately on match
            return {
                "board_id": board_id,
                "board_name": board_name,
                "item": match
            }
            
    return None

def update_monday_item(board_id, item_id, updates):
    # updates is a dict of {column_title: value}
    # We need to map titles to IDs
    
    col_map = {}
    query = """
    query ($board_id: [ID!]) {
        boards (ids: $board_id) {
            columns {
                id
                title
            }
        }
    }
    """
    variables = {"board_id": [int(board_id)]}
    result = _monday_request(query, variables)
    if result and "data" in result and result["data"]["boards"]:
        for col in result["data"]["boards"][0]["columns"]:
            col_map[col["title"].strip()] = col["id"]
            
    column_values = {}
    for title, value in updates.items():
        col_id = col_map.get(title)
        if col_id:
            column_values[col_id] = value
        else:
            logger.warning(f"Column '{title}' not found in board {board_id}")
            
    if not column_values:
        return False
        
    mutation = """
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
    
    result = _monday_request(mutation, variables)
    if result and "data" in result and result["data"]["change_multiple_column_values"]:
        return True
    else:
        logger.error(f"Failed to update item: {result}")
        return False

def create_update(item_id, body):
    mutation = """
    mutation ($item_id: ID!, $body: String!) {
        create_update (item_id: $item_id, body: $body) {
            id
        }
    }
    """
    variables = {"item_id": int(item_id), "body": body}
    result = _monday_request(mutation, variables)
    if result and "data" in result and "create_update" in result["data"]:
        return result["data"]["create_update"]["id"]
    return None

def upload_file_to_update(update_id, file_path):
    """
    Uploads a file to a Monday.com update (item update).
    Uses the correct multipart/form-data flow with 'operations' and 'map'.
    """
    url = "https://api.monday.com/v2/file"
    
    headers = {
        "Authorization": MONDAY_API_TOKEN
    }
    
    # Mutation to add file to update
    # Note: update_id is ID! type
    query = "mutation ($update_id: ID!, $file: File!) { add_file_to_update (update_id: $update_id, file: $file) { id } }"

    try:
        int_update_id = int(update_id)
    except ValueError:
        logger.error(f"Invalid update_id: {update_id}")
        return None

    # 1. operations: JSON string with query and variables
    operations = json.dumps({
        "query": query,
        "variables": {
            "update_id": int_update_id,
            "file": None
        }
    })

    # 2. map: JSON string mapping file key to variable path
    # Standard GraphQL multipart convention uses "0" as key
    map_data = json.dumps({
        "0": ["variables.file"]
    })

    filename = os.path.basename(file_path)
    import mimetypes
    mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    try:
        with open(file_path, 'rb') as f:
            # 3. files: Dict mapping key from 'map' to file tuple
            files = {
                "0": (filename, f, mime_type)
            }
            
            data = {
                "operations": operations,
                "map": map_data
            }
            
            logger.debug(f"Uploading file {filename} to update {update_id}")
            
            response = requests.post(
                url,
                headers=headers,
                data=data,
                files=files
            )

        if response.status_code == 200:
            result = response.json()
            if "data" in result and result["data"].get("add_file_to_update"):
                logger.info(f"Successfully uploaded file {filename} to update {update_id}")
                return result["data"]["add_file_to_update"]["id"]
            else:
                logger.error(f"Upload failed, response: {result}")
        else:
            logger.error(f"Monday API File Upload Error: {response.status_code} {response.text}")

    except Exception as e:
        logger.exception(f"Exception during file upload: {e}")

    return None

def post_image_to_item_update(item_id, file_path, message="Attached image"):
    """
    Two-step process:
    1. Creates a text update on the item.
    2. Uploads the file to that specific update.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None

    # --- Step 1: Create the Text Update ---
    create_update_query = """
    mutation ($item_id: ID!, $body: String!) {
        create_update (item_id: $item_id, body: $body) {
            id
        }
    }
    """
    
    # Standard JSON request for text update
    update_variables = {"item_id": int(item_id), "body": message}
    headers = {
        "Authorization": MONDAY_API_TOKEN,
        "API-Version": "2023-10"
    }
    
    # We use requests.post with 'json' here
    resp = requests.post(
        MONDAY_API_URL, 
        json={"query": create_update_query, "variables": update_variables}, 
        headers=headers
    )
    
    if resp.status_code != 200:
        logger.error(f"Failed to create update: {resp.text}")
        return None
        
    try:
        data = resp.json()
        if "errors" in data:
            logger.error(f"Monday API returned errors: {data['errors']}")
            return None
        update_id = data["data"]["create_update"]["id"]
        logger.info(f"Created update {update_id} on item {item_id}. Now uploading file...")
    except Exception as e:
        logger.error(f"Error parsing create_update response: {e}")
        return None

    # --- Step 2: Upload File to the Update ---
    
    # This specific mutation attaches a file to an existing Update
    file_query = """
    mutation ($update_id: ID!, $file: File!) {
        add_file_to_update (update_id: $update_id, file: $file) {
            id
            name
        }
    }
    """

    # PREPARE MULTIPART REQUEST
    # The 'map' key tells Monday which variable in the query ('$file') 
    # maps to the binary file part named 'image' in the multipart data.
    payload = {
        'query': file_query,
        'variables': json.dumps({'update_id': int(update_id)}),
        'map': json.dumps({"image": ["variables.file"]}) 
    }

    # IMPORTANT: Do NOT include 'Content-Type' in headers for multipart uploads.
    # The requests library must generate the boundary string automatically.
    upload_headers = {
        "Authorization": MONDAY_API_TOKEN,
        "API-Version": "2023-10"
    }

    try:
        with open(file_path, 'rb') as f:
            # The key 'image' matches the key inside the 'map' dictionary above
            files = {'image': (os.path.basename(file_path), f)}
            
            file_resp = requests.post(
                "https://api.monday.com/v2/file", # Note: specialized endpoint for files
                data=payload, 
                files=files, 
                headers=upload_headers
            )
            
        if file_resp.status_code == 200:
            result = file_resp.json()
            if "data" in result and result["data"]["add_file_to_update"]:
                logger.info(f"Successfully uploaded {file_path} to update {update_id}")
                return result
            else:
                logger.error(f"File upload logic error: {result}")
                return None
        else:
            logger.error(f"File upload HTTP error: {file_resp.status_code} {file_resp.text}")
            return None
            
    except Exception as e:
        logger.error(f"Exception during file upload: {e}")
        return None

def get_item_columns(item_id):
    query = """
    query ($item_id: [ID!]) {
        items (ids: $item_id) {
            column_values {
                id
                text
                column {
                    title
                }
            }
        }
    }
    """
    variables = {"item_id": [int(item_id)]}
    result = _monday_request(query, variables)
    if result and "data" in result and result["data"]["items"]:
        item = result["data"]["items"][0]
        columns = {}
        for cv in item["column_values"]:
            if cv["column"] and cv["column"]["title"]:
                columns[cv["column"]["title"]] = cv["text"]
        return columns
    return None

def map_column_values(column_values: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Helper function to convert a list of column values to a dictionary {Title: Text}.
    """
    result = {}
    if not column_values:
        return result
        
    for cv in column_values:
        if not cv.get("column") or not cv["column"].get("title"):
            continue
        title = cv["column"]["title"].strip()
        text = cv.get("text") or ""
        result[title] = text
        
    return result

def search_subitem_by_vendor_box(subitem_board_id: int, vendor_box_number: str) -> Optional[Dict[str, Any]]:
    """
    Searches for a subitem by '廠商箱號' directly on the subitem board.
    Fetches parent item details in the same query.
    """
    logger.info(f"[Subitem Search] Searching subitem board {subitem_board_id} for vendor box {vendor_box_number}")
    
    # 1. Get the column ID for "廠商箱號"
    col_id = get_column_id_by_title(subitem_board_id, "廠商箱號")
    if not col_id:
        logger.warning(f"Column '廠商箱號' not found in board {subitem_board_id}")
        return None
        
    # 2. Query items_page_by_column_values
    query = """
    query ($board_id: ID!, $col_id: String!, $value: String!) {
        items_page_by_column_values (board_id: $board_id, columns: [{column_id: $col_id, column_values: [$value]}]) {
            items {
                id
                name
                column_values {
                    text
                    column {
                        title
                    }
                }
                parent_item {
                    id
                    name
                    column_values {
                        text
                        column {
                            title
                        }
                    }
                }
            }
        }
    }
    """
    variables = {
        "board_id": int(subitem_board_id),
        "col_id": col_id,
        "value": vendor_box_number
    }
    
    result = _monday_request(query, variables)
    
    if not result or "data" not in result or not result["data"]["items_page_by_column_values"]["items"]:
        logger.info(f"[Subitem Search] No subitems found for {vendor_box_number}")
        return None
        
    # 3. Process the first match
    item = result["data"]["items_page_by_column_values"]["items"][0]
    
    subitem_values = map_column_values(item.get("column_values", []))
    
    parent_item = item.get("parent_item")
    parent_values = {}
    parent_info = {}
    
    if parent_item:
        parent_values = map_column_values(parent_item.get("column_values", []))
        parent_info = {
            "parent_id": parent_item["id"],
            "parent_name": parent_item["name"]
        }
        
    # Merge values, handling duplicates by prefixing parent columns
    merged_values = subitem_values.copy()
    for k, v in parent_values.items():
        if k in merged_values:
            merged_values[f"Parent {k}"] = v
        else:
            merged_values[k] = v
            
    return {
        "id": item["id"],
        "name": item["name"],
        **parent_info,
        "values": merged_values,
        "subitem_raw_values": subitem_values,
        "parent_raw_values": parent_values
    }

def search_subitem_efficiently(target):
    """
    Efficiently searches for a subitem across known boards using items_page_by_column_values.
    Returns the first match found.
    """
    target = target.strip()
    
    # 1. Identify all parent boards to check
    # Start with the static mapping keys
    parent_boards_to_check = list(SUBITEM_BOARD_MAPPING.keys())
    
    # Add the specific board mentioned by user if not present
    # 4814336467 is a known parent board that might need dynamic resolution
    if 4814336467 not in parent_boards_to_check:
        parent_boards_to_check.append(4814336467)
        
    # Add any other boards from registry to be thorough
    for board in BOARD_REGISTRY:
        bid = board["id"]
        if bid not in parent_boards_to_check:
            parent_boards_to_check.append(bid)

    # Iterate through all potential parent boards
    for parent_id in parent_boards_to_check:
        # 2. Resolve Subitem Board ID
        # First check static mapping
        sub_board_id = SUBITEM_BOARD_MAPPING.get(parent_id)
        
        # If not in mapping, try dynamic discovery
        if not sub_board_id:
            try:
                sub_board_id = get_subitem_board_id(parent_id)
            except Exception as e:
                logger.warning(f"[Subitem Search] Failed to resolve subitem board for parent {parent_id}: {e}")
                continue
        
        if not sub_board_id:
            continue
            
        logger.info(f"[Subitem Search] Scanning sub-board {sub_board_id} (Parent: {parent_id}) for box: {target}")

        # 3. GraphQL Query (Single Trip)
        # Fetches subitem + parent item columns in one go
        # Uses strict type [String]! for column_values
        query = """
        query ($board_id: ID!, $col_id: String!, $val: [String]!) {
            items_page_by_column_values (
                board_id: $board_id, 
                columns: [{column_id: $col_id, column_values: $val}],
                limit: 1
            ) {
                items {
                    id
                    name
                    column_values {
                        text
                        column { title }
                    }
                    parent_item {
                        id
                        name
                        column_values {
                            text
                            column { title }
                        }
                    }
                }
            }
        }
        """
        # Define columns to search in priority order: Vendor Box Number, then Name
        search_columns = ["text57__1", "name"]

        for col_id in search_columns:
            variables = {
                "board_id": int(sub_board_id),
                "col_id": col_id,
                "val": [target]
            }

            try:
                result = _monday_request(query, variables)
                
                # 4. Error Handling
                if result and "errors" in result:
                    error_msg = str(result["errors"])
                    # Handle InvalidBoardIdException or similar API errors
                    if "InvalidBoardIdException" in error_msg or "Board not found" in error_msg:
                        logger.warning(f"[Subitem Search] Invalid board ID {sub_board_id} for parent {parent_id}. Removing from cache.")
                        if parent_id in SUBITEM_BOARD_CACHE:
                            del SUBITEM_BOARD_CACHE[parent_id]
                        break
                    else:
                        logger.error(f"[Subitem Search] GraphQL error on board {sub_board_id}: {error_msg}")
                        continue

                if result and "data" in result and result["data"].get("items_page_by_column_values"):
                    items = result["data"]["items_page_by_column_values"]["items"]
                    if items:
                        item = items[0]
                        logger.info(f"[Subitem Search] MATCH FOUND in board {sub_board_id} using column {col_id}")
                        
                        sub_cols = map_column_values(item.get("column_values", []))
                        parent_item = item.get("parent_item") or {}
                        parent_cols = map_column_values(parent_item.get("column_values", []))
                        
                        return {
                            "id": item["id"],
                            "name": item["name"],
                            "parent_id": parent_item.get("id"),
                            "parent_name": parent_item.get("name"),
                            "subitem_cols": sub_cols,
                            "parent_cols": parent_cols
                        }
                        
            except Exception as e:
                logger.error(f"[Subitem Search] Unexpected error searching board {sub_board_id}: {e}")
                continue
                
    return None




def rename_monday_item(board_id, item_id, new_name):
    """
    Renames a Monday item using change_multiple_column_values.
    """
    query = """
    mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
        change_multiple_column_values (item_id: $item_id, board_id: $board_id, column_values: $column_values) {
            id
            name
        }
    }
    """
    variables = {
        "board_id": int(board_id),
        "item_id": int(item_id),
        "column_values": json.dumps({"name": new_name})
    }
    
    result = _monday_request(query, variables)
    if not result or "data" not in result or not result["data"]["change_multiple_column_values"]:
        logger.error(f"Failed to rename item {item_id}: {result}")
        return False
        
    logger.info(f"Successfully renamed item {item_id} to '{new_name}'")
    return True
