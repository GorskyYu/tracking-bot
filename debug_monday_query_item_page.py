import os
import json
import logging
import requests
from config import MONDAY_API_TOKEN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONDAY_API_URL = "https://api.monday.com/v2"

def _monday_request(query, variables=None):
    headers = {
        "Authorization": MONDAY_API_TOKEN,
        "API-Version": "2023-10",
        "Content-Type": "application/json"
    }
    data = {"query": query, "variables": variables}
    response = requests.post(MONDAY_API_URL, json=data, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Monday API Error: {response.text}")
        return None

def test_items_page_query(board_id):
    logger.info(f"Testing items_page query on board {board_id}")

    # 1. Fetch Columns to find Status Column ID
    query_cols = """
    query ($board_id: ID!) {
        boards (ids: [$board_id]) {
            columns {
                id
                title
                type
            }
        }
    }
    """
    res = _monday_request(query_cols, {"board_id": board_id})
    if not res or "data" not in res:
        logger.error("Failed to fetch columns")
        return

    columns = res["data"]["boards"][0]["columns"]
    status_col_id = next((col["id"] for col in columns if col["title"] == "Status"), None)
    logger.info(f"Status Column ID: {status_col_id}")

    if not status_col_id:
        logger.error("Status column not found")
        # specific for this board
        status_col_id = "status"

    # 2. Test items_page with rules (Name contains "折讓" OR Status is empty?)
    # Note: items_page query usually supports basic filtering. If we need OR logic (Name OR Status), it might be tricky.
    # Monday API `items_page` `query_params` supports `rules`.
    # Let's try to fetch items containing "折讓" or "Discount" in name first.
    
    query_name_contains = """
    query ($board_id: ID!, $val: String!) {
        boards (ids: [$board_id]) {
            items_page (
                query_params: {
                    rules: [
                        {column_id: "name", compare_value: [$val], operator: contains_text}
                    ]
                }
            ) {
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    
    logger.info("Searching for items containing 'Discount'...")
    res_discount = _monday_request(query_name_contains, {"board_id": board_id, "val": "Discount"})
    if res_discount and "data" in res_discount:
        items = res_discount["data"]["boards"][0]["items_page"]["items"]
        logger.info(f"Found {len(items)} items containing 'Discount'")
        for item in items[:3]:
            logger.info(f"  - {item['name']}")

    logger.info("Searching for items containing '折讓'...")
    res_zherang = _monday_request(query_name_contains, {"board_id": board_id, "val": "折讓"})
    if res_zherang and "data" in res_zherang:
        items = res_zherang["data"]["boards"][0]["items_page"]["items"]
        logger.info(f"Found {len(items)} items containing '折讓'")
        for item in items[:3]:
            logger.info(f"  - {item['name']}")
            
    # 3. Test searching for empty status
    # This is tricky. compare_value for status might need index or label.
    # Operator `is_empty` might exist? Documentation says: `is_empty`, `is_not_empty` for some column types.
    
    query_status_empty = """
    query ($board_id: ID!, $col_id: ID!) {
        boards (ids: [$board_id]) {
            items_page (
                query_params: {
                    rules: [
                        {column_id: $col_id, operator: is_empty}
                    ]
                }
            ) {
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    
    logger.info(f"Searching for items with empty status ({status_col_id})...")
    res_empty_status = _monday_request(query_status_empty, {"board_id": board_id, "col_id": status_col_id})
    if res_empty_status and "data" in res_empty_status:
        # Check if error or success
        if "errors" in res_empty_status:
             logger.error(f"Error: {res_empty_status['errors']}")
        else:
            items = res_empty_status["data"]["boards"][0]["items_page"]["items"]
            logger.info(f"Found {len(items)} items with empty status")
            for item in items[:3]:
                 logger.info(f"  - {item['name']} Status: {[cv['text'] for cv in item['column_values'] if cv['id'] == status_col_id]}")


if __name__ == "__main__":
    # Test on a known subitem board ID
    # Using 8783157868 from SUBITEM_BOARD_MAPPING which seems to be subitem board for 8783157722
    test_items_page_query(8783157868)
