"""
Monday.com webhook handler for location status changes.

Handles webhook events when items change location to 'International Transport',
and sends notifications to the appropriate LINE group.
"""
import logging
import requests
from flask import request, jsonify

from config import MONDAY_API_TOKEN, CLIENT_TO_GROUP
from services.line_service import line_push

log = logging.getLogger(__name__)


def handle_monday_webhook():
    """
    Handle Monday.com webhook for location status changes.
    
    When an item's location changes to '國際運輸' (International Transport),
    this sends a notification to the appropriate LINE group based on client mapping.
    
    Returns:
        tuple: (response_body, status_code)
    """
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    evt = data.get("event", data)
    
    # Respond to Monday's handshake/challenge
    if "challenge" in data:
        return jsonify({"challenge": data["challenge"]}), 200

    # Extract item IDs
    sub_id = evt.get("pulseId") or evt.get("itemId")
    parent_id = evt.get("parentItemId")
    lookup_id = parent_id or sub_id
    new_txt = evt.get("value", {}).get("label", {}).get("text")

    # Only act when Location flips to 國際運輸 (International Transport)
    if new_txt != "國際運輸" or not lookup_id:
        return "OK", 200

    # Fetch the formula column to get client name
    gql = '''
    query ($itemIds: [ID!]!) {
      items(ids: $itemIds) {
        column_values(ids: ["formula8__1"]) {
          id
          text
          ... on FormulaValue { display_value }
        }
      }
    }'''
    variables = {"itemIds": [str(lookup_id)]}
    
    resp = requests.post(
        "https://api.monday.com/v2",
        json={"query": gql, "variables": variables},
        headers={
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json"
        }
    )
    data2 = resp.json()

    # Extract client name from column value
    try:
        cv = data2["data"]["items"][0]["column_values"][0]
        client = (cv.get("text") or cv.get("display_value") or "").strip()
    except (KeyError, IndexError) as e:
        log.error(f"[MondayLINE] Failed to extract client: {e}")
        return "OK", 200
        
    key = client.lower()  # e.g. "yumi" or "vicky"

    # Map client to LINE group
    group_id = CLIENT_TO_GROUP.get(key)
    if not group_id:
        log.warning(f"[MondayLINE] No mapping for client='{client}' key={key}, skipping.")
        return "OK", 200

    # Send notification
    item_name = evt.get("pulseName") or str(lookup_id)
    message = f" {item_name} 已送往機場，準備進行國際運輸。"

    line_push(group_id, message)
    log.info(f"[MondayLINE] Sent notification to {client}")

    return "OK", 200
