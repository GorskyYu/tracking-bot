import requests
import logging
from typing import Optional, Dict, Any, List

from config import LINE_PUSH_URL, LINE_REPLY_URL, LINE_HEADERS

log = logging.getLogger(__name__)


def line_push(target_id: str, text: str) -> requests.Response:
    """Send a LINE push message to a user or group.
    
    Args:
        target_id: LINE user ID or group ID
        text: Message text to send
        
    Returns:
        Response from LINE API
    """
    payload = {
        "to": target_id,
        "messages": [{"type": "text", "text": text}]
    }
    resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    log.info(f"[line_push] to {target_id}: {resp.status_code}")
    return resp


def line_reply(reply_token: str, text: str) -> requests.Response:
    """Reply to a LINE message using reply token.
    
    Args:
        reply_token: Reply token from webhook event
        text: Message text to send
        
    Returns:
        Response from LINE API
    """
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}]
    }
    resp = requests.post(LINE_REPLY_URL, headers=LINE_HEADERS, json=payload)
    log.info(f"[line_reply] status: {resp.status_code}")
    return resp


def line_push_flex(target_id: str, alt_text: str,
                   contents: dict) -> requests.Response:
    """Push a Flex Message to a user or group.

    Args:
        target_id: LINE user ID or group ID
        alt_text: Fallback text shown in notifications
        contents: Flex Message contents (bubble or carousel dict)

    Returns:
        Response from LINE API
    """
    payload = {
        "to": target_id,
        "messages": [{
            "type": "flex",
            "altText": alt_text,
            "contents": contents,
        }],
    }
    resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    log.info(f"[line_push_flex] to {target_id}: {resp.status_code}")
    return resp


def line_reply_flex(reply_token: str, alt_text: str,
                    contents: dict) -> requests.Response:
    """Reply with a Flex Message using a reply token.

    Args:
        reply_token: Reply token from webhook event
        alt_text: Fallback text shown in notifications
        contents: Flex Message contents (bubble or carousel dict)

    Returns:
        Response from LINE API
    """
    payload = {
        "replyToken": reply_token,
        "messages": [{
            "type": "flex",
            "altText": alt_text,
            "contents": contents,
        }],
    }
    resp = requests.post(LINE_REPLY_URL, headers=LINE_HEADERS, json=payload)
    log.info(f"[line_reply_flex] status: {resp.status_code}")
    return resp


def line_push_messages(target_id: str,
                       messages: List[Dict[str, Any]]) -> requests.Response:
    """Push multiple messages (text, flex, etc.) in a single API call.

    Args:
        target_id: LINE user ID or group ID
        messages: List of LINE message objects (max 5)

    Returns:
        Response from LINE API
    """
    payload = {
        "to": target_id,
        "messages": messages[:5],  # LINE allows max 5 messages per push
    }
    resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    if resp.status_code >= 400:
        log.error(f"[line_push_messages] failed to {target_id}: {resp.status_code} {resp.text}")
    else:
        log.info(f"[line_push_messages] to {target_id}: {resp.status_code}")
    return resp


def line_push_mention(
    group_id: str,
    message_template: str,
    mentions: Dict[str, str]
) -> requests.Response:
    """Send a LINE push message with @mentions (textV2).
    
    Args:
        group_id: LINE group ID to send to
        message_template: Message with placeholders like {user1}
        mentions: Dict mapping placeholder to user_id, e.g. {"user1": "U..."}
        
    Returns:
        Response from LINE API
    """
    substitution = {
        key: {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": user_id
            }
        }
        for key, user_id in mentions.items()
    }
    payload = {
        "to": group_id,
        "messages": [{
            "type": "textV2",
            "text": message_template,
            "substitution": substitution
        }]
    }
    resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
    log.info(f"[line_push_mention] to {group_id}: {resp.status_code}")
    return resp
