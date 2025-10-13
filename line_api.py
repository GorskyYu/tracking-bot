import requests
from .config import LINE_PUSH_URL, LINE_REPLY_URL, LINE_HEADERS
from .log import log

def line_push(to: str, text: str):
    payload = {"to": to, "messages": [{"type": "text", "text": text}]}
    try:
        return requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload, timeout=10)
    except Exception as e:
        log.error(f"[LINE PUSH] failed: {e}")

def line_reply(reply_token: str, text: str):
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
    try:
        return requests.post(LINE_REPLY_URL, headers=LINE_HEADERS, json=payload, timeout=10)
    except Exception as e:
        log.error(f"[LINE REPLY] failed: {e}")
