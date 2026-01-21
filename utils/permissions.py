# utils/permissions.py
from config import YVES_USER_ID, GORSKY_USER_ID

# 定義統一管理員清單
ADMIN_USER_IDS = {YVES_USER_ID, GORSKY_USER_ID}

def is_authorized_for_event(event_name: str, group_id: str, user_id: str) -> bool:
    return user_id in ADMIN_USER_IDS
