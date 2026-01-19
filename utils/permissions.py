# utils/permissions.py
from config import YVES_USER_ID, GORSKY_USER_ID

def is_authorized_for_event(event_name: str, group_id: str, user_id: str) -> bool:
    # 僅允許 Yves 或 Gorsky 執行
    authorized_users = {YVES_USER_ID, GORSKY_USER_ID}
    return user_id in authorized_users
