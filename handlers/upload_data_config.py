"""
上傳資料流程 - 權限設定 (Upload Data Flow Configuration)
──────────────────────────────────────────────────────────
Permission management for the upload data feature.

Access Rules:
  - Yves and Gorsky: Allowed everywhere
  - Others: Only in PDF group and Ken's group
"""

from dataclasses import dataclass
from typing import Optional, Set

from config import (
    YVES_USER_ID, 
    GORSKY_USER_ID,
)

# Ken's group ID
KEN_GROUP_ID = "Ce00f9a5d56f815c87b4241d8eb12cbf1"

# Universal access users (for private chats)
UNIVERSAL_USERS: Set[Optional[str]] = {
    YVES_USER_ID,
    GORSKY_USER_ID,
}


def can_use_upload_data(user_id: str, group_id: Optional[str] = None) -> bool:
    """
    Check if a user can use the upload data feature.
    
    Args:
        user_id: LINE user ID
        group_id: LINE group ID (None for 1-on-1 chat)
        
    Returns:
        True if user has permission, False otherwise
    
    Access Rules:
      - In Ken's group: anyone can use
      - In private chat (1-on-1): only Yves and Gorsky
      - In other groups: NOT allowed
    """
    # In a group: only Ken's group is allowed
    if group_id:
        return group_id == KEN_GROUP_ID
    
    # In private chat (group_id is None): only Yves and Gorsky
    return user_id in UNIVERSAL_USERS
