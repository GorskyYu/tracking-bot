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
    PDF_GROUP_ID, 
    YVES_USER_ID, 
    GORSKY_USER_ID,
)

# Ken's group ID (from requirement)
KEN_GROUP_ID = "Ce00f9a5d56f815c87b4241d8eb12cbf1"

# Universal access users (can use anywhere)
UNIVERSAL_USERS: Set[Optional[str]] = {
    YVES_USER_ID,
    GORSKY_USER_ID,
}

# Allowed group IDs (anyone can use in these groups)
ALLOWED_GROUPS: Set[Optional[str]] = {
    PDF_GROUP_ID,
    KEN_GROUP_ID,
}


def can_use_upload_data(user_id: str, group_id: Optional[str] = None) -> bool:
    """
    Check if a user can use the upload data feature.
    
    Args:
        user_id: LINE user ID
        group_id: LINE group ID (None for 1-on-1 chat)
        
    Returns:
        True if user has permission, False otherwise
    """
    # Universal access for Yves and Gorsky
    if user_id in UNIVERSAL_USERS:
        return True
    
    # In allowed groups, anyone can use
    if group_id and group_id in ALLOWED_GROUPS:
        return True
    
    return False
