"""
報價流程 - 群組/使用者 設定檔 (Strategy Pattern)
──────────────────────────────────────────────────
Each QuoteProfile defines behaviour overrides for a specific group context.

Adding a new group:
  1. Create a new QuoteProfile instance
  2. Register it in PROFILE_REGISTRY with its group_id
  3. Done — the quote handler reads the profile at runtime.
"""

from dataclasses import dataclass, field
from typing import Optional, Set

from config import (
    PDF_GROUP_ID, IRIS_GROUP_ID, YVES_USER_ID, GORSKY_USER_ID,
)

# ─── Warning Services ────────────────────────────────────────────────────────
# These services have known discrepancies between TE API and TE GUI pricing.
WARN_SERVICE_NAMES: Set[str] = {
    "FEDEX_EXPRESS_SAVER",
    "STANDARD_OVERNIGHT",
    "UPS Expedited",
}

WARN_DISCLAIMER = (
    "⚠️ 注意：FEDEX_EXPRESS_SAVER, STANDARD_OVERNIGHT, "
    "UPS Expedited 的系統報價可能與 TE 網站不同，"
    "請務必進入 TE 網站確認金額。"
)


def is_warn_service(service_name: str) -> bool:
    """Return True if this service has known API/GUI price discrepancy."""
    return any(ws in service_name for ws in WARN_SERVICE_NAMES)


# ─── Quote Profile ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class QuoteProfile:
    """
    Configurable behaviour for the quote flow in different contexts.

    Attributes
    ----------
    name : str                    Human-readable profile label.
    allow_service_select : bool   If False, auto-select `forced_service`.
    forced_service : str | None   Service name to auto-select (e.g. "FEDEX_GROUND").
    allow_mode_select : bool      If False, auto-select `forced_mode`.
    forced_mode : str | None      Mode to auto-use (e.g. "加台空運").
    show_cost_in_group : bool     Show prices in the group chat.
    cost_push_target : str|None   If set, push cost info to this user/group instead.
    show_result_flex_in_group : bool  Show comparison flex in group chat.
    result_flex_push_target : str|None  If set, push result flex to this user.
    post_quote_actions : set      Which post-quote buttons to show.
    allowed_users : set|None      If set, only these user IDs may use the flow.
                                  None = anyone in the group.
    """
    name: str = "default"

    # Service selection
    allow_service_select: bool = True
    forced_service: Optional[str] = None

    # Mode selection (air / sea)
    allow_mode_select: bool = True
    forced_mode: Optional[str] = None

    # Visibility
    show_cost_in_group: bool = True
    cost_push_target: Optional[str] = None
    show_result_flex_in_group: bool = True
    result_flex_push_target: Optional[str] = None

    # Post-quote actions
    post_quote_actions: frozenset = frozenset({
        "switch_mode", "reselect_service", "new_quote", "done"
    })

    # Access control
    allowed_users: Optional[frozenset] = None


# ─── Profile Instances ────────────────────────────────────────────────────────

# Default profile: full features (used for Yves direct chat, PDF group, etc.)
DEFAULT_PROFILE = QuoteProfile(
    name="default",
)

# Iris profile: restricted flow
IRIS_PROFILE = QuoteProfile(
    name="iris",
    allow_service_select=False,
    forced_service="FEDEX_GROUND",
    allow_mode_select=False,
    forced_mode="加台空運",
    show_cost_in_group=True,
    cost_push_target=None,
    show_result_flex_in_group=False,
    result_flex_push_target=YVES_USER_ID,
    post_quote_actions=frozenset({"new_quote", "done"}),
)


# ─── Profile Registry ────────────────────────────────────────────────────────

# Map group_id → profile.  None key = DM / unknown group.
PROFILE_REGISTRY: dict = {}

# Register known groups
if PDF_GROUP_ID:
    PROFILE_REGISTRY[PDF_GROUP_ID] = DEFAULT_PROFILE
if IRIS_GROUP_ID:
    PROFILE_REGISTRY[IRIS_GROUP_ID] = IRIS_PROFILE

# Allowed in DMs for Yves and Gorsky
_DM_ALLOWED_USERS = set()
if YVES_USER_ID:
    _DM_ALLOWED_USERS.add(YVES_USER_ID)
if GORSKY_USER_ID:
    _DM_ALLOWED_USERS.add(GORSKY_USER_ID)


def get_profile(group_id: Optional[str], user_id: str) -> Optional[QuoteProfile]:
    """
    Resolve the QuoteProfile for a given context.

    Returns None if the user/group is not allowed to use the quote feature.
    """
    if group_id and group_id in PROFILE_REGISTRY:
        profile = PROFILE_REGISTRY[group_id]
        # Access control
        if profile.allowed_users and user_id not in profile.allowed_users:
            return None
        return profile

    # DM (no group)
    if not group_id and user_id in _DM_ALLOWED_USERS:
        return DEFAULT_PROFILE

    return None
