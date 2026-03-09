import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Optional
from threading import Lock

log = logging.getLogger(__name__)

class SenderGroupMapper:
    """Maps sender names to Monday board groups using dynamic lookup"""
    
    def __init__(self, monday_service=None):
        self.monday_service = monday_service
        self._group_cache = {}
        self._cache_time = None
        self.cache_duration = timedelta(hours=1)
        
    def map_sender_to_group(self, sender_name: str) -> Optional[str]:
        """
        Map sender name to Monday board group based on patterns
        
        Args:
            sender_name: The sender name from column E (打包資料表)
            
        Returns:
            The matched group title or None if no match found
        """
        if not sender_name:
            return None
            
        sender_upper = sender_name.strip().upper()
        
        # Direct abbreviation mapping
        abbreviation_map = {
            'MM': '(MM)',
            'AD': '(AD)', 
            'KT': '(KT)'
        }
        
        # Check for direct abbreviation matches first
        if sender_upper in abbreviation_map:
            target_pattern = abbreviation_map[sender_upper]
            return self._find_group_containing_pattern(target_pattern)
        
        # Check if sender contains abbreviations (e.g., "Yves MM Lai")
        for abbrev, pattern in abbreviation_map.items():
            if abbrev in sender_upper:
                log.info(f"[SenderMapping] Found abbreviation '{abbrev}' in sender '{sender_name}'")
                return self._find_group_containing_pattern(pattern)
                
        # Special mapping for Yves Lai variations
        yves_patterns = ['YVES LAI', 'YVES', 'YL']
        if any(pattern in sender_upper for pattern in yves_patterns):
            # Try to find SQ/Ace group
            for pattern in ['SQ', 'ACE', 'SOQUICK']:
                group = self._find_group_containing_pattern(pattern)
                if group:
                    return group
                    
        log.info(f"[SenderMapping] No group mapping found for sender: '{sender_name}'")
        return None
    
    def _find_group_containing_pattern(self, pattern: str) -> Optional[str]:
        """Find Monday board group that contains the specified pattern"""
        if not self.monday_service:
            log.warning("[SenderMapping] No Monday service available")
            return None
            
        try:
            groups = self._get_all_groups()
            pattern_upper = pattern.upper()
            
            for group in groups:
                group_title = group.get('title', '').upper()
                if pattern_upper in group_title:
                    log.info(f"[SenderMapping] Found group '{group['title']}' containing pattern '{pattern}'")
                    return group['title']  # Return original case
                    
        except Exception as e:
            log.error(f"[SenderMapping] Error finding group with pattern '{pattern}': {e}")
            
        return None
    
    def _get_all_groups(self) -> list:
        """Get all groups from Monday board 7745917861 with caching"""
        
        # Check cache validity
        if (self._cache_time and 
            datetime.now() - self._cache_time < self.cache_duration and 
            'groups' in self._group_cache):
            return self._group_cache['groups']
            
        if not self.monday_service:
            log.warning("[SenderMapping] No monday_service in _get_all_groups")
            return []
            
        try:
            query = '''
            query {
              boards (ids: [7745917861]) {
                groups {
                  id
                  title
                  items_page(limit: 100) {
                    items {
                      name
                    }
                  }
                }
              }
            }
            '''
            
            response = self.monday_service._post_with_backoff(
                self.monday_service.api_url, 
                {"query": query}
            )
            
            data = response.json()
            log.info(f"[SenderMapping] Monday API response keys: {list(data.keys())}")
            if "errors" in data:
                log.error(f"[SenderMapping] API errors: {data['errors']}")
                return []
            if "error_message" in data:
                log.error(f"[SenderMapping] API error_message: {data['error_message']}")
                return []
            boards = data.get("data", {}).get("boards", [])
            log.info(f"[SenderMapping] Boards count: {len(boards)}")
            
            if boards:
                groups = boards[0].get("groups", [])
                log.info(f"[SenderMapping] Groups: {len(groups)}, titles: {[g.get('title','?') for g in groups]}")
                self._group_cache['groups'] = groups
                self._cache_time = datetime.now()
                return groups
            else:
                log.warning("[SenderMapping] No boards returned from Monday API")
                
        except Exception as e:
            log.error(f"[SenderMapping] Error fetching groups: {e}")
            import traceback
            log.error(traceback.format_exc())
            
        return []

class DynamicNamesManager:
    """管理從 Monday board 動態獲取的名單，提供緩存和 fallback 機制"""
    
    def __init__(self, monday_service=None, fallback_config=None):
        self.monday_service = monday_service
        self.fallback_config = fallback_config or {}
        
        # 緩存機制（1小時有效期）
        self._cache = {}
        self._cache_time = {}
        self._cache_lock = Lock()
        self.cache_duration = timedelta(hours=1)
        
        # Initialize sender group mapper
        self.sender_mapper = SenderGroupMapper(monday_service)
        
    def get_sender_group_mapping(self, sender_name: str) -> Optional[str]:
        """
        Map sender name to Monday board group
        
        Args:
            sender_name: Sender name from column E (打包資料表)
            
        Returns:
            Group title if mapping found, None otherwise
        """
        return self.sender_mapper.map_sender_to_group(sender_name)
    
    def get_group_members(self, group_title: str) -> Set[str]:
        """
        Get all member names from a specific Monday board group
        
        Args:
            group_title: The title of the group
            
        Returns:
            Set of member names in the group
        """
        cache_key = f"group_{group_title}_members"
        
        with self._cache_lock:
            # Check cache
            cached_members = self._get_cached_names(cache_key)
            if cached_members is not None:
                return cached_members
        
        # Get from Monday board
        try:
            groups = self.sender_mapper._get_all_groups()
            for group in groups:
                if group.get('title', '') == group_title:
                    items = group.get('items_page', {}).get('items', []) if 'items_page' in group else group.get('items', [])
                    member_names = {item.get('name', '').strip() for item in items if item.get('name', '').strip()}
                    self._update_cache(cache_key, member_names)
                    log.info(f"[DynamicNames] Updated group '{group_title}' with {len(member_names)} members")
                    return member_names
                    
        except Exception as e:
            log.error(f"[DynamicNames] Error getting members for group '{group_title}': {e}")
            
        return set()
        
    def get_team_names(self, team_name: str) -> Set[str]:
        """
        獲取團隊名單（Vicky, Yumi, 等）
        返回 set of available member names
        """
        cache_key = f"{team_name.lower()}_names"
        
        with self._cache_lock:
            # 檢查緩存
            cached_names = self._get_cached_names(cache_key)
            if cached_names is not None:
                return cached_names
        
        # 固定載入 config 靜態名單作為基底
        fallback_key = f"{team_name.upper()}_NAMES"
        fallback_names = self.fallback_config.get(fallback_key, set())

        # 從 Monday board 獲取（動態名單補充）
        if self.monday_service:
            try:
                team_members = self.monday_service.get_team_members_from_board(team_name)
                if team_members:
                    dynamic_names = {member["name"] for member in team_members if member["available"]}
                    merged_names = fallback_names | dynamic_names  # 合併靜態 + 動態
                    self._update_cache(cache_key, merged_names)
                    log.info(f"[DynamicNames] Updated {team_name} with {len(merged_names)} names ({len(dynamic_names)} dynamic + {len(fallback_names)} static)")
                    return merged_names
            except Exception as e:
                log.error(f"[DynamicNames] Failed to get {team_name} names: {e}")
        
        log.warning(f"[DynamicNames] Using fallback for {team_name}: {len(fallback_names)} names")
        return fallback_names
    
    def get_yves_names(self) -> Set[str]:
        """獲取 Yves 名單（SQ/Ace group）"""
        cache_key = "yves_names"
        
        with self._cache_lock:
            # 檢查緩存
            cached_names = self._get_cached_names(cache_key)
            if cached_names is not None:
                return cached_names
        
        # 從 Monday board 獲取
        if self.monday_service:
            try:
                yves_names = self.monday_service.get_yves_names_from_board()
                if yves_names:
                    self._update_cache(cache_key, yves_names)
                    log.info(f"[DynamicNames] Updated Yves with {len(yves_names)} names")
                    return yves_names
            except Exception as e:
                log.error(f"[DynamicNames] Failed to get Yves names: {e}")
        
        # Fallback to config
        fallback_names = self.fallback_config.get('YVES_NAMES', set())
        log.warning(f"[DynamicNames] Using fallback for Yves: {len(fallback_names)} names")
        return fallback_names
    
    def _get_cached_names(self, cache_key: str) -> Optional[Set[str]]:
        """檢查緩存是否有效並返回名單"""
        if cache_key not in self._cache or cache_key not in self._cache_time:
            return None
            
        cache_time = self._cache_time[cache_key]
        if datetime.now() - cache_time < self.cache_duration:
            return self._cache[cache_key].copy()  # 返回副本避免意外修改
            
        return None
    
    def _update_cache(self, cache_key: str, names: Set[str]):
        """更新緩存"""
        self._cache[cache_key] = names.copy()
        self._cache_time[cache_key] = datetime.now()
    
    def clear_cache(self, team_name: str = None):
        """清除緩存（可選指定團隊）"""
        with self._cache_lock:
            if team_name:
                cache_key = f"{team_name.lower()}_names"
                self._cache.pop(cache_key, None)
                self._cache_time.pop(cache_key, None)
            else:
                self._cache.clear()
                self._cache_time.clear()
        
        log.info(f"[DynamicNames] Cleared cache for {team_name or 'all teams'}")

# 全域實例（單例模式）
_dynamic_names_manager: Optional[DynamicNamesManager] = None

def get_dynamic_names_manager() -> DynamicNamesManager:
    """獲取全域動態名單管理器實例"""
    return _dynamic_names_manager

def init_dynamic_names_manager(monday_service=None, fallback_config=None):
    """初始化全域動態名單管理器"""
    global _dynamic_names_manager
    _dynamic_names_manager = DynamicNamesManager(monday_service, fallback_config)
    log.info("[DynamicNames] Manager initialized")
    return _dynamic_names_manager