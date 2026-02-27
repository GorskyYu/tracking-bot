import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Optional
from threading import Lock

log = logging.getLogger(__name__)

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
        
        # 從 Monday board 獲取
        if self.monday_service:
            try:
                team_members = self.monday_service.get_team_members_from_board(team_name)
                if team_members:
                    available_names = {member["name"] for member in team_members if member["available"]}
                    self._update_cache(cache_key, available_names)
                    log.info(f"[DynamicNames] Updated {team_name} with {len(available_names)} available members")
                    return available_names
            except Exception as e:
                log.error(f"[DynamicNames] Failed to get {team_name} names: {e}")
        
        # Fallback to config
        fallback_key = f"{team_name.upper()}_NAMES"
        fallback_names = self.fallback_config.get(fallback_key, set())
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