"""
Sender mapping utilities for Monday board integration
Handles dynamic mapping between sender names and Monday board groups
"""

import logging
from typing import Set, Dict, Optional

log = logging.getLogger(__name__)

class SenderMappingService:
    """Service for mapping sender names to Monday board groups"""
    
    def __init__(self, monday_service=None):
        self.monday_service = monday_service
        self._group_cache = {}
        
    def get_sender_group_mapping(self, sender_name: str) -> Optional[str]:
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
        """
        Find Monday board group that contains the specified pattern
        
        Args:
            pattern: The pattern to search for in group titles
            
        Returns:
            The group title if found, None otherwise
        """
        if not self.monday_service:
            log.warning("[SenderMapping] No Monday service available")
            return None
            
        try:
            # Get all groups from the Monday board
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
        
        if 'groups' in self._group_cache:
            return self._group_cache['groups']
            
        if not self.monday_service:
            log.warning("[SenderMapping] No monday_service in _get_all_groups")
            return []
            
        try:
            # Use Monday service to get board groups
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
            log.info(f"[SenderMapping] Monday API raw response keys: {list(data.keys())}")
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
                log.info(f"[SenderMapping] Groups count: {len(groups)}, titles: {[g.get('title','?') for g in groups]}")
                self._group_cache['groups'] = groups
                return groups
            else:
                log.warning("[SenderMapping] No boards returned from Monday API")
                
        except Exception as e:
            log.error(f"[SenderMapping] Error fetching groups: {e}")
            import traceback
            log.error(traceback.format_exc())
            
        return []
    
    def get_group_members(self, group_title: str) -> Set[str]:
        """
        Get all member names from a specific group
        
        Args:
            group_title: The title of the group to get members from
            
        Returns:
            Set of member names in the group
        """
        try:
            groups = self._get_all_groups()
            for group in groups:
                if group.get('title', '') == group_title:
                    items = group.get('items_page', {}).get('items', []) if 'items_page' in group else group.get('items', [])
                    member_names = {item.get('name', '').strip() for item in items if item.get('name', '').strip()}
                    log.info(f"[SenderMapping] Group '{group_title}' has {len(member_names)} members")
                    return member_names
                    
        except Exception as e:
            log.error(f"[SenderMapping] Error getting members for group '{group_title}': {e}")
            
        return set()
    
    def clear_cache(self):
        """Clear the group cache to force refresh"""
        self._group_cache.clear()
        log.info("[SenderMapping] Cache cleared")

def get_sender_mapping_service(monday_service=None):
    """Get sender mapping service instance"""
    return SenderMappingService(monday_service)