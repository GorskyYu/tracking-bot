# Monday Board Vicky 群組 - 改動日誌

## 改動摘要

已移除所有 **fallback 到靜態配置** 的機制。現在系統必須直接從 Monday board 讀取 Vicky 群組成員，如果讀取失敗會直接拋出異常而不是降級使用舊的內存的列表。

---

## 改動清單

### 1. **services/monday_service.py**

#### 修改 `get_team_members_from_board(team_name)` 方法
- **改動**: 移除 `try-except` 中的 fallback，直接拋出異常
- **新行為**: 
  - API 錯誤 → 拋出 Exception（包含錯誤詳情）
  - 無法找到 group → 拋出 Exception（列出可用的 groups）
  - 成功讀取 → 返回所有成員清單（包含 available/priority 狀態）

**關鍵改動**:
```python
# 舊邏輯: except Exception as e: ... return []
# 新邏輯: 直接拋出 Exception，包含詳細錯誤訊息
if "errors" in response_data:
    raise Exception(f"[Monday API Error for {team_name}] {response_data['errors']}")
```

---

#### 修改 `get_yves_names_from_board()` 方法
- **改動**: 同樣移除 fallback，直接拋出異常
- **新行為**: 與 `get_team_members_from_board()` 相同

---

### 2. **utils/dynamic_names.py**

#### 修改 `get_team_names(team_name)` 方法
- **移除**: `fallback_names = self.fallback_config.get(fallback_key, set())` 的邏輯
- **新行為**:
  1. 檢查緩存
  2. **必須** 從 Monday board 讀取
  3. 失敗 → 拋出 `RuntimeError`，包含詳細錯誤訊息

**流程圖**:
```
讀取 'vicky_names' 緩存?
├─ 有 → 返回
└─ 無 → 
   Monday service 存在?
   ├─ 無 → 拋出 RuntimeError
   └─ 有 → 呼叫 get_team_members_from_board('Vicky')
      ├─ 成功 → 快取 + 返回
      └─ 失敗 → 拋出 RuntimeError
```

---

#### 修改 `get_yves_names()` 方法
- **移除**: `fallback_names = self.fallback_config.get('YVES_NAMES', set())` 的邏輯
- **新行為**: 與 `get_team_names()` 相同

---

### 3. **handlers/handlers.py**

#### 修改 `get_vicky_names()` 函數
- **移除**: 
  ```python
  from config import VICKY_NAMES
  return VICKY_NAMES  # 舊的 fallback
  ```
- **新行為**:
  ```python
  manager = get_dynamic_names_manager()
  if not manager:
      raise RuntimeError("[Handlers] Dynamic names manager not initialized for Vicky")
  return manager.get_team_names('Vicky')  # 必須成功，否則拋出異常
  ```

---

#### 修改 `get_yumi_names()` 和 `get_yves_names()` 函數
- 同樣移除 fallback，直接拋出異常

---

## 如果出現錯誤，系統會報告什麼？

### 情況 1: Monday API 連線失敗
```
RuntimeError: [DynamicNames] FAILED to get Vicky from Monday board: 
[Monday API Error for Vicky] [{'message': 'Unauthorized', 'extensions': {'...', 'statusCode': 401}}]
```

### 情況 2: API Token 無效或過期
```
RuntimeError: [DynamicNames] FAILED to get Vicky from Monday board: 
[Monday API Error for Vicky] [{'message': 'Invalid token', ...}]
```

### 情況 3: Vicky Group 不存在
```
RuntimeError: [DynamicNames] FAILED to get Vicky from Monday board: 
[Monday] Vicky group NOT FOUND in board 7745917861. 
Available groups: ['SQ/Ace', 'Yumi', 'Vicky Ku', '其他 group...']
```

### 情況 4: Dynamic Names Manager 未初始化
```
RuntimeError: [Handlers] Dynamic names manager not initialized for Vicky
```

---

## 驗證配置

### 檢查事項
1. ✅ `MONDAY_API_TOKEN` 環境變數已設定
2. ✅ Monday board ID `7745917861` 可存取
3. ✅ Vicky group title 包含 "vicky"（不區分大小寫）
4. ✅ Status column ID 正確為 `status__1`
5. ✅ Phone column ID 正確為 `phone__1`

### 已知的可用人員狀態
系統現在只會選擇 **available = True** 的成員，這些狀態被視為可用：
- ✓ 「可用人頭」
- ✓ 「優先使用」 (priority = True)
- ✓ 空白或其他狀態
- ✗ 「不可用」 (available = False)

---

## 發生錯誤時的建議排查步驟

1. **檢查 Monday API Token**
   ```bash
   echo $env:MONDAY_API_TOKEN   # Windows PowerShell
   echo $MONDAY_API_TOKEN       # Linux/Mac
   ```

2. **檢查 Monday Board 存取權限**
   - 登入 Monday.com 查看 board 7745917861 是否可存取

3. **檢查 Group Title**
   - Board 中是否存在包含 "vicky" 的 group（區分大小寫無關）

4. **查看應用日誌**
   - 搜尋關鍵字: `[Monday]`, `[DynamicNames]`, `[Handlers]`

5. **清除緩存（如果需要立即重新載入）**
   - 重啟應用程式（因為現在沒有 fallback，會強制重新查詢）

---

## 配置文件狀態

### config.py
- **VICKY_NAMES** (Line 80): 保留靜態列表（作為初始化參數，但不再使用）
- **YUMI_NAMES** (Line 81): 同上
- **YVES_NAMES**: 同上

> **注意**: 這些靜態列表現在只用於 `init_dynamic_names_manager()` 的 fallback_config 參數初始化，運行時不會被使用。

---

## 驗證日誌範例（成功案例）

```
[DynamicNames] Using cached Vicky names: 12 members
[Monday] Retrieved 12 members for Vicky team
[Monday] 12/12 members are available for Vicky
[DynamicNames] Successfully retrieved Vicky with 12 available members: ['周志明', '周大偉', '周佩樺', '李詠奇', '林寶玲', '廖芯儀', '崔書鳳', '高懿欣', '黃佩絨', '顧家琪', '顧志忠', '顧郭蓮梅']
```

---

## 測試方法

### 執行驗證腳本
```bash
cd c:\Users\yves.lai\Documents\tracking-bot
python test_monday_vicky.py
```

**預期輸出**:
- ✓ 成功讀取 Vicky 群組
- ✓ 顯示完整成員列表及狀態
- ✓ 或清楚的錯誤訊息說明問題所在

---

## 總結

| 方面 | 改動前 | 改動後 |
|-----|-------|-------|
| 讀取失敗時的行為 | 降級到 config.py 的靜態列表 | 直接拋出異常 |
| 錯誤報告 | 隱藏在 warning 日誌中 | 明確的 RuntimeError |
| 缓存失败時的恢復 | 自動使用舊列表 | 必須修復問題並重啟 |
| 可視化程度 | 低（難以察覺問題） | 高（清楚知道何時失敗） |

現在系統能夠準確反映 Monday board 的實時狀態，不會因隱藏的 fallback 導致使用過時的人員列表。

