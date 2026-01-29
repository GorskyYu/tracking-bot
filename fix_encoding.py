# -*- coding: utf-8 -*-
"""Fix encoding issues in main.py"""
import re

with open('main.py', 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

# Fix the garbled comments and strings
fixes = [
    # Section comments
    ('# ???????', '# 業務邏輯處理器'),
    
    # Emoji fixes
    ('"? Updated packages:', '"✅ Updated packages:'),
    ('?? PDF System Error', '⚠️ PDF System Error'),
    
    # Line 151 area
    ('# ??? source / group_id', '# 立刻抓 source / group_id'),
    
    # Barcode section  
    ('# ?? ??:????????', '# 🟢 新增：圖片條碼辨識邏輯'),
    ('# ?? barcode_service ??,????????????', '# 呼叫 barcode_service 處理，傳入所需的緩存與回呼函式'),
    ('continue # ??????(?????),???????', 'continue  # 如果處理成功（是條碼圖片），則跳過後續邏輯'),
    
    # TWWS section
    ('# ?? NEW: TWWS ??????? (????????? Yves ??)', '# 🟢 NEW: TWWS 兩段式互動邏輯 (限定個人私訊且限定 Yves 使用)'),
    ('# ?? userId ??????', '# 使用 userId 確保狀態唯一'),
    ('# ???????????????????? (Yves)?', '# 檢查是否為「個人私訊」且為「指定的管理員 (Yves)」'),
    ('# ????????????????????', '# 檢查是否正在等待使用者輸入「子項目名稱」'),
    ('# ???????,?????? text ??????', '# 如果有狀態存在，把這次輸入的 text 當作名稱去查'),
    ('# ?? user_id ??????,??????', '# 使用 user_id 作為推播對象，確保私訊回傳'),
    ('"?? ???? ({text}):', '"🔍 查詢結果 ({text}):'),
    ('"?? ????:', '"💰 應付金額:'),
    ('# ???????', '# 查完後刪除狀態'),
    ('# ??????:????? twws', '# 觸發第一階段：使用者輸入 twws'),
    ('# ??????? 5 ?? (300?) ???', '# 設定狀態並給予 5 分鐘 (300秒) 的時限'),
    ('"??,????????:"', '"好的，請輸入子項目名稱："'),
    
    # PDF Scanning section
    ('# --- ????????:?? PDF Scanning ???? ---', '# --- 金額自動錄入邏輯：僅限 PDF Scanning 群組觸發 ---'),
    ('# ?????????? (? 43.10)', '# 檢查是否為純數字金額 (如 43.10)'),
    ('# ??? Key ????????? PDF ?? ID, ???? ID ? Board ?????', '# 從全局 Key 抓取最後一次上傳的 PDF 項目 ID'),
    ('# ????? ID ??? ID', '# 拆分出項目 ID 與板塊 ID'),
    ('# ???????? ID', '# 呼叫時多傳入板塊 ID'),
    ('"? ?????????:', '"✅ 已成功登記境內支出:'),
    ('"?? ??:', '"📌 項目:'),
    ('"? ????:', '"❌ 登記失敗:'),
    ("'??'", "'未知'"),
    
    # Bill section
    ('# --- ???????? ---', '# ─── 查看帳單觸發入口 ───'),
    ('text.startswith("????")', 'text.startswith("查看帳單")'),
    
    # Admin section
    ('# ?????? (???????)', '# 目前功能指令 (僅限管理員私訊)'),
    ('text.strip() == "????"', 'text.strip() == "目前功能"'),
    
    # Unpaid section
    ('# 1. ????????', '# 1. 判斷是否為管理員'),
    ('# 2. ??????????????', '# 2. 判斷是否為有效的自動查詢群組'),
    ('# ?? ???:???????;?????????????? "unpaid"', '# 🟢 新邏輯：管理員隨時可用；一般成員僅限在指定群組內輸入 "unpaid"'),
    
    # Paid section
    ('# Paid ????:??????', '# Paid 指令處理：分為兩種情況'),
    ('# 1. ???????:paid YYMMDD [AbowbowID]', '# 1. 查看已付款帳單：paid YYMMDD [AbowbowID]'),
    ('# 2. ??????:paid ?? [ntd|twd]', '# 2. 錄入實收金額：paid 金額 [ntd|twd]'),
    ('# ?????????????? (paid YYMMDD ...)', '# 檢查是否為查看已付款帳單格式 (paid YYMMDD ...)'),
    ('# ???????? (paid ?? [ntd|twd])', '# 錄入實收金額格式 (paid 金額 [ntd|twd])'),
    
    # UPS and ACE sections
    ('# 1) ?? UPS ???????????', '# 1) 處理 UPS 批量更新與單筆尺寸錄入'),
    ('# 3) Ace schedule (??/????) & ACE EZ-Way check', '# 3) Ace schedule (週四/週日出貨) & ACE EZ-Way check'),
    ('"????" in text or "????" in text', '"週四出貨" in text or "週日出貨" in text'),
    ('# ?? ShipmentParserService ??????', '# 使用 ShipmentParserService 實例呼叫邏輯'),
    ('# ???????????', '# 負責發送到各負責人小群'),
    ('# ?? Iris ????? Sender ? Yves', '# 負責 Iris 分流與發送 Sender 給 Yves'),
    
    # Confirmation section
    ('# 4) ???????????? (?? Danny ????????????)', '# 4) 處理「申報相符」通知分流 (包含 Danny 自動觸發與管理員手動觸發)'),
    
    # Richmond section
    ('"[Richmond, Canada] ???????"', '"[Richmond, Canada] 已到達派送中心"'),
    ('"{user1} ?????????????"', '"{user1} 請提供此包裹的內容物清單："'),
    
    # Soquick section
    ('"????????????"', '"上周六出貨包裹的派件單號"'),
    ('"????" in text and "????" in text', '"出貨單號" in text and "宅配單號" in text'),
    ('"??,?"', '"您好，請"'),
    ('and "?" in text', 'and "按" in text'),
    ('and "????" in text', 'and "申報相符" in text'),
    
    # Tracking section
    ('# 8) Your existing "????" logic', '# 8) Your existing "追蹤包裹" logic'),
    ('if text == "????":',  'if text == "追蹤包裹":'),
    
    # Holiday section  
    ('# 9) Your existing "??????" logic', '# 9) Your existing "下個國定假日" logic'),
    ('if text == "??????":',  'if text == "下個國定假日":'),
    
    # ACE manual trigger
    ('# ?? NEW: ACE manual trigger', '# 🟢 NEW: ACE manual trigger'),
    ('text.strip() == "????????"', 'text.strip() == "已上傳資料可出貨"'),
    
    # Monday webhook comment
    ('#  Monday.com Webhook ', '# ─── Monday.com Webhook ────────────────────────────────────────────────────────'),
]

for old, new in fixes:
    content = content.replace(old, new)

with open('main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed encoding issues in main.py")
print("Checking for remaining question marks in comments...")

# Check for remaining issues
lines = content.split('\n')
for i, line in enumerate(lines, 1):
    if '#' in line and '?' in line.split('#', 1)[-1]:
        print(f"  Line {i}: {line.strip()[:80]}")
