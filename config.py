import os
import re
from typing import Dict, Set, Pattern, Optional

# ─── Timezone ─────────────────────────────────────────────────────────────────
TIMEZONE: str = os.getenv("TIMEZONE", "America/Vancouver")

# ─── LINE API ─────────────────────────────────────────────────────────────────
LINE_TOKEN: Optional[str] = os.getenv("LINE_TOKEN")
LINE_PUSH_URL: str = "https://api.line.me/v2/bot/message/push"
LINE_REPLY_URL: str = "https://api.line.me/v2/bot/message/reply"
LINE_HEADERS: Dict[str, str] = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

# ─── Monday.com API ───────────────────────────────────────────────────────────
MONDAY_API_URL: str = "https://api.monday.com/v2"
MONDAY_TOKEN: Optional[str] = os.getenv("MONDAY_TOKEN")
MONDAY_API_TOKEN: Optional[str] = os.getenv("MONDAY_API_TOKEN")

# ─── Redis ────────────────────────────────────────────────────────────────────
REDIS_URL: Optional[str] = os.getenv("REDIS_URL")

# ─── TE API ───────────────────────────────────────────────────────────────────
APP_ID: Optional[str] = os.getenv("TE_APP_ID")
APP_SECRET: Optional[str] = os.getenv("TE_SECRET")

# ─── OpenAI ───────────────────────────────────────────────────────────────────
OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ─── LINE Group IDs ───────────────────────────────────────────────────────────
ACE_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_ACE")
SOQUICK_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_SQ")
VICKY_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_VICKY")
YUMI_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_YUMI")
IRIS_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_IRIS")
ANGELA_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_ANGELA")
JOYCE_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_JOYCE")
PDF_GROUP_ID: Optional[str] = os.getenv("LINE_GROUP_ID_PDF")

# ─── LINE User IDs ────────────────────────────────────────────────────────────
YVES_USER_ID: Optional[str] = os.getenv("YVES_USER_ID")
GORSKY_USER_ID: Optional[str] = os.getenv("GORSKY_USER_ID")
DANNY_USER_ID: Optional[str] = os.getenv("DANNY_USER_ID")
VICKY_USER_ID: Optional[str] = os.getenv("VICKY_USER_ID")
IRIS_USER_ID: Optional[str] = os.getenv("IRIS_USER_ID")

# ─── Google Sheet URLs ────────────────────────────────────────────────────────
ACE_SHEET_URL: Optional[str] = os.getenv("ACE_SHEET_URL")
SQ_SHEET_URL: Optional[str] = os.getenv("SQ_SHEET_URL")
VICKY_SHEET_URL: Optional[str] = os.getenv("VICKY_SHEET_URL")

# ─── Monday Board IDs ─────────────────────────────────────────────────────────
AIR_BOARD_ID: Optional[str] = os.getenv("AIR_BOARD_ID")
AIR_PARENT_BOARD_ID: Optional[str] = os.getenv("AIR_PARENT_BOARD_ID")
VICKY_SUBITEM_BOARD_ID: int = 4815120249
VICKY_STATUS_COLUMN_ID: str = "status__1"

# ─── Client → LINE Group Mapping ──────────────────────────────────────────────
CLIENT_TO_GROUP: Dict[str, Optional[str]] = {
    "yumi": YUMI_GROUP_ID,
    "vicky": VICKY_GROUP_ID,
}

# ─── Customer Filters (for tracking) ──────────────────────────────────────────
CUSTOMER_FILTERS: Dict[Optional[str], list] = {
    YUMI_GROUP_ID: ["yumi", "shu-yen"],
    VICKY_GROUP_ID: ["vicky", "chia-chi"]
}

# ─── Regex Patterns ───────────────────────────────────────────────────────────
CODE_TRIGGER_RE: Pattern[str] = re.compile(r"\b(?:ACE|\d+N)\d*[A-Z0-9]*\b")
MISSING_CONFIRM: str = "這幾位還沒有按申報相符"

# ─── Name Sets ────────────────────────────────────────────────────────────────
VICKY_NAMES: Set[str] = {"顧家琪", "顧志忠", "周佩樺", "顧郭蓮梅", "廖芯儀", "林寶玲", "高懿欣", "崔書鳳", "周志明"}
YUMI_NAMES: Set[str] = {"劉淑燕", "竇永裕", "劉淑玫", "劉淑茹", "陳富美", "劉福祥", "郭淨崑", "陳卉怡", "洪瑜駿", "李祈霈", "邱啓倫", "許霈珩"}
IRIS_NAMES: Set[str] = {"廖偉廷", "廖本堂", "李成艷"}
ANGELA_NAMES: Set[str] = {"蕭仁富", "呂鎰利", "謝秀珠"}
YVES_NAMES = {
    "梁穎琦",
    "張詠凱",
    "劉育伶",
    "羅唯英",
    "陳品茹",
    "張碧蓮",
    "吳政融",
    "解瑋庭",
    "洪君豪",
    "洪芷翎",
    "羅木癸",
    "洪金珠",
    "林憶慧",
    "葉怡秀",
    "葉詹明",
    "廖聰毅",
    "蔡英豪",
    "魏媴蓁",
    "黃淑芬",
    "解佩頴",
    "曹芷茜",
    "王詠皓",
    "曹亦芳",
    "李慧芝",
    "李錦祥",
    "詹欣陵",
    "陳志賢",
    "曾惠玲",
    "李白秀",
    "陳聖玄",
    "柯雅甄",
    "游玉慧",
    "鄭詠渝",
    "鄭芸婷",
    "游繼堯",
    "游承哲",
    "游傳杰",
    "陳秀華",
    "陳秀玲",
    "陳恒楷"
}
EXCLUDED_SENDERS = {"Yves Lai", "Yves KT Lai", "Yves MM Lai", "Yumi Liu", "Vicky Ku"}

# app/config.py
import os
from linebot import LineBotApi
import json
import base64
from pathlib import Path

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
line_bot_api = LineBotApi(LINE_TOKEN)
ABOW_GROUP_ID = os.getenv("ABOW_GROUP_ID", "C8f48ab6318c34a9dee6e98ee035139e5")
GHEHON_GROUP_ID = os.getenv("GHEHON_GROUP_ID", "C7c91af29fb566fd4881868f9d6941f62")

DEFAULT_FALLBACK_GROUP = os.getenv("DEFAULT_FALLBACK_GROUP", "ABOWBOW_TW_INTERNAL")
# LINE_CHANNEL_ACCESS_TOKEN="txsXfDociy6EiwOMT5LINqnZvqt99cyWlZ5XeQ6tRCIuHmUMIFb1hAx6MswRHD8r9UhhFTVia7vassR1CsNwSwBdPMc8eeILxi"

# 入庫拆箱scan3
DASHBOARD_SHEET_ID = "1xrb2tqN7GwpZNNc-_rroOAebA5nKLYNEmTxZhxPxrAQ"
DASHBOARD_SHEET_NAME = "入庫拆箱scan3"
SKU_MAPPING_SHEET_NAME = "Database"
DASHBOARD_DATE_CELL = "D3"
DASHBOARD_BARCODE_COLUMN = "E"
# GOOGLE_CREDENTIALS_FILE = "credentials/service_account.json"
# GOOGLE_CREDENTIALS = "credentials/service_account.json"

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/gcloud-sa.json"))

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_PROCUREMENT_BOARD_ID = "7708370955"
MONDAY_AIR_BOARD_ID = os.getenv("MONDAY_AIR_BOARD_ID", "1548866591")
MONDAY_SEA_BOARD_ID = os.getenv("MONDAY_SEA_BOARD_ID", "1548866592")

CUSTOMER_SHEET_MAP = {
    # os.getenv("ABOW_LINE_ID"): os.getenv("ABOW_SHEET_URL"),
    # os.getenv("GORSKY_USER_ID"): os.getenv("MOMO_SHEET_URL"),
    # os.getenv("YVES_LINE_ID"): os.getenv("YVES_SHEET_URL"),
    # # 更多客戶可繼續擴充
}

PROCUREMENT_SHEET_MAP = {
    "C4a79c82a68de645459541d346767d905": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit?gid=545798007#gid=545798007"
}

SKU_MAPPING_SHEET_MAP = {
"C8f48ab6318c34a9dee6e98ee035139e5": "https://docs.google.com/spreadsheets/d/12PLvVJCpYgaFE1XKvarnBh0-tLrCCCS4W7splLlSlVw/edit?gid=0#gid=0"
    # os.getenv("ABOW_LINE_ID"): os.getenv("ABOW_SKU_MAPPING_URL"),
    # os.getenv("ABOW_USER_ID"): os.getenv("ABOW_SKU_MAPPING_URL"),
}

FEDEX_SHEET_MAP = {
    "C8f48ab6318c34a9dee6e98ee035139e5": "https://docs.google.com/spreadsheets/d/18iDpFj8zj1BW7vsPuW6OKxdcseJabH9Q3zmcirmjnA8/edit?gid=0#gid=0",
    "C29f47c54b73f52cacbdfda2e26122f82": "https://docs.google.com/spreadsheets/d/1lDqkQ8Vu4trpCzTmOa4wsDmfMNxQiTsg-zMkhj_9UTI/edit?gid=0#gid=0",

}

RECEIPT_SHEET_URL_MAP = {
    # 新格式：dict，支援 "sheet_url" 與 "columns"（columns 可用欄位字母或數字；數字可為 0-based 或 1-based）
    "us": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1GD-SG7racPxmJ4qWTb5NhENKmgUad8AEnZ1hoYZpis8/edit",
        "columns": {
            "order_number": "A",
            "purchase_amount": "C",
            "purchase_fee": "D",
            "share_link": "E"
        }
    },

    # 舊格式：直接字串（仍向後相容）
    "ca": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/14w4Y3jXddjLXF0GcXGLS6iYotK5IUViAIs5d0Q4TjIY/edit",
        "columns": {
            "order_number": "A",
            "purchase_amount": "B",
            "purchase_fee": "N",
            "share_link": "O"
        }
    },

    # 支援 1-based 整數（會被視為 1-based 並轉為 0-based）
    "de": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1LMnK13e8ZaIQV1onR2t-3tOPkFe3duZqwolT5NaiwPQ/edit",
        "columns": {
            "order_number": "C",
            "purchase_amount": "K",
            "purchase_fee": "L",
            "share_link": "R"
        }
    },

    # 支援 0-based 整數索引
    "es": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1VonytTEa3AUpHL2F7B2NkyzHWrc9BLz2up9cwb8W0F4/edit",
        "columns": {
            "order_number": 0,
            "purchase_amount": 5,
            "purchase_fee": 6,
            "share_link": 12
        }
    },

    # 範例：不同欄位位置
    "uk": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1VonytTEa3AUpHL2F7B2NkyzHWrc9BLz2up9cwb8W0F4/edit",
        "columns": {
            "order_number": "A",
            "purchase_amount": "F",
            "purchase_fee": "G",
            "share_link": "M"
        }
    }
}

# Group ID to default country mapping for skipping country selection
GROUP_COUNTRY_MAP = {
    "C4a79c82a68de645459541d346767d905": "us",
    # Add more mappings as needed
}

RECEIPT_SHEET_URL_MAP_REPORT = {
    # 新格式：dict，支援 "sheet_url" 與 "columns"（columns 可用欄位字母或數字；數字可為 0-based 或 1-based）
    "us": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1VOzoIasGDvbbTiMlirHB7OqKEM1Mk1zx3QF_1YeW0Rk/edit",

    },

    # 舊格式：直接字串（仍向後相容）
    "ca": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/14w4Y3jXddjLXF0GcXGLS6iYotK5IUViAIs5d0Q4TjIY/edit",

    },

    # 支援 1-based 整數（會被視為 1-based 並轉為 0-based）
    "de": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/11zy0FGoPjeAIjOmLjGSBdWuIqedGIIXZtG2-atZoibo/edit",
    },

    # 支援 0-based 整數索引
    "es": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1VonytTEa3AUpHL2F7B2NkyzHWrc9BLz2up9cwb8W0F4/edit",

    },

    # 範例：不同欄位位置
    "uk": {
        "sheet_url": "https://docs.google.com/spreadsheets/d/1VonytTEa3AUpHL2F7B2NkyzHWrc9BLz2up9cwb8W0F4/edit",

    }
}

COUNTRY_SHEET_URLS = {
    "ca": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit#gid=1897914111",
    "es": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit#gid=1197937268",
    "de": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit#gid=143054490",
    "uk": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit#gid=833326799",
    "us": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit#gid=545798007",
    "fr": "https://docs.google.com/spreadsheets/d/1Fd7YPcUUrHiAJK5xlieireYkjcxIDGpmgyt_kl8LY8Q/edit?gid=155191165",
}

# ⚙️ 建立觸發來源群組對應的客戶通知群組
CUSTOMER_NOTIFICATION_GROUPS = {
    "C8f48ab6318c34a9dee6e98ee035139e5": [
        "Cc699f6ad13c0b3183f8f696a58af39ad",
        "Cdafd0e1036312b0d4b9df47789d53edb",
        "C18048c2ddc08601d7a806132ad15695e",
        "Cee2dca73ea898fcb3e971618d134a226",
        ],
    "C7c91af29fb566fd4881868f9d6941f62": [
        "Cc699f6ad13c0b3183f8f696a58af39ad",
        "Cdafd0e1036312b0d4b9df47789d53edb",
        "C18048c2ddc08601d7a806132ad15695e",
        "Cee2dca73ea898fcb3e971618d134a226",
        ],
    # 依實際情況新增更多對應
}

# 推播群組對應：供 quickquote2.resolve_push_targets 使用
PUSH_GROUPS = {
    "groups": {
        "katie": ["Cdafd0e1036312b0d4b9df47789d53edb"],
        "misshsieh": ["C18048c2ddc08601d7a806132ad15695e"],
        "abow": ["Cc699f6ad13c0b3183f8f696a58af39ad"],
        "yngabow": ["Cc598e5d1c3c846476471290d5d1cc301"],
        "misshsu": ["Cee2dca73ea898fcb3e971618d134a226"],
        "molly": ["C6b77ef56a6bf2add8fea3548e5520ae6"],
        "lynn": ["C29f47c54b73f52cacbdfda2e26122f82"],
        "onepiece": ["Cfc755c918dbbf7c13f07247040bfb8a1"],
        "Chilton": ["C8ac35d5ef89240d8166d2ea20cc57e5b"],
        "linebotsandbox": ["C8f48ab6318c34a9dee6e98ee035139e5"],
        "momo": ["C8f4e8a87924cf0ac3044dcad3237d327"],
        "starjessie": ["C1e4d88a5b127aa823fbdb3b53b7660ae"],
        "KORY": ["Cc89eeab3deb08404bbbd23fd9a59fe1d"],
        "UTFSIMCLIENTA": ["Cad3d7e63ddbea0bbced830c2607284c5"],
    },
    
        "all": [
        "Cdafd0e1036312b0d4b9df47789d53edb",
        "C18048c2ddc08601d7a806132ad15695e",
        # "Cc699f6ad13c0b3183f8f696a58af39ad",
        # "Cc598e5d1c3c846476471290d5d1cc301",
        "Cee2dca73ea898fcb3e971618d134a226",
        "C6b77ef56a6bf2add8fea3548e5520ae6",
        "C29f47c54b73f52cacbdfda2e26122f82",
        "Cfc755c918dbbf7c13f07247040bfb8a1",
        "C8ac35d5ef89240d8166d2ea20cc57e5b",
        # "C8f4e8a87924cf0ac3044dcad3237d327",momo
        "C1e4d88a5b127aa823fbdb3b53b7660ae",
        # "Cc89eeab3deb08404bbbd23fd9a59fe1d",
    ]
}

EZWAY_NAME_TO_GROUP = {
    # 範例：實際請填你們的群組 ID
    "游繼堯 0975538536": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "鄭詠渝 0952025151": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "鄭芸婷 0917353699": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "游承哲 0972951960": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "游傳杰 0936430817": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "陳秀華 0926330889": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "陳秀玲 0928316962": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "詹欣陵 0910481060": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "陳志賢 0935008584": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "曾惠玲 0952564850": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "柯雅甄 0932962703": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "游玉慧 0911367696": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "李白秀 0922093628": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "陳聖玄 0910492845": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "陳恒楷 0906336124": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "鄭湧璋 0933410911": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "陳璿任 0909620351": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "游依珊 0932629713": "Cc699f6ad13c0b3183f8f696a58af39ad",
    "林厚吉 0987869256": "Cc699f6ad13c0b3183f8f696a58af39ad",



    # Katie GROUP
    "李慧芝 0922950101": "Cdafd0e1036312b0d4b9df47789d53edb",
    "李錦祥 0932581818": "Cdafd0e1036312b0d4b9df47789d53edb",

    # MissHsieh GROUP
    "謝茹羽 0921267520": "C18048c2ddc08601d7a806132ad15695e",

    # MissHsu GROUP
    "許雅惠 0906575741": "Cee2dca73ea898fcb3e971618d134a226",
    "林建興 0965580565": "Cee2dca73ea898fcb3e971618d134a226",

    # Momo GROUP
    "解佩頴 0903025258": "C8f4e8a87924cf0ac3044dcad3237d327",
    "梁穎琦 0968196670": "C8f4e8a87924cf0ac3044dcad3237d327",
    "張詠凱 0916013256": "C8f4e8a87924cf0ac3044dcad3237d327",
    "劉育伶 0930600056": "C8f4e8a87924cf0ac3044dcad3237d327",
    "羅唯英 0937902979": "C8f4e8a87924cf0ac3044dcad3237d327",
    "陳品茹 0917376261": "C8f4e8a87924cf0ac3044dcad3237d327",
    "張碧蓮 0902151973": "C8f4e8a87924cf0ac3044dcad3237d327",
    "吳政融 0926538882": "C8f4e8a87924cf0ac3044dcad3237d327",
    "解瑋庭 0984122360": "C8f4e8a87924cf0ac3044dcad3237d327",
    "洪君豪 0970333752": "C8f4e8a87924cf0ac3044dcad3237d327",
    "洪芷翎 0965265575": "C8f4e8a87924cf0ac3044dcad3237d327",
    "羅木癸 0937902977": "C8f4e8a87924cf0ac3044dcad3237d327",
    "洪金珠 0919932110": "C8f4e8a87924cf0ac3044dcad3237d327",
    "林憶慧 0931314035": "C8f4e8a87924cf0ac3044dcad3237d327",
    "葉怡秀 0909514821": "C8f4e8a87924cf0ac3044dcad3237d327",
    "葉詹明 0955964527": "C8f4e8a87924cf0ac3044dcad3237d327",
    "廖聰毅 0968150355": "C8f4e8a87924cf0ac3044dcad3237d327",
    "蔡英豪 0918774251": "C8f4e8a87924cf0ac3044dcad3237d327",
    "魏媴蓁 0913931950": "C8f4e8a87924cf0ac3044dcad3237d327",
    "黃淑芬 0938383159": "C8f4e8a87924cf0ac3044dcad3237d327",
    "許哲維 0922205285": "C8f4e8a87924cf0ac3044dcad3237d327",

    # StarJessie GROUP
    "林堃昇 0925080189": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "林春風 0929663954": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "林菁珍 0939887677": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "許郁雯 0953319161": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "林樺珵 0919574036": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "陳林玉秀 0927371319": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "陳香伶 0988823934": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "連文瑄 0935182233": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "林秋芬 0938587679": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "林玉花 0930795882": "C1e4d88a5b127aa823fbdb3b53b7660ae",
    "陳星妤 0921513609": "C1e4d88a5b127aa823fbdb3b53b7660ae",

    "林明慧 0985030904": "C8ac35d5ef89240d8166d2ea20cc57e5b",  # Chilton GROUP

    "鐘偲芸 0923550656": "Cc89eeab3deb08404bbbd23fd9a59fe1d",  # Lynn GROUP

    "沈冠羽 0912847328": "C9baf098999062356e8a0dcb6ca82fe7d",  # Peter Shan GROUP


 # YNGABOW GROUP
    "顧志忠 0986789323": "Cc598e5d1c3c846476471290d5d1cc301", 
    "周佩樺 0968432889": "Cc598e5d1c3c846476471290d5d1cc301",
    "顧郭蓮梅 0983458148": "Cc598e5d1c3c846476471290d5d1cc301",
    "顧嘉玲 0921000815": "Cc598e5d1c3c846476471290d5d1cc301",
    "廖芯儀 0939724951": "Cc598e5d1c3c846476471290d5d1cc301",
    
    "劉淑玫 09711800915": "Cc598e5d1c3c846476471290d5d1cc301", 

}

CLIENT_SHEETS = [
    {"name": "許小姐", "url": "1oUyI1uEOHTtzHeDjIK8GrF-dozj1P4xBlSGIYAazeNs"},
    {"name": "謝小姐", "url": "1Imml3ESw8pEixvZjz2P5OkgyyBSmOurgU7oqgxQhWLw"},
    {"name": "Katie",   "url": "1tTyRcg7XpHfYHCx0W2o7cqthSCh_1SZwiMyMNYyPTvU"},
    {"name": "Momo",    "url": "1D2kWplbshNWMYskiHcm7F3OOAjJZa8lFylRaYhTlCRA"},
    {"name": "散客",     "url": "1mPOEhKPp7UpQp-NPY8fUYvba-9jvUhOqPpOxQyZ0_hU"},
    {"name": "大航家",   "url": "1yEz6t7djHA5JEGBGN1q7LT_iIoSYbNsyrViLUNV-7JM"},
    {"name": "Chilton",  "url": "1SxuAnRbUPr2-wScT5OX9esw9gkpYnxjvJR9kE6araX0"},
    {"name": "Molly",    "url": "1E6TyF1cSMgZzrwzA2zNNNzwOZlyAyYkVanI3v-EZqmI"},
    {"name": "StarJessie",  "url": "15_CRh7g-rm029THue05K-lhpBWuIst-4oGWIkuDsOhY"},
    {"name": "Sandbox", "url": "18iDpFj8zj1BW7vsPuW6OKxdcseJabH9Q3zmcirmjnA8"},
    {"name": "LYNN", "url": "1lDqkQ8Vu4trpCzTmOa4wsDmfMNxQiTsg-zMkhj_9UTI"},
]

# Hardcoded client data: client_name -> {group_id, master_sheet_url, sku_mapping_sheet_url}
CLIENT_DATA = {
    "LINEBOTSANDBOX": {
        "group_id": "C8f48ab6318c34a9dee6e98ee035139e5",
        "master_sheet_url": "https://docs.google.com/spreadsheets/d/18iDpFj8zj1BW7vsPuW6OKxdcseJabH9Q3zmcirmjnA8/edit",  # Replace with actual URL
        "sku_mapping_sheet_url": "https://docs.google.com/spreadsheets/d/12PLvVJCpYgaFE1XKvarnBh0-tLrCCCS4W7splLlSlVw/edit"  # Replace with actual URL
    },
    "Katie": {
        "group_id": "Cdafd0e1036312b0d4b9df47789d53edb",
        "master_sheet_url": os.getenv("KATIE_SHEET_URL", "https://docs.google.com/spreadsheets/d/1tTyRcg7XpHfYHCx0W2o7cqthSCh_1SZwiMyMNYyPTvU/edit"),
        "sku_mapping_sheet_url": os.getenv("KATIE_SKU_MAPPING_URL", "https://docs.google.com/spreadsheets/d/1PxI5hRphnkTItE1B9vzKJGit_jgHgYJZD4tyc8DUegU/edit")
    },
    "Katie2": {
        "group_id": "C0edf6ac6b03abcd448ff30e4bcd4925b",
        "master_sheet_url": os.getenv("KATIE2_SHEET_URL", os.getenv("KATIE_SHEET_URL", "https://docs.google.com/spreadsheets/d/1tTyRcg7XpHfYHCx0W2o7cqthSCh_1SZwiMyMNYyPTvU/edit")),
        "sku_mapping_sheet_url": os.getenv("KATIE2_SKU_MAPPING_URL", os.getenv("KATIE_SKU_MAPPING_URL", "https://docs.google.com/spreadsheets/d/1PxI5hRphnkTItE1B9vzKJGit_jgHgYJZD4tyc8DUegU/edit"))
    },
    "MissHsieh": {
        "group_id": "C18048c2ddc08601d7a806132ad15695e",
        "display_name": "謝小姐",
        "master_sheet_url": os.getenv("MISSHSIEH_SHEET_URL", "https://docs.google.com/spreadsheets/d/1Imml3ESw8pEixvZjz2P5OkgyyBSmOurgU7oqgxQhWLw/edit"),  # Temporary fallback to Katie's sheet
        "sku_mapping_sheet_url": os.getenv("MISSHSIEH_SKU_MAPPING_URL", "https://docs.google.com/spreadsheets/d/1PxI5hRphnkTItE1B9vzKJGit_jgHgYJZD4tyc8DUegU/edit")  # Temporary fallback to Katie's SKU sheet
    },
    "MissHsu": {
        "group_id": "Cee2dca73ea898fcb3e971618d134a226",
        "display_name": "許小姐",
        "master_sheet_url": os.getenv("MISSHSU_SHEET_URL", "https://docs.google.com/spreadsheets/d/1oUyI1uEOHTtzHeDjIK8GrF-dozj1P4xBlSGIYAazeNs/edit"),
        "sku_mapping_sheet_url": os.getenv("MISSHSU_SKU_MAPPING_URL")
    },
    "Lynn": {
        "group_id": "C29f47c54b73f52cacbdfda2e26122f82",
        "master_sheet_url": os.getenv("LYNN_SHEET_URL"),
        "sku_mapping_sheet_url": os.getenv("LYNN_SKU_MAPPING_URL")
    },
    "Alvie": {
        "group_id": "Cc699f6ad13c0b3183f8f696a58af39ad",
        "master_sheet_url": os.getenv("ACE_SHEET_URL"),  # Assuming ACE is Alvie
        "sku_mapping_sheet_url": os.getenv("ACE_SKU_MAPPING_URL")
    },

    "Chilton": {
        "group_id": "C8ac35d5ef89240d8166d2ea20cc57e5b",
        "master_sheet_url": os.getenv("CHILTON_SHEET_URL","https://docs.google.com/spreadsheets/d/1SxuAnRbUPr2-wScT5OX9esw9gkpYnxjvJR9kE6araX0/edit"),
        "sku_mapping_sheet_url": os.getenv("CHILTON_SKU_MAPPING_URL","https://docs.google.com/spreadsheets/d/1qFUwGxJ_sHDoz3J_vX6F9kAu01ib8MwjBv9TKNmED2E/edit")
    },

    "OnePiece": {
        "group_id": "Cfc755c918dbbf7c13f07247040bfb8a1",
        "master_sheet_url": os.getenv("ONEPIECE_SHEET_URL"),
        "sku_mapping_sheet_url": os.getenv("ONEPIECE_SKU_MAPPING_URL")
    },
    "Momo": {
        "group_id": "C8f4e8a87924cf0ac3044dcad3237d327",
        "master_sheet_url": "https://docs.google.com/spreadsheets/d/1D2kWplbshNWMYskiHcm7F3OOAjJZa8lFylRaYhTlCRA/edit",
        "sku_mapping_sheet_url": "https://docs.google.com/spreadsheets/d/1UFcucAkWuoXOFfiTAVyaoY0s21hYiNJoSoSHwrja-K0/edit"
    },
    "散客": {
        "group_id": "",
        "master_sheet_url": "https://docs.google.com/spreadsheets/d/1mPOEhKPp7UpQp-NPY8fUYvba-9jvUhOqPpOxQyZ0_hU/edit",
        "sku_mapping_sheet_url": ""
    },
    "StarJessie": {
        "group_id": "C1e4d88a5b127aa823fbdb3b53b7660ae",
        "master_sheet_url": os.getenv("STARJESSIE_SHEET_URL"),
        "sku_mapping_sheet_url": os.getenv("STARJESSIE_SKU_MAPPING_URL")
    },
    "Molly": {
        "group_id": "C6b77ef56a6bf2add8fea3548e5520ae6",
        "master_sheet_url": os.getenv("MOLLY_SHEET_URL"),
        "sku_mapping_sheet_url": os.getenv("MOLLY_SKU_MAPPING_URL")
    },

    "KORY": {
        "group_id": "Cc89eeab3deb08404bbbd23fd9a59fe1d",
        "master_sheet_url": os.getenv("KORY_SHEET_URL"),
        "sku_mapping_sheet_url": os.getenv("KORY_SKU_MAPPING_URL")
    },

    # Add more clients as needed
}

CLIENT_DATA_ALLOCATION = [
    {"name": data.get("display_name", name), "url": data["master_sheet_url"]}
    for name, data in CLIENT_DATA.items()
    if data.get("master_sheet_url")
]

def get_customer_name_by_group_id(group_id):
    for name, data in CLIENT_DATA.items():
        if data["group_id"] == group_id:
            return name
    return None

def get_customer_sheet_url(group_id, sheet_type):
    for name, data in CLIENT_DATA.items():
        if data["group_id"] == group_id:
            if sheet_type == "master":
                return data["master_sheet_url"]
            elif sheet_type == "sku_mapping":
                return data["sku_mapping_sheet_url"]
    return None

# 注意：已在上方定義完整的 RECEIPT_SHEET_URL_MAP（支援 dict 格式包含 "sheet_url" 與 "columns"）。
# 刪除底部的覆寫，避免把範例 mapping 覆蓋成無效的 YOUR_*_SHEET_ID。
# 若要在部署時覆寫對應值，請直接編輯上方的 mapping 或使用環境變數來設定實際 sheet URL/ID。

# # 可觸發 handle_order_event 的使用者 ID
# ALLOWED_USER_ID = {
#     "U867fd2e288b0d68ebed980cfa022d565",  # 你的個人 LINE ID
#
#     # 可再加入其他管理員 ID
# }
#
# # 可觸發 handle_order_event 的 群組 ID
# ALLOWED_GROUP_ID = {
#
#     "C8f48ab6318c34a9dee6e98ee035139e5",  # 群組 ID
#     # 可再加入其他管理員 ID
# }

# 自動將 GOOGLE_CREDENTIALS 環境變數內容寫入 credentials/service_account.json
if os.getenv("GOOGLE_CREDENTIALS", "").startswith("{"):
    os.makedirs("credentials", exist_ok=True)
    with open("credentials/service_account.json", "w") as f:
        f.write(os.getenv("GOOGLE_CREDENTIALS"))

# Path for service account json used by gdrive agent
CREDENTIAL_FILE = os.environ.get("CREDENTIAL_FILE", "/app/gcloud-sa.json")

# Local dev fallback: ./credentials/credentials.json if exists
_local_creds = Path.cwd() / "credentials" / "credentials.json"
if os.environ.get("ENVIRONMENT", "development") == "development" and _local_creds.exists() and not Path(CREDENTIAL_FILE).exists():
    CREDENTIAL_FILE = str(_local_creds)

# If GCP_SA_KEY provided (raw JSON or base64), write it to CREDENTIAL_FILE for libraries to consume
_gcp_key = os.environ.get("GCP_SA_KEY")
if _gcp_key:
    Path(CREDENTIAL_FILE).parent.mkdir(parents=True, exist_ok=True)
    try:
        try:
            key_data = json.loads(_gcp_key)
        except Exception:
            key_data = json.loads(base64.b64decode(_gcp_key).decode("utf-8"))
        with open(CREDENTIAL_FILE, "w", encoding="utf-8") as f:
            json.dump(key_data, f, ensure_ascii=False)
        try:
            os.chmod(CREDENTIAL_FILE, 0o600)
        except Exception:
            pass
    except Exception:
        # 寫入失敗不要阻斷啟動，agent 啟動時可檢查檔案是否存在
        pass

# Optional: email to impersonate when using domain-wide delegation
IMPERSONATE_USER = os.environ.get("IMPERSONATE_USER")  # e.g. "admin@your-domain.com"

# Optional convenience: service account email and root upload folder id
SA_CLIENT_EMAIL = os.environ.get("SA_CLIENT_EMAIL") or os.environ.get("SERVICE_ACCOUNT_EMAIL")
ROOT_FOLDER_ID = os.environ.get("ROOT_FOLDER_ID")

# Export names for other modules
__all__ = [
    # ...existing exports...
] + ["CREDENTIAL_FILE", "IMPERSONATE_USER", "SA_CLIENT_EMAIL", "ROOT_FOLDER_ID", "RECEIPT_SHEET_URL_MAP", "COUNTRY_SHEET_URLS"]

# # List of customer names for dynamic lookup (add new customers here)
# CUSTOMER_NAMES = ["LINEBOTSANDBOX", "KATIE"]  # Add the new customer name


