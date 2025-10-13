import os
import re

TIMEZONE = os.getenv("TIMEZONE", "America/Vancouver")

# LINE
LINE_TOKEN  = os.getenv("LINE_TOKEN")         # Channel access token
LINE_PUSH_URL  = "https://api.line.me/v2/bot/message/push"
LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
LINE_HEADERS = {
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

# Groups / Users / Sheets
# ─── LINE & ACE/SQ 設定 ──────────────────────────────────────────────────────
ACE_GROUP_ID     = os.getenv("LINE_GROUP_ID_ACE")
SOQUICK_GROUP_ID = os.getenv("LINE_GROUP_ID_SQ")
VICKY_GROUP_ID   = os.getenv("LINE_GROUP_ID_VICKY")
VICKY_USER_ID    = os.getenv("VICKY_USER_ID")
YUMI_GROUP_ID    = os.getenv("LINE_GROUP_ID_YUMI")
JOYCE_GROUP_ID   = os.getenv("LINE_GROUP_ID_JOYCE")
PDF_GROUP_ID     = os.getenv("LINE_GROUP_ID_PDF")
YVES_USER_ID     = os.getenv("YVES_USER_ID")

ACE_SHEET_URL    = os.getenv("ACE_SHEET_URL")
SQ_SHEET_URL     = os.getenv("SQ_SHEET_URL")

# Monday / OpenAI / TE API…
MONDAY_TOKEN     = os.getenv("MONDAY_TOKEN")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

APP_ID     = os.getenv("TE_APP_ID")
APP_SECRET = os.getenv("TE_SECRET")          # your TE App Secret

# Regex / Names
CODE_TRIGGER_RE = re.compile(r"\b(?:ACE|250N)\d+[A-Z0-9]*\b")
# Names to look for in each group’s list
VICKY_NAMES = {"顧家琪","顧志忠","周佩樺","顧郭蓮梅","廖芯儀","林寶玲","高懿欣","崔書鳳"}
YUMI_NAMES  = {"劉淑燕","竇永裕","劉淑玫","劉淑茹","陳富美","劉福祥","郭淨崑","陳卉怡","洪瑜駿"}
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
    "游繼堯",
    "游承哲",
    "游傳杰",
    "陳秀華",
    "陳秀玲",
    "陳恒楷"
}
EXCLUDED_SENDERS = {"Yves Lai", "Yves KT Lai", "Yves MM Lai", "Yumi Liu", "Vicky Ku"}
