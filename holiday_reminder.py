import datetime
import holidays
import requests
import os

def send_canada_holiday_reminder():
    # ⚠️ 只有星期一才會檢查
    if datetime.date.today().weekday() != 0:  # 0 = Monday
        print("⏭️ 不是星期一，跳過提醒")
        return   
    # print("✅ 測試中，強制執行假日檢查")

    # 查詢「下週」的國定假日
    # today = datetime.date.today()
    today = datetime.date(2025, 7, 28)  # 任意週一，測試下週一是否為假日
    start = today + datetime.timedelta(days=1)
    end = today + datetime.timedelta(days=7)

    # 整合 Federal + Ontario 假期
    year = start.year
    ca_federal = holidays.CA(years=start.year)
    ca_provincial = holidays.CA(prov='ON', years=start.year)

    all_holidays = dict(ca_federal.items())
    all_holidays.update(ca_provincial.items())

    print("📅 已載入的假期清單:")
    for dt, name in sorted(all_holidays.items()):
        print(f"{dt}: {name}")

    matched = [(dt, name) for dt, name in all_holidays.items() if start <= dt <= end]

    if not matched:
        print("✅ 下週沒有國定假日，不發送提醒")
        return

    # 組訊息並推播
    msg = "🇨🇦 下週加拿大國定假日提醒：\n\n"
    for dt, name in matched:
        msg += f"📌 {dt.strftime('%Y-%m-%d')}：{name}\n"

    push_line_notify(msg)

def push_line_notify(msg):
    token = os.getenv("LINE_TOKEN")  # Messaging API Token
    target = os.getenv("LINE_GROUP_ID_YUMI")  # 群組 ID

    if not token or not target:
        print(f"⚠️ LINE_TOKEN 或 LINE_GROUP_ID_YUMI 未設定\nLINE_TOKEN={bool(token)} LINE_GROUP_ID_YUMI={bool(target)}")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "to": target,
        "messages": [{
            "type": "text",
            "text": msg
        }]
    }

    try:
        r = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload)
        r.raise_for_status()
        print("✅ LINE 推送成功")
    except Exception as e:
        print(f"❌ LINE 推送失敗: {e}")