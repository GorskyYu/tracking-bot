import datetime
import holidays
import requests
import os

def send_canada_holiday_reminder():
    # ⚠️ 只有星期一才會檢查
    if datetime.date.today().weekday() != 0:  # 0 = Monday
        print("⏭️ 不是星期一，跳過提醒")
        return   

    # 查詢「下週」的國定假日
    today = datetime.date.today()
    next_monday = today + datetime.timedelta(days=(7 - today.weekday()))  # 下週一
    next_sunday = next_monday + datetime.timedelta(days=6)                # 下週日

    ca_holidays = holidays.CA(prov='ON')  # 可改成 'BC', 'QC' 等
    matched = [(dt, name) for dt, name in ca_holidays.items() if next_monday <= dt <= next_sunday]

    if not matched:
        print("✅ 下週沒有國定假日，不發送提醒")
        return

    # 組訊息並推播
    msg = "🇨🇦 下週加拿大國定假日提醒：\n\n"
    for dt, name in matched:
        msg += f"📌 {dt.strftime('%Y-%m-%d')}：{name}\n"

    push_line_notify(msg)

def push_line_notify(msg):
    token = os.getenv("LINE_NOTIFY_TOKEN")
    if not token:
        print("⚠️ LINE_NOTIFY_TOKEN 未設定")
        return

    headers = {"Authorization": f"Bearer {token}"}
    data = {"message": msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, data=data)
    print("📤 LINE Notify 發送結果:", r.status_code)
