# holiday_reminder.py

import datetime
import holidays
import requests
import os

def send_canada_holiday_reminder():
    start = datetime.date.today() + datetime.timedelta(days=(7 - datetime.date.today().weekday()))
    end   = start + datetime.timedelta(days=6)

    ca_holidays = holidays.CA(prov='ON')  # 改為你要的省，例如 BC、QC、AB
    matched = [(dt, name) for dt, name in ca_holidays.items() if start <= dt <= end]

    if matched:
        msg = "🇨🇦 下週加拿大國定假日提醒：\n\n"
        for dt, name in matched:
            msg += f"📌 {dt.strftime('%Y-%m-%d')}：{name}\n"
    else:
        msg = "✅ 下週沒有加拿大國定假日。"

    push_line_notify(msg)

def push_line_notify(msg):
    token = os.getenv("LINE_NOTIFY_TOKEN")
    if not token:
        print("⚠️ LINE_NOTIFY_TOKEN 未設定")
        return

    headers = {"Authorization": f"Bearer {token}"}
    data = {"message": msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, data=data)
    print("LINE Notify 發送結果:", r.status_code)
