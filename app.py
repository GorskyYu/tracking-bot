from flask import Flask, request
import json
from log import log
from config import VICKY_GROUP_ID, YUMI_GROUP_ID, JOYCE_GROUP_ID, PDF_GROUP_ID
from line_api import line_push, line_reply
from schedulers import ensure_ace_scheduler, ensure_sq_scheduler
import handlers  # import 你在 handlers.py 暴露的函式

app = Flask(__name__)

# 啟動 APScheduler（可當作 web dyno 備援；Heroku Scheduler 依然存在）
try:
    ensure_ace_scheduler()
except Exception as e:
    log.error(f"[ACE Today] Scheduler init failed: {e}")
try:
    ensure_sq_scheduler()
except Exception as e:
    log.error(f"[SQ Weekly] Scheduler init failed: {e}")

@app.route("/webhook", methods=["GET","POST"])
def webhook():
    if request.method == "GET":
        return "OK", 200

    data = request.get_json()
    print("[Webhook] Payload:", json.dumps(data, ensure_ascii=False))

    for event in data.get("events", []):
        if event.get("type") != "message":  # 忽略 unsend 等
            continue
        m = event["message"]
        if m.get("type") == "file" and m.get("fileName","").lower().endswith(".pdf"):
            # 這裡呼叫你 OCR 的處理器（在 handlers.py）
            handlers.handle_pdf_upload(event)
            continue

        if m.get("type") == "text":
            handlers.handle_soquick_and_ace_shipments(event)
            handlers.handle_ace_shipments(event)
            handlers.handle_missing_confirm(event)
            handlers.handle_ace_ezway_check_and_push_to_yves(event)

    return "OK", 200
