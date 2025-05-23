# poller.py

import os
import json
from datetime import datetime
import pytz
import redis
from main import check_te_updates, CLIENT_TO_GROUP, LINE_PUSH_URL, LINE_HEADERS, load_state, save_state, get_statuses_for

# Initialize Redis
redis_url = os.getenv("REDIS_URL") or os.getenv("REDISCLOUD_URL")
r = redis.from_url(redis_url, decode_responses=True)

def in_window(now):
    if now.weekday() == 6:
        return False
    if now.hour < 4 or now.hour > 19:
        return False
    if now.minute not in (0, 30):
        return False
    return True

def log(msg):
    print(f"[Poller {datetime.utcnow().isoformat()}] {msg}", flush=True)

def main():
    tz = pytz.timezone("America/Vancouver")
    now = datetime.now(tz)
    log(f"Started; now={now.strftime('%Y-%m-%d %H:%M')} PT")
    if not in_window(now):
        log("Outside time window, exiting")
        return

    # Load existing state
    state_data = r.get("last_seen") or "{}"
    state = json.loads(state_data)

    # Gather updates
    updates = {}
    for group_id, keywords in CLIENT_TO_GROUP.items():
        lines = get_statuses_for(keywords)
        new_lines = []
        for line in lines[1:]:
            order_key, ts = line.rsplit("@", 1)
            ts = ts.strip()
            if state.get(order_key) != ts:
                state[order_key] = ts
                new_lines.append(line)
        if new_lines:
            updates[group_id] = new_lines

    # Persist state
    r.set("last_seen", json.dumps(state))

    # Push any updates
    if not updates:
        log("No new updates, exiting")
        return

    for group_id, lines in updates.items():
        text = "\n\n".join(lines)
        log(f"Pushing {len(lines)} updates to group {group_id}")
        payload = {"to": group_id, "messages":[{"type":"text","text":text}]}
        resp = __import__("requests").post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log(f"LINE push status={resp.status_code}")

if __name__ == "__main__":
    main()
