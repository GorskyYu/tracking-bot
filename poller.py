# poller.py

import os
import json
import re
from datetime import datetime
import pytz
import redis
from main import CUSTOMER_FILTERS, LINE_PUSH_URL, LINE_HEADERS, get_statuses_for

# Initialize Redis
redis_url = os.getenv("REDIS_URL") or os.getenv("REDISCLOUD_URL")
r = redis.from_url(redis_url, decode_responses=True)

# Regex to pull the order ID at the start of each line
ID_RE = re.compile(r"^([^ ]+)")

def in_window(now):
    # Mon=0 … Sun=6
    if now.weekday() == 6:
        return False    # skip Sunday
    # 7 am ET → 4 am PT; 7 pm PT → 19 pm PT
    if now.hour < 4 or now.hour > 19:
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

    # Load existing state; detect if this is our very first run
    state_data = r.get("last_seen")
    initial_run = state_data is None
    state = json.loads(state_data) if state_data else {}

    updates = {}

    # For each group, fetch its shipments
    for group_id, keywords in CUSTOMER_FILTERS.items():
        lines = get_statuses_for(keywords)
        new_lines = []

        # Skip the header (timestamp) at lines[0]
        for line in lines[1:]:
            # Extract shipment ID
            m = ID_RE.match(line)
            oid = m.group(1) if m else line

            # Extract timestamp (the part after "@")
            _, ts = line.rsplit("@", 1)
            ts = ts.strip()

            # Only include if timestamp advanced
            if state.get(oid) != ts:
                state[oid] = ts
                new_lines.append(line)

        if new_lines:
            updates[group_id] = new_lines

    # Persist updated state back to Redis
    r.set("last_seen", json.dumps(state))
    
    # On our very first run, we just seed state—no pushes
    if initial_run:
        log("Initial run: state seeded, no pushes")
        return    

    # Push batched updates for each group
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