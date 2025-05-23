# poller.py

import os
import json
import re
from datetime import datetime, timezone
import pytz
import redis
import requests
from main import (
    CUSTOMER_FILTERS,
    LINE_PUSH_URL,
    LINE_HEADERS,
    call_api,
    TIMEZONE,
    TRANSLATIONS,
    get_statuses_for,
)

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
    # Use a timezone-aware now() in UTC
    now_utc = datetime.now(timezone.utc)
    print(f"[Poller {now_utc.isoformat()}] {msg}", flush=True)

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
    
    # Normalize any stored timestamps to ints
    state = { oid: int(ts) for oid, ts in state.items() }

    updates = {}

    # For each group, fetch its shipments
    for group_id, keywords in CUSTOMER_FILTERS.items():
        # 1) Get the filtered list of IDs
        lines = get_statuses_for(keywords)
        # lines[0] is header; each subsequent line starts with "<OID> (<NUM>)"
        oids = [ID_RE.match(l).group(1) for l in lines[1:]]

        if not oids:
            continue

        # 2) Fetch raw tracking in one call
        resp = call_api("shipment/tracking", {
            "keyword": ",".join(oids),
            "rsync":   0,
            "timezone": TIMEZONE
        })

        new_lines = []
        for item in resp.get("response", []):
            oid = str(item["id"])
            num = item.get("number","")
            events = item.get("list") or []
            if not events:
                continue

            # pick the newest event record
            ev = max(events, key=lambda e: int(e["timestamp"]))
            ts_raw = int(ev["timestamp"])

            # format
            loc_raw = ev.get("location","")
            loc      = f"[{loc_raw.replace(',',', ')}] " if loc_raw else ""
            ctx_lc   = ev.get("context","").strip().lower()
            human    = TRANSLATIONS.get(ctx_lc, ev.get("context","").replace("Triple Eagle","system"))
            tme      = ev["datetime"].get(TIMEZONE, ev["datetime"].get("GMT",""))
            line     = f"{oid} ({num}) → {loc}{human}  @ {tme}"

            # Only include if this event is strictly newer
            if state.get(oid, 0) < ts_raw:
                state[oid] = ts_raw
                new_lines.append(line)

        if new_lines:
            updates[group_id] = new_lines

    # Persist updated state back to Redis
    r.set("last_seen", json.dumps(state))
    
    # On our very first run, seed only—no pushes
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
        resp = requests.post(LINE_PUSH_URL, headers=LINE_HEADERS, json=payload)
        log(f"LINE push status={resp.status_code}")

if __name__ == "__main__":
    main()