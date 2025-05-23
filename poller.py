# poller.py

import os
from datetime import datetime
import pytz

from main import check_te_updates  # ← your Flask app module

def in_window(now):
    # Mon=0 … Sun=6
    if now.weekday() == 6:
        return False      # skip Sunday
    h = now.hour
    m = now.minute
    # 7 am ET → 4 am PT; 7 pm PT → 19 pm PT
    if h < 4 or h > 19:
        return False
    # only run on :00 and :30
    if m not in (0, 30):
        return False
    return True

def main():
    tz = pytz.timezone("America/Vancouver")
    now = datetime.now(tz)
    if not in_window(now):
        return
    # This will load last_seen.json and push any new updates
    check_te_updates()

if __name__ == "__main__":
    main()
