"""
holiday_reminder.py
────────────────────────────────────────────────────────────────
Canadian federal + BC provincial public holidays.
Provides:
  get_next_holiday()              → str  (reply text for LINE)
  send_canada_holiday_reminder()  → None (push reminder to groups)
"""

import os
import logging
from datetime import date, timedelta

import requests

log = logging.getLogger(__name__)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"


def _line_headers():
    token = os.getenv("LINE_TOKEN", "")
    return {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}


# ─── Holiday calendar (Canada / BC) ──────────────────────────────────────────

def _holidays(year: int) -> list[tuple[date, str]]:
    """Return sorted list of (date, name) for the given year."""
    from calendar import monthrange

    def nth_weekday(y, m, weekday, n):
        """Return the date of the n-th occurrence (1-based) of weekday in month m of year y."""
        first = date(y, m, 1)
        delta = (weekday - first.weekday()) % 7
        first_occurrence = first + timedelta(days=delta)
        return first_occurrence + timedelta(weeks=n - 1)

    holidays = [
        (date(year, 1, 1),   "元旦 New Year's Day"),
        (nth_weekday(year, 2, 0, 3),  "家庭日 Family Day (BC)"),     # 3rd Monday of Feb
        (_good_friday(year),           "耶穌受難日 Good Friday"),
        (_easter(year) + timedelta(1), "復活節星期一 Easter Monday"),
        (date(year, 5, 23) if date(year, 5, 23).weekday() == 0
         else nth_weekday(year, 5, 0, 3),  "維多利亞日 Victoria Day"),  # Mon before May 25
        (date(year, 7, 1),   "加拿大國慶日 Canada Day"),
        (nth_weekday(year, 8, 0, 1),  "卑詩省日 BC Day"),             # 1st Monday of Aug
        (nth_weekday(year, 9, 0, 1),  "勞工節 Labour Day"),           # 1st Monday of Sep
        (nth_weekday(year, 10, 0, 2), "感恩節 Thanksgiving"),         # 2nd Monday of Oct
        (date(year, 11, 11), "國殤日 Remembrance Day"),
        (date(year, 12, 25), "聖誕節 Christmas Day"),
        (date(year, 12, 26), "節禮日 Boxing Day"),
    ]

    # Victoria Day: Monday on or before May 25
    may25 = date(year, 5, 25)
    victoria = may25 - timedelta(days=(may25.weekday() + 1) % 7 or 7)
    # Replace the placeholder above with the correct value
    holidays = [(victoria if name == holidays[4][1] else d, n) for d, n in holidays]
    # Replace with correct Victoria Day
    holidays[4] = (victoria, "維多利亞日 Victoria Day")

    # Canada Day: if July 1 falls on Sunday, observed Monday
    canada_day = date(year, 7, 1)
    if canada_day.weekday() == 6:
        canada_day = date(year, 7, 2)
    elif canada_day.weekday() == 5:
        canada_day = date(year, 7, 3)
    holidays[5] = (canada_day, "加拿大國慶日 Canada Day")

    return sorted(holidays, key=lambda x: x[0])


def _easter(year: int) -> date:
    """Compute Easter Sunday (Western) using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(114 + h + l - 7 * m, 31)
    return date(year, month, day + 1)


def _good_friday(year: int) -> date:
    return _easter(year) - timedelta(days=2)


# ─── Public API ───────────────────────────────────────────────────────────────

def get_next_holiday() -> str:
    """
    Return a LINE-reply string describing the next upcoming Canadian/BC holiday.
    """
    today = date.today()
    year = today.year
    candidates = _holidays(year) + _holidays(year + 1)

    for hdate, hname in candidates:
        if hdate >= today:
            days_left = (hdate - today).days
            if days_left == 0:
                when = "今天 (Today)"
            elif days_left == 1:
                when = "明天 (Tomorrow)"
            else:
                when = f"{days_left} 天後 (in {days_left} days)"
            return (
                f"📅 下一個加拿大/卑詩省國定假日\n"
                f"🗓 {hdate.strftime('%Y-%m-%d')} ({hdate.strftime('%A')})\n"
                f"🏖 {hname}\n"
                f"⏳ {when}"
            )

    return "📅 找不到假日資料"


def send_canada_holiday_reminder():
    """
    Push a holiday reminder to ACE and SoQuick groups if a holiday is
    within 7 days.
    """
    today = date.today()
    year = today.year
    candidates = _holidays(year) + _holidays(year + 1)

    upcoming = [
        (hdate, hname)
        for hdate, hname in candidates
        if 0 <= (hdate - today).days <= 7
    ]

    if not upcoming:
        log.info("[Holiday] No holidays within 7 days, skipping reminder.")
        return

    ace_group = os.getenv("LINE_GROUP_ID_ACE", "")
    sq_group = os.getenv("LINE_GROUP_ID_SQ", "")

    for hdate, hname in upcoming:
        days_left = (hdate - today).days
        if days_left == 0:
            when = "今天"
        elif days_left == 1:
            when = "明天"
        else:
            when = f"{days_left} 天後"

        msg = (
            f"⚠️ 加拿大假日提醒\n"
            f"📅 {hdate.strftime('%Y-%m-%d')} ({hdate.strftime('%A')})\n"
            f"🏖 {hname}\n"
            f"⏳ {when}"
        )

        for group_id in [ace_group, sq_group]:
            if not group_id:
                continue
            try:
                requests.post(
                    LINE_PUSH_URL,
                    headers=_line_headers(),
                    json={"to": group_id, "messages": [{"type": "text", "text": msg}]},
                    timeout=10,
                )
                log.info(f"[Holiday] Sent reminder to {group_id}: {hname}")
            except Exception as e:
                log.error(f"[Holiday] Failed to send reminder to {group_id}: {e}")
