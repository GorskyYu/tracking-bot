import atexit
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
from .config import TIMEZONE
from .log import log
from .ace import push_ace_today_shipments
from .sq  import push_sq_weekly_shipments

_ace_sched = None
_sq_sched  = None

def ensure_ace_scheduler():
    global _ace_sched
    if _ace_sched: return _ace_sched
    tz = pytz.timezone(TIMEZONE)
    s = BackgroundScheduler(timezone=tz)
    s.add_job(push_ace_today_shipments, trigger="cron",
              day_of_week="thu,sun", hour=16, minute=0,
              kwargs={"force": False},
              id="ace_today_shipments_thu_sun_4pm",
              replace_existing=True, misfire_grace_time=600,
              coalesce=True, max_instances=1)
    s.start(); atexit.register(lambda: s.shutdown(wait=False))
    log.info("[ACE Today] Scheduler started (Thu/Sun 16:00 America/Vancouver).")
    _ace_sched = s; return s

def ensure_sq_scheduler():
    global _sq_sched
    if _sq_sched: return _sq_sched
    tz = pytz.timezone(TIMEZONE)
    s = BackgroundScheduler(timezone=tz)
    s.add_job(push_sq_weekly_shipments, trigger="cron",
              day_of_week="sat", hour=9, minute=0,
              kwargs={"force": False},
              id="sq_weekly_shipments_sat_9am",
              replace_existing=True, misfire_grace_time=600,
              coalesce=True, max_instances=1)
    s.start(); atexit.register(lambda: s.shutdown(wait=False))
    log.info("[SQ Weekly] Scheduler started (Sat 09:00 America/Vancouver).")
    _sq_sched = s; return s
