"""
Centralized APScheduler configuration for background jobs.

This module initializes and manages all scheduled tasks:
- ACE shipments: Thu/Sun 16:00 (America/Vancouver)
- SQ shipments: Sat 09:00 (America/Vancouver)
"""
import atexit
import logging
import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from config import TIMEZONE
from jobs.ace_tasks import push_ace_today_shipments
from jobs.sq_tasks import push_sq_weekly_shipments

log = logging.getLogger(__name__)

# Module-level scheduler instances (singleton pattern)
_ace_scheduler = None
_sq_scheduler = None


def init_ace_scheduler() -> BackgroundScheduler:
    """
    Initialize the ACE shipments scheduler.
    Runs every Thursday and Sunday at 16:00 (America/Vancouver).
    
    Uses coalesce/max_instances to prevent stacked triggers on restart.
    Uses misfire_grace_time to allow short wake delays.
    
    Returns:
        The BackgroundScheduler instance
    """
    global _ace_scheduler
    if _ace_scheduler is not None:
        return _ace_scheduler

    tz = pytz.timezone(TIMEZONE)
    sched = BackgroundScheduler(timezone=tz)
    sched.add_job(
        push_ace_today_shipments,
        trigger="cron",
        day_of_week="thu,sun",
        hour=15,
        minute=0,
        kwargs={"force": False},
        id="ace_today_shipments_thu_sun_4pm",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))
    log.info("[ACE] Scheduler started (Thu/Sun 16:00 America/Vancouver)")

    _ace_scheduler = sched
    return _ace_scheduler


def init_sq_scheduler() -> BackgroundScheduler:
    """
    Initialize the SoQuick shipments scheduler.
    Runs every Saturday at 09:00 (America/Vancouver).
    
    Returns:
        The BackgroundScheduler instance
    """
    global _sq_scheduler
    if _sq_scheduler is not None:
        return _sq_scheduler

    tz = pytz.timezone(TIMEZONE)
    sched = BackgroundScheduler(timezone=tz)
    sched.add_job(
        push_sq_weekly_shipments,
        trigger="cron",
        day_of_week="sat",
        hour=9,
        minute=0,
        kwargs={"force": False},
        id="sq_weekly_shipments_sat_9am",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))
    log.info("[SQ] Scheduler started (Sat 09:00 America/Vancouver)")

    _sq_scheduler = sched
    return _sq_scheduler


def init_all_schedulers() -> None:
    """Initialize all schedulers. Safe to call multiple times."""
    try:
        init_ace_scheduler()
    except Exception as e:
        log.error(f"[ACE] Scheduler init failed: {e}")
    
    try:
        init_sq_scheduler()
    except Exception as e:
        log.error(f"[SQ] Scheduler init failed: {e}")
