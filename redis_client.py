import os
import redis
from log import log

REDIS_URL = os.getenv("REDIS_URL")

if not REDIS_URL:
    # 選配：本地 fallback，省得每次 set REDIS_URL
    try:
        import fakeredis
        log.info("[DEV MODE] No REDIS_URL found, using fakeredis")
        r = fakeredis.FakeStrictRedis(decode_responses=True)
    except Exception:
        raise RuntimeError("REDIS_URL is required (or install fakeredis for local testing)")
else:
    r = redis.from_url(REDIS_URL, decode_responses=True)
