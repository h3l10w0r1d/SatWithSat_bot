import os
from typing import Set
from zoneinfo import ZoneInfo

TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()
TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "UTC").strip()

ADMIN_TELEGRAM_IDS_RAW = (os.environ.get("ADMIN_TELEGRAM_IDS") or "").strip()  # "123,456"
DISABLE_SCHEDULER = (os.environ.get("DISABLE_SCHEDULER") or "").strip() == "1"

# Optional SAT AI tutor
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini").strip()

MAX_DAILY_TESTS = 6
COOLDOWN_MINUTES = 30
SAVER_EARN_THRESHOLD = 3  # streak saver earned when daily tests reach 3 (max 1/day)

def parse_admin_ids(raw: str) -> Set[int]:
    out: Set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            pass
    return out

ADMIN_IDS = parse_admin_ids(ADMIN_TELEGRAM_IDS_RAW)

def tz():
    return ZoneInfo(TIMEZONE_NAME)
