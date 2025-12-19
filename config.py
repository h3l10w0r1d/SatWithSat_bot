import os
import json
from typing import Set
from zoneinfo import ZoneInfo

TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()
TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "UTC").strip()

# Admins: supports "123,456" OR "[123,456]"
ADMIN_TELEGRAM_IDS_RAW = (os.environ.get("ADMIN_TELEGRAM_IDS") or "").strip()

# Optional SAT AI tutor
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini").strip()

# Scheduler switch
DISABLE_SCHEDULER = (os.environ.get("DISABLE_SCHEDULER") or "").strip() == "1"

# Rules
MAX_DAILY_TESTS = 6
COOLDOWN_MINUTES = 30
SAVER_EARN_THRESHOLD = 3  # earn 1 saver/day once you log 3 tests in a day

def parse_admin_ids(raw: str) -> Set[int]:
    raw = (raw or "").strip()
    out: Set[int] = set()
    if not raw:
        return out

    # JSON list
    if raw.startswith("[") and raw.endswith("]"):
        try:
            arr = json.loads(raw)
            for x in arr:
                out.add(int(x))
            return out
        except Exception:
            pass

    # Comma-separated
    for part in raw.split(","):
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
