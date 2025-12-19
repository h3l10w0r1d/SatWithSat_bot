from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from config import TIMEZONE_NAME, DISABLE_SCHEDULER
from telegram_client import send_message, main_menu_keyboard
from db import db, tests_today_count, set_user_fields

NUDGE_MESSAGES = [
    "Hey {name} 😈 time to do SAT Math. Don’t make me beg.",
    "{name}, your future self called. They want you to log a Math score today 📈",
    "Daily quest: log 1 Math score (0–44). Reward: less panic later 🧠",
    "You’ve got this, {name}. 25 minutes of Math. Then log it. 💪",
    "Why aren't you doing SAT 😡 (this is your friendly chaos reminder)",
]

_scheduler = None
_started = False

def notification_tick() -> None:
    tz = ZoneInfo(TIMEZONE_NAME)
    now_local = datetime.now(tz=tz)
    today = now_local.date()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, chat_id, first_name, nickname, approved, banned,
                       pref_hour, pref_minute, last_nudge_at
                FROM users
                WHERE approved=TRUE AND banned=FALSE
                """
            )
            users = cur.fetchall()

    for u in users:
        uid = int(u["id"])

        if tests_today_count(uid) > 0:
            continue

        last_nudge = u.get("last_nudge_at")
        if last_nudge:
            last_local = last_nudge.astimezone(tz)
            if last_local.date() == today:
                continue

        h = u.get("pref_hour")
        m = u.get("pref_minute")
        if h is None or m is None:
            h, m = 19, 0

        target = now_local.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
        if not (target - timedelta(minutes=10) <= now_local <= target + timedelta(minutes=25)):
            continue

        name = (u.get("nickname") or u.get("first_name") or "champ").strip()
        msg = NUDGE_MESSAGES[(uid + now_local.hour) % len(NUDGE_MESSAGES)].format(name=name)

        try:
            send_message(int(u["chat_id"]), msg, reply_markup=main_menu_keyboard())
            set_user_fields(uid, {"last_nudge_at": datetime.now(tz=ZoneInfo("UTC"))})
        except Exception:
            pass

def start_scheduler() -> None:
    global _scheduler, _started
    if _started or DISABLE_SCHEDULER:
        return
    _scheduler = BackgroundScheduler(timezone=TIMEZONE_NAME)
    _scheduler.add_job(notification_tick, "interval", minutes=5, id="nudges", max_instances=1, coalesce=True)
    _scheduler.start()
    _started = True
