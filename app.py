import os
import re
import hmac
import uuid
import csv
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask, request, abort, jsonify

# In-process scheduler (no extra worker service)
from apscheduler.schedulers.background import BackgroundScheduler

# -----------------------------
# Config
# -----------------------------
TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()
TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "UTC").strip()

ADMIN_TELEGRAM_IDS_RAW = (os.environ.get("ADMIN_TELEGRAM_IDS") or "").strip()  # "123,456"
DISABLE_SCHEDULER = (os.environ.get("DISABLE_SCHEDULER") or "").strip() == "1"

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

MAX_DAILY_TESTS = 6
COOLDOWN_MINUTES = 30

# streak saver rule: earn 1 when daily tests hits 3
SAVER_EARN_THRESHOLD = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sat-helpdesk-bot")

app = Flask(__name__)

def parse_admin_ids(raw: str) -> set[int]:
    out: set[int] = set()
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

# -----------------------------
# Telegram helpers
# -----------------------------
def verify_webhook_secret() -> None:
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(got, TELEGRAM_WEBHOOK_SECRET):
        abort(401)

def tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data

def send_message(chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> int:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    res = tg_api("sendMessage", payload)
    return int((res.get("result") or {}).get("message_id"))

def send_document(chat_id: int, filename: str, content_bytes: bytes, caption: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    files = {
        "document": (filename, content_bytes),
    }
    data = {
        "chat_id": str(chat_id),
        "caption": caption,
    }
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram sendDocument error: {j}")

def delete_message(chat_id: int, message_id: int) -> None:
    try:
        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception:
        pass

def inline_kb(rows: List[List[Tuple[str, str]]]) -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": text, "callback_data": data} for (text, data) in row]
            for row in rows
        ]
    }

def remove_keyboard() -> Dict[str, Any]:
    return {"remove_keyboard": True}

def main_menu_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "📝 Record Math Score"}, {"text": "📊 My Stats"}],
            [{"text": "🏆 Daily Leaderboard"}, {"text": "🏆 Lifetime Leaderboard"}],
            [{"text": "🎯 Set Goal"}, {"text": "❓ Help"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }

def extract_message(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return update.get("message")

def extract_callback(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return update.get("callback_query")

def is_private_chat(msg: Dict[str, Any]) -> bool:
    return (msg.get("chat") or {}).get("type") == "private"

def text_or_caption(msg: Dict[str, Any]) -> str:
    return (msg.get("text") or msg.get("caption") or "").strip()

# -----------------------------
# DB
# -----------------------------
def db():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL (Render Postgres).")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db() -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
              id SERIAL PRIMARY KEY,
              telegram_id BIGINT UNIQUE NOT NULL,
              chat_id BIGINT NOT NULL,
              first_name TEXT,
              surname TEXT,
              nickname TEXT,
              email TEXT,
              registered_at TIMESTAMPTZ,
              reg_step SMALLINT NOT NULL DEFAULT 1,
              state TEXT,
              approved BOOLEAN NOT NULL DEFAULT FALSE,
              banned BOOLEAN NOT NULL DEFAULT FALSE,
              goal_math SMALLINT,
              total_points BIGINT NOT NULL DEFAULT 0,
              tests_count INTEGER NOT NULL DEFAULT 0,
              last_test_at TIMESTAMPTZ,
              last_nudge_at TIMESTAMPTZ,
              pref_hour SMALLINT,
              pref_minute SMALLINT,
              streak_savers INTEGER NOT NULL DEFAULT 0,
              saver_awarded_date DATE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS tests (
              id SERIAL PRIMARY KEY,
              user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              math_score SMALLINT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              created_by_admin BIGINT
            );
            """)

            # Migrations (safe)
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS approved BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS goal_math SMALLINT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_test_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_nudge_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS pref_hour SMALLINT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS pref_minute SMALLINT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS streak_savers INTEGER NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS saver_awarded_date DATE;")
            cur.execute("ALTER TABLE tests ADD COLUMN IF NOT EXISTS math_score SMALLINT;")
            cur.execute("ALTER TABLE tests ADD COLUMN IF NOT EXISTS created_by_admin BIGINT;")

            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_created_at ON tests(created_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_user_created ON tests(user_id, created_at);")

            # Legacy schema cleanup:
            # If old column "score" exists and is NOT NULL, drop constraint and backfill math_score.
            cur.execute("""
            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tests' AND column_name='score') THEN
                -- backfill math_score from old score if needed
                UPDATE tests SET math_score = score WHERE math_score IS NULL AND score IS NOT NULL;

                -- allow old score to be NULL (so new inserts won’t fail)
                BEGIN
                  ALTER TABLE tests ALTER COLUMN score DROP NOT NULL;
                EXCEPTION WHEN OTHERS THEN
                END;
              END IF;

              -- delete garbage rows with null math_score
              DELETE FROM tests WHERE math_score IS NULL;

              -- enforce math_score not null going forward (safe after delete)
              BEGIN
                ALTER TABLE tests ALTER COLUMN math_score SET NOT NULL;
              EXCEPTION WHEN OTHERS THEN
              END;
            END $$;
            """)

        conn.commit()

_db_inited = False

@app.before_request
def _ensure_db():
    global _db_inited
    if _db_inited:
        return
    init_db()
    _db_inited = True
    log.info("DB initialized")

@dataclass
class User:
    id: int
    telegram_id: int
    chat_id: int
    first_name: Optional[str]
    surname: Optional[str]
    nickname: Optional[str]
    email: Optional[str]
    registered_at: Optional[datetime]
    reg_step: int
    state: Optional[str]
    approved: bool
    banned: bool
    goal_math: Optional[int]
    total_points: int
    tests_count: int
    last_test_at: Optional[datetime]
    last_nudge_at: Optional[datetime]
    pref_hour: Optional[int]
    pref_minute: Optional[int]
    streak_savers: int
    saver_awarded_date: Optional[date]

def row_to_user(row: Dict[str, Any]) -> User:
    return User(
        id=row["id"],
        telegram_id=int(row["telegram_id"]),
        chat_id=int(row["chat_id"]),
        first_name=row.get("first_name"),
        surname=row.get("surname"),
        nickname=row.get("nickname"),
        email=row.get("email"),
        registered_at=row.get("registered_at"),
        reg_step=int(row.get("reg_step") or 1),
        state=row.get("state"),
        approved=bool(row.get("approved")),
        banned=bool(row.get("banned")),
        goal_math=row.get("goal_math"),
        total_points=int(row.get("total_points") or 0),
        tests_count=int(row.get("tests_count") or 0),
        last_test_at=row.get("last_test_at"),
        last_nudge_at=row.get("last_nudge_at"),
        pref_hour=row.get("pref_hour"),
        pref_minute=row.get("pref_minute"),
        streak_savers=int(row.get("streak_savers") or 0),
        saver_awarded_date=row.get("saver_awarded_date"),
    )

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

def get_or_create_user(telegram_id: int, chat_id: int) -> User:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
            row = cur.fetchone()
            if row:
                # keep chat_id updated
                if int(row["chat_id"]) != chat_id:
                    cur.execute("UPDATE users SET chat_id=%s WHERE telegram_id=%s", (chat_id, telegram_id))
                    conn.commit()
                    cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
                    row = cur.fetchone()

                # auto-approve admins (UX)
                if is_admin(telegram_id) and not bool(row.get("approved")):
                    cur.execute("UPDATE users SET approved=TRUE WHERE telegram_id=%s", (telegram_id,))
                    conn.commit()
                    cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
                    row = cur.fetchone()

                return row_to_user(row)

            approved = is_admin(telegram_id)
            cur.execute(
                "INSERT INTO users (telegram_id, chat_id, reg_step, approved) VALUES (%s,%s,1,%s) RETURNING *",
                (telegram_id, chat_id, approved),
            )
            row = cur.fetchone()
            conn.commit()
            return row_to_user(row)

def set_user_field(user_id: int, field: str, value: Any) -> None:
    allowed = {
        "first_name", "surname", "nickname", "email",
        "reg_step", "state", "approved", "banned",
        "goal_math", "last_nudge_at", "pref_hour", "pref_minute",
        "streak_savers", "saver_awarded_date"
    }
    if field not in allowed:
        raise ValueError("Invalid field")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE users SET {field}=%s WHERE id=%s", (value, user_id))
        conn.commit()

def set_user_state(user_id: int, state: Optional[str]) -> None:
    set_user_field(user_id, "state", state)

def finalize_registration(user_id: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET registered_at=now(), reg_step=0, approved=FALSE WHERE id=%s", (user_id,))
        conn.commit()

def find_user_by_tg(tg_id: int) -> Optional[User]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            row = cur.fetchone()
    return row_to_user(row) if row else None

def approve_user_by_telegram_id(tg_id: int, approved: bool) -> Optional[User]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET approved=%s,
                    reg_step=0,
                    state=NULL,
                    registered_at = COALESCE(registered_at, now())
                WHERE telegram_id=%s
                RETURNING *
                """,
                (approved, tg_id),
            )
            row = cur.fetchone()
        conn.commit()
    return row_to_user(row) if row else None

def ban_user_by_telegram_id(tg_id: int, banned: bool) -> Optional[User]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET banned=%s WHERE telegram_id=%s RETURNING *", (banned, tg_id))
            row = cur.fetchone()
        conn.commit()
    return row_to_user(row) if row else None

def delete_user_hard(tg_id: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE telegram_id=%s", (tg_id,))
        conn.commit()

def tz_bounds_for_today() -> Tuple[datetime, datetime]:
    tz = ZoneInfo(TIMEZONE_NAME)
    now_local = datetime.now(tz=tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def tests_today_count(user_id: int) -> int:
    start_utc, end_utc = tz_bounds_for_today()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::int AS c FROM tests WHERE user_id=%s AND created_at >= %s AND created_at < %s",
                (user_id, start_utc, end_utc),
            )
            return int(cur.fetchone()["c"])

def can_add_test(user: User) -> Tuple[bool, str]:
    start_utc, end_utc = tz_bounds_for_today()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::int AS c FROM tests WHERE user_id=%s AND created_at >= %s AND created_at < %s",
                (user.id, start_utc, end_utc),
            )
            c = int(cur.fetchone()["c"])
            if c >= MAX_DAILY_TESTS:
                return False, f"Daily limit reached ({MAX_DAILY_TESTS}/day)."

            cur.execute(
                "SELECT created_at FROM tests WHERE user_id=%s ORDER BY created_at DESC LIMIT 1",
                (user.id,),
            )
            last = cur.fetchone()
            if last and last.get("created_at"):
                last_at = last["created_at"]
                if datetime.now(timezone.utc) - last_at < timedelta(minutes=COOLDOWN_MINUTES):
                    mins_left = int((timedelta(minutes=COOLDOWN_MINUTES) - (datetime.now(timezone.utc) - last_at)).total_seconds() // 60) + 1
                    return False, f"Cooldown active. Try again in ~{mins_left} min."
    return True, "OK"

def update_preferred_time(user_id: int) -> None:
    # Compute typical time from last ~10 tests (local time)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(HOUR FROM (created_at AT TIME ZONE %s))::int AS h,
                       EXTRACT(MINUTE FROM (created_at AT TIME ZONE %s))::int AS m
                FROM tests
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (TIMEZONE_NAME, TIMEZONE_NAME, user_id),
            )
            rows = cur.fetchall()
    if not rows:
        return
    hs = [r["h"] for r in rows if r.get("h") is not None]
    ms = [r["m"] for r in rows if r.get("m") is not None]
    if not hs or not ms:
        return
    # Simple average is fine here
    avg_h = int(round(sum(hs) / len(hs)))
    avg_m = int(round(sum(ms) / len(ms)))
    avg_h = max(0, min(23, avg_h))
    avg_m = max(0, min(59, avg_m))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET pref_hour=%s, pref_minute=%s WHERE id=%s", (avg_h, avg_m, user_id))
        conn.commit()

def maybe_award_streak_saver(user_id: int) -> Optional[str]:
    # If user hits 3 tests today, award 1 saver (max 1/day)
    tz = ZoneInfo(TIMEZONE_NAME)
    today_local = datetime.now(tz=tz).date()
    c = tests_today_count(user_id)
    if c < SAVER_EARN_THRESHOLD:
        return None

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT streak_savers, saver_awarded_date FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            awarded = row.get("saver_awarded_date")
            if awarded == today_local:
                return None

            cur.execute(
                "UPDATE users SET streak_savers = streak_savers + 1, saver_awarded_date=%s WHERE id=%s",
                (today_local, user_id),
            )
        conn.commit()

    return "🛡️ Streak Saver earned! (You logged 3 tests today.)"

def add_math_score(user: User, score: int, created_by_admin: Optional[int] = None) -> int:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tests (user_id, math_score, created_by_admin) VALUES (%s,%s,%s) RETURNING id",
                (user.id, score, created_by_admin),
            )
            test_id = int(cur.fetchone()["id"])
            cur.execute(
                """
                UPDATE users
                SET total_points = total_points + %s,
                    tests_count = tests_count + 1,
                    last_test_at = now()
                WHERE id=%s
                """,
                (score, user.id),
            )
        conn.commit()

    # Update preferred time stats after insert
    update_preferred_time(user.id)
    return test_id

def remove_test_by_id(test_id: int) -> Optional[int]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, math_score FROM tests WHERE id=%s", (test_id,))
            row = cur.fetchone()
            if not row:
                return None
            user_id = int(row["user_id"])
            score = int(row["math_score"])
            cur.execute("DELETE FROM tests WHERE id=%s", (test_id,))
            cur.execute(
                """
                UPDATE users
                SET total_points = GREATEST(0, total_points - %s),
                    tests_count = GREATEST(0, tests_count - 1)
                WHERE id=%s
                """,
                (score, user_id),
            )
        conn.commit()
    return user_id

def fetch_user_stats(user: User) -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT total_points, tests_count, goal_math, streak_savers FROM users WHERE id=%s", (user.id,))
            u = cur.fetchone()

            cur.execute("SELECT MAX(math_score)::int AS best FROM tests WHERE user_id=%s AND math_score IS NOT NULL", (user.id,))
            best = (cur.fetchone() or {}).get("best")

            cur.execute(
                "SELECT math_score::int AS s, created_at FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 1",
                (user.id,),
            )
            last = cur.fetchone()

            cur.execute("SELECT AVG(math_score)::float AS avg FROM tests WHERE user_id=%s AND math_score IS NOT NULL", (user.id,))
            avg = (cur.fetchone() or {}).get("avg")

            cur.execute(
                "SELECT math_score::int AS s, created_at FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 60",
                (user.id,),
            )
            history = cur.fetchall()

            cur.execute(
                "SELECT math_score::int AS s FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 12",
                (user.id,),
            )
            rows = cur.fetchall()
            last12 = [int(r["s"]) for r in rows if r.get("s") is not None]

    return {
        "total_points": int(u["total_points"]),
        "tests_count": int(u["tests_count"]),
        "goal_math": u.get("goal_math"),
        "streak_savers": int(u.get("streak_savers") or 0),
        "best": int(best) if best is not None else None,
        "last": {"score": int(last["s"]), "at": last["created_at"]} if last else None,
        "avg": float(avg) if avg is not None else None,
        "last12": list(reversed(last12)),
        "history": history,  # newest first
    }

def sparkline(scores: List[int], lo: int = 0, hi: int = 44) -> str:
    if not scores:
        return "(no scores yet)"
    blocks = "▁▂▃▄▅▆▇█"
    def to_block(x: int) -> str:
        x = max(lo, min(hi, x))
        t = (x - lo) / (hi - lo) if hi > lo else 0
        idx = int(round(t * (len(blocks) - 1)))
        return blocks[idx]
    return "".join(to_block(s) for s in scores)

def estimate_goal(user: User, history: List[Dict[str, Any]], goal: int) -> str:
    # Very simple trend estimate: score/day using last ~10 points
    if not history or len(history) < 4:
        return "Not enough data for a goal estimate yet. Log a few more tests."

    tz = ZoneInfo(TIMEZONE_NAME)
    points = []
    for r in reversed(history[:30]):  # oldest first
        s = r.get("s")
        at = r.get("created_at")
        if s is None or at is None:
            continue
        at_local = at.astimezone(tz)
        points.append((at_local, int(s)))

    if len(points) < 4:
        return "Not enough data for a goal estimate yet."

    # use last 10 points
    pts = points[-10:]
    t0, s0 = pts[0]
    t1, s1 = pts[-1]
    days = max(1e-6, (t1 - t0).total_seconds() / 86400.0)
    slope = (s1 - s0) / days  # score per day (rough)

    current = s1
    if current >= goal:
        return f"🎯 You’ve already hit your goal ({goal}/44). Time to set a scarier one."

    if slope <= 0.05:
        return (
            f"🎯 Goal: {goal}/44\n"
            "Estimate: trend is flat right now.\n"
            "Suggestion: aim for consistency (e.g., 4–6 logs/week) and we’ll re-estimate."
        )

    days_needed = (goal - current) / slope
    eta = datetime.now(tz=tz) + timedelta(days=days_needed)
    return (
        f"🎯 Goal: {goal}/44\n"
        f"Current: {current}/44\n"
        f"Trend: ~{slope:.2f} points/day\n"
        f"Estimated reach: ~{eta.strftime('%Y-%m-%d')} (± a bunch, because humans)."
    )

def time_of_day_effectiveness(user_id: int) -> str:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(HOUR FROM (created_at AT TIME ZONE %s))::int AS h,
                       AVG(math_score)::float AS a,
                       COUNT(*)::int AS c
                FROM tests
                WHERE user_id=%s
                GROUP BY h
                HAVING COUNT(*) >= 2
                ORDER BY a DESC
                LIMIT 1
                """,
                (TIMEZONE_NAME, user_id),
            )
            row = cur.fetchone()
    if not row:
        return "Not enough data yet for “best time of day”."
    return f"Best hour (avg): ~{row['h']:02d}:00 with {row['a']:.1f}/44 (n={row['c']})."

def streak_days_with_saver(user: User) -> Tuple[int, bool]:
    # Returns (streak_days, saver_used_now)
    tz = ZoneInfo(TIMEZONE_NAME)
    today = datetime.now(tz=tz).date()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT (created_at AT TIME ZONE %s)::date AS d
                FROM tests
                WHERE user_id=%s
                ORDER BY d DESC
                """,
                (TIMEZONE_NAME, user.id),
            )
            days = [r["d"] for r in cur.fetchall()]

    if not days:
        return 0, False

    dayset = set(days)
    saver_used = False

    # If they missed today but had yesterday and have saver, spend 1 to preserve streak
    if today not in dayset:
        yesterday = today - timedelta(days=1)
        if yesterday in dayset and user.streak_savers > 0:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET streak_savers = GREATEST(0, streak_savers - 1) WHERE id=%s", (user.id,))
                conn.commit()
            user.streak_savers -= 1
            dayset.add(today)
            saver_used = True

    streak = 0
    cur_day = today
    while cur_day in dayset:
        streak += 1
        cur_day = cur_day - timedelta(days=1)
    return streak, saver_used

def daily_leaderboard(limit: int = 10) -> List[Dict[str, Any]]:
    start_utc, end_utc = tz_bounds_for_today()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.telegram_id, u.nickname, u.first_name, u.surname,
                       SUM(t.math_score)::bigint AS points,
                       COUNT(*)::int AS tests
                FROM tests t
                JOIN users u ON u.id=t.user_id
                WHERE t.created_at >= %s AND t.created_at < %s
                  AND u.approved=TRUE AND u.banned=FALSE
                GROUP BY u.telegram_id, u.nickname, u.first_name, u.surname
                ORDER BY points DESC, tests DESC
                LIMIT %s
                """,
                (start_utc, end_utc, limit),
            )
            return list(cur.fetchall())

def lifetime_leaderboard(limit: int = 10) -> List[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_id, nickname, first_name, surname,
                       total_points AS points,
                       tests_count AS tests
                FROM users
                WHERE approved=TRUE AND banned=FALSE
                ORDER BY total_points DESC, tests_count DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cur.fetchall())

def format_lb(rows: List[Dict[str, Any]], title: str) -> str:
    if not rows:
        return f"{title}\n\nNo results yet."
    lines = [title, ""]
    for i, r in enumerate(rows, start=1):
        name = (r.get("nickname") or "").strip()
        if not name:
            who = f"{(r.get('first_name') or '').strip()} {(r.get('surname') or '').strip()}".strip()
            name = who or "-"
        pts = int(r.get("points") or 0)
        tests = int(r.get("tests") or 0)
        lines.append(f"{i}. {name} — {pts} pts ({tests} tests)")
    return "\n".join(lines)

# -----------------------------
# Registration / approval
# -----------------------------
def registration_prompt(step: int, first_name_hint: str = "") -> str:
    prefix = f"Hi {first_name_hint}! " if first_name_hint else ""
    return {
        1: prefix + "Let’s register you.\n\n1/4 — What is your *name*?",
        2: "2/4 — What is your *surname*?",
        3: "3/4 — What is your *nickname* (display name)?",
        4: "4/4 — What is your *email address*?",
    }.get(step, "Registration step error.")

def notify_admins_new_user(user: User) -> None:
    if not ADMIN_IDS:
        return
    who = f"{user.first_name or ''} {user.surname or ''}".strip()
    nick = user.nickname or "-"
    email = user.email or "-"
    text = (
        "🆕 New registration pending approval\n\n"
        f"Telegram ID: {user.telegram_id}\n"
        f"Name: {who or '-'}\n"
        f"Nickname: {nick}\n"
        f"Email: {email}\n"
    )
    kb = inline_kb([[("✅ Approve", f"approve:{user.telegram_id}"), ("⛔ Reject", f"reject:{user.telegram_id}")]])
    for admin_id in ADMIN_IDS:
        try:
            send_message(admin_id, text, reply_markup=kb)
        except Exception as e:
            log.warning(f"Failed to notify admin {admin_id}: {e}")

def handle_registration(user: User, chat_id: int, incoming: str) -> None:
    if user.reg_step in (1, 2, 3, 4) and not incoming:
        send_message(chat_id, registration_prompt(user.reg_step), reply_markup=remove_keyboard())
        return

    text = incoming.strip()

    if user.reg_step == 1:
        set_user_field(user.id, "first_name", text)
        set_user_field(user.id, "reg_step", 2)
        send_message(chat_id, registration_prompt(2, first_name_hint=text), reply_markup=remove_keyboard())
        return

    if user.reg_step == 2:
        set_user_field(user.id, "surname", text)
        set_user_field(user.id, "reg_step", 3)
        send_message(chat_id, registration_prompt(3), reply_markup=remove_keyboard())
        return

    if user.reg_step == 3:
        set_user_field(user.id, "nickname", text)
        set_user_field(user.id, "reg_step", 4)
        send_message(chat_id, registration_prompt(4), reply_markup=remove_keyboard())
        return

    if user.reg_step == 4:
        if not EMAIL_RE.match(text):
            send_message(chat_id, "That email doesn’t look valid. Please enter a real email (like name@example.com).")
            return
        set_user_field(user.id, "email", text)
        finalize_registration(user.id)
        send_message(chat_id, "✅ Registration submitted.\nWaiting for teacher approval…", reply_markup=remove_keyboard())
        refreshed = get_or_create_user(user.telegram_id, user.chat_id)
        notify_admins_new_user(refreshed)
        return

# -----------------------------
# Admin section
# -----------------------------
def admin_help() -> str:
    return (
        "Admin commands:\n"
        "/pending — list pending users\n"
        "/approve <telegram_id> | /reject <telegram_id>\n"
        "/users — list recent users\n"
        "/inactive — users inactive 7+ days\n"
        "/improvers — top improvers (rough)\n"
        "/dashboard — quick overview\n"
        "/broadcast <message>  OR  /broadcast then send message\n"
        "/add <telegram_id> <score0-44> — manual add\n"
        "/deltest <test_id> — remove a test\n"
        "/ban <telegram_id> | /unban <telegram_id>\n"
        "/delete <telegram_id> — hard delete user\n"
        "/exportcsv — export users + tests summary\n"
    )

def list_pending_users(limit: int = 30) -> str:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_id, first_name, surname, nickname, email, registered_at
                FROM users
                WHERE reg_step=0 AND approved=FALSE AND banned=FALSE
                ORDER BY registered_at DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    if not rows:
        return "No pending users."
    lines = ["Pending users:", ""]
    for r in rows:
        who = f"{(r.get('first_name') or '').strip()} {(r.get('surname') or '').strip()}".strip()
        nick = (r.get("nickname") or "").strip()
        email = (r.get("email") or "").strip()
        lines.append(f"- {r['telegram_id']} | {nick or who or '-'} | {email or '-'}")
    return "\n".join(lines)

def admin_dashboard() -> str:
    start_utc, end_utc = tz_bounds_for_today()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int AS c FROM users")
            total = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*)::int AS c FROM users WHERE approved=TRUE AND banned=FALSE")
            approved = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*)::int AS c FROM users WHERE approved=FALSE AND reg_step=0 AND banned=FALSE")
            pending = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*)::int AS c FROM users WHERE banned=TRUE")
            banned = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*)::int AS c FROM tests WHERE created_at >= %s AND created_at < %s", (start_utc, end_utc))
            tests_today = cur.fetchone()["c"]
            cur.execute("SELECT AVG(math_score)::float AS a FROM tests WHERE created_at >= %s AND created_at < %s", (start_utc, end_utc))
            avg_today = cur.fetchone()["a"]

    avg_txt = f"{avg_today:.1f}/44" if avg_today is not None else "—"
    return (
        "📋 Admin Dashboard\n\n"
        f"Users: {total}\n"
        f"Approved: {approved}\n"
        f"Pending: {pending}\n"
        f"Banned: {banned}\n\n"
        f"Tests today: {tests_today}\n"
        f"Avg today: {avg_txt}\n"
    )

def list_inactive(days: int = 7, limit: int = 30) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_id, first_name, surname, nickname, last_test_at
                FROM users
                WHERE approved=TRUE AND banned=FALSE AND (last_test_at IS NULL OR last_test_at < %s)
                ORDER BY last_test_at NULLS FIRST
                LIMIT %s
                """,
                (cutoff, limit),
            )
            rows = cur.fetchall()
    if not rows:
        return f"No inactive users (>{days} days) found."
    lines = [f"Inactive users (>{days} days):", ""]
    for r in rows:
        who = f"{(r.get('first_name') or '').strip()} {(r.get('surname') or '').strip()}".strip()
        nick = (r.get("nickname") or "").strip()
        last = r.get("last_test_at")
        last_txt = last.strftime("%Y-%m-%d") if last else "never"
        lines.append(f"- {r['telegram_id']} | {nick or who or '-'} | last: {last_txt}")
    return "\n".join(lines)

def top_improvers(limit: int = 10) -> str:
    # Simple: compare avg of last 3 vs previous 3 within last 12 tests
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, telegram_id, first_name, surname, nickname
                FROM users
                WHERE approved=TRUE AND banned=FALSE
                """
            )
            users = cur.fetchall()

    improvers = []
    with db() as conn:
        with conn.cursor() as cur:
            for u in users:
                cur.execute(
                    """
                    SELECT math_score::int AS s
                    FROM tests
                    WHERE user_id=%s
                    ORDER BY created_at DESC
                    LIMIT 12
                    """,
                    (u["id"],),
                )
                scores = [r["s"] for r in cur.fetchall() if r.get("s") is not None]
                if len(scores) < 6:
                    continue
                last3 = scores[0:3]
                prev3 = scores[3:6]
                d = (sum(last3) / 3.0) - (sum(prev3) / 3.0)
                improvers.append((d, u))

    improvers.sort(key=lambda x: x[0], reverse=True)
    improvers = improvers[:limit]
    if not improvers:
        return "Not enough data to compute improvers yet."

    lines = ["📈 Top improvers (last3 avg − previous3 avg):", ""]
    for d, u in improvers:
        who = f"{(u.get('first_name') or '').strip()} {(u.get('surname') or '').strip()}".strip()
        nick = (u.get("nickname") or "").strip()
        name = nick or who or "-"
        lines.append(f"- {name}: {d:+.2f}")
    return "\n".join(lines)

def export_csv(chat_id: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_id, first_name, surname, nickname, email, approved, banned,
                       tests_count, total_points, goal_math, streak_savers, last_test_at
                FROM users
                ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["telegram_id","first_name","surname","nickname","email","approved","banned","tests_count","total_points","goal_math","streak_savers","last_test_at"])
    for r in rows:
        w.writerow([
            r.get("telegram_id"),
            r.get("first_name"),
            r.get("surname"),
            r.get("nickname"),
            r.get("email"),
            r.get("approved"),
            r.get("banned"),
            r.get("tests_count"),
            r.get("total_points"),
            r.get("goal_math"),
            r.get("streak_savers"),
            r.get("last_test_at").isoformat() if r.get("last_test_at") else "",
        ])
    send_document(chat_id, "users_export.csv", buf.getvalue().encode("utf-8"), caption="Users export")

# -----------------------------
# User texts
# -----------------------------
def help_text_user() -> str:
    return (
        "How this works:\n\n"
        "• Register once (name/surname/nickname/email)\n"
        "• Teacher approves you\n"
        "• Log Math section score (0–44)\n"
        "• Stats + goal estimate + streak savers\n\n"
        f"Limits: max {MAX_DAILY_TESTS} tests/day, {COOLDOWN_MINUTES} min cooldown."
    )

# -----------------------------
# Notifications (Duolingo-ish)
# -----------------------------
NUDGE_MESSAGES = [
    "Hey {name} 😈 time to do SAT Math. Don’t make me beg.",
    "{name}, your future self called. They want you to log a Math score today 📈",
    "Daily quest: log 1 Math score (0–44). Reward: less panic later 🧠",
    "You’ve got this, {name}. 25 minutes of Math. Then log it. 💪",
    "Why aren't you doing SAT 😡 (this is your friendly chaos reminder)",
]

def notification_tick() -> None:
    # Runs every few minutes inside the web process.
    # Works only while instance is awake.
    try:
        init_db()  # safe
    except Exception:
        pass

    tz = ZoneInfo(TIMEZONE_NAME)
    now_local = datetime.now(tz=tz)
    today = now_local.date()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, chat_id, first_name, nickname, approved, banned,
                       pref_hour, pref_minute, last_test_at, last_nudge_at
                FROM users
                WHERE approved=TRUE AND banned=FALSE
                """
            )
            users = cur.fetchall()

    for u in users:
        # already tested today?
        uid = int(u["id"])
        if tests_today_count(uid) > 0:
            continue

        # if nudged recently today, skip
        last_nudge = u.get("last_nudge_at")
        if last_nudge:
            last_nudge_local = last_nudge.astimezone(tz)
            if last_nudge_local.date() == today:
                continue

        h = u.get("pref_hour")
        m = u.get("pref_minute")
        # fallback time if no preference yet
        if h is None or m is None:
            h, m = 19, 0  # default “evening ping”

        target = now_local.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
        # send within a window around target time
        if not (target - timedelta(minutes=10) <= now_local <= target + timedelta(minutes=25)):
            continue

        name = (u.get("nickname") or u.get("first_name") or "champ").strip()
        msg = NUDGE_MESSAGES[(uid + now_local.hour) % len(NUDGE_MESSAGES)].format(name=name)

        try:
            send_message(int(u["chat_id"]), msg, reply_markup=main_menu_keyboard())
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET last_nudge_at=now() WHERE id=%s", (uid,))
                conn.commit()
        except Exception as e:
            log.info(f"nudge failed for {uid}: {e}")

scheduler = None
if not DISABLE_SCHEDULER:
    try:
        scheduler = BackgroundScheduler(timezone=TIMEZONE_NAME)
        scheduler.add_job(notification_tick, "interval", minutes=5, id="nudges", max_instances=1, coalesce=True)
        scheduler.start()
        log.info("Scheduler started (in-process).")
    except Exception as e:
        log.warning(f"Scheduler failed to start: {e}")

# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return "SAT Help Desk bot is running."

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/setup")
def setup_webhook():
    token = (request.args.get("token") or request.headers.get("X-Setup-Token") or "").strip()
    if not SETUP_TOKEN or not hmac.compare_digest(token, SETUP_TOKEN):
        abort(401)
    if not WEBHOOK_BASE_URL:
        return jsonify({"ok": False, "error": "WEBHOOK_BASE_URL is not set"}), 400

    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"
    payload: Dict[str, Any] = {
        "url": webhook_url,
        "drop_pending_updates": True,
        "allowed_updates": ["message", "callback_query"],
    }
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET

    res = tg_api("setWebhook", payload)
    return jsonify({"ok": True, "webhook_url": webhook_url, "telegram": res})

@app.post("/webhook")
def webhook():
    verify_webhook_secret()
    req_id = str(uuid.uuid4())
    update = request.get_json(force=True, silent=False) or {}

    # Callbacks (approve/reject buttons)
    cb = extract_callback(update)
    if cb:
        from_user = cb.get("from") or {}
        admin_id = int(from_user.get("id", 0))
        if not is_admin(admin_id):
            return jsonify({"ok": True})

        data = (cb.get("data") or "").strip()
        msg = cb.get("message") or {}
        chat = msg.get("chat") or {}
        admin_chat_id = int(chat.get("id", admin_id))

        def reply_admin(t: str):
            send_message(admin_chat_id, t)

        try:
            if data.startswith("approve:"):
                tg_id = int(data.split(":", 1)[1])
                u = approve_user_by_telegram_id(tg_id, True)
                if not u:
                    reply_admin("User not found.")
                else:
                    name = (u.nickname or u.first_name or "there").strip()
                    send_message(u.chat_id, f"✅ Approved! Welcome, {name}.\nHere’s your menu:", reply_markup=main_menu_keyboard())
                    reply_admin(f"Approved {tg_id}.")
            elif data.startswith("reject:"):
                tg_id = int(data.split(":", 1)[1])
                u = approve_user_by_telegram_id(tg_id, False)
                if not u:
                    reply_admin("User not found.")
                else:
                    send_message(u.chat_id, "⛔ Registration rejected. Please contact your teacher.")
                    reply_admin(f"Rejected {tg_id}.")
        except Exception as e:
            log.exception(f"[{req_id}] callback error: {e}")
            reply_admin(f"Callback error (req {req_id}).")

        return jsonify({"ok": True})

    # Messages
    msg = extract_message(update)
    if not msg:
        return jsonify({"ok": True})
    if not is_private_chat(msg) or (msg.get("from") or {}).get("is_bot"):
        return jsonify({"ok": True})

    chat_id = int((msg.get("chat") or {}).get("id"))
    from_user = msg.get("from") or {}
    telegram_id = int(from_user.get("id"))
    incoming = text_or_caption(msg)

    user = get_or_create_user(telegram_id, chat_id)

    # banned => silent ignore
    if user.banned:
        return jsonify({"ok": True})

    # Admin commands
    if is_admin(telegram_id) and incoming.startswith("/"):
        parts = incoming.strip().split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        def say(t: str):
            send_message(chat_id, t)

        try:
            if cmd == "/admin":
                say(admin_help()); return jsonify({"ok": True})
            if cmd == "/pending":
                say(list_pending_users()); return jsonify({"ok": True})
            if cmd == "/dashboard":
                say(admin_dashboard()); return jsonify({"ok": True})
            if cmd == "/inactive":
                say(list_inactive()); return jsonify({"ok": True})
            if cmd == "/improvers":
                say(top_improvers()); return jsonify({"ok": True})
            if cmd == "/users":
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT telegram_id, first_name, surname, nickname, approved, banned, tests_count, total_points
                            FROM users
                            ORDER BY created_at DESC
                            LIMIT 25
                            """
                        )
                        rows = cur.fetchall()
                lines = ["Recent users:"]
                for r in rows:
                    who = f"{(r.get('first_name') or '').strip()} {(r.get('surname') or '').strip()}".strip()
                    nick = (r.get("nickname") or "").strip()
                    name = nick or who or "-"
                    lines.append(
                        f"- {r['telegram_id']} | {name} | appr={r['approved']} ban={r['banned']} | tests={r['tests_count']} pts={r['total_points']}"
                    )
                say("\n".join(lines)); return jsonify({"ok": True})

            if cmd == "/broadcast":
                if arg:
                    # immediate broadcast
                    txt = arg
                else:
                    set_user_state(user.id, "admin_broadcast")
                    say("📣 Send the broadcast message now (or /cancel).")
                    return jsonify({"ok": True})

                # send broadcast
                sent = 0
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT chat_id, first_name, nickname FROM users WHERE approved=TRUE AND banned=FALSE")
                        recipients = cur.fetchall()
                for r in recipients:
                    name = (r.get("nickname") or r.get("first_name") or "there").strip()
                    msg_txt = f"📣 {txt}\n\n(Hi {name} 👋)"
                    try:
                        send_message(int(r["chat_id"]), msg_txt, reply_markup=main_menu_keyboard())
                        sent += 1
                    except Exception:
                        pass
                say(f"Broadcast sent to {sent} users.")
                return jsonify({"ok": True})

            if cmd == "/exportcsv":
                export_csv(chat_id)
                return jsonify({"ok": True})

            # commands with numeric args
            bits = incoming.strip().split()
            if cmd in ("/approve", "/reject", "/ban", "/unban", "/delete") and len(bits) >= 2:
                tg = int(bits[1])
                if cmd == "/approve":
                    u2 = approve_user_by_telegram_id(tg, True)
                    if u2:
                        send_message(u2.chat_id, "✅ Approved! Here’s your menu:", reply_markup=main_menu_keyboard())
                        say(f"Approved {tg}.")
                    else:
                        say("User not found.")
                elif cmd == "/reject":
                    u2 = approve_user_by_telegram_id(tg, False)
                    if u2:
                        send_message(u2.chat_id, "⛔ Registration rejected. Please contact your teacher.")
                        say(f"Rejected {tg}.")
                    else:
                        say("User not found.")
                elif cmd == "/ban":
                    u2 = ban_user_by_telegram_id(tg, True)
                    say(f"Banned {tg}." if u2 else "User not found.")
                elif cmd == "/unban":
                    u2 = ban_user_by_telegram_id(tg, False)
                    say(f"Unbanned {tg}." if u2 else "User not found.")
                elif cmd == "/delete":
                    delete_user_hard(tg)
                    say(f"Deleted {tg}.")
                return jsonify({"ok": True})

            if cmd == "/add":
                bits = incoming.strip().split()
                if len(bits) < 3:
                    say("Usage: /add <telegram_id> <0-44>"); return jsonify({"ok": True})
                tg = int(bits[1]); score = int(bits[2])
                if score < 0 or score > 44:
                    say("Score must be 0–44."); return jsonify({"ok": True})
                u2 = find_user_by_tg(tg)
                if not u2:
                    say("User not found."); return jsonify({"ok": True})
                test_id = add_math_score(u2, score, created_by_admin=telegram_id)
                say(f"Added {score}/44 for {tg}. test_id={test_id}")
                try:
                    send_message(u2.chat_id, f"✅ Teacher added a Math score: {score}/44", reply_markup=main_menu_keyboard())
                except Exception:
                    pass
                return jsonify({"ok": True})

            if cmd == "/deltest":
                bits = incoming.strip().split()
                if len(bits) < 2:
                    say("Usage: /deltest <test_id>"); return jsonify({"ok": True})
                tid = int(bits[1])
                uid = remove_test_by_id(tid)
                say("Deleted." if uid else "Test not found.")
                return jsonify({"ok": True})

            say("Unknown admin command. Try /admin.")
            return jsonify({"ok": True})

        except Exception as e:
            log.exception(f"[{req_id}] admin cmd error: {e}")
            send_message(chat_id, f"Admin error (req {req_id}).")
            return jsonify({"ok": True})

    # Admin broadcast state
    if user.state == "admin_broadcast" and is_admin(telegram_id):
        if incoming.strip().lower() in ("/cancel", "cancel"):
            set_user_state(user.id, None)
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})

        txt = incoming.strip()
        set_user_state(user.id, None)

        sent = 0
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT chat_id, first_name, nickname FROM users WHERE approved=TRUE AND banned=FALSE")
                recipients = cur.fetchall()
        for r in recipients:
            name = (r.get("nickname") or r.get("first_name") or "there").strip()
            msg_txt = f"📣 {txt}\n\n(Hi {name} 👋)"
            try:
                send_message(int(r["chat_id"]), msg_txt, reply_markup=main_menu_keyboard())
                sent += 1
            except Exception:
                pass
        send_message(chat_id, f"Broadcast sent to {sent} users.")
        return jsonify({"ok": True})

    # /start UX
    if incoming.lower().startswith("/start"):
        if user.reg_step != 0 or user.registered_at is None:
            handle_registration(user, chat_id, "")
            return jsonify({"ok": True})

        if not user.approved:
            send_message(chat_id, "⏳ You’re registered, waiting for teacher approval.")
            return jsonify({"ok": True})

        # streak + saver logic
        streak, used = streak_days_with_saver(user)
        stats = fetch_user_stats(user)
        saver_note = " (used 1 streak saver 🛡️)" if used else ""
        name = (user.nickname or user.first_name or "there").strip()
        send_message(
            chat_id,
            f"Welcome back, {name}.\n🔥 Daily streak: {streak} day(s){saver_note}\n🛡️ Streak savers: {stats['streak_savers']}\n\nChoose an option:",
            reply_markup=main_menu_keyboard(),
        )
        return jsonify({"ok": True})

    # Registration flow
    if user.reg_step != 0 or user.registered_at is None:
        handle_registration(user, chat_id, incoming)
        return jsonify({"ok": True})

    # Not approved
    if not user.approved:
        send_message(chat_id, "⏳ Waiting for teacher approval.")
        return jsonify({"ok": True})

    # State machine: awaiting score
    if user.state == "awaiting_score":
        txt = incoming.strip()
        if txt.lower() in ("/cancel", "cancel"):
            set_user_state(user.id, None)
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})
        try:
            score = int(txt)
            if score < 0 or score > 44:
                raise ValueError()
        except Exception:
            send_message(chat_id, "Enter a Math score from 0 to 44 (or /cancel).")
            return jsonify({"ok": True})

        ok, why = can_add_test(user)
        if not ok:
            send_message(chat_id, f"⛔ {why}", reply_markup=main_menu_keyboard())
            set_user_state(user.id, None)
            return jsonify({"ok": True})

        add_math_score(user, score)
        set_user_state(user.id, None)

        earned = maybe_award_streak_saver(user.id)
        extra = f"\n\n{earned}" if earned else ""

        name = (user.nickname or user.first_name or "there").strip()
        send_message(chat_id, f"✅ Saved {score}/44, {name}.{extra}", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # State: awaiting goal
    if user.state == "awaiting_goal":
        txt = incoming.strip()
        if txt.lower() in ("/cancel", "cancel"):
            set_user_state(user.id, None)
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})
        try:
            g = int(txt)
            if g < 0 or g > 44:
                raise ValueError()
        except Exception:
            send_message(chat_id, "Enter a goal from 0 to 44 (or /cancel).")
            return jsonify({"ok": True})

        set_user_field(user.id, "goal_math", g)
        set_user_state(user.id, None)
        send_message(chat_id, f"🎯 Goal set: {g}/44", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # Menu actions
    text = incoming.strip()

    if text == "📝 Record Math Score":
        set_user_state(user.id, "awaiting_score")
        send_message(
            chat_id,
            f"Send your Math score (0–44).\nLimits: {MAX_DAILY_TESTS}/day, {COOLDOWN_MINUTES} min cooldown.\nType /cancel to stop.",
        )
        return jsonify({"ok": True})

    if text == "🎯 Set Goal":
        set_user_state(user.id, "awaiting_goal")
        send_message(chat_id, "Send your goal Math score (0–44). Type /cancel to stop.")
        return jsonify({"ok": True})

    if text == "📊 My Stats":
        stats = fetch_user_stats(user)
        # refresh user saver count
        user = get_or_create_user(user.telegram_id, user.chat_id)

        streak, _ = streak_days_with_saver(user)
        graph = sparkline(stats["last12"]) if stats["last12"] else "(no tests yet)"
        avg = stats["avg"]
        avg_txt = f"{avg:.1f}/44" if avg is not None else "—"
        best_txt = f"{stats['best']}/44" if stats["best"] is not None else "—"
        last_txt = f"{stats['last']['score']}/44" if stats["last"] else "—"
        goal = stats["goal_math"]
        goal_txt = f"{goal}/44" if goal is not None else "—"

        best_time = time_of_day_effectiveness(user.id)

        goal_block = ""
        if goal is not None:
            goal_block = "\n\n" + estimate_goal(user, stats["history"], goal)

        name = (user.nickname or user.first_name or "there").strip()
        send_message(
            chat_id,
            "📊 My Stats\n\n"
            f"Name: {name}\n"
            f"Streak: {streak} day(s)\n"
            f"Streak savers: {user.streak_savers}\n"
            f"Tests: {stats['tests_count']}\n"
            f"Total points: {stats['total_points']}\n"
            f"Average: {avg_txt}\n"
            f"Best: {best_txt}\n"
            f"Last: {last_txt}\n"
            f"Goal: {goal_txt}\n\n"
            f"Last 12: {graph}\n\n"
            f"{best_time}"
            f"{goal_block}",
            reply_markup=main_menu_keyboard(),
        )
        return jsonify({"ok": True})

    if text == "🏆 Daily Leaderboard":
        rows = daily_leaderboard(10)
        send_message(chat_id, format_lb(rows, f"🏆 Daily Leaderboard ({TIMEZONE_NAME})"), reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    if text == "🏆 Lifetime Leaderboard":
        rows = lifetime_leaderboard(10)
        send_message(chat_id, format_lb(rows, "🏆 Lifetime Leaderboard"), reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    if text == "❓ Help" or text.lower().startswith("/help"):
        send_message(chat_id, help_text_user(), reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    send_message(chat_id, "Use the menu buttons 🙂", reply_markup=main_menu_keyboard())
    return jsonify({"ok": True})
