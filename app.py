import os
import re
import hmac
import uuid
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask, request, abort, jsonify

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
CRON_TOKEN = (os.environ.get("CRON_TOKEN") or "").strip()

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

MAX_DAILY_TESTS = 6
COOLDOWN_MINUTES = 30

NUDGE_1_AFTER_HOURS = 24
NUDGE_2_AFTER_HOURS = 72

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

def delete_message(chat_id: int, message_id: int) -> None:
    try:
        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception:
        pass

def inline_kb(rows: List[List[Tuple[str, str]]]) -> Dict[str, Any]:
    # rows: [[("Approve","approve:123"),("Reject","reject:123")], ...]
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
            # Base tables
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
              reg_step SMALLINT NOT NULL DEFAULT 1, -- 1..4, 0=done
              state TEXT,
              approved BOOLEAN NOT NULL DEFAULT FALSE,
              banned BOOLEAN NOT NULL DEFAULT FALSE,
              goal_math SMALLINT,
              total_points BIGINT NOT NULL DEFAULT 0,  -- we’ll use math score as “points”
              tests_count INTEGER NOT NULL DEFAULT 0,
              last_test_at TIMESTAMPTZ,
              last_nudge_at TIMESTAMPTZ,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS tests (
              id SERIAL PRIMARY KEY,
              user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              math_score SMALLINT NOT NULL, -- 0..44
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              created_by_admin BIGINT
            );
            """)

            # Helpful indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_created_at ON tests(created_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_user_created ON tests(user_id, created_at);")

            # Migrations for older deployments (safe to run)
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS approved BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS goal_math SMALLINT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_test_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_nudge_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE tests ADD COLUMN IF NOT EXISTS math_score SMALLINT;")
            cur.execute("ALTER TABLE tests ADD COLUMN IF NOT EXISTS created_by_admin BIGINT;")

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
        reg_step=int(row["reg_step"]),
        state=row.get("state"),
        approved=bool(row.get("approved")),
        banned=bool(row.get("banned")),
        goal_math=row.get("goal_math"),
        total_points=int(row.get("total_points") or 0),
        tests_count=int(row.get("tests_count") or 0),
        last_test_at=row.get("last_test_at"),
        last_nudge_at=row.get("last_nudge_at"),
    )

def get_or_create_user(telegram_id: int, chat_id: int) -> User:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
            row = cur.fetchone()
            if row:
                if int(row["chat_id"]) != chat_id:
                    cur.execute("UPDATE users SET chat_id=%s WHERE telegram_id=%s", (chat_id, telegram_id))
                    conn.commit()
                return row_to_user(row)

            cur.execute(
                "INSERT INTO users (telegram_id, chat_id, reg_step) VALUES (%s,%s,1) RETURNING *",
                (telegram_id, chat_id),
            )
            row = cur.fetchone()
            conn.commit()
            return row_to_user(row)

def set_user_field(user_id: int, field: str, value: Any) -> None:
    allowed = {"first_name", "surname", "nickname", "email", "reg_step", "state", "approved", "banned", "goal_math", "last_nudge_at"}
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

def approve_user_by_telegram_id(tg_id: int, approved: bool) -> Optional[User]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET approved=%s WHERE telegram_id=%s RETURNING *", (approved, tg_id))
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

def can_add_test(user: User) -> Tuple[bool, str]:
    # daily limit
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
    return test_id

def remove_test_by_id(test_id: int) -> Optional[int]:
    # returns affected user_id
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
            cur.execute("SELECT total_points, tests_count, goal_math FROM users WHERE id=%s", (user.id,))
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
                "SELECT math_score::int AS s FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 12",
                (user.id,),
            )
            last12_rows = cur.fetchall()
            last12 = [int(r["s"]) for r in last12_rows if r.get("s") is not None]

    return {
        "total_points": int(u["total_points"]),
        "tests_count": int(u["tests_count"]),
        "goal_math": u.get("goal_math"),
        "best": int(best) if best is not None else None,
        "last": {"score": int(last["s"]), "at": last["created_at"]} if last else None,
        "avg": float(avg) if avg is not None else None,
        "last12": last12,
    }

def streak_days(user: User) -> int:
    tz = ZoneInfo(TIMEZONE_NAME)
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
        return 0

    dayset = set(days)
    today = datetime.now(tz=tz).date()
    streak = 0
    cur_day = today
    while cur_day in dayset:
        streak += 1
        cur_day = cur_day - timedelta(days=1)
    return streak

def time_of_day_effectiveness(user: User) -> str:
    # crude: best average by hour bucket
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
                (TIMEZONE_NAME, user.id),
            )
            row = cur.fetchone()
    if not row:
        return "Not enough data yet for “best time of day”."
    return f"Best hour (avg): ~{row['h']:02d}:00 with {row['a']:.1f} avg (n={row['c']})."

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
            name = f"{(r.get('first_name') or '').strip()} {(r.get('surname') or '').strip()}".strip()
        pts = int(r.get("points") or 0)
        tests = int(r.get("tests") or 0)
        lines.append(f"{i}. {name} — {pts} pts ({tests} tests)")
    return "\n".join(lines)

# -----------------------------
# Registration / approval workflow
# -----------------------------
def registration_prompt(step: int) -> str:
    return {
        1: "Welcome! Let’s register you.\n\n1/4 — What is your *name*?",
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
        f"Name: {who}\n"
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
        send_message(chat_id, registration_prompt(2), reply_markup=remove_keyboard())
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
        # refresh user from DB then notify admins
        refreshed = get_or_create_user(user.telegram_id, user.chat_id)
        notify_admins_new_user(refreshed)
        return


# -----------------------------
# Admin commands
# -----------------------------
def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

def admin_help() -> str:
    return (
        "Admin commands:\n"
        "/pending — list pending users\n"
        "/approve <telegram_id>\n"
        "/reject <telegram_id>\n"
        "/add <telegram_id> <score0-44> — manual add\n"
        "/deltest <test_id> — remove a test entry\n"
        "/ban <telegram_id> | /unban <telegram_id>\n"
        "/delete <telegram_id> — hard delete user\n"
        "/users — list recent users\n"
    )

def list_pending_users(limit: int = 20) -> str:
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

def find_user_by_tg(tg_id: int) -> Optional[User]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            row = cur.fetchone()
    return row_to_user(row) if row else None


# -----------------------------
# User messages
# -----------------------------
def help_text_user() -> str:
    return (
        "How this works:\n\n"
        "• Register once (name/surname/nickname/email)\n"
        "• Teacher approves you\n"
        "• Log Math section score (0–44)\n"
        "• See your stats + leaderboards\n\n"
        "Limits: max 6 tests/day, 30 min cooldown."
    )


# -----------------------------
# Cron / nudges
# -----------------------------
def should_send_nudge(u: Dict[str, Any], now_utc: datetime) -> Optional[str]:
    # Return message or None
    last_test_at = u.get("last_test_at")
    last_nudge_at = u.get("last_nudge_at")

    # avoid spamming: at most 1 nudge per 24h
    if last_nudge_at and now_utc - last_nudge_at < timedelta(hours=24):
        return None

    if not last_test_at:
        # never tested; nudge after 24h post-approval
        approved_at = u.get("registered_at") or u.get("created_at")
        if approved_at and now_utc - approved_at >= timedelta(hours=NUDGE_1_AFTER_HOURS):
            return "Why aren't you doing SAT 😡\nLog a Math score today. I’m watching. 👀"
        return None

    delta_h = (now_utc - last_test_at).total_seconds() / 3600.0
    if delta_h >= NUDGE_2_AFTER_HOURS:
        return "72h no SAT? That’s illegal in this household 😡\nGo do a Math section and log it."
    if delta_h >= NUDGE_1_AFTER_HOURS:
        return "Why aren't you doing SAT 😡\n30 minutes of Math. Now."
    return None


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
        # IMPORTANT: include callback_query for admin approve buttons
        "allowed_updates": ["message", "callback_query"],
    }
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET

    res = tg_api("setWebhook", payload)
    return jsonify({"ok": True, "webhook_url": webhook_url, "telegram": res})

@app.post("/cron/nudges")
def cron_nudges():
    # Render Cron Job should call this endpoint
    token = (request.args.get("token") or request.headers.get("X-Cron-Token") or "").strip()
    if not CRON_TOKEN or not hmac.compare_digest(token, CRON_TOKEN):
        abort(401)

    now_utc = datetime.now(timezone.utc)
    sent = 0

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, telegram_id, chat_id, approved, banned, registered_at, created_at, last_test_at, last_nudge_at
                FROM users
                WHERE approved=TRUE AND banned=FALSE
                """
            )
            users = cur.fetchall()

    for u in users:
        msg = should_send_nudge(u, now_utc)
        if not msg:
            continue
        try:
            send_message(int(u["chat_id"]), msg, reply_markup=main_menu_keyboard())
            # update last_nudge_at
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET last_nudge_at=now() WHERE id=%s", (u["id"],))
                conn.commit()
            sent += 1
        except Exception as e:
            log.warning(f"Failed to nudge {u['telegram_id']}: {e}")

    return jsonify({"ok": True, "sent": sent})

@app.post("/webhook")
def webhook():
    verify_webhook_secret()
    req_id = str(uuid.uuid4())

    update = request.get_json(force=True, silent=False) or {}

    # --- Handle admin inline buttons (callback_query) ---
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

        def reply_admin(text: str):
            send_message(admin_chat_id, text)

        try:
            if data.startswith("approve:"):
                tg_id = int(data.split(":", 1)[1])
                u = approve_user_by_telegram_id(tg_id, True)
                if not u:
                    reply_admin("User not found.")
                else:
                    send_message(u.chat_id, "✅ Approved! Welcome. Here’s your menu:", reply_markup=main_menu_keyboard())
                    reply_admin(f"Approved {tg_id}.")
            elif data.startswith("reject:"):
                tg_id = int(data.split(":", 1)[1])
                u = approve_user_by_telegram_id(tg_id, False)
                if not u:
                    reply_admin("User not found.")
                else:
                    # keep unapproved; optionally ban:
                    # ban_user_by_telegram_id(tg_id, True)
                    send_message(u.chat_id, "⛔ Registration rejected. Please contact your teacher.")
                    reply_admin(f"Rejected {tg_id}.")
        except Exception as e:
            log.exception(f"[{req_id}] callback error: {e}")
            reply_admin(f"Callback error (req {req_id}).")

        return jsonify({"ok": True})

    # --- Handle normal messages ---
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

    # Hard block banned users
    if user.banned:
        return jsonify({"ok": True})

    # Admin commands (in DM to bot)
    if is_admin(telegram_id) and incoming.startswith("/"):
        parts = incoming.strip().split()
        cmd = parts[0].lower()

        def say(t: str): send_message(chat_id, t)

        try:
            if cmd == "/admin":
                say(admin_help())
                return jsonify({"ok": True})

            if cmd == "/pending":
                say(list_pending_users())
                return jsonify({"ok": True})

            if cmd in ("/approve", "/reject", "/ban", "/unban", "/delete") and len(parts) >= 2:
                tg_id = int(parts[1])
                if cmd == "/approve":
                    u = approve_user_by_telegram_id(tg_id, True)
                    if u:
                        send_message(u.chat_id, "✅ Approved! Welcome. Here’s your menu:", reply_markup=main_menu_keyboard())
                        say(f"Approved {tg_id}.")
                    else:
                        say("User not found.")
                elif cmd == "/reject":
                    u = approve_user_by_telegram_id(tg_id, False)
                    if u:
                        send_message(u.chat_id, "⛔ Registration rejected. Please contact your teacher.")
                        say(f"Rejected {tg_id}.")
                    else:
                        say("User not found.")
                elif cmd == "/ban":
                    u = ban_user_by_telegram_id(tg_id, True)
                    say(f"Banned {tg_id}." if u else "User not found.")
                elif cmd == "/unban":
                    u = ban_user_by_telegram_id(tg_id, False)
                    say(f"Unbanned {tg_id}." if u else "User not found.")
                elif cmd == "/delete":
                    delete_user_hard(tg_id)
                    say(f"Deleted {tg_id}.")
                return jsonify({"ok": True})

            if cmd == "/add" and len(parts) >= 3:
                tg_id = int(parts[1])
                score = int(parts[2])
                if score < 0 or score > 44:
                    say("Score must be 0–44.")
                    return jsonify({"ok": True})
                u = find_user_by_tg(tg_id)
                if not u:
                    say("User not found.")
                    return jsonify({"ok": True})
                test_id = add_math_score(u, score, created_by_admin=telegram_id)
                say(f"Added score {score} for {tg_id}. test_id={test_id}")
                try:
                    send_message(u.chat_id, f"✅ Teacher added a Math score: {score}/44", reply_markup=main_menu_keyboard())
                except Exception:
                    pass
                return jsonify({"ok": True})

            if cmd == "/deltest" and len(parts) >= 2:
                test_id = int(parts[1])
                uid = remove_test_by_id(test_id)
                say("Deleted." if uid else "Test not found.")
                return jsonify({"ok": True})

            if cmd == "/users":
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT telegram_id, nickname, approved, banned, tests_count, total_points FROM users ORDER BY created_at DESC LIMIT 20"
                        )
                        rows = cur.fetchall()
                lines = ["Recent users:"]
                for r in rows:
                    lines.append(f"- {r['telegram_id']} | {r.get('nickname') or '-'} | appr={r['approved']} ban={r['banned']} | tests={r['tests_count']} pts={r['total_points']}")
                say("\n".join(lines))
                return jsonify({"ok": True})

            # fallthrough
            say("Unknown admin command. Try /admin.")
            return jsonify({"ok": True})

        except Exception as e:
            log.exception(f"[{req_id}] admin cmd error: {e}")
            send_message(chat_id, f"Admin error (req {req_id}).")
            return jsonify({"ok": True})

    # /start UX: show streak + registration/menu
    if incoming.lower().startswith("/start"):
        if user.reg_step != 0 or user.registered_at is None:
            handle_registration(user, chat_id, "")
            return jsonify({"ok": True})

        if not user.approved:
            send_message(chat_id, "⏳ You’re registered, waiting for teacher approval.")
            return jsonify({"ok": True})

        s = streak_days(user)
        send_message(chat_id, f"🔥 Daily streak: {s} day(s)\n\nChoose an option:", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # Registration flow
    if user.reg_step != 0 or user.registered_at is None:
        handle_registration(user, chat_id, incoming)
        return jsonify({"ok": True})

    # Not approved yet
    if not user.approved:
        send_message(chat_id, "⏳ Waiting for teacher approval.")
        return jsonify({"ok": True})

    # State machine
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
        send_message(chat_id, f"✅ Saved: {score}/44", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

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

    # Menu buttons
    text = incoming.strip()

    if text == "📝 Record Math Score":
        set_user_state(user.id, "awaiting_score")
        send_message(chat_id, f"Send your Math score (0–44).\nLimits: {MAX_DAILY_TESTS}/day, {COOLDOWN_MINUTES} min cooldown.\nType /cancel to stop.")
        return jsonify({"ok": True})

    if text == "🎯 Set Goal":
        set_user_state(user.id, "awaiting_goal")
        send_message(chat_id, "Send your goal Math score (0–44). Type /cancel to stop.")
        return jsonify({"ok": True})

    if text == "📊 My Stats":
        stats = fetch_user_stats(user)
        s = streak_days(user)
        best_time = time_of_day_effectiveness(user)
        last12 = stats["last12"]
        graph = sparkline(last12) if last12 else "(no tests yet)"

        avg = stats["avg"]
        avg_txt = f"{avg:.1f}/44" if avg is not None else "—"
        best_txt = f"{stats['best']}/44" if stats["best"] is not None else "—"
        last_txt = f"{stats['last']['score']}/44" if stats["last"] else "—"

        goal = stats["goal_math"]
        goal_txt = f"{goal}/44" if goal is not None else "—"
        goal_note = ""
        if goal is not None and stats["best"] is not None:
            remaining = max(0, goal - stats["best"])
            goal_note = f"\nGoal gap (best→goal): {remaining}"

        send_message(
            chat_id,
            "📊 My Stats\n\n"
            f"Streak: {s} day(s)\n"
            f"Tests: {stats['tests_count']}\n"
            f"Total points: {stats['total_points']}\n"
            f"Average: {avg_txt}\n"
            f"Best: {best_txt}\n"
            f"Last: {last_txt}\n"
            f"Goal: {goal_txt}{goal_note}\n\n"
            f"Last 12: {graph}\n\n"
            f"{best_time}",
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

    # Default: gentle nudge
    send_message(chat_id, "Use the menu buttons 🙂", reply_markup=main_menu_keyboard())
    return jsonify({"ok": True})
