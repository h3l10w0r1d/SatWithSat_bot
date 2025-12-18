import os
import re
import hmac
import uuid
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask, request, abort, jsonify

# -----------------------------
# Config (Render env vars)
# -----------------------------
TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()
TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "UTC").strip()  # e.g. "Asia/Yerevan"

# Optional AI tutor (leave OPENAI_API_KEY empty to disable)
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip()

THINKING_TEXT = "Wait a couple of seconds, I am thinking 🤔"
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sat-helpdesk-bot")

app = Flask(__name__)

# Optional OpenAI client
openai_client = None
_openai_rate_limit_error = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        import openai as openai_pkg
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        _openai_rate_limit_error = getattr(openai_pkg, "RateLimitError", None)
    except Exception as e:
        log.warning(f"OpenAI init failed (AI disabled): {e}")
        openai_client = None


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
    except Exception as e:
        log.info(f"deleteMessage ignored: {e}")

def remove_keyboard() -> Dict[str, Any]:
    return {"remove_keyboard": True}

def main_menu_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "📝 Record Test Score"}, {"text": "📊 My Stats"}],
            [{"text": "🏆 Daily Leaderboard"}, {"text": "🏆 Lifetime Leaderboard"}],
            [{"text": "❓ Help"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }

def extract_message(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return update.get("message")

def is_private_chat(msg: Dict[str, Any]) -> bool:
    return (msg.get("chat") or {}).get("type") == "private"

def text_or_caption(msg: Dict[str, Any]) -> str:
    return (msg.get("text") or msg.get("caption") or "").strip()


# -----------------------------
# DB helpers
# -----------------------------
def db():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL (create Render Postgres and set DATABASE_URL).")
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
              reg_step SMALLINT NOT NULL DEFAULT 1,  -- 1..4 during registration, 0 = registered
              state TEXT,                            -- e.g. 'awaiting_score'
              total_points BIGINT NOT NULL DEFAULT 0,
              tests_count INTEGER NOT NULL DEFAULT 0,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS tests (
              id SERIAL PRIMARY KEY,
              user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              score INTEGER NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_created_at ON tests(created_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_user_created ON tests(user_id, created_at);")
        conn.commit()

# Flask 3 compatible: init once when first request arrives
_db_inited = False

@app.before_request
def _ensure_db_initialized():
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
    total_points: int
    tests_count: int
    created_at: datetime

def get_or_create_user(telegram_id: int, chat_id: int) -> User:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
            row = cur.fetchone()
            if row:
                if row["chat_id"] != chat_id:
                    cur.execute("UPDATE users SET chat_id=%s WHERE telegram_id=%s", (chat_id, telegram_id))
                    conn.commit()
                return User(**row)

            cur.execute(
                "INSERT INTO users (telegram_id, chat_id, reg_step) VALUES (%s, %s, 1) RETURNING *",
                (telegram_id, chat_id),
            )
            row = cur.fetchone()
            conn.commit()
            return User(**row)

def update_user_step(user_id: int, reg_step: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET reg_step=%s WHERE id=%s", (reg_step, user_id))
        conn.commit()

def set_user_state(user_id: int, state: Optional[str]) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET state=%s WHERE id=%s", (state, user_id))
        conn.commit()

def set_user_field(user_id: int, field: str, value: str) -> None:
    if field not in ("first_name", "surname", "nickname", "email"):
        raise ValueError("Invalid field")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE users SET {field}=%s WHERE id=%s", (value, user_id))
        conn.commit()

def finalize_registration(user_id: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET registered_at=now(), reg_step=0 WHERE id=%s", (user_id,))
        conn.commit()

def add_test_score(user_id: int, score: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO tests (user_id, score) VALUES (%s, %s)", (user_id, score))
            cur.execute(
                "UPDATE users SET total_points = total_points + %s, tests_count = tests_count + 1 WHERE id=%s",
                (score, user_id),
            )
        conn.commit()

def fetch_user_stats(user_id: int) -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT total_points, tests_count FROM users WHERE id=%s", (user_id,))
            u = cur.fetchone()
    return {"total_points": int(u["total_points"]), "tests_count": int(u["tests_count"])}

def tz_bounds_for_today() -> Tuple[datetime, datetime]:
    tz = ZoneInfo(TIMEZONE_NAME)
    now_local = datetime.now(tz=tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def daily_leaderboard(limit: int = 10) -> List[Dict[str, Any]]:
    start_utc, end_utc = tz_bounds_for_today()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.telegram_id, u.first_name, u.surname, u.nickname,
                       SUM(t.score)::bigint AS points,
                       COUNT(*)::int AS tests
                FROM tests t
                JOIN users u ON u.id = t.user_id
                WHERE t.created_at >= %s AND t.created_at < %s
                GROUP BY u.telegram_id, u.first_name, u.surname, u.nickname
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
                SELECT telegram_id, first_name, surname, nickname,
                       total_points AS points,
                       tests_count AS tests
                FROM users
                WHERE registered_at IS NOT NULL
                ORDER BY total_points DESC, tests_count DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cur.fetchall())


# -----------------------------
# Registration + Menu logic
# -----------------------------
def registration_prompt(step: int) -> str:
    return {
        1: "Welcome! Let’s register you.\n\n1/4 — What is your *name*?",
        2: "2/4 — What is your *surname*?",
        3: "3/4 — What is your *nickname* (display name)?",
        4: "4/4 — What is your *email address*?",
    }.get(step, "Registration step error.")

def handle_registration(user: User, chat_id: int, incoming: str) -> None:
    if user.reg_step in (1, 2, 3, 4) and not incoming:
        send_message(chat_id, registration_prompt(user.reg_step), reply_markup=remove_keyboard())
        return

    text = incoming.strip()

    if user.reg_step == 1:
        set_user_field(user.id, "first_name", text)
        update_user_step(user.id, 2)
        send_message(chat_id, registration_prompt(2), reply_markup=remove_keyboard())
        return

    if user.reg_step == 2:
        set_user_field(user.id, "surname", text)
        update_user_step(user.id, 3)
        send_message(chat_id, registration_prompt(3), reply_markup=remove_keyboard())
        return

    if user.reg_step == 3:
        set_user_field(user.id, "nickname", text)
        update_user_step(user.id, 4)
        send_message(chat_id, registration_prompt(4), reply_markup=remove_keyboard())
        return

    if user.reg_step == 4:
        if not EMAIL_RE.match(text):
            send_message(chat_id, "That email doesn’t look valid. Please enter a real email (like name@example.com).")
            return
        set_user_field(user.id, "email", text)
        finalize_registration(user.id)
        send_message(
            chat_id,
            "✅ Registration complete!\n\nUse the menu buttons below.",
            reply_markup=main_menu_keyboard(),
        )
        return

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

def help_text() -> str:
    return (
        "Help Desk:\n\n"
        "• 📝 Record Test Score — add your SAT score\n"
        "• 📊 My Stats — total points + tests written\n"
        "• 🏆 Daily Leaderboard — Leaderboard of all points earned today\n"
        "• 🏆 Lifetime Leaderboard — Leaderboard of all-time points\n\n"
        "Tip: Type /start anytime to show the menu.\n\n"
        "For AI Sat mode,type command /sat and ask your question near it."
    )


# -----------------------------
# Optional AI tutor (if enabled)
# -----------------------------
def ai_answer(question: str) -> str:
    if not openai_client:
        return "AI tutor is disabled."
    resp = openai_client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": "You are an SAT tutor. Be clear and step-by-step."},
            {"role": "user", "content": question},
        ],
        max_output_tokens=500,
        temperature=0.3,
        store=False,
    )
    return (getattr(resp, "output_text", None) or "").strip() or "No output."


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
        "allowed_updates": ["message"],  # DM-only
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
    msg = extract_message(update)
    if not msg:
        return jsonify({"ok": True})

    if not is_private_chat(msg) or (msg.get("from") or {}).get("is_bot"):
        return jsonify({"ok": True})

    chat_id = int((msg.get("chat") or {}).get("id"))
    telegram_id = int((msg.get("from") or {}).get("id"))
    incoming = text_or_caption(msg)

    user = get_or_create_user(telegram_id, chat_id)

    # /start shows registration or menu
    if incoming.lower().startswith("/start"):
        if user.reg_step != 0 or user.registered_at is None:
            handle_registration(user, chat_id, "")
        else:
            send_message(chat_id, "Welcome back. Choose an option:", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # Registration flow consumes messages
    if user.reg_step != 0 or user.registered_at is None:
        handle_registration(user, chat_id, incoming)
        return jsonify({"ok": True})

    # State: awaiting score
    if user.state == "awaiting_score":
        text = incoming.strip()
        if text.lower() in ("/cancel", "cancel"):
            set_user_state(user.id, None)
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})
        try:
            score = int(text)
            if score < 0 or score > 44:
                raise ValueError("out of range")
        except Exception:
            send_message(chat_id, "Please enter a number between 0 and 1600 (or /cancel).")
            return jsonify({"ok": True})

        add_test_score(user.id, score)
        set_user_state(user.id, None)
        stats = fetch_user_stats(user.id)
        send_message(
            chat_id,
            f"✅ Saved: {score} points.\n"
            f"Total points: {stats['total_points']}\n"
            f"Tests written: {stats['tests_count']}",
            reply_markup=main_menu_keyboard(),
        )
        return jsonify({"ok": True})

    # Menu actions
    text = incoming.strip()

    if text == "📝 Record Test Score":
        set_user_state(user.id, "awaiting_score")
        send_message(chat_id, "Send your SAT score (0–44). Type /cancel to stop.")
        return jsonify({"ok": True})

    if text == "📊 My Stats":
        stats = fetch_user_stats(user.id)
        send_message(
            chat_id,
            f"📊 My Stats\n\nTotal points: {stats['total_points']}\nTests written: {stats['tests_count']}",
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
        send_message(chat_id, help_text(), reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # Optional: AI tutor for any other message
    if openai_client:
        thinking_id = send_message(chat_id, THINKING_TEXT)
        try:
            answer = ai_answer(text)
            send_message(chat_id, answer, reply_markup=main_menu_keyboard())
        except Exception as e:
            log.exception(f"[{req_id}] AI error: {e}")
            send_message(chat_id, f"⚠️ AI error (req {req_id}). Check OpenAI quota/billing.", reply_markup=main_menu_keyboard())
        finally:
            delete_message(chat_id, thinking_id)
    else:
        send_message(chat_id, "Use the menu buttons to record scores and view leaderboards.", reply_markup=main_menu_keyboard())

    return jsonify({"ok": True})
