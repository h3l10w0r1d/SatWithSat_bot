import psycopg
from psycopg.rows import dict_row
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo

from config import (
    DATABASE_URL,
    ADMIN_IDS,
    TIMEZONE_NAME,
    MAX_DAILY_TESTS,
    COOLDOWN_MINUTES,
)

def db():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

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

            # dedup telegram updates
            cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_updates (
              update_id BIGINT PRIMARY KEY,
              processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)

            # safe migrations
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

            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_user_created ON tests(user_id, created_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tests_created_at ON tests(created_at);")

            # legacy schema cleanup: if tests.score existed
            cur.execute("""
            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tests' AND column_name='score') THEN
                UPDATE tests SET math_score = score WHERE math_score IS NULL AND score IS NOT NULL;
                BEGIN
                  ALTER TABLE tests ALTER COLUMN score DROP NOT NULL;
                EXCEPTION WHEN OTHERS THEN
                END;
              END IF;

              DELETE FROM tests WHERE math_score IS NULL;

              BEGIN
                ALTER TABLE tests ALTER COLUMN math_score SET NOT NULL;
              EXCEPTION WHEN OTHERS THEN
              END;
            END $$;
            """)

        conn.commit()

def mark_update_processed(update_id: int) -> bool:
    """
    Returns True if newly inserted (process it),
    False if already processed (skip).
    """
    with db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("INSERT INTO processed_updates(update_id) VALUES (%s)", (update_id,))
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                return False

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

def get_user_by_tg(tg_id: int) -> Optional[User]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            row = cur.fetchone()
    return row_to_user(row) if row else None

def get_or_create_user(telegram_id: int, chat_id: int) -> User:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
            row = cur.fetchone()

            if row:
                if int(row["chat_id"]) != chat_id:
                    cur.execute("UPDATE users SET chat_id=%s WHERE telegram_id=%s", (chat_id, telegram_id))
                    conn.commit()
                    cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
                    row = cur.fetchone()

                # auto-set approved for admin IDs, but DO NOT change reg_step mid-registration
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

def set_user_fields(user_id: int, updates: Dict[str, Any]) -> None:
    allowed = {
        "first_name", "surname", "nickname", "email",
        "reg_step", "state", "approved", "banned",
        "goal_math", "last_nudge_at", "pref_hour", "pref_minute",
        "streak_savers", "saver_awarded_date", "registered_at",
        "chat_id"
    }

    # handle increments: {"streak_savers": ("__INC__", 1)}
    inc_updates: Dict[str, int] = {}
    normal_updates: Dict[str, Any] = {}

    for k, v in updates.items():
        if k not in allowed:
            continue
        if isinstance(v, tuple) and len(v) == 2 and v[0] == "__INC__":
            inc_updates[k] = int(v[1])
        else:
            normal_updates[k] = v

    with db() as conn:
        with conn.cursor() as cur:
            for k, inc in inc_updates.items():
                cur.execute(f"UPDATE users SET {k} = COALESCE({k},0) + %s WHERE id=%s", (inc, user_id))

            if normal_updates:
                keys = list(normal_updates.keys())
                sets = ", ".join([f"{k}=%s" for k in keys])
                vals = [normal_updates[k] for k in keys] + [user_id]
                cur.execute(f"UPDATE users SET {sets} WHERE id=%s", vals)

        conn.commit()

def approve_user_by_telegram_id(tg_id: int, approved: bool) -> Optional[User]:
    # hard-finish registration to avoid state desync
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

def hard_delete_user(tg_id: int) -> None:
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

def can_add_test(user_id: int) -> Tuple[bool, str]:
    start_utc, end_utc = tz_bounds_for_today()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::int AS c FROM tests WHERE user_id=%s AND created_at >= %s AND created_at < %s",
                (user_id, start_utc, end_utc),
            )
            c = int(cur.fetchone()["c"])
            if c >= MAX_DAILY_TESTS:
                return False, f"Daily limit reached ({MAX_DAILY_TESTS}/day)."

            cur.execute("SELECT created_at FROM tests WHERE user_id=%s ORDER BY created_at DESC LIMIT 1", (user_id,))
            last = cur.fetchone()
            if last and last.get("created_at"):
                last_at = last["created_at"]
                if datetime.now(timezone.utc) - last_at < timedelta(minutes=COOLDOWN_MINUTES):
                    mins_left = int(
                        (timedelta(minutes=COOLDOWN_MINUTES) - (datetime.now(timezone.utc) - last_at)).total_seconds() // 60
                    ) + 1
                    return False, f"Cooldown active. Try again in ~{mins_left} min."
    return True, "OK"

def update_preferred_time(user_id: int) -> None:
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
    avg_h = max(0, min(23, int(round(sum(hs) / len(hs)))))
    avg_m = max(0, min(59, int(round(sum(ms) / len(ms)))))
    set_user_fields(user_id, {"pref_hour": avg_h, "pref_minute": avg_m})

def add_math_score(user_id: int, score: int, created_by_admin: Optional[int] = None) -> int:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tests (user_id, math_score, created_by_admin) VALUES (%s,%s,%s) RETURNING id",
                (user_id, score, created_by_admin),
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
                (score, user_id),
            )
        conn.commit()
    update_preferred_time(user_id)
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

def list_users(limit: int = 25) -> List[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_id, first_name, surname, nickname, approved, banned, tests_count, total_points
                FROM users
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cur.fetchall())
