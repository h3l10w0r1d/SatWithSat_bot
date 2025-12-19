from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from config import TIMEZONE_NAME, SAVER_EARN_THRESHOLD
from db import db, tests_today_count, tz_bounds_for_today, set_user_fields

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

def fetch_user_stats(user_id: int) -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT total_points, tests_count, goal_math, streak_savers FROM users WHERE id=%s", (user_id,))
            u = cur.fetchone()

            cur.execute("SELECT MAX(math_score)::int AS best FROM tests WHERE user_id=%s AND math_score IS NOT NULL", (user_id,))
            best = (cur.fetchone() or {}).get("best")

            cur.execute(
                "SELECT math_score::int AS s, created_at FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 1",
                (user_id,),
            )
            last = cur.fetchone()

            cur.execute("SELECT AVG(math_score)::float AS avg FROM tests WHERE user_id=%s AND math_score IS NOT NULL", (user_id,))
            avg = (cur.fetchone() or {}).get("avg")

            cur.execute(
                "SELECT math_score::int AS s, created_at FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 60",
                (user_id,),
            )
            history = cur.fetchall()

            cur.execute(
                "SELECT math_score::int AS s FROM tests WHERE user_id=%s AND math_score IS NOT NULL ORDER BY created_at DESC LIMIT 12",
                (user_id,),
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

def estimate_goal(history: List[Dict[str, Any]], goal: int) -> str:
    if not history or len(history) < 4:
        return "Not enough data for a goal estimate yet. Log a few more tests."

    tz = ZoneInfo(TIMEZONE_NAME)
    points = []
    for r in reversed(history[:30]):  # oldest first
        s = r.get("s")
        at = r.get("created_at")
        if s is None or at is None:
            continue
        points.append((at.astimezone(tz), int(s)))

    if len(points) < 4:
        return "Not enough data for a goal estimate yet."

    pts = points[-10:]
    t0, s0 = pts[0]
    t1, s1 = pts[-1]
    days = max(1e-6, (t1 - t0).total_seconds() / 86400.0)
    slope = (s1 - s0) / days

    current = s1
    if current >= goal:
        return f"🎯 You’ve already hit your goal ({goal}/44)."

    if slope <= 0.05:
        return (
            f"🎯 Goal: {goal}/44\n"
            "Estimate: trend is flat right now.\n"
            "Suggestion: log consistently and we’ll re-estimate."
        )

    days_needed = (goal - current) / slope
    eta = datetime.now(tz=tz) + timedelta(days=days_needed)
    return (
        f"🎯 Goal: {goal}/44\n"
        f"Current: {current}/44\n"
        f"Trend: ~{slope:.2f} points/day\n"
        f"Estimated reach: ~{eta.strftime('%Y-%m-%d')} (approx)."
    )

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

def maybe_award_streak_saver(user_id: int) -> Optional[str]:
    tz = ZoneInfo(TIMEZONE_NAME)
    today_local = datetime.now(tz=tz).date()
    c = tests_today_count(user_id)
    if c < SAVER_EARN_THRESHOLD:
        return None

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT saver_awarded_date FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            if row.get("saver_awarded_date") == today_local:
                return None

    set_user_fields(user_id, {"streak_savers": ("__INC__", 1), "saver_awarded_date": today_local})
    return "🛡️ Streak Saver earned! (You logged 3 tests today.)"

def streak_days_with_saver(user_id: int, current_savers: int) -> Tuple[int, bool, int]:
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
                (TIMEZONE_NAME, user_id),
            )
            days = [r["d"] for r in cur.fetchall()]

    if not days:
        return 0, False, current_savers

    dayset = set(days)
    saver_used = False
    savers = current_savers

    if today not in dayset:
        yesterday = today - timedelta(days=1)
        if yesterday in dayset and savers > 0:
            savers -= 1
            saver_used = True
            set_user_fields(user_id, {"streak_savers": savers})
            dayset.add(today)

    streak = 0
    cur_day = today
    while cur_day in dayset:
        streak += 1
        cur_day -= timedelta(days=1)
    return streak, saver_used, savers
