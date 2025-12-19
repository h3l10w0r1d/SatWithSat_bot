import io, csv
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
from telegram_client import send_message, send_document, inline_kb, main_menu_keyboard
from db import db, approve_user_by_telegram_id, ban_user_by_telegram_id, hard_delete_user, list_users
from config import ADMIN_IDS, TIMEZONE_NAME

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

def notify_admins_new_user(user) -> None:
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
        except Exception:
            pass

def admin_help() -> str:
    return (
        "Admin commands:\n"
        "/pending — list pending users\n"
        "/approve <telegram_id> | /reject <telegram_id>\n"
        "/users — list recent users\n"
        "/inactive — users inactive 7+ days\n"
        "/improvers — top improvers\n"
        "/dashboard — overview\n"
        "/broadcast <message> OR /broadcast then send message\n"
        "/add <telegram_id> <0-44> — manual add\n"
        "/deltest <test_id> — remove a test\n"
        "/ban <telegram_id> | /unban <telegram_id>\n"
        "/delete <telegram_id> — hard delete user\n"
        "/exportcsv — export users summary\n"
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
    from db import tz_bounds_for_today
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
        f"Users: {total}\nApproved: {approved}\nPending: {pending}\nBanned: {banned}\n\n"
        f"Tests today: {tests_today}\nAvg today: {avg_txt}\n"
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
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, telegram_id, first_name, surname, nickname FROM users WHERE approved=TRUE AND banned=FALSE"
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

def broadcast_to_all(text: str) -> int:
    sent = 0
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id, first_name, nickname FROM users WHERE approved=TRUE AND banned=FALSE")
            recipients = cur.fetchall()
    for r in recipients:
        name = (r.get("nickname") or r.get("first_name") or "there").strip()
        msg_txt = f"📣 {text}\n\n(Hi {name} 👋)"
        try:
            send_message(int(r["chat_id"]), msg_txt, reply_markup=main_menu_keyboard())
            sent += 1
        except Exception:
            pass
    return sent
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
from telegram_client import send_message, send_document, inline_kb, main_menu_keyboard
from db import db, approve_user_by_telegram_id, ban_user_by_telegram_id, hard_delete_user, list_users
from config import ADMIN_IDS, TIMEZONE_NAME

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

def notify_admins_new_user(user) -> None:
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
        except Exception:
            pass

def admin_help() -> str:
    return (
        "Admin commands:\n"
        "/pending — list pending users\n"
        "/approve <telegram_id> | /reject <telegram_id>\n"
        "/users — list recent users\n"
        "/inactive — users inactive 7+ days\n"
        "/improvers — top improvers\n"
        "/dashboard — overview\n"
        "/broadcast <message> OR /broadcast then send message\n"
        "/add <telegram_id> <0-44> — manual add\n"
        "/deltest <test_id> — remove a test\n"
        "/ban <telegram_id> | /unban <telegram_id>\n"
        "/delete <telegram_id> — hard delete user\n"
        "/exportcsv — export users summary\n"
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
    from db import tz_bounds_for_today
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
        f"Users: {total}\nApproved: {approved}\nPending: {pending}\nBanned: {banned}\n\n"
        f"Tests today: {tests_today}\nAvg today: {avg_txt}\n"
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
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, telegram_id, first_name, surname, nickname FROM users WHERE approved=TRUE AND banned=FALSE"
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

def broadcast_to_all(text: str) -> int:
    sent = 0
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id, first_name, nickname FROM users WHERE approved=TRUE AND banned=FALSE")
            recipients = cur.fetchall()
    for r in recipients:
        name = (r.get("nickname") or r.get("first_name") or "there").strip()
        msg_txt = f"📣 {text}\n\n(Hi {name} 👋)"
        try:
            send_message(int(r["chat_id"]), msg_txt, reply_markup=main_menu_keyboard())
            sent += 1
        except Exception:
            pass
    return sent
