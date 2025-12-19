import uuid
import logging
from flask import Flask, request, abort, jsonify
from config import TELEGRAM_WEBHOOK_SECRET, SETUP_TOKEN, WEBHOOK_BASE_URL, ADMIN_IDS
from telegram_client import tg_api, safe_compare, send_message, main_menu_keyboard
from db import init_db, get_or_create_user, get_user_by_tg, is_admin, approve_user_by_telegram_id, ban_user_by_telegram_id, hard_delete_user
from db import can_add_test, add_math_score, remove_test_by_id, list_users
from registration import handle_registration
from stats import fetch_user_stats, sparkline, daily_leaderboard, lifetime_leaderboard, format_lb, estimate_goal, time_of_day_effectiveness, maybe_award_streak_saver, streak_days_with_saver
from admin import admin_help, list_pending_users, admin_dashboard, list_inactive, top_improvers, export_csv, broadcast_to_all
from scheduler import start_scheduler
from ai_tutor import handle_sat

log = logging.getLogger("bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)
_db_inited = False

@app.before_request
def _boot():
    global _db_inited
    if not _db_inited:
        init_db()
        _db_inited = True
    # start scheduler inside the same web process (no worker)
    start_scheduler()

def verify_webhook_secret():
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not safe_compare(got, TELEGRAM_WEBHOOK_SECRET):
        abort(401)

def is_private_chat(msg):
    return (msg.get("chat") or {}).get("type") == "private"

def text_or_caption(msg):
    return (msg.get("text") or msg.get("caption") or "").strip()

@app.get("/")
def root():
    return "Bot is running."

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/setup")
def setup_webhook():
    token = (request.args.get("token") or request.headers.get("X-Setup-Token") or "").strip()
    if not SETUP_TOKEN or not safe_compare(token, SETUP_TOKEN):
        abort(401)
    if not WEBHOOK_BASE_URL:
        return jsonify({"ok": False, "error": "WEBHOOK_BASE_URL not set"}), 400

    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"
    payload = {
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

    # callback buttons for approve/reject
    cb = update.get("callback_query")
    if cb:
        from_user = cb.get("from") or {}
        admin_id = int(from_user.get("id", 0))
        if admin_id not in ADMIN_IDS:
            return jsonify({"ok": True})

        data = (cb.get("data") or "").strip()
        msg = cb.get("message") or {}
        chat_id = int((msg.get("chat") or {}).get("id", admin_id))

        try:
            if data.startswith("approve:"):
                tg_id = int(data.split(":", 1)[1])
                u2 = approve_user_by_telegram_id(tg_id, True)
                if u2:
                    send_message(u2.chat_id, "✅ Approved! Here’s your menu:", reply_markup=main_menu_keyboard())
                    send_message(chat_id, f"Approved {tg_id}.")
                else:
                    send_message(chat_id, "User not found.")
            elif data.startswith("reject:"):
                tg_id = int(data.split(":", 1)[1])
                u2 = approve_user_by_telegram_id(tg_id, False)
                if u2:
                    send_message(u2.chat_id, "⛔ Registration rejected. Please contact your teacher.")
                    send_message(chat_id, f"Rejected {tg_id}.")
                else:
                    send_message(chat_id, "User not found.")
        except Exception as e:
            log.exception(f"[{req_id}] callback error: {e}")
            send_message(chat_id, f"Callback error (req {req_id}).")
        return jsonify({"ok": True})

    msg = update.get("message")
    if not msg or not is_private_chat(msg) or (msg.get("from") or {}).get("is_bot"):
        return jsonify({"ok": True})

    chat_id = int((msg.get("chat") or {}).get("id"))
    from_user = msg.get("from") or {}
    telegram_id = int(from_user.get("id"))
    incoming = text_or_caption(msg)

    user = get_or_create_user(telegram_id, chat_id)
    # always refresh after DB writes
    user = get_user_by_tg(telegram_id) or user

    if user.banned:
        return jsonify({"ok": True})

    # Admin commands
    if is_admin(telegram_id) and incoming.startswith("/"):
        parts = incoming.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        try:
            if cmd == "/admin":
                send_message(chat_id, admin_help()); return jsonify({"ok": True})
            if cmd == "/pending":
                send_message(chat_id, list_pending_users()); return jsonify({"ok": True})
            if cmd == "/dashboard":
                send_message(chat_id, admin_dashboard()); return jsonify({"ok": True})
            if cmd == "/inactive":
                send_message(chat_id, list_inactive()); return jsonify({"ok": True})
            if cmd == "/improvers":
                send_message(chat_id, top_improvers()); return jsonify({"ok": True})
            if cmd == "/users":
                rows = list_users(25)
                lines = ["Recent users:"]
                for r in rows:
                    who = f"{(r.get('first_name') or '').strip()} {(r.get('surname') or '').strip()}".strip()
                    nick = (r.get("nickname") or "").strip()
                    name = nick or who or "-"
                    lines.append(
                        f"- {r['telegram_id']} | {name} | appr={r['approved']} ban={r['banned']} | tests={r['tests_count']} pts={r['total_points']}"
                    )
                send_message(chat_id, "\n".join(lines)); return jsonify({"ok": True})

            if cmd == "/broadcast":
                if not arg:
                    # state based broadcast
                    from db import set_user_fields
                    set_user_fields(user.id, {"state": "admin_broadcast"})
                    send_message(chat_id, "📣 Send the broadcast message now (or /cancel).")
                    return jsonify({"ok": True})
                sent = broadcast_to_all(arg)
                send_message(chat_id, f"Broadcast sent to {sent} users.")
                return jsonify({"ok": True})

            if cmd == "/exportcsv":
                export_csv(chat_id); return jsonify({"ok": True})

            bits = incoming.split()
            if cmd in ("/approve", "/reject", "/ban", "/unban", "/delete") and len(bits) >= 2:
                tg = int(bits[1])
                if cmd == "/approve":
                    u2 = approve_user_by_telegram_id(tg, True)
                    if u2:
                        send_message(u2.chat_id, "✅ Approved! Here’s your menu:", reply_markup=main_menu_keyboard())
                        send_message(chat_id, f"Approved {tg}.")
                    else:
                        send_message(chat_id, "User not found.")
                elif cmd == "/reject":
                    u2 = approve_user_by_telegram_id(tg, False)
                    if u2:
                        send_message(u2.chat_id, "⛔ Registration rejected. Please contact your teacher.")
                        send_message(chat_id, f"Rejected {tg}.")
                    else:
                        send_message(chat_id, "User not found.")
                elif cmd == "/ban":
                    ban_user_by_telegram_id(tg, True); send_message(chat_id, f"Banned {tg}.")
                elif cmd == "/unban":
                    ban_user_by_telegram_id(tg, False); send_message(chat_id, f"Unbanned {tg}.")
                elif cmd == "/delete":
                    hard_delete_user(tg); send_message(chat_id, f"Deleted {tg}.")
                return jsonify({"ok": True})

            if cmd == "/add":
                bits = incoming.split()
                if len(bits) < 3:
                    send_message(chat_id, "Usage: /add <telegram_id> <0-44>"); return jsonify({"ok": True})
                tg = int(bits[1]); score = int(bits[2])
                if score < 0 or score > 44:
                    send_message(chat_id, "Score must be 0–44."); return jsonify({"ok": True})
                u2 = get_user_by_tg(tg)
                if not u2:
                    send_message(chat_id, "User not found."); return jsonify({"ok": True})
                tid = add_math_score(u2.id, score, created_by_admin=telegram_id)
                send_message(chat_id, f"Added {score}/44 for {tg}. test_id={tid}")
                send_message(u2.chat_id, f"✅ Teacher added a Math score: {score}/44", reply_markup=main_menu_keyboard())
                return jsonify({"ok": True})

            if cmd == "/deltest":
                bits = incoming.split()
                if len(bits) < 2:
                    send_message(chat_id, "Usage: /deltest <test_id>"); return jsonify({"ok": True})
                tid = int(bits[1])
                ok = remove_test_by_id(tid)
                send_message(chat_id, "Deleted." if ok else "Test not found.")
                return jsonify({"ok": True})

            # optional SAT AI tutor command
            if cmd == "/sat":
                if not arg:
                    send_message(chat_id, "Usage: /sat <your SAT question>")
                else:
                    handle_sat(chat_id, arg)
                return jsonify({"ok": True})

            send_message(chat_id, "Unknown admin command. Try /admin.")
            return jsonify({"ok": True})

        except Exception as e:
            log.exception(f"[{req_id}] admin cmd error: {e}")
            send_message(chat_id, f"Admin error (req {req_id}).")
            return jsonify({"ok": True})

    # Admin broadcast state
    if user.state == "admin_broadcast" and is_admin(telegram_id):
        if incoming.lower() in ("/cancel", "cancel"):
            from db import set_user_fields
            set_user_fields(user.id, {"state": None})
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})
        from db import set_user_fields
        set_user_fields(user.id, {"state": None})
        sent = broadcast_to_all(incoming)
        send_message(chat_id, f"Broadcast sent to {sent} users.")
        return jsonify({"ok": True})

    # /start
    if incoming.lower().startswith("/start"):
        if user.reg_step != 0 or user.registered_at is None:
            handle_registration(user, chat_id, "")
            return jsonify({"ok": True})

        if not user.approved:
            send_message(chat_id, "⏳ You’re registered, waiting for teacher approval.")
            return jsonify({"ok": True})

        stats = fetch_user_stats(user.id)
        streak, used, savers = streak_days_with_saver(user.id, stats["streak_savers"])
        saver_note = " (used 1 streak saver 🛡️)" if used else ""
        name = (user.nickname or user.first_name or "there").strip()
        send_message(
            chat_id,
            f"Welcome back, {name}.\n🔥 Daily streak: {streak} day(s){saver_note}\n🛡️ Streak savers: {savers}\n\nChoose an option:",
            reply_markup=main_menu_keyboard(),
        )
        return jsonify({"ok": True})

    # Registration flow
    if user.reg_step != 0 or user.registered_at is None:
        handle_registration(user, chat_id, incoming)
        return jsonify({"ok": True})

    if not user.approved:
        send_message(chat_id, "⏳ Waiting for teacher approval.")
        return jsonify({"ok": True})

    # state machine: awaiting score
    if user.state == "awaiting_score":
        txt = incoming.strip()
        if txt.lower() in ("/cancel", "cancel"):
            from db import set_user_fields
            set_user_fields(user.id, {"state": None})
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})
        try:
            score = int(txt)
            if score < 0 or score > 44:
                raise ValueError()
        except Exception:
            send_message(chat_id, "Enter a Math score from 0 to 44 (or /cancel).")
            return jsonify({"ok": True})

        ok, why = can_add_test(user.id)
        from db import set_user_fields
        set_user_fields(user.id, {"state": None})

        if not ok:
            send_message(chat_id, f"⛔ {why}", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})

        add_math_score(user.id, score)
        earned = maybe_award_streak_saver(user.id)
        extra = f"\n\n{earned}" if earned else ""
        name = (user.nickname or user.first_name or "there").strip()
        send_message(chat_id, f"✅ Saved {score}/44, {name}.{extra}", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # awaiting goal
    if user.state == "awaiting_goal":
        txt = incoming.strip()
        if txt.lower() in ("/cancel", "cancel"):
            from db import set_user_fields
            set_user_fields(user.id, {"state": None})
            send_message(chat_id, "Cancelled.", reply_markup=main_menu_keyboard())
            return jsonify({"ok": True})
        try:
            g = int(txt)
            if g < 0 or g > 44:
                raise ValueError()
        except Exception:
            send_message(chat_id, "Enter a goal from 0 to 44 (or /cancel).")
            return jsonify({"ok": True})

        from db import set_user_fields
        set_user_fields(user.id, {"goal_math": g, "state": None})
        send_message(chat_id, f"🎯 Goal set: {g}/44", reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    # menu actions
    if incoming == "📝 Record Math Score":
        from db import set_user_fields
        set_user_fields(user.id, {"state": "awaiting_score"})
        send_message(chat_id, "Send your Math score (0–44). Type /cancel to stop.")
        return jsonify({"ok": True})

    if incoming == "🎯 Set Goal":
        from db import set_user_fields
        set_user_fields(user.id, {"state": "awaiting_goal"})
        send_message(chat_id, "Send your goal Math score (0–44). Type /cancel to stop.")
        return jsonify({"ok": True})

    if incoming == "📊 My Stats":
        stats = fetch_user_stats(user.id)
        graph = sparkline(stats["last12"])
        avg = stats["avg"]
        avg_txt = f"{avg:.1f}/44" if avg is not None else "—"
        best_txt = f"{stats['best']}/44" if stats["best"] is not None else "—"
        last_txt = f"{stats['last']['score']}/44" if stats["last"] else "—"
        goal = stats["goal_math"]
        goal_txt = f"{goal}/44" if goal is not None else "—"
        best_time = time_of_day_effectiveness(user.id)
        goal_block = "\n\n" + estimate_goal(stats["history"], goal) if goal is not None else ""
        name = (user.nickname or user.first_name or "there").strip()

        streak, used, savers = streak_days_with_saver(user.id, stats["streak_savers"])
        saver_note = " (used 1 saver)" if used else ""

        send_message(
            chat_id,
            "📊 My Stats\n\n"
            f"Name: {name}\n"
            f"Streak: {streak} day(s){saver_note}\n"
            f"Streak savers: {savers}\n"
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

    if incoming == "🏆 Daily Leaderboard":
        rows = daily_leaderboard(10)
        send_message(chat_id, format_lb(rows, "🏆 Daily Leaderboard"), reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    if incoming == "🏆 Lifetime Leaderboard":
        rows = lifetime_leaderboard(10)
        send_message(chat_id, format_lb(rows, "🏆 Lifetime Leaderboard"), reply_markup=main_menu_keyboard())
        return jsonify({"ok": True})

    if incoming == "❓ Help" or incoming.lower().startswith("/help"):
        send_message(
            chat_id,
            "How it works:\n• Register once\n• Teacher approves\n• Log Math score (0–44)\n• Stats + goals + streak savers\n\nLimits: 6/day, 30min cooldown.",
            reply_markup=main_menu_keyboard(),
        )
        return jsonify({"ok": True})

    # optional /sat for normal users too
    if incoming.lower().startswith("/sat "):
        handle_sat(chat_id, incoming[5:].strip())
        return jsonify({"ok": True})

    send_message(chat_id, "Use the menu buttons 🙂", reply_markup=main_menu_keyboard())
    return jsonify({"ok": True})
