import re
from datetime import datetime, timezone

from telegram_client import send_message, remove_keyboard, main_menu_keyboard
from db import set_user_fields, approve_user_by_telegram_id, is_admin, get_user_by_tg
from admin import notify_admins_new_user

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

def registration_prompt(step: int, first_name_hint: str = "") -> str:
    prefix = f"Hi {first_name_hint}! " if first_name_hint else ""
    return {
        1: prefix + "Let’s register you.\n\n1/4 — What is your *name*?",
        2: "2/4 — What is your *surname*?",
        3: "3/4 — What is your *nickname* (display name)?",
        4: "4/4 — What is your *email address*?",
    }.get(step, "Registration step error.")

def handle_registration(user, chat_id: int, incoming: str) -> None:
    text = (incoming or "").strip()

    if user.reg_step == 1:
        if not text:
            send_message(chat_id, registration_prompt(1), reply_markup=remove_keyboard())
            return
        set_user_fields(user.id, {"first_name": text, "reg_step": 2})
        send_message(chat_id, registration_prompt(2, first_name_hint=text), reply_markup=remove_keyboard())
        return

    if user.reg_step == 2:
        if not text:
            send_message(chat_id, registration_prompt(2), reply_markup=remove_keyboard())
            return
        set_user_fields(user.id, {"surname": text, "reg_step": 3})
        send_message(chat_id, registration_prompt(3), reply_markup=remove_keyboard())
        return

    if user.reg_step == 3:
        if not text:
            send_message(chat_id, registration_prompt(3), reply_markup=remove_keyboard())
            return
        set_user_fields(user.id, {"nickname": text, "reg_step": 4})
        send_message(chat_id, registration_prompt(4), reply_markup=remove_keyboard())
        return

    if user.reg_step == 4:
        if not text:
            send_message(chat_id, registration_prompt(4), reply_markup=remove_keyboard())
            return
        if not EMAIL_RE.match(text):
            send_message(chat_id, "That email doesn’t look valid. Please enter a real email (like name@example.com).")
            return

        # IMPORTANT FIX: set registered_at to NOW (not None)
        set_user_fields(user.id, {
            "email": text,
            "registered_at": datetime.now(timezone.utc),
            "reg_step": 0,
            "state": None
        })

        # Admins: instant menu, no “waiting approval”
        if is_admin(user.telegram_id):
            approve_user_by_telegram_id(user.telegram_id, True)
            send_message(chat_id, "✅ Approved! Here’s your menu:", reply_markup=main_menu_keyboard())
            return

        send_message(chat_id, "✅ Registration submitted.\nWaiting for teacher approval…", reply_markup=remove_keyboard())

        refreshed = get_user_by_tg(user.telegram_id)
        if refreshed:
            notify_admins_new_user(refreshed)
        return

    # fallback
    set_user_fields(user.id, {"reg_step": 1})
    send_message(chat_id, registration_prompt(1), reply_markup=remove_keyboard())
