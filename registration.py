import re
from telegram_client import send_message, remove_keyboard
from db import set_user_fields, approve_user_by_telegram_id, is_admin, get_user_by_tg
from telegram_client import main_menu_keyboard
from admin import notify_admins_new_user  # safe import (only function)

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
    if user.reg_step in (1, 2, 3, 4) and not incoming:
        send_message(chat_id, registration_prompt(user.reg_step), reply_markup=remove_keyboard())
        return

    text = (incoming or "").strip()

    if user.reg_step == 1:
        set_user_fields(user.id, {"first_name": text, "reg_step": 2})
        send_message(chat_id, registration_prompt(2, first_name_hint=text), reply_markup=remove_keyboard())
        return

    if user.reg_step == 2:
        set_user_fields(user.id, {"surname": text, "reg_step": 3})
        send_message(chat_id, registration_prompt(3), reply_markup=remove_keyboard())
        return

    if user.reg_step == 3:
        set_user_fields(user.id, {"nickname": text, "reg_step": 4})
        send_message(chat_id, registration_prompt(4), reply_markup=remove_keyboard())
        return

    if user.reg_step == 4:
        if not EMAIL_RE.match(text):
            send_message(chat_id, "That email doesn’t look valid. Please enter a real email (like name@example.com).")
            return

        # finish registration
        set_user_fields(user.id, {"email": text, "registered_at": None, "reg_step": 0, "state": None})

        # admins: instant approve + menu (removes “waiting…” spam)
        if is_admin(user.telegram_id):
            approve_user_by_telegram_id(user.telegram_id, True)
            send_message(chat_id, "✅ Approved! Here’s your menu:", reply_markup=main_menu_keyboard())
            return

        send_message(chat_id, "✅ Registration submitted.\nWaiting for teacher approval…", reply_markup=remove_keyboard())

        refreshed = get_user_by_tg(user.telegram_id)
        if refreshed:
            notify_admins_new_user(refreshed)

        return
