import os
import hmac
import json
import time
import uuid
import logging
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, abort, jsonify
from openai import OpenAI

# -----------------------------
# Env vars (set these on Render)
# -----------------------------
TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()

OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-5-mini").strip()

# Your public base URL, e.g. https://sat-dm-bot.onrender.com
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()

# Protect webhook setup endpoint
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()

# Optional webhook secret token (recommended). If set, Telegram must be configured to send it.
TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

# Debug endpoints (strongly recommended to set)
DEBUG_TOKEN = (os.environ.get("DEBUG_TOKEN") or "").strip()
DEBUG_ENABLED = (os.environ.get("DEBUG_ENABLED") or "1").strip() == "1"

# Teacher style prompt
TEACHER_STYLE_PROMPT = (os.environ.get("TEACHER_STYLE_PROMPT") or """
You are an SAT tutor bot.
Explain like a good teacher:
- Start with a 1-sentence plan.
- Then show step-by-step reasoning in plain language.
- Keep it friendly and efficient.
- End with: Final answer, Quick check, Common trap.
If the question is ambiguous, ask ONE clarifying question.
Do not claim you are a real human teacher; you are a tutor bot.
""").strip()

MAX_OUTPUT_TOKENS = int(os.environ.get("MAX_OUTPUT_TOKENS") or "450")
TEMPERATURE = float(os.environ.get("TEMPERATURE") or "0.3")

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sat-dm-bot")

app = Flask(__name__)
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# In-memory debug state (resets on deploy/restart)
LAST_UPDATE: Optional[Dict[str, Any]] = None
LAST_UPDATE_AT: Optional[float] = None
LAST_WEBHOOK_REQUEST: Optional[Dict[str, Any]] = None
LAST_WEBHOOK_REQUEST_AT: Optional[float] = None


# -----------------------------
# Small helpers
# -----------------------------
def redacted(s: str, keep: int = 4) -> str:
    s = s or ""
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "…" + ("*" * 6)

def require_debug_auth() -> None:
    if not DEBUG_ENABLED:
        abort(404)
    if not DEBUG_TOKEN:
        abort(403)
    token = (request.args.get("token") or request.headers.get("X-Debug-Token") or "").strip()
    if not hmac.compare_digest(token, DEBUG_TOKEN):
        abort(401)

def require_setup_auth() -> None:
    token = (request.args.get("token") or request.headers.get("X-Setup-Token") or "").strip()
    if not SETUP_TOKEN or not hmac.compare_digest(token, SETUP_TOKEN):
        abort(401)

def verify_webhook_secret() -> None:
    """
    If TELEGRAM_WEBHOOK_SECRET is set, we require Telegram to send the same value
    via X-Telegram-Bot-Api-Secret-Token header.
    """
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(got, TELEGRAM_WEBHOOK_SECRET):
        abort(401)

def tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data

def tg_get(method: str) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data

def send_reply(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    tg_api("sendMessage", payload)

def extract_message(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # DM-only: accept only normal messages
    return update.get("message")

def is_private_chat(msg: Dict[str, Any]) -> bool:
    return (msg.get("chat") or {}).get("type") == "private"

def is_bot_message(msg: Dict[str, Any]) -> bool:
    return bool((msg.get("from") or {}).get("is_bot"))

def text_of(msg: Dict[str, Any]) -> str:
    return (msg.get("text") or "").strip()

def normalize_command(text: str) -> str:
    """
    Converts '/sat@BotName blah' -> '/sat blah' for group compatibility,
    harmless in DMs too.
    """
    if not text.startswith("/"):
        return text
    try:
        me = tg_api("getMe", {}).get("result") or {}
        username = (me.get("username") or "").strip()
        if username:
            return text.replace(f"@{username}", "").strip()
    except Exception:
        pass
    return text

def sat_tutor_answer(user_text: str) -> str:
    if not openai_client:
        return "Server is missing OPENAI_API_KEY."

    resp = openai_client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": TEACHER_STYLE_PROMPT},
            {"role": "user", "content": user_text},
        ],
        max_output_tokens=MAX_OUTPUT_TOKENS,
        temperature=TEMPERATURE,
        store=False,
    )
    out = (getattr(resp, "output_text", None) or "").strip()
    return out or "I couldn't generate an answer. Try rephrasing the question."


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return "SAT DM bot is running."

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/webhook")
def webhook():
    """
    Telegram webhook target.
    DM-only: ignores groups/channels.
    """
    req_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())

    # record last webhook request info (for debugging)
    global LAST_WEBHOOK_REQUEST, LAST_WEBHOOK_REQUEST_AT
    LAST_WEBHOOK_REQUEST_AT = time.time()
    LAST_WEBHOOK_REQUEST = {
        "req_id": req_id,
        "remote_addr": request.remote_addr,
        "headers_subset": {
            "X-Telegram-Bot-Api-Secret-Token": request.headers.get("X-Telegram-Bot-Api-Secret-Token", ""),
            "User-Agent": request.headers.get("User-Agent", ""),
            "Content-Type": request.headers.get("Content-Type", ""),
        },
    }

    try:
        verify_webhook_secret()

        update = request.get_json(force=True, silent=False) or {}
        log.info(f"[{req_id}] webhook received keys={list(update.keys())}")

        # store last update (for debugging)
        global LAST_UPDATE, LAST_UPDATE_AT
        LAST_UPDATE = update
        LAST_UPDATE_AT = time.time()

        msg = extract_message(update)
        if not msg:
            log.info(f"[{req_id}] no message in update (ignored)")
            return jsonify({"ok": True})

        if is_bot_message(msg):
            log.info(f"[{req_id}] bot message ignored")
            return jsonify({"ok": True})

        if not is_private_chat(msg):
            log.info(f"[{req_id}] non-private chat ignored")
            return jsonify({"ok": True})

        chat_id = int((msg.get("chat") or {}).get("id"))
        message_id = msg.get("message_id")

        user_text = text_of(msg)
        if not user_text:
            log.info(f"[{req_id}] empty text ignored")
            return jsonify({"ok": True})

        normalized = normalize_command(user_text).strip()
        lower = normalized.lower()

        # Always answer /start and /help
        if lower.startswith("/start") or lower.startswith("/help"):
            log.info(f"[{req_id}] responding to /start or /help")
            send_reply(
                chat_id,
                "Hi! Send me an SAT question.\n\n"
                "You can just type it normally, or use:\n"
                "• /sat <question>\n\n"
                "Example: /sat If 3x + 5 = 20, what is x?",
                reply_to_message_id=message_id,
            )
            return jsonify({"ok": True})

        # If they used /sat with no question
        if lower == "/sat" or lower.startswith("/sat ") is False and lower.startswith("/sat") and len(lower.split()) == 1:
            send_reply(
                chat_id,
                "Send the question after /sat.\nExample: /sat If 3x+5=20, find x.",
                reply_to_message_id=message_id,
            )
            return jsonify({"ok": True})

        # If message starts with /sat, use remainder as question; else treat whole message as question
        question = normalized
        if lower.startswith("/sat"):
            parts = normalized.split(maxsplit=1)
            question = parts[1].strip() if len(parts) > 1 else ""

        if not question:
            send_reply(chat_id, "Please send an SAT question.", reply_to_message_id=message_id)
            return jsonify({"ok": True})

        log.info(f"[{req_id}] calling OpenAI model={OPENAI_MODEL}")
        answer = sat_tutor_answer(question)
        send_reply(chat_id, answer, reply_to_message_id=message_id)
        log.info(f"[{req_id}] replied ok")
        return jsonify({"ok": True})

    except Exception as e:
        # Always ACK to avoid Telegram retry storms; log the actual error.
        log.exception(f"[{req_id}] webhook error: {e}")
        return jsonify({"ok": True})


@app.post("/setup")
def setup_webhook():
    """
    One-time webhook setup:
      POST https://YOUR-SERVICE.onrender.com/setup?token=SETUP_TOKEN
    """
    require_setup_auth()

    if not WEBHOOK_BASE_URL:
        return jsonify({"ok": False, "error": "WEBHOOK_BASE_URL (or RENDER_EXTERNAL_URL) is not set"}), 400

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


# -----------------------------
# Debug endpoints (protected)
# -----------------------------
@app.get("/debug/config")
def debug_config():
    require_debug_auth()
    return jsonify({
        "DEBUG_ENABLED": DEBUG_ENABLED,
        "WEBHOOK_BASE_URL": WEBHOOK_BASE_URL,
        "OPENAI_MODEL": OPENAI_MODEL,
        "has_OPENAI_API_KEY": bool(OPENAI_API_KEY),
        "has_TELEGRAM_BOT_TOKEN": bool(TELEGRAM_BOT_TOKEN),
        "TELEGRAM_BOT_TOKEN_redacted": redacted(TELEGRAM_BOT_TOKEN),
        "OPENAI_API_KEY_redacted": redacted(OPENAI_API_KEY),
        "has_TELEGRAM_WEBHOOK_SECRET": bool(TELEGRAM_WEBHOOK_SECRET),
        "TELEGRAM_WEBHOOK_SECRET_redacted": redacted(TELEGRAM_WEBHOOK_SECRET),
    })

@app.get("/debug/webhookinfo")
def debug_webhookinfo():
    require_debug_auth()
    info = tg_get("getWebhookInfo")
    return jsonify(info)

@app.get("/debug/lastupdate")
def debug_lastupdate():
    require_debug_auth()
    return jsonify({
        "last_update_at": LAST_UPDATE_AT,
        "last_update": LAST_UPDATE,
    })

@app.get("/debug/lastwebhookrequest")
def debug_lastwebhookrequest():
    require_debug_auth()
    return jsonify({
        "last_webhook_request_at": LAST_WEBHOOK_REQUEST_AT,
        "last_webhook_request": LAST_WEBHOOK_REQUEST,
    })

@app.post("/debug/delete-webhook")
def debug_delete_webhook():
    require_debug_auth()
    res = tg_api("deleteWebhook", {"drop_pending_updates": True})
    return jsonify(res)

@app.post("/debug/set-webhook")
def debug_set_webhook():
    require_debug_auth()
    if not WEBHOOK_BASE_URL:
        return jsonify({"ok": False, "error": "WEBHOOK_BASE_URL is not set"}), 400

    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"
    payload: Dict[str, Any] = {
        "url": webhook_url,
        "drop_pending_updates": True,
        "allowed_updates": ["message"],
    }
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET

    res = tg_api("setWebhook", payload)
    return jsonify({"ok": True, "webhook_url": webhook_url, "telegram": res})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
