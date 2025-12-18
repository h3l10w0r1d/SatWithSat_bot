import os
import hmac
import logging
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Flask, request, abort, jsonify
from openai import OpenAI

# -----------------------------
# Env vars (set these on Render)
# -----------------------------
TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()

OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-5-mini").strip()

# Your Render public URL, e.g. https://sat-telegram-bot.onrender.com
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()

# Protect /setup endpoint (so strangers can’t reset your webhook)
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()

# Optional Telegram webhook secret token (Telegram will include it in a header on every webhook call)
TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

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
# App setup
# -----------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sat-dm-bot")

app = Flask(__name__)

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
_cached_bot_username: Optional[str] = None


# -----------------------------
# Telegram helpers
# -----------------------------
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


def get_bot_username() -> str:
    global _cached_bot_username
    if _cached_bot_username:
        return _cached_bot_username
    me = tg_api("getMe", {})
    username = ((me.get("result") or {}).get("username") or "").strip()
    _cached_bot_username = username
    return username


def send_reply(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    tg_api("sendMessage", payload)


def verify_webhook_secret() -> None:
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(got, TELEGRAM_WEBHOOK_SECRET):
        abort(401)


# -----------------------------
# Update parsing
# -----------------------------
def extract_message(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # DM-only: we only care about normal messages (not channel_post, etc.)
    return update.get("message")


def is_private_chat(msg: Dict[str, Any]) -> bool:
    chat = msg.get("chat") or {}
    return chat.get("type") == "private"


def is_bot_message(msg: Dict[str, Any]) -> bool:
    frm = msg.get("from") or {}
    return bool(frm.get("is_bot"))


def text_of(msg: Dict[str, Any]) -> str:
    return (msg.get("text") or "").strip()


def normalize_command(text: str) -> str:
    """
    Converts '/sat@BotName blah' -> '/sat blah'
    """
    if not text.startswith("/"):
        return text
    bot_username = get_bot_username()
    if not bot_username:
        return text
    return text.replace(f"@{bot_username}", "").strip()


def parse_sat_prompt(text: str) -> Tuple[str, bool]:
    """
    Returns (prompt, is_command)
    - If /sat is used, prompt is the remainder
    - If plain text, prompt is the whole text
    """
    t = normalize_command(text).strip()
    lower = t.lower()

    if lower.startswith("/sat"):
        # Allow: "/sat" (no args) or "/sat 2+2"
        parts = t.split(maxsplit=1)
        if len(parts) == 1:
            return "", True
        return parts[1].strip(), True

    if lower.startswith("/start") or lower.startswith("/help"):
        return "", True

    return t, False


# -----------------------------
# OpenAI call
# -----------------------------
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
    verify_webhook_secret()

    update = request.get_json(force=True, silent=False) or {}
    msg = extract_message(update)
    if not msg:
        return jsonify({"ok": True})

    if is_bot_message(msg):
        return jsonify({"ok": True})

    if not is_private_chat(msg):
        # DM-only behavior: ignore groups/channels entirely
        return jsonify({"ok": True})

    chat = msg.get("chat") or {}
    chat_id = int(chat.get("id"))
    message_id = msg.get("message_id")

    user_text = text_of(msg)
    if not user_text:
        return jsonify({"ok": True})

    normalized = normalize_command(user_text).strip()
    lower = normalized.lower()

    # Always respond to /start and /help
    if lower.startswith("/start") or lower.startswith("/help"):
        send_reply(
            chat_id,
            "Hi! Send me an SAT question.\n\n"
            "Examples:\n"
            "• Solve: If 3x + 5 = 20, what is x?\n"
            "• /sat A circle has radius 6. Find area.\n",
            reply_to_message_id=message_id,
        )
        return jsonify({"ok": True})

    prompt, is_command = parse_sat_prompt(user_text)

    # If user typed "/sat" with nothing else
    if is_command and (normalized.lower().startswith("/sat")) and not prompt:
        send_reply(
            chat_id,
            "Send the question after /sat.\nExample: /sat If 3x+5=20, find x.",
            reply_to_message_id=message_id,
        )
        return jsonify({"ok": True})

    # DM-only: any non-command text becomes a question
    question = prompt if prompt else user_text

    try:
        answer = sat_tutor_answer(question)
        send_reply(chat_id, answer, reply_to_message_id=message_id)
    except Exception as e:
        log.exception(f"Failed to answer: {e}")
        send_reply(chat_id, "Something went wrong on the server. Try again in a moment.", reply_to_message_id=message_id)

    return jsonify({"ok": True})


@app.post("/setup")
def setup_webhook():
    """
    One-time webhook setup:
      POST https://YOUR-SERVICE.onrender.com/setup?token=SETUP_TOKEN
    """
    token_qs = (request.args.get("token") or "").strip()
    token_hdr = (request.headers.get("X-Setup-Token") or "").strip()
    if not SETUP_TOKEN or not hmac.compare_digest(token_qs or token_hdr, SETUP_TOKEN):
        abort(401)

    if not WEBHOOK_BASE_URL:
        return jsonify({"ok": False, "error": "WEBHOOK_BASE_URL (or RENDER_EXTERNAL_URL) is not set"}), 400

    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/webhook"

    payload: Dict[str, Any] = {
        "url": webhook_url,
        "drop_pending_updates": True,
        # DM-only: only request message updates
        "allowed_updates": ["message"],
    }
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET

    try:
        res = tg_api("setWebhook", payload)
        return jsonify({"ok": True, "webhook_url": webhook_url, "telegram": res})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # Local dev only. On Render, use gunicorn.
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
