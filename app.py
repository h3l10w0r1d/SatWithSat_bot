import os
import time
import hmac
import json
import logging
from typing import Any, Dict, Optional, Tuple, List

import requests
from flask import Flask, request, abort, jsonify

from openai import OpenAI

# -----------------------------
# Config
# -----------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

# Model: pick what you want (example: gpt-5-mini). Keep it configurable.
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini").strip()

# Telegram webhook protection:
# If you set TELEGRAM_WEBHOOK_SECRET, we will:
# 1) setWebhook(secret_token=TELEGRAM_WEBHOOK_SECRET) during /setup
# 2) verify header X-Telegram-Bot-Api-Secret-Token on incoming webhook calls
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "").strip()

# /setup endpoint protection:
SETUP_TOKEN = os.environ.get("SETUP_TOKEN", "").strip()

# How bot decides to respond:
# - "mention": reply only when bot is @mentioned or message starts with /sat
# - "all": reply to any text message in allowed chats
RESPOND_MODE = os.environ.get("RESPOND_MODE", "mention").strip().lower()

# Optional: restrict to specific chats/channels (comma-separated ids: -100..., etc)
ALLOWED_CHAT_IDS = os.environ.get("ALLOWED_CHAT_IDS", "").strip()

# Teacher style prompt (tune this!)
TEACHER_STYLE_PROMPT = os.environ.get(
    "TEACHER_STYLE_PROMPT",
    """You are an SAT tutor bot.
Explain like a good teacher:
- Start with a 1-sentence plan.
- Then show step-by-step reasoning in plain language.
- Use minimal fluff; be friendly.
- End with: (1) final answer, (2) a quick check, (3) one common trap.
If the question is ambiguous, ask ONE clarifying question.
Do NOT claim you are a real person; you are a tutor bot.
""",
).strip()

# Limits / knobs
MAX_OUTPUT_TOKENS = int(os.environ.get("MAX_OUTPUT_TOKENS", "450"))
TEMPERATURE = float(os.environ.get("TEMPERATURE", "0.3"))

# Webhook base URL:
# On Render you'll usually set WEBHOOK_BASE_URL to your service URL (e.g. https://yourservice.onrender.com)
# We also try RENDER_EXTERNAL_URL if you use it, but don't depend on it.
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sat-telegram-bot")

app = Flask(__name__)

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

_cached_bot_username: Optional[str] = None
_cached_allowed_chat_ids: Optional[set] = None


# -----------------------------
# Helpers
# -----------------------------
def parse_allowed_chat_ids(raw: str) -> Optional[set]:
    raw = (raw or "").strip()
    if not raw:
        return None
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            pass
    return ids or None


def allowed_chat(chat_id: int) -> bool:
    global _cached_allowed_chat_ids
    if _cached_allowed_chat_ids is None:
        _cached_allowed_chat_ids = parse_allowed_chat_ids(ALLOWED_CHAT_IDS)
    if _cached_allowed_chat_ids is None:
        return True
    return chat_id in _cached_allowed_chat_ids


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
    username = (me.get("result", {}) or {}).get("username") or ""
    _cached_bot_username = username
    return username


def extract_message(update: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Returns (message_obj, update_type)
    Handles:
      - message
      - channel_post
      - edited_channel_post
    """
    for key in ("message", "channel_post", "edited_channel_post"):
        if key in update:
            return update[key], key
    return None, ""


def is_bot_message(msg: Dict[str, Any]) -> bool:
    frm = msg.get("from") or {}
    return bool(frm.get("is_bot"))


def text_of(msg: Dict[str, Any]) -> str:
    # Telegram can deliver text in "text" or captions for media
    t = msg.get("text") or msg.get("caption") or ""
    return t.strip()


def mentioned_bot(msg: Dict[str, Any], bot_username: str) -> bool:
    entities = msg.get("entities") or []
    txt = msg.get("text") or ""
    bot_username = bot_username.lower()

    # Check entities for mention
    for e in entities:
        if e.get("type") == "mention":
            offset = e.get("offset", 0)
            length = e.get("length", 0)
            mention_text = txt[offset : offset + length].lower()
            if mention_text == f"@{bot_username}":
                return True

    # Also allow direct "@botname" at start even if entities missing
    return txt.lower().startswith(f"@{bot_username}")


def should_respond(msg: Dict[str, Any], update_type: str) -> bool:
    """
    Policy:
      - Always ignore bot messages
      - If RESPOND_MODE=all: respond to any text in allowed chats
      - If RESPOND_MODE=mention: respond only if:
          - message starts with /sat OR
          - bot is @mentioned OR
          - reply-to-bot message
    """
    if is_bot_message(msg):
        return False

    txt = text_of(msg)
    if not txt:
        return False

    chat = msg.get("chat") or {}
    chat_id = int(chat.get("id"))
    if not allowed_chat(chat_id):
        return False

    if RESPOND_MODE == "all":
        return True

    # mention mode
    bot_username = get_bot_username()

    if txt.lower().startswith("/sat"):
        return True
    if mentioned_bot(msg, bot_username):
        return True

    reply_to = msg.get("reply_to_message")
    if reply_to and (reply_to.get("from") or {}).get("is_bot"):
        return True

    return False


def moderation_flagged(user_text: str) -> bool:
    """
    Uses omni-moderation-latest.  [oai_citation:3‡OpenAI Platform](https://platform.openai.com/docs/api-reference/moderations/create?lang=python&utm_source=chatgpt.com)
    """
    if not openai_client:
        return False
    try:
        mod = openai_client.moderations.create(
            model="omni-moderation-latest",
            input=user_text,
        )
        results = getattr(mod, "results", None) or []
        if results and getattr(results[0], "flagged", False):
            return True
    except Exception as e:
        # If moderation fails, fail open (you can choose fail-closed if you prefer)
        log.warning(f"Moderation call failed: {e}")
    return False


def sat_tutor_answer(user_text: str) -> str:
    if not openai_client:
        return "OPENAI_API_KEY is not set on the server."

    if moderation_flagged(user_text):
        return "I can’t help with that request. Please ask an SAT-related question."

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
    # Python SDK convenience aggregator for text output  [oai_citation:4‡OpenAI Platform](https://platform.openai.com/docs/api-reference/responses/create?lang=python&utm_source=chatgpt.com)
    out = (getattr(resp, "output_text", None) or "").strip()
    return out or "I couldn't generate an answer—try rephrasing the question."


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
    """
    If TELEGRAM_WEBHOOK_SECRET is set, Telegram will send it in
    the X-Telegram-Bot-Api-Secret-Token header (when you setWebhook with secret_token).  [oai_citation:5‡core.telegram.org](https://core.telegram.org/bots/api)
    """
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(got, TELEGRAM_WEBHOOK_SECRET):
        abort(401)


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return "SAT Telegram bot is running."


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.post("/webhook")
def webhook():
    verify_webhook_secret()

    update = request.get_json(force=True, silent=False)
    msg, update_type = extract_message(update or {})

    if not msg:
        return jsonify({"ok": True})

    if not should_respond(msg, update_type):
        return jsonify({"ok": True})

    chat = msg.get("chat") or {}
    chat_id = int(chat.get("id"))
    message_id = msg.get("message_id")

    user_text = text_of(msg)

    try:
        answer = sat_tutor_answer(user_text)
        send_reply(chat_id, answer, reply_to_message_id=message_id)
    except Exception as e:
        log.exception(f"Failed to handle update: {e}")
        # Don't break Telegram retries forever; acknowledge receipt.
    return jsonify({"ok": True})


@app.post("/setup")
def setup_webhook():
    """
    Sets Telegram webhook to: {WEBHOOK_BASE_URL}/webhook
    Protected by SETUP_TOKEN in query string or header:
      - ?token=...
      - X-Setup-Token: ...
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
        "allowed_updates": ["message", "channel_post", "edited_channel_post"],
    }
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET  #  [oai_citation:6‡core.telegram.org](https://core.telegram.org/bots/api)

    try:
        res = tg_api("setWebhook", payload)
        return jsonify({"ok": True, "webhook_url": webhook_url, "telegram": res})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # Local dev only. On Render use gunicorn.
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
