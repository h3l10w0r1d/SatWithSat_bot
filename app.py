import os
import hmac
import time
import uuid
import logging
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, abort, jsonify
from openai import OpenAI

# -----------------------------
# Env vars
# -----------------------------
TELEGRAM_BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()

# Set this to a model you actually have access to.
# Good default for many accounts: gpt-4o-mini
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip()

WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
SETUP_TOKEN = (os.environ.get("SETUP_TOKEN") or "").strip()

TELEGRAM_WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()

DEBUG_ENABLED = (os.environ.get("DEBUG_ENABLED") or "1").strip() == "1"
DEBUG_TOKEN = (os.environ.get("DEBUG_TOKEN") or "").strip()

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
# Logging / app
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sat-dm-bot")

app = Flask(__name__)
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# In-memory debug state (resets on restart)
LAST_UPDATE: Optional[Dict[str, Any]] = None
LAST_UPDATE_AT: Optional[float] = None
LAST_ERROR: Optional[Dict[str, Any]] = None
LAST_ERROR_AT: Optional[float] = None


# -----------------------------
# Helpers
# -----------------------------
def require_setup_auth() -> None:
    token = (request.args.get("token") or request.headers.get("X-Setup-Token") or "").strip()
    if not SETUP_TOKEN or not hmac.compare_digest(token, SETUP_TOKEN):
        abort(401)

def require_debug_auth() -> None:
    if not DEBUG_ENABLED:
        abort(404)
    token = (request.args.get("token") or request.headers.get("X-Debug-Token") or "").strip()
    if not DEBUG_TOKEN or not hmac.compare_digest(token, DEBUG_TOKEN):
        abort(401)

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

def tg_get(method: str) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    r = requests.get(url, timeout=25)
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
    return update.get("message")

def is_private_chat(msg: Dict[str, Any]) -> bool:
    return (msg.get("chat") or {}).get("type") == "private"

def is_bot_message(msg: Dict[str, Any]) -> bool:
    return bool((msg.get("from") or {}).get("is_bot"))

def text_or_caption(msg: Dict[str, Any]) -> str:
    # IMPORTANT: photos come as caption, not text
    return (msg.get("text") or msg.get("caption") or "").strip()

def sat_tutor_answer(user_text: str) -> str:
    if not openai_client:
        return "Server is missing OPENAI_API_KEY."

    # Newer SDKs: Responses API
    if hasattr(openai_client, "responses"):
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
        return out or "No output from model."

    # Older SDKs: Chat Completions API (supported indefinitely)
    resp = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": TEACHER_STYLE_PROMPT},
            {"role": "user", "content": user_text},
        ],
        max_tokens=MAX_OUTPUT_TOKENS,
        temperature=TEMPERATURE,
    )
    return (resp.choices[0].message.content or "").strip() or "No output from model."


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return "SAT DM bot is running."

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/setup")
def setup_webhook():
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

@app.post("/webhook")
def webhook():
    verify_webhook_secret()
    req_id = str(uuid.uuid4())

    global LAST_UPDATE, LAST_UPDATE_AT
    update = request.get_json(force=True, silent=False) or {}
    LAST_UPDATE = update
    LAST_UPDATE_AT = time.time()

    msg = extract_message(update)
    if not msg:
        return jsonify({"ok": True})

    if is_bot_message(msg) or not is_private_chat(msg):
        return jsonify({"ok": True})

    chat_id = int((msg.get("chat") or {}).get("id"))
    message_id = msg.get("message_id")

    user_text = text_or_caption(msg)
    if not user_text:
        return jsonify({"ok": True})

    lower = user_text.lower().strip()

    # /start + /help always respond
    if lower.startswith("/start") or lower.startswith("/help"):
        send_reply(
            chat_id,
            "Hi! Send me an SAT question.\n\n"
            "You can type it normally, or use:\n"
            "• /sat <question>\n\n"
            "Example: /sat If 3x + 5 = 20, what is x?",
            reply_to_message_id=message_id,
        )
        return jsonify({"ok": True})

    # If they used /sat, strip it
    question = user_text
    if lower.startswith("/sat"):
        parts = user_text.split(maxsplit=1)
        question = parts[1].strip() if len(parts) > 1 else ""
        if not question:
            send_reply(chat_id, "Send the question after /sat. Example: /sat If 3x+5=20, find x.", reply_to_message_id=message_id)
            return jsonify({"ok": True})

    # Call OpenAI (and NEVER be silent if it fails)
    try:
        answer = sat_tutor_answer(question)
        send_reply(chat_id, answer, reply_to_message_id=message_id)
    except Exception as e:
        global LAST_ERROR, LAST_ERROR_AT
        LAST_ERROR_AT = time.time()
        LAST_ERROR = {"req_id": req_id, "error": repr(e)}

        log.exception(f"[{req_id}] OpenAI/handler error: {e}")
        send_reply(
            chat_id,
            f"⚠️ AI error (req {req_id}).\n"
            f"Most common causes: missing OPENAI_API_KEY, wrong OPENAI_MODEL, or OpenAI request failing.\n"
            f"Check Render logs + /debug/last_error.",
            reply_to_message_id=message_id
        )

    return jsonify({"ok": True})


# -----------------------------
# Debug endpoints (protected)
# -----------------------------
@app.get("/debug/webhookinfo")
def debug_webhookinfo():
    require_debug_auth()
    return jsonify(tg_get("getWebhookInfo"))

@app.get("/debug/last_update")
def debug_last_update():
    require_debug_auth()
    return jsonify({"last_update_at": LAST_UPDATE_AT, "last_update": LAST_UPDATE})

@app.get("/debug/last_error")
def debug_last_error():
    require_debug_auth()
    return jsonify({"last_error_at": LAST_ERROR_AT, "last_error": LAST_ERROR})

@app.post("/debug/test_openai")
def debug_test_openai():
    require_debug_auth()
    try:
        out = sat_tutor_answer("Solve: If 3x + 5 = 20, what is x?")
        return jsonify({"ok": True, "model": OPENAI_MODEL, "sample": out})
    except Exception as e:
        return jsonify({"ok": False, "error": repr(e), "model": OPENAI_MODEL}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
