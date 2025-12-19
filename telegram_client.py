import hmac
import requests
from typing import Any, Dict, List, Optional, Tuple
from config import TELEGRAM_BOT_TOKEN

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

def send_message(chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> int:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    res = tg_api("sendMessage", payload)
    return int((res.get("result") or {}).get("message_id"))

def delete_message(chat_id: int, message_id: int) -> None:
    try:
        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception:
        pass

def send_document(chat_id: int, filename: str, content_bytes: bytes, caption: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    files = {"document": (filename, content_bytes)}
    data = {"chat_id": str(chat_id), "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram sendDocument error: {j}")

def inline_kb(rows: List[List[Tuple[str, str]]]) -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": text, "callback_data": data} for (text, data) in row]
            for row in rows
        ]
    }

def remove_keyboard() -> Dict[str, Any]:
    return {"remove_keyboard": True}

def main_menu_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "📝 Record Math Score"}, {"text": "📊 My Stats"}],
            [{"text": "🏆 Daily Leaderboard"}, {"text": "🏆 Lifetime Leaderboard"}],
            [{"text": "🎯 Set Goal"}, {"text": "❓ Help"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }

def safe_compare(a: str, b: str) -> bool:
    return hmac.compare_digest(a or "", b or "")
