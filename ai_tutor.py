import requests
from config import OPENAI_API_KEY, OPENAI_MODEL
from telegram_client import send_message, delete_message

def sat_answer(question: str) -> str:
    if not OPENAI_API_KEY:
        return "⚠️ OPENAI_API_KEY is missing on the server."

    url = "https://api.openai.com/v1/responses"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": "You are an SAT math tutor. Explain clearly step-by-step and give a final answer."},
            {"role": "user", "content": question},
        ],
        "max_output_tokens": 600,
        "store": False,
    }

    r = requests.post(url, headers=headers, json=payload, timeout=45)
    if r.status_code >= 400:
        try:
            j = r.json()
        except Exception:
            j = {"error": {"message": r.text}}
        return f"⚠️ AI error: {j.get('error', {}).get('message', 'unknown')}"

    j = r.json()
    if "output_text" in j and j["output_text"]:
        return j["output_text"].strip()

    out = []
    for item in j.get("output", []):
        for c in item.get("content", []):
            if c.get("type") == "output_text" and c.get("text"):
                out.append(c["text"])
    return ("\n".join(out)).strip() or "⚠️ AI returned empty output."

def handle_sat(chat_id: int, question: str) -> None:
    thinking_id = send_message(chat_id, "Wait a couple of seconds, I am thinking 🤔")
    try:
        ans = sat_answer(question)
        delete_message(chat_id, thinking_id)
        send_message(chat_id, ans)
    except Exception:
        delete_message(chat_id, thinking_id)
        send_message(chat_id, "⚠️ AI failed unexpectedly. Check server logs.")
