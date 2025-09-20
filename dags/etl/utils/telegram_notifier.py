import logging
import os
from html import escape

import requests

log = logging.getLogger(__name__)
DETAIL_LIMIT = 3900

def _get_creds():
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    token = os.getenv("TELEGRAM_TOKEN")

    # если Airflow Variables доступны — попробуем ими подстраховаться
    try:
        from airflow.models import Variable
    except Exception:
        Variable = None

    if not chat_id and Variable:
        chat_id = Variable.get("telegram_chat_id", default_var=None)
    if not token and Variable:
        token = Variable.get("telegram_token", default_var=None)

    # ничего не рейзим
    return chat_id, token

def _tail(text: str, max_chars: int = 3900) -> str:
    if text is None:
        return ""
    text = str(text)
    if len(text) <= max_chars:
        return text
    return "…\n" + text[-max_chars:]

def send_telegram_message(message: str) -> None:
    """Отправляет сообщение в Telegram или тихо скипает при отсутствии кредов."""
    chat_id, token = _get_creds()
    if not chat_id or not token:
        log.warning("Telegram creds missing (chat_id/token). Skipping notification.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("✅ Telegram message sent.")
    except requests.RequestException as e:
        log.exception("⚠️ Failed to send Telegram message: %s", e)

def telegram_notifier(context):
    """Уведомление о статусе задачи (успех или ошибка)."""
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    logical_date = context.get("logical_date") or context.get("execution_date")
    exception = context.get("exception")
    state = ti.state or context.get("task_state")
    run_id = (
        context.get("run_id")
        or getattr(context.get("dag_run"), "run_id", None)
        or getattr(ti, "run_id", None)
        or "unknown"
    )

    if state == "success":
        msg = (
            f"✅Задача <b><code>{escape(task_id)}</code></b>\n"
            f"DAG <b><code>{escape(dag_id)}</code></b> успешно завершена.\n"
            f"Дата выполнения: <code>{escape(str(logical_date))}</code>\n"
            f"Run ID: <code>{escape(str(run_id))}</code>"
        )
    else:
        details_raw = str(exception) if exception else "нет данных"
        details_tail = _tail(details_raw, DETAIL_LIMIT)
        msg = (
            f"❌Ошибка в задаче <b><code>{escape(task_id)}</code></b>\n"
            f"DAG <b><code>{escape(dag_id)}</code></b>\n"
            f"Дата выполнения: <code>{escape(str(logical_date))}</code>\n"
            f"Run ID: <code>{escape(str(run_id))}</code>\n"
            f"Детали:\n<pre>{escape(details_tail)}</pre>"
        )

    send_telegram_message(msg)
