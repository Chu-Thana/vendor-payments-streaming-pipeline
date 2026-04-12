import requests

from common.config import (
    ENABLE_TELEGRAM_ALERTS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)
from common.logging_config import get_logger

logger = get_logger(__name__)


def send_telegram_alert(message: str) -> bool:
    if not ENABLE_TELEGRAM_ALERTS:
        logger.info("telegram alerts disabled")
        return False

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("telegram config missing")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("telegram alert sent successfully")
        return True
    except Exception as e:
        logger.exception("failed to send telegram alert: %s", e)
        return False