"""Скрапер UNICEF Ukraine — програми допомоги та розвитку дітей.

Примітка: unicef.org використовує Cloudflare Bot Management, який блокує
запити без браузерного TLS-fingerprint і JS-виконання. Повертає порожньо.
"""
import logging

logger = logging.getLogger(__name__)

SOURCE_NAME = "UNICEF Ukraine"


async def fetch_all() -> list[dict]:
    logger.info(
        "UNICEF Ukraine scraper skipped — site blocks non-browser requests "
        "(Cloudflare WAF, 403 on all /ukraine/en/* paths)"
    )
    return []
