"""Скрапер House of Europe — творчі програми ЄС для молоді.

houseofeurope.org.ua/en/opportunities — сторінка з актуальними можливостями.
Більшість програм орієнтована на молодих спеціалістів, митців та викладачів,
тому нормалізатор може відхиляти частину як нерелевантні для дітей 0–18.
"""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "House of Europe"
BASE_URL = "https://houseofeurope.org.ua"

LIST_URLS = [
    "https://houseofeurope.org.ua/en/opportunities",
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers=_BROWSER_HEADERS,
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        links: set[str] = set()

        for list_url in LIST_URLS:
            try:
                resp = await client.get(list_url)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.select("a[href*='/en/opportunity/']"):
                    href = a.get("href", "")
                    if href:
                        full = href if href.startswith("http") else BASE_URL + href
                        links.add(full.split("?")[0])
            except Exception as e:
                logger.warning(f"Failed {list_url}: {e}")

        logger.info(f"Found {len(links)} House of Europe opportunities")

        if not links:
            return []

        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    title_tag = s.select_one("h1")
                    content = s.select_one("main") or s.select_one("body")
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title_tag.get_text(strip=True) if title_tag else None,
                        "raw_text": (
                            "House of Europe — програма ЄС для молодих митців, "
                            "дослідників і культурних діячів України.\n\n" + text
                        ),
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in list(links)[:30]]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
