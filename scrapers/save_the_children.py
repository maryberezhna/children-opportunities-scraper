"""Скрапер Save the Children Ukraine — освіта, захист, цифрові центри."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Save the Children Ukraine"
BASE_URL = "https://ukraine.savethechildren.net"

PROGRAM_PATHS = [
    "/what-we-do",
    "/what-we-do/education",
    "/what-we-do/child-protection",
    "/what-we-do/mental-health-psychosocial-support",
    "/what-we-do/digital-learning-centres",
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers=_BROWSER_HEADERS,
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(path: str):
            async with semaphore:
                url = BASE_URL + path
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    title_tag = s.select_one("h1")
                    content = s.select_one("main") or s.select_one("article") or s.select_one("body")
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title_tag.get_text(strip=True) if title_tag else None,
                        "raw_text": f"Save the Children програма для дітей в Україні.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        logger.info(f"Fetching {len(PROGRAM_PATHS)} Save the Children pages")
        tasks = [fetch_detail(p) for p in PROGRAM_PATHS]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
