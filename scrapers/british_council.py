"""Скрапер British Council Ukraine — мови, обміни, навчання у UK."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "British Council Ukraine"
BASE_URL = "https://www.britishcouncil.org.ua"

PROGRAM_PATHS = [
    "/en/programmes",
    "/en/english",
    "/en/english/children",
    "/en/exams-for-children",
    "/en/study-uk",
]


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 ChildrenOppBot/1.0"},
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
                        "raw_text": f"British Council програма в Україні.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        logger.info(f"Fetching {len(PROGRAM_PATHS)} British Council pages")
        tasks = [fetch_detail(p) for p in PROGRAM_PATHS]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
