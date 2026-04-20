"""Скрапер UNICEF Ukraine — програми допомоги та розвитку дітей."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "UNICEF Ukraine"
BASE_URL = "https://www.unicef.org/ukraine"

PROGRAM_PATHS = [
    "/en/what-we-do",
    "/en/spilno-social-support",
    "/en/upshift-ukraine",
    "/en/child-protection",
    "/en/education",
    "/en/mental-health-and-psychosocial-support",
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
                        "raw_text": f"UNICEF програма для дітей в Україні.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        logger.info(f"Fetching {len(PROGRAM_PATHS)} UNICEF program pages")
        tasks = [fetch_detail(p) for p in PROGRAM_PATHS]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
