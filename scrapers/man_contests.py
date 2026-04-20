"""Скрапер Малої академії наук — конкурси для школярів."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Мала академія наук України"
BASE_URL = "https://man.gov.ua"
LIST_URL = "https://man.gov.ua/contests"


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 ChildrenOppBot/1.0"},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        resp = await client.get(LIST_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        links = set()
        for a in soup.select("a[href*='/contests/']"):
            href = a.get("href")
            if href and href not in ("/contests", "/contests/"):
                full = href if href.startswith("http") else BASE_URL + href
                links.add(full.split("?")[0])

        logger.info(f"Found {len(links)} MAN contests")

        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    title_tag = s.select_one("h1")
                    content = s.select_one("article") or s.select_one("main")
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title_tag.get_text(strip=True) if title_tag else None,
                        "raw_text": f"Конкурс МАН для школярів.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in list(links)[:20]]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
