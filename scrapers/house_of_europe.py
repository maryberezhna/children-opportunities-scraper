"""Скрапер House of Europe — творчі програми ЄС для молоді."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "House of Europe"
BASE_URL = "https://houseofeurope.org.ua"
LIST_URL = "https://houseofeurope.org.ua/en/opportunities"


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
        for a in soup.select("a[href*='/en/opportunity/']"):
            href = a.get("href")
            if href:
                full = href if href.startswith("http") else BASE_URL + href
                links.add(full.split("?")[0])

        logger.info(f"Found {len(links)} URLs")

        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    title_tag = s.select_one("h1")
                    content = s.select_one("main") or s.select_one("body")
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title_tag.get_text(strip=True) if title_tag else None,
                        "raw_text": content.get_text(separator="\n", strip=True)[:6000] if content else "",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in list(links)[:30]]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
