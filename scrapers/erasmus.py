"""Скрапер Erasmus+ Ukraine — новини про обміни, гранти, тренінги."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Erasmus+ Ukraine"
BASE_URL = "https://erasmusplus.org.ua"

# Фільтр — новини про можливості
RELEVANT_KEYWORDS = [
    "молод", "школяр", "студент", "обмін", "стипенді", "грант",
    "волонтер", "тренінг", "конкурс", "програм", "solidarity",
]


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 ChildrenOppBot/1.0"},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        # Збираємо посилання з перших 2 сторінок
        all_links = set()
        for page in range(1, 3):
            list_url = BASE_URL if page == 1 else f"{BASE_URL}/page/{page}/"
            try:
                resp = await client.get(list_url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed page {page}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            for article in soup.select("article, div.post, div.news-item, div.entry"):
                title_el = article.find(["h1", "h2", "h3"])
                if not title_el:
                    continue
                link_el = title_el.find("a") or article.find("a", href=True)
                if not link_el:
                    continue

                title = title_el.get_text(strip=True)
                href = link_el.get("href", "")

                if not title or not href or len(title) < 10:
                    continue

                # Фільтр за ключовими словами
                if not any(kw in title.lower() for kw in RELEVANT_KEYWORDS):
                    continue

                full_url = href if href.startswith("http") else BASE_URL + href
                all_links.add(full_url.split("?")[0])

        logger.info(f"Found {len(all_links)} Erasmus+ articles")

        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    title_tag = s.select_one("h1")
                    content = (
                        s.select_one("article")
                        or s.select_one("main")
                        or s.select_one("div.entry-content")
                        or s.select_one("body")
                    )
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title_tag.get_text(strip=True) if title_tag else None,
                        "raw_text": f"Erasmus+ можливість для молоді.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in list(all_links)[:25]]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
