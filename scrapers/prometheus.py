"""Скрапер Prometheus — безкоштовні онлайн-курси для школярів."""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Prometheus"
BASE_URL = "https://prometheus.org.ua"
LIST_URL = "https://prometheus.org.ua/courses/"

# Фільтр — тільки курси для школярів
SCHOOL_KEYWORDS = [
    "зно", "нмт", "школ", "клас", "учн", "підліт", "підготов",
    "математика", "фізика", "хімія", "біологія", "історія",
    "англійська", "українська", "література",
]


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 ChildrenOppBot/1.0"},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        try:
            resp = await client.get(LIST_URL)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch Prometheus list: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        links = set()
        for a in soup.select("a[href*='/course/'], a[href*='/prometheus/']"):
            href = a.get("href", "")
            if href:
                full = href if href.startswith("http") else BASE_URL + href
                links.add(full.split("?")[0])

        logger.info(f"Found {len(links)} Prometheus courses")

        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    title_tag = s.select_one("h1")
                    title = title_tag.get_text(strip=True) if title_tag else ""

                    # Пропускаємо якщо курс не для школярів
                    if not any(kw in title.lower() for kw in SCHOOL_KEYWORDS):
                        return None

                    content = s.select_one("main") or s.select_one("article") or s.select_one("body")
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title,
                        "raw_text": f"Безкоштовний онлайн-курс Prometheus для школярів.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in list(links)[:40]]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
