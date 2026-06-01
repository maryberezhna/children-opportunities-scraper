"""Скрапер Prometheus — безкоштовні онлайн-курси для школярів.

Джерело URL: democourse-sitemap.xml (69 курсових лендингів, статичний HTML).
Попередній підхід (/courses/) повертав 404 після редизайну сайту.
"""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Prometheus"
BASE_URL = "https://prometheus.org.ua"
SITEMAP_URL = "https://prometheus.org.ua/democourse-sitemap.xml"

SCHOOL_KEYWORDS = [
    "зно", "нмт", "школ", "клас", "учн", "підліт", "підготов",
    "математика", "фізика", "хімія", "біологія", "історія",
    "англійська", "українська", "література", "інформатик",
    "програмування", "python", "web", "веб", "stem",
    "олімпіад", "конкурс", "університет", "вступ",
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
}


async def _get_course_urls(client: httpx.AsyncClient) -> list[str]:
    try:
        r = await client.get(SITEMAP_URL)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml-xml")
        return [
            loc.get_text().strip()
            for loc in soup.select("loc")
            if "/democourse/" in loc.get_text()
               and not any(ext in loc.get_text() for ext in (".png", ".jpg", ".webp", ".pdf"))
        ]
    except Exception as e:
        logger.error(f"Failed to fetch Prometheus sitemap: {e}")
        return []


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers=_BROWSER_HEADERS,
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        urls = await _get_course_urls(client)
        logger.info(f"Found {len(urls)} Prometheus course pages in sitemap")

        if not urls:
            return []

        semaphore = asyncio.Semaphore(4)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")

                    title_tag = s.select_one("h1")
                    title = title_tag.get_text(strip=True) if title_tag else ""

                    content = s.select_one("main") or s.select_one("article") or s.select_one("body")
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""

                    title_and_text = (title + " " + text).lower()
                    if not any(kw in title_and_text for kw in SCHOOL_KEYWORDS):
                        return None

                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title,
                        "raw_text": f"Безкоштовний онлайн-курс Prometheus.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
