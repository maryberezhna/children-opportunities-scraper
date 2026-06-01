"""Скрапер Erasmus+ Ukraine — новини про обміни, гранти, тренінги.

erasmusplus.org.ua — сайт Національного Еразмус+ офісу в Україні.
Після редизайну статті живуть за адресами /novyny/XXXXX/.
Головна сторінка і список новин мають href='#' у картках (JS-рендер),
тому збираємо посилання через прямий пошук за шаблоном /novyny/.
"""
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Erasmus+ Ukraine"
BASE_URL = "https://erasmusplus.org.ua"

NEWS_PAGES = [
    f"{BASE_URL}/news/novyny-ofisu/",
    f"{BASE_URL}/news/novyny-ofisu/page/2/",
    f"{BASE_URL}/news/novyny-ofisu/page/3/",
]

RELEVANT_KEYWORDS = [
    "молод", "школяр", "студент", "учн", "підліт",
    "обмін", "стипенді", "грант", "конкурс", "програм",
    "youth", "exchange", "scholarship", "fellowship",
    "solidarity", "volunteering", "training",
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

        # Collect unique article URLs from news listing pages
        all_links: set[str] = set()
        for page_url in NEWS_PAGES:
            try:
                resp = await client.get(page_url)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                # Articles at /novyny/XXXXX/ — skip duplicates and non-articles
                for a in soup.select("a[href*='/novyny/']"):
                    href = a.get("href", "")
                    if not href:
                        continue
                    full = href if href.startswith("http") else BASE_URL + href
                    full = full.split("?")[0].rstrip("/")
                    # Must end with a numeric ID, not category pages
                    slug = full.rstrip("/").split("/")[-1]
                    if slug.isdigit() or (slug and not slug.startswith("novyny")):
                        all_links.add(full)
            except Exception as e:
                logger.warning(f"Failed listing page {page_url}: {e}")

        logger.info(f"Found {len(all_links)} Erasmus+ article URLs")

        if not all_links:
            return []

        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(url: str):
            async with semaphore:
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")

                    title_tag = s.select_one("h1")
                    title = title_tag.get_text(strip=True) if title_tag else ""

                    content = (
                        s.select_one("article")
                        or s.select_one(".entry-content")
                        or s.select_one("main")
                        or s.select_one("body")
                    )
                    text = content.get_text(separator="\n", strip=True)[:6000] if content else ""

                    combined = (title + " " + text).lower()
                    if not any(kw in combined for kw in RELEVANT_KEYWORDS):
                        return None

                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title or None,
                        "raw_text": f"Erasmus+ можливість для молоді.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
                    return None

        tasks = [fetch_detail(url) for url in list(all_links)[:25]]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
