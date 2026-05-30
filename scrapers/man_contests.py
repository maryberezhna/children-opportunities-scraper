"""Скрапер Малої академії наук — конкурси для школярів.

man.gov.ua/contests — це Next.js SSR сторінка, де ВСІ конкурси
вшиті у JS object literal всередині __NEXT_DATA__. Класичний
BeautifulSoup-пошук `a[href*='/contests/']` бачить тільки 2 лінки
(ті, що рендеряться як CTA), але реальний список — 28+ контестів —
у JSON. Тож парсимо JSON, потім робимо follow-up GET за деталями.
"""
import asyncio
import logging
import re
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "Мала академія наук України"
BASE_URL = "https://man.gov.ua"
LIST_URL = "https://man.gov.ua/contests"

# Pattern matches: slug:"X",[startDate:..,endDate:..,]title:"T"
# inside the embedded JS data. Captures slug + title (with escapes).
_ENTRY_RE = re.compile(
    r'slug:"([a-z0-9-]+)"[^}]*?title:"((?:\\.|[^"\\])+)"',
    re.DOTALL,
)


def _clean_title(raw: str) -> str:
    return (
        raw.replace("\\n", " ")
        .replace('\\"', '"')
        .replace("\\/", "/")
        .strip()
    )


async def _resolve_canonical_url(client: httpx.AsyncClient, slug: str) -> str | None:
    """MAN serves contests at either /contests/<slug> or /contests/olympiad/<slug>.
    Try both with HEAD; return whichever is live (2xx/3xx)."""
    for path in (f"/contests/{slug}", f"/contests/olympiad/{slug}"):
        url = BASE_URL + path
        try:
            r = await client.head(url, follow_redirects=True)
            if 200 <= r.status_code < 400:
                return str(r.url) or url
        except Exception:
            continue
    return None


async def fetch_all() -> list[dict]:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 ChildrenOppBot/1.0"},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        resp = await client.get(LIST_URL)
        resp.raise_for_status()

        # Extract all (slug, title) pairs from embedded JSON data.
        entries: dict[str, str] = {}
        for m in _ENTRY_RE.finditer(resp.text):
            slug = m.group(1)
            title = _clean_title(m.group(2))
            if len(title) >= 5 and slug not in entries:
                entries[slug] = title

        if not entries:
            logger.warning("MAN: no contests parsed from embedded JSON")
            return []

        logger.info(f"Found {len(entries)} MAN contests in embedded JSON")

        # Resolve each slug to its canonical URL (some live under /olympiad/).
        semaphore = asyncio.Semaphore(3)

        async def fetch_detail(slug: str, title: str) -> dict | None:
            async with semaphore:
                url = await _resolve_canonical_url(client, slug)
                if not url:
                    logger.warning(f"MAN: no live URL for slug={slug}")
                    return None
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    s = BeautifulSoup(r.text, "lxml")
                    h1 = s.select_one("h1")
                    content = s.select_one("article") or s.select_one("main")
                    text = (
                        content.get_text(separator="\n", strip=True)[:6000]
                        if content
                        else ""
                    )
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": (h1.get_text(strip=True) if h1 else None) or title,
                        "raw_text": f"Конкурс/олімпіада МАН для школярів.\n\n{text}",
                    }
                except Exception as e:
                    logger.warning(f"MAN: detail fetch failed for {url}: {e}")
                    # Even without detail body, the listing-level data is enough
                    # for normalizer to produce a row.
                    return {
                        "source": SOURCE_NAME,
                        "source_url": url,
                        "raw_title": title,
                        "raw_text": f"Конкурс/олімпіада МАН для школярів. {title}",
                    }

        tasks = [fetch_detail(slug, title) for slug, title in entries.items()]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]
