"""Головний скрипт-оркестратор."""
import asyncio
import logging

from scrapers import (
    house_of_europe,
    man_contests,
    unicef,
    save_the_children,
    british_council,
    prometheus,
    erasmus,
)
from normalizer import Normalizer
from db import get_client, upsert_opportunity, archive_expired

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


SCRAPERS = [
    house_of_europe,
    man_contests,
    unicef,
    save_the_children,
    british_council,
    prometheus,
    erasmus,
]


async def main():
    logger.info("=" * 50)
    logger.info("STARTING OPPORTUNITIES AGGREGATOR")
    logger.info("=" * 50)

    all_raw = []
    for scraper in SCRAPERS:
        try:
            logger.info(f"Fetching from {scraper.SOURCE_NAME}")
            items = await scraper.fetch_all()
            all_raw.extend(items)
            logger.info(f"  Got {len(items)} items")
        except Exception as e:
            logger.error(f"Scraper {scraper.SOURCE_NAME} failed: {e}", exc_info=True)

    logger.info(f"Total raw items: {len(all_raw)}")

    if not all_raw:
        logger.warning("Nothing scraped, exiting")
        return

    logger.info("Normalizing with Claude Haiku...")
    normalizer = Normalizer()
    normalized = []
    for raw in all_raw:
        result = normalizer.normalize(
            raw_text=raw["raw_text"],
            source=raw["source"],
            source_url=raw["source_url"],
            raw_title=raw.get("raw_title"),
        )
        if result:
            normalized.append(result)

    logger.info(f"Normalized: {len(normalized)}/{len(all_raw)}")

    if not normalized:
        return

    logger.info("Saving to Supabase...")
    client = get_client()
    saved = 0
    for item in normalized:
        result = upsert_opportunity(client, item)
        if result:
            saved += 1
            logger.info(f"  Saved: {item['title'][:60]}")

    logger.info(f"Saved {saved} opportunities")

    archived = archive_expired(client)
    logger.info(f"Archived {archived} expired opportunities")


if __name__ == "__main__":
    asyncio.run(main())
