"""main.py — запуск скраперів для dityam.com.ua

Запуск:
    python main.py                       # усі скрапери
    python main.py --only man            # тільки один (підрядок у назві)
    python main.py --skip unicef,erasmus # всі крім цих
    python main.py --ukrainian           # тільки українські
    python main.py --thematic            # тільки тематичні
"""
import argparse
import asyncio
import sys
import time
from datetime import datetime

from db import archive_expired, get_client, upsert_opportunity
from normalizer import Normalizer
from scrapers import (
    british_council,
    erasmus,
    house_of_europe,
    man_contests,
    prometheus,
    save_the_children,
    unicef,
)

SCRAPERS = [
    ("MAN", man_contests, "ukrainian"),
    ("Prometheus", prometheus, "ukrainian"),
    ("Erasmus+ UA", erasmus, "ukrainian"),
    ("House of Europe", house_of_europe, "ukrainian"),
    ("UNICEF", unicef, "thematic"),
    ("Save the Children", save_the_children, "thematic"),
    ("British Council", british_council, "thematic"),
]


async def run_scraper(name, module, normalizer, sb_client):
    print(f"\n{'=' * 70}\n▶️  {name}\n{'=' * 70}")
    start = time.time()
    try:
        raw_items = await module.fetch_all()
    except Exception as e:
        return {"name": name, "status": "error",
                "error": f"{type(e).__name__}: {e}"[:200],
                "count": 0, "duration": time.time() - start}

    if not raw_items:
        return {"name": name, "status": "empty", "count": 0,
                "duration": time.time() - start}

    saved = 0
    for raw in raw_items:
        normalized = normalizer.normalize(
            raw_text=raw.get("raw_text", ""),
            source=raw.get("source", name),
            source_url=raw.get("source_url", ""),
            raw_title=raw.get("raw_title"),
        )
        if not normalized:
            continue
        if upsert_opportunity(sb_client, normalized):
            saved += 1

    duration = time.time() - start
    print(f"✅ {name}: {saved}/{len(raw_items)} збережено за {duration:.1f}с")
    return {"name": name, "status": "success", "count": saved,
            "duration": duration}


def print_summary(results, archived):
    print(f"\n\n{'=' * 70}\n📊 ФІНАЛЬНИЙ ЗВІТ\n{'=' * 70}")
    success = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    empty = [r for r in results if r["status"] == "empty"]
    total = sum(r.get("count", 0) for r in results)
    total_time = sum(r.get("duration", 0) for r in results)

    print(f"✅ Успішно: {len(success)}/{len(results)}")
    print(f"⚠️  Порожні:  {len(empty)}")
    print(f"❌ Помилки: {len(errors)}")
    print(f"📦 Всього записів: {total}")
    print(f"🗄️  Архівовано прострочених: {archived}")
    print(f"⏱️  Загальний час: {total_time:.1f}с ({total_time / 60:.1f} хв)\n")

    for r in results:
        icon = {"success": "✅", "error": "❌", "empty": "⚠️ "}[r["status"]]
        print(f"  {icon} {r['name']:25s} {r.get('count', 0):3d} записів  "
              f"{r.get('duration', 0):5.1f}s")

    if errors:
        print("\n❌ Помилки детально:")
        for r in errors:
            print(f"  • {r['name']}: {r.get('error', 'Unknown')}")


def parse_args():
    p = argparse.ArgumentParser(description="Скрапери dityam.com.ua")
    p.add_argument("--only", help="Підрядок у назві скрапера")
    p.add_argument("--skip", help="Через кому, які пропустити")
    p.add_argument("--ukrainian", action="store_true")
    p.add_argument("--thematic", action="store_true")
    return p.parse_args()


def filter_scrapers(scrapers, args):
    out = scrapers
    if args.only:
        needle = args.only.lower()
        out = [s for s in out if needle in s[0].lower()]
        if not out:
            print(f"❌ Не знайдено '{args.only}'. Доступні: "
                  f"{[s[0] for s in scrapers]}")
            sys.exit(1)
    if args.skip:
        skip = [s.strip().lower() for s in args.skip.split(",")]
        out = [s for s in out
               if not any(n in s[0].lower() for n in skip)]
    if args.ukrainian:
        out = [s for s in out if s[2] == "ukrainian"]
    elif args.thematic:
        out = [s for s in out if s[2] == "thematic"]
    return out


async def amain():
    args = parse_args()
    start = datetime.now()
    print(f"\n🕷️  DITYAM SCRAPERS\n📅 Старт: {start:%Y-%m-%d %H:%M:%S}")

    scrapers = filter_scrapers(SCRAPERS, args)
    print(f"🎯 Запуск {len(scrapers)} скраперів:")
    for name, _, tag in scrapers:
        print(f"  • {name} [{tag}]")

    normalizer = Normalizer()
    sb_client = get_client()

    results = []
    for name, module, tag in scrapers:
        result = await run_scraper(name, module, normalizer, sb_client)
        result["tag"] = tag
        results.append(result)
        await asyncio.sleep(2)

    archived = archive_expired(sb_client)
    print_summary(results, archived)

    end = datetime.now()
    total = (end - start).total_seconds()
    print(f"\n🏁 Фініш: {end:%Y-%m-%d %H:%M:%S}")
    print(f"⏱️  Всього: {total:.1f}с ({total / 60:.1f} хв)")

    if any(r["status"] == "error" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(amain())
