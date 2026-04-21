"""
main.py — запуск усіх скраперів для dityam.com.ua

Запускає 15 скраперів послідовно:
- 7 українських: MON, MAN, Prometheus, Osvita.ua, НУШ, Erasmus+ UA, House of Europe
- 3 тематичні: UNICEF, Save the Children, British Council
- 5 міжнародних (НОВІ): Opportunity Desk, YouthOp, TeenLife, US State Dept, EU Youth Portal

Запуск:
    python main.py                   # усі скрапери
    python main.py --only opportunity_desk  # тільки один
    python main.py --skip state_dept,youthop  # всі крім цих
    python main.py --international   # тільки міжнародні
    python main.py --ukrainian       # тільки українські
"""
import sys
import time
import argparse
from datetime import datetime

from supabase_client import SupabaseClient

# ============ УКРАЇНСЬКІ СКРАПЕРИ ============
from scrape_mon import scrape_mon
from scrape_man import scrape_man
from scrape_prometheus import scrape_prometheus
from scrape_osvita import scrape_osvita
from scrape_nus import scrape_nus
from scrape_erasmus import scrape_erasmus
from scrape_house_of_europe import scrape_house_of_europe

# ============ ТЕМАТИЧНІ ============
from scrape_unicef import scrape_unicef
from scrape_savethechildren import scrape_savethechildren
from scrape_british_council import scrape_british_council

# ============ НОВІ МІЖНАРОДНІ ============
from scrape_opportunity_desk import scrape_opportunity_desk
from scrape_youthop import scrape_youthop
from scrape_teenlife import scrape_teenlife
from scrape_state_department import scrape_state_department
from scrape_eu_youth import scrape_eu_youth


# ============ РЕЄСТР СКРАПЕРІВ ============
# (name, function, tag)
SCRAPERS = [
    # Українські
    ("MON", scrape_mon, "ukrainian"),
    ("MAN", scrape_man, "ukrainian"),
    ("Prometheus", scrape_prometheus, "ukrainian"),
    ("Osvita.ua", scrape_osvita, "ukrainian"),
    ("NUS", scrape_nus, "ukrainian"),
    ("Erasmus+ UA", scrape_erasmus, "ukrainian"),
    ("House of Europe", scrape_house_of_europe, "ukrainian"),

    # Тематичні
    ("UNICEF", scrape_unicef, "thematic"),
    ("Save the Children", scrape_savethechildren, "thematic"),
    ("British Council", scrape_british_council, "thematic"),

    # НОВІ міжнародні
    ("Opportunity Desk", scrape_opportunity_desk, "international"),
    ("Youth Opportunities", scrape_youthop, "international"),
    ("TeenLife", scrape_teenlife, "international"),
    ("US State Department", scrape_state_department, "international"),
    ("EU Youth Portal", scrape_eu_youth, "international"),
]


def run_scraper(name, scraper_fn, client):
    """Запускає один скрапер і повертає статистику."""
    print(f"\n{'=' * 70}")
    print(f"▶️  Запускаю: {name}")
    print(f"{'=' * 70}")
    start = time.time()

    try:
        opportunities = scraper_fn()
        if not opportunities:
            print(f"⚠️  {name}: нічого не зібрано")
            return {"name": name, "status": "empty", "count": 0, "duration": 0}

        print(f"\n📤 {name}: завантажую {len(opportunities)} записів у Supabase...")
        client.upsert_opportunities(opportunities)

        duration = time.time() - start
        print(f"✅ {name}: готово за {duration:.1f}с, {len(opportunities)} записів")
        return {
            "name": name,
            "status": "success",
            "count": len(opportunities),
            "duration": duration,
        }

    except Exception as e:
        duration = time.time() - start
        print(f"❌ {name}: помилка — {type(e).__name__}: {e}")
        return {
            "name": name,
            "status": "error",
            "error": str(e)[:200],
            "duration": duration,
        }


def print_summary(results):
    """Друкує фінальний звіт."""
    print(f"\n\n{'=' * 70}")
    print("📊 ФІНАЛЬНИЙ ЗВІТ")
    print(f"{'=' * 70}")

    total_count = sum(r.get("count", 0) for r in results)
    total_time = sum(r.get("duration", 0) for r in results)
    success = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    empty = [r for r in results if r["status"] == "empty"]

    print(f"\n✅ Успішно: {len(success)}/{len(results)}")
    print(f"⚠️  Порожні:  {len(empty)}")
    print(f"❌ Помилки: {len(errors)}")
    print(f"📦 Всього записів: {total_count}")
    print(f"⏱️  Загальний час: {total_time:.1f}с ({total_time/60:.1f} хв)")

    print(f"\nДеталі по скраперах:")
    for r in results:
        icon = {"success": "✅", "error": "❌", "empty": "⚠️ "}[r["status"]]
        count = f"{r.get('count', 0):3d}"
        duration = f"{r.get('duration', 0):5.1f}s"
        print(f"  {icon} {r['name']:25s} {count} записів  {duration}")

    if errors:
        print(f"\n❌ Помилки детально:")
        for r in errors:
            print(f"  • {r['name']}: {r.get('error', 'Unknown')}")


def parse_args():
    parser = argparse.ArgumentParser(description="Скрапери dityam.com.ua")
    parser.add_argument(
        "--only",
        help="Запустити тільки конкретний скрапер (наприклад: opportunity_desk)"
    )
    parser.add_argument(
        "--skip",
        help="Скрапери що пропустити (через кому, наприклад: state_dept,youthop)"
    )
    parser.add_argument(
        "--international",
        action="store_true",
        help="Тільки міжнародні скрапери (5 нових)"
    )
    parser.add_argument(
        "--ukrainian",
        action="store_true",
        help="Тільки українські скрапери"
    )
    parser.add_argument(
        "--thematic",
        action="store_true",
        help="Тільки тематичні (UNICEF, Save the Children, British Council)"
    )
    return parser.parse_args()


def filter_scrapers(scrapers, args):
    """Фільтрація скраперів за аргументами."""
    filtered = scrapers

    # --only
    if args.only:
        normalized = args.only.lower().replace("-", "_").replace(" ", "_")
        filtered = [
            s for s in filtered
            if normalized in s[0].lower().replace(" ", "_")
        ]
        if not filtered:
            print(f"❌ Не знайдено скрапер для '{args.only}'")
            print(f"Доступні: {[s[0] for s in scrapers]}")
            sys.exit(1)

    # --skip
    if args.skip:
        skip_names = [
            s.strip().lower().replace("-", "_")
            for s in args.skip.split(",")
        ]
        filtered = [
            s for s in filtered
            if not any(skip in s[0].lower().replace(" ", "_") for skip in skip_names)
        ]

    # --international / --ukrainian / --thematic
    if args.international:
        filtered = [s for s in filtered if s[2] == "international"]
    elif args.ukrainian:
        filtered = [s for s in filtered if s[2] == "ukrainian"]
    elif args.thematic:
        filtered = [s for s in filtered if s[2] == "thematic"]

    return filtered


def main():
    args = parse_args()
    start_time = datetime.now()

    print(f"\n🕷️  DITYAM SCRAPERS")
    print(f"📅 Старт: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    scrapers_to_run = filter_scrapers(SCRAPERS, args)
    print(f"🎯 Запуск {len(scrapers_to_run)} скраперів:")
    for name, _, tag in scrapers_to_run:
        print(f"  • {name} [{tag}]")

    # Ініціалізація клієнта
    client = SupabaseClient()

    # Запускаємо всі по черзі
    results = []
    for name, scraper_fn, tag in scrapers_to_run:
        result = run_scraper(name, scraper_fn, client)
        result["tag"] = tag
        results.append(result)
        # Пауза між скраперами щоб не перевантажувати БД
        time.sleep(2)

    # Звіт
    print_summary(results)

    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    print(f"\n🏁 Фініш: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Всього: {total_duration:.1f}с ({total_duration/60:.1f} хв)")

    # Exit code - для GitHub Actions
    errors_count = sum(1 for r in results if r["status"] == "error")
    if errors_count > 0:
        print(f"\n⚠️  Закінчено з {errors_count} помилками")
        sys.exit(1)
    else:
        print(f"\n🎉 Всі скрапери виконано успішно!")
        sys.exit(0)


if __name__ == "__main__":
    main()
