"""
Скрапер Youth Opportunities — популярний агрегатор для молоді до 18.
Джерело: https://www.youthop.com/
Витягує: обміни, конкурси, конференції, літні програми.
"""
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from supabase_client import (
    SupabaseClient, make_slug, content_hash,
    extract_deadline, clean_text, BROWSER_HEADERS,
)

BASE_URL = "https://www.youthop.com"
SOURCE_NAME = "Youth Opportunities"
# YouthOp має окремі сторінки за типами
CATEGORY_URLS = [
    f"{BASE_URL}/opportunities/competitions",
    f"{BASE_URL}/opportunities/conferences",
    f"{BASE_URL}/opportunities/exchange-programs",
    f"{BASE_URL}/opportunities/scholarships",
]
MAX_PAGES_PER_CATEGORY = 2


def fetch_page(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=BROWSER_HEADERS, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"  ⚠️  Спроба {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def detect_type_from_url(url):
    """Визначає тип з URL категорії."""
    if "scholarship" in url:
        return "scholarship"
    if "competition" in url:
        return "competition"
    if "exchange" in url:
        return "exchange"
    if "conference" in url:
        return "exchange"
    if "fellowship" in url:
        return "scholarship"
    if "internship" in url:
        return "internship"
    return "competition"


def is_for_teens(title, summary):
    """Фільтр: чи для підлітків 14-18."""
    text = (title + " " + summary).lower()
    # Виключаємо програми явно для дорослих
    adult_only = ["phd", "doctoral", "postdoc", "professionals", "working"]
    if any(m in text for m in adult_only):
        return False
    # Позитивні маркери для підлітків
    teen_markers = [
        "high school", "secondary school", "youth", "teenager",
        "young", "student", "undergrad", "ages 14", "ages 15",
        "ages 16", "ages 17", "ages 18", "15-18", "16-18", "14-18",
    ]
    return any(m in text for m in teen_markers) or True  # fallback - бере


def parse_list_page(url):
    print(f"  📄 {url}")
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # YouthOp структура: article або div.opportunity-card
    selectors = [
        "article.opp-list__item",
        "div.opportunity-card",
        "article.card",
        "div.card",
        "article.post",
    ]

    articles = []
    for sel in selectors:
        articles = soup.select(sel)
        if articles:
            break

    # Fallback - шукаємо будь-які посилання з текстом-заголовком
    if not articles:
        articles = soup.find_all("a", href=re.compile(r"/opportunities/"))

    for article in articles[:20]:  # макс 20 на сторінку
        title_el = article.find(["h3", "h2"]) if hasattr(article, "find") else None
        link_el = article if article.name == "a" else (
            article.find("a", href=re.compile(r"/opportunities/")) if hasattr(article, "find") else None
        )

        if not link_el:
            continue

        # Title
        if title_el:
            title = clean_text(title_el.get_text(strip=True), 200)
        else:
            title = clean_text(link_el.get_text(strip=True), 200)

        href = link_el.get("href", "")
        if not href or len(title) < 15:
            continue

        full_url = urljoin(BASE_URL, href)

        # Excerpt
        excerpt_el = article.find("p") if hasattr(article, "find") else None
        excerpt = clean_text(excerpt_el.get_text(strip=True), 400) if excerpt_el else ""

        items.append({
            "title": title,
            "url": full_url,
            "excerpt": excerpt,
        })

    return items


def parse_article_detail(url):
    """Деталі зі сторінки опп-ті."""
    html = fetch_page(url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    content_el = (
        soup.find("article") or
        soup.find("div", class_=re.compile(r"opp-content|opportunity-content|main-content"))
    )

    summary = ""
    if content_el:
        first_p = content_el.find("p")
        if first_p:
            summary = clean_text(first_p.get_text(strip=True), 400)

    full_text = content_el.get_text(" ", strip=True) if content_el else ""
    deadline = extract_deadline(full_text)
    return summary, deadline


def scrape_youthop():
    print(f"🔍 Скрапінг {BASE_URL}")

    all_items = []
    for cat_url in CATEGORY_URLS:
        opp_type = detect_type_from_url(cat_url)
        print(f"  Категорія: {opp_type}")
        for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
            url = cat_url if page == 1 else f"{cat_url}?page={page}"
            items = parse_list_page(url)
            for it in items:
                it["type_hint"] = opp_type
            all_items.extend(items)
            time.sleep(1.5)

    # Дедуплікація
    seen = set()
    unique = []
    for it in all_items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        unique.append(it)

    print(f"🔗 Знайдено {len(unique)} унікальних записів")

    opportunities = []
    for i, item in enumerate(unique[:30], 1):
        if not is_for_teens(item["title"], item["excerpt"]):
            continue

        print(f"  [{i}/{len(unique)}] {item['title'][:60]}")
        summary, deadline = parse_article_detail(item["url"])
        time.sleep(1.0)

        record = {
            "title": item["title"][:200],
            "slug": make_slug(item["title"], SOURCE_NAME),
            "summary": (summary or item["excerpt"] or
                       "Міжнародна можливість для молоді від Youth Opportunities.")[:400],
            "age_from": 14,
            "age_to": 17,
            "opportunity_type": item.get("type_hint", "competition"),
            "categories": "{leadership,education}",
            "child_needs": "{}",
            "format": "Онлайн + за кордоном",
            "cost_type": "free",
            "deadline": deadline,
            "source_url": item["url"],
            "source": SOURCE_NAME,
            "content_hash": content_hash(item["title"], item["url"]),
        }
        opportunities.append(record)

    return opportunities


if __name__ == "__main__":
    print(f"🕷️  {SOURCE_NAME} Scraper")
    print("=" * 60)

    client = SupabaseClient()
    opps = scrape_youthop()

    if not opps:
        print("\n⚠️  Нічого не зібрано")
    else:
        print(f"\n📤 Завантажую {len(opps)} записів у Supabase...")
        client.upsert_opportunities(opps)
