"""
Скрапер Teen Life — літні програми, табори і можливості для підлітків.
Джерело: https://www.teenlife.com/
Спеціалізація: літні табори, програми навчання за кордоном, літні академії.
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

BASE_URL = "https://www.teenlife.com"
SOURCE_NAME = "TeenLife"
# TeenLife каталог - окремі сторінки за категоріями
CATEGORY_URLS = [
    f"{BASE_URL}/category/summer-programs/",
    f"{BASE_URL}/category/gap-year/",
    f"{BASE_URL}/category/volunteer/",
    f"{BASE_URL}/category/community-service/",
]
MAX_PAGES = 2


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


def detect_type(title, summary):
    text = (title + " " + summary).lower()
    if "camp" in text:
        return "camp"
    if "summer program" in text or "summer academy" in text or "summer institute" in text:
        return "camp"
    if "gap year" in text:
        return "exchange"
    if "volunteer" in text or "community service" in text:
        return "volunteer"
    if "course" in text or "class" in text:
        return "course"
    return "camp"


def detect_categories(title, summary):
    text = (title + " " + summary).lower()
    cats = []
    if any(k in text for k in ["stem", "math", "science", "coding", "robotics", "engineering"]):
        cats.append("STEM")
    if any(k in text for k in ["art", "music", "theatre", "film", "photography", "design"]):
        cats.append("arts")
    if any(k in text for k in ["sport", "athletic", "basketball", "tennis", "soccer"]):
        cats.append("sports")
    if any(k in text for k in ["language", "english", "spanish", "french", "german"]):
        cats.append("languages")
    if any(k in text for k in ["leadership", "business", "entrepreneur", "debate"]):
        cats.append("leadership")
    if not cats:
        cats.append("education")
    return "{" + ",".join(cats) + "}"


def is_international(title, summary):
    """Бере тільки якщо відкрито для міжнародних учасників або абстрактно 'для підлітків'."""
    text = (title + " " + summary).lower()
    # Виключаємо явні локальні для резидентів США
    local_only = [
        "connecticut residents", "new york state residents",
        "california residents", "must be us resident",
        "us residents only", "american citizens only"
    ]
    if any(m in text for m in local_only):
        return False
    return True


def parse_list_page(url):
    print(f"  📄 {url}")
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # TeenLife структура: article.post, div.program-card
    selectors = [
        "article.post",
        "div.program-card",
        "article.card",
        "div.entry",
    ]

    articles = []
    for sel in selectors:
        articles = soup.select(sel)
        if articles:
            break

    for article in articles:
        title_el = article.find(["h2", "h3"])
        if not title_el:
            continue

        link_el = title_el.find("a") or article.find("a", href=True)
        if not link_el:
            continue

        title = clean_text(title_el.get_text(strip=True), 200)
        href = link_el.get("href", "")
        if not href or len(title) < 10:
            continue

        full_url = urljoin(BASE_URL, href)

        # Excerpt
        excerpt_el = article.find("p") or article.find(class_=re.compile(r"excerpt|summary"))
        excerpt = clean_text(excerpt_el.get_text(strip=True), 400) if excerpt_el else ""

        items.append({
            "title": title,
            "url": full_url,
            "excerpt": excerpt,
        })

    return items


def parse_detail(url):
    html = fetch_page(url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    content_el = (
        soup.find("article") or
        soup.find("div", class_=re.compile(r"post-content|entry-content"))
    )

    summary = ""
    if content_el:
        first_p = content_el.find("p")
        if first_p:
            summary = clean_text(first_p.get_text(strip=True), 400)

    full_text = content_el.get_text(" ", strip=True) if content_el else ""
    deadline = extract_deadline(full_text)
    return summary, deadline


def scrape_teenlife():
    print(f"🔍 Скрапінг {BASE_URL}")

    all_items = []
    for cat_url in CATEGORY_URLS:
        for page in range(1, MAX_PAGES + 1):
            url = cat_url if page == 1 else f"{cat_url}page/{page}/"
            items = parse_list_page(url)
            all_items.extend(items)
            time.sleep(1.5)

    seen = set()
    unique = []
    for it in all_items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        unique.append(it)

    print(f"🔗 Знайдено {len(unique)} унікальних записів")

    opportunities = []
    for i, item in enumerate(unique[:35], 1):
        if not is_international(item["title"], item["excerpt"]):
            continue

        print(f"  [{i}/{len(unique)}] {item['title'][:60]}")
        summary, deadline = parse_detail(item["url"])
        time.sleep(1.0)

        opp_type = detect_type(item["title"], item["excerpt"] + " " + (summary or ""))
        categories = detect_categories(item["title"], item["excerpt"] + " " + (summary or ""))

        record = {
            "title": item["title"][:200],
            "slug": make_slug(item["title"], SOURCE_NAME),
            "summary": (summary or item["excerpt"] or
                       "Літня програма або табір від TeenLife. Детальніше на сайті.")[:400],
            "age_from": 13,
            "age_to": 17,
            "opportunity_type": opp_type,
            "categories": categories,
            "child_needs": "{}",
            "format": "Офлайн за кордоном + онлайн",
            "cost_type": "paid_affordable",
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
    opps = scrape_teenlife()

    if not opps:
        print("\n⚠️  Нічого не зібрано")
    else:
        print(f"\n📤 Завантажую {len(opps)} записів у Supabase...")
        client.upsert_opportunities(opps)
