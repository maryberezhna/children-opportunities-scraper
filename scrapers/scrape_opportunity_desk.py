"""
Скрапер Opportunity Desk — найбільший міжнародний агрегатор можливостей.
Джерело: https://opportunitydesk.org/category/high-school-students/
Витягує: стипендії, конкурси, фелоушипи для школярів по всьому світу.
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

BASE_URL = "https://opportunitydesk.org"
CATEGORY_URLS = [
    f"{BASE_URL}/category/high-school-students/",
    f"{BASE_URL}/category/youth/",
]
SOURCE_NAME = "Opportunity Desk"
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
    if "scholarship" in text or "стипенд" in text:
        return "scholarship"
    if "fellowship" in text or "фелоушип" in text:
        return "scholarship"
    if "contest" in text or "competition" in text or "award" in text or "prize" in text:
        return "competition"
    if "camp" in text or "summer program" in text or "institute" in text:
        return "camp"
    if "exchange" in text or "mobility" in text:
        return "exchange"
    if "internship" in text:
        return "internship"
    if "grant" in text or "funding" in text:
        return "grant"
    if "conference" in text or "summit" in text or "forum" in text:
        return "exchange"
    return "competition"


def detect_age(title, summary):
    text = (title + " " + summary).lower()
    if "middle school" in text or "12 to 14" in text:
        return 12, 14
    if "elementary" in text or "younger" in text:
        return 8, 12
    # Default for high school
    return 14, 17


def is_relevant_for_ukrainians(title, summary):
    """Фільтруємо: тільки програми що приймають міжнародних учасників або конкретно Ukraine."""
    text = (title + " " + summary).lower()
    # Явно для США-only / UK-only
    exclusion_markers = [
        "us citizens only", "u.s. citizens only", "american citizens only",
        "uk citizens only", "only uk", "domestic only",
        "canadian residents only", "must be a resident of",
    ]
    if any(m in text for m in exclusion_markers):
        return False
    # Позитивні маркери
    inclusion_markers = [
        "international", "worldwide", "global", "any country",
        "all countries", "open to", "ukraine", "ukrainian",
    ]
    # Якщо прямо вказано international/worldwide — точно бере
    if any(m in text for m in inclusion_markers):
        return True
    # За замовчуванням — бере (якщо не було exclusion)
    return True


def parse_list_page(url):
    print(f"  📄 {url}")
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # Opportunity Desk використовує article.post або div.td-module-container
    selectors = [
        "article.td-animation-stack",
        "div.td-module-container",
        "article.post",
        "div.post",
    ]

    articles = []
    for sel in selectors:
        articles = soup.select(sel)
        if articles:
            break

    for article in articles:
        title_el = article.find(["h3", "h2"], class_=re.compile(r"title|entry"))
        if not title_el:
            title_el = article.find(["h3", "h2"])
        if not title_el:
            continue

        link_el = title_el.find("a") or article.find("a", href=True)
        if not link_el:
            continue

        title = clean_text(title_el.get_text(strip=True), 200)
        href = link_el.get("href", "")
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)

        # Excerpt (якщо є)
        excerpt_el = article.find(class_=re.compile(r"excerpt|summary|description"))
        excerpt = clean_text(excerpt_el.get_text(strip=True), 400) if excerpt_el else ""

        if title and len(title) > 15:
            items.append({
                "title": title,
                "url": full_url,
                "excerpt": excerpt,
            })

    return items


def parse_article_detail(url):
    """Витягуємо summary і deadline зі сторінки статті."""
    html = fetch_page(url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    content_el = (
        soup.find("div", class_="td-post-content") or
        soup.find("article") or
        soup.find("div", class_=re.compile(r"post-content|entry-content"))
    )

    if not content_el:
        return None, None

    # Перший параграф як summary
    summary = ""
    first_p = content_el.find("p")
    if first_p:
        summary = clean_text(first_p.get_text(strip=True), 400)

    # Deadline
    full_text = content_el.get_text(" ", strip=True)
    deadline = extract_deadline(full_text)

    return summary, deadline


def scrape_opportunity_desk():
    print(f"🔍 Скрапінг {BASE_URL}")

    all_items = []
    for cat_url in CATEGORY_URLS:
        for page in range(1, MAX_PAGES + 1):
            url = cat_url if page == 1 else f"{cat_url}page/{page}/"
            items = parse_list_page(url)
            all_items.extend(items)
            time.sleep(1.5)

    # Дедуплікація по URL
    seen = set()
    unique = []
    for it in all_items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        unique.append(it)

    print(f"🔗 Знайдено {len(unique)} унікальних записів")

    opportunities = []
    for i, item in enumerate(unique[:40], 1):  # Ліміт 40 щоб не спамити
        if not is_relevant_for_ukrainians(item["title"], item["excerpt"]):
            continue

        print(f"  [{i}/{len(unique)}] {item['title'][:60]}")
        summary, deadline = parse_article_detail(item["url"])
        time.sleep(1.0)

        opp_type = detect_type(item["title"], item["excerpt"] + " " + (summary or ""))
        age_from, age_to = detect_age(item["title"], item["excerpt"])

        record = {
            "title": item["title"][:200],
            "slug": make_slug(item["title"], SOURCE_NAME),
            "summary": (summary or item["excerpt"] or
                       "Міжнародна можливість від Opportunity Desk для школярів.")[:400],
            "age_from": age_from,
            "age_to": age_to,
            "opportunity_type": opp_type,
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
    opps = scrape_opportunity_desk()

    if not opps:
        print("\n⚠️  Нічого не зібрано")
    else:
        print(f"\n📤 Завантажую {len(opps)} записів у Supabase...")
        client.upsert_opportunities(opps)
