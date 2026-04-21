"""
Скрапер Exchanges.state.gov — офіційні програми обмінів Держдепу США.
Джерело: https://exchanges.state.gov/non-us/programs
Витягує: FLEX, YES, Fulbright, TechGirls, тощо - все що доступно для школярів з України.
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

BASE_URL = "https://exchanges.state.gov"
CATALOG_URL = f"{BASE_URL}/non-us/programs"
SOURCE_NAME = "US State Department"


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


def is_for_school_age(title, summary):
    """Фільтр: тільки для підлітків і молоді."""
    text = (title + " " + summary).lower()
    # Явно для школярів/підлітків
    teen_keywords = [
        "high school", "secondary school", "youth", "flex",
        "future leaders", "kennedy-lugar yes", "techgirls",
        "benjamin franklin", "study of u.s. institutes",
        "ages 15", "ages 16", "ages 17", "15-17", "15-18",
        "teen", "adolescent",
    ]
    # Явно для дорослих
    adult_keywords = [
        "phd", "doctoral", "postdoc", "professionals", "mid-career",
        "faculty", "researchers", "diplomats", "officials",
    ]

    has_teen = any(k in text for k in teen_keywords)
    has_adult = any(k in text for k in adult_keywords)

    if has_adult and not has_teen:
        return False
    return has_teen


def detect_type(title, summary):
    text = (title + " " + summary).lower()
    if "exchange" in text or "study abroad" in text or "flex" in text or "yes" in text:
        return "exchange"
    if "fellowship" in text or "scholar" in text:
        return "scholarship"
    if "institute" in text or "summer" in text:
        return "camp"
    if "workshop" in text or "training" in text:
        return "course"
    return "exchange"


def is_ukraine_eligible(title, summary):
    """Спеціально шукає програми для України/Східної Європи."""
    text = (title + " " + summary).lower()
    if "ukraine" in text or "ukrainian" in text:
        return True
    # Програми що явно беруть всіх країн
    if any(m in text for m in ["worldwide", "global", "international", "europe and eurasia"]):
        return True
    # За замовчуванням - бере якщо не виключений
    not_eligible = ["africa only", "asia only", "latin america only", "middle east only"]
    return not any(m in text for m in not_eligible)


def parse_list_page(url):
    print(f"  📄 {url}")
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # State Department використовує різну розмітку
    selectors = [
        "div.program-card",
        "article.program",
        "div.view-content article",
        "li.program-item",
        "div.card",
    ]

    articles = []
    for sel in selectors:
        articles = soup.select(sel)
        if articles:
            break

    # Fallback - всі посилання на /non-us/program/
    if not articles:
        articles = soup.find_all("a", href=re.compile(r"/non-us/program/"))

    for article in articles:
        if article.name == "a":
            title = clean_text(article.get_text(strip=True), 200)
            href = article.get("href", "")
            link_el = article
            excerpt = ""
        else:
            title_el = article.find(["h2", "h3", "h4"])
            if not title_el:
                continue
            link_el = title_el.find("a") or article.find("a", href=True)
            if not link_el:
                continue
            title = clean_text(title_el.get_text(strip=True), 200)
            href = link_el.get("href", "")

            excerpt_el = article.find("p") or article.find(class_=re.compile(r"desc|summary"))
            excerpt = clean_text(excerpt_el.get_text(strip=True), 400) if excerpt_el else ""

        if not href or len(title) < 10:
            continue

        full_url = urljoin(BASE_URL, href)

        items.append({
            "title": title,
            "url": full_url,
            "excerpt": excerpt if article.name != "a" else "",
        })

    return items


def parse_detail(url):
    html = fetch_page(url)
    if not html:
        return None, None, ""

    soup = BeautifulSoup(html, "html.parser")

    content_el = (
        soup.find("div", class_="field--name-body") or
        soup.find("article") or
        soup.find("main") or
        soup.find("div", class_=re.compile(r"content|main"))
    )

    summary = ""
    if content_el:
        first_p = content_el.find("p")
        if first_p:
            summary = clean_text(first_p.get_text(strip=True), 400)

    full_text = content_el.get_text(" ", strip=True) if content_el else ""
    deadline = extract_deadline(full_text)
    return summary, deadline, full_text


def scrape_state_department():
    print(f"🔍 Скрапінг {BASE_URL}")

    items = parse_list_page(CATALOG_URL)

    # Дедуплікація
    seen = set()
    unique = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        unique.append(it)

    print(f"🔗 Знайдено {len(unique)} унікальних записів")

    opportunities = []
    for i, item in enumerate(unique[:30], 1):
        print(f"  [{i}/{len(unique)}] {item['title'][:60]}")

        summary, deadline, full_text = parse_detail(item["url"])
        time.sleep(1.2)

        check_text = item["title"] + " " + (summary or "") + " " + full_text[:500]

        if not is_for_school_age(item["title"], check_text):
            print(f"     ⏩ Пропущено (не для школярів)")
            continue

        if not is_ukraine_eligible(item["title"], check_text):
            print(f"     ⏩ Пропущено (не для України)")
            continue

        opp_type = detect_type(item["title"], item["excerpt"] + " " + (summary or ""))

        record = {
            "title": item["title"][:200],
            "slug": make_slug(item["title"], SOURCE_NAME),
            "summary": (summary or item["excerpt"] or
                       "Офіційна програма обмінів Державного Департаменту США для школярів.")[:400],
            "age_from": 15,
            "age_to": 17,
            "opportunity_type": opp_type,
            "categories": "{leadership,languages,education}",
            "child_needs": "{}",
            "format": "За кордоном США",
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
    opps = scrape_state_department()

    if not opps:
        print("\n⚠️  Нічого не зібрано")
    else:
        print(f"\n📤 Завантажую {len(opps)} записів у Supabase...")
        client.upsert_opportunities(opps)
