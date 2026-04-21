"""
Скрапер EU Youth Portal — молодіжний портал Європейського Союзу.
Джерело: https://youth.europa.eu/
Витягує: DiscoverEU, European Solidarity Corps, Erasmus+ для школярів, молодіжні обміни.
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

BASE_URL = "https://youth.europa.eu"
SOURCE_NAME = "EU Youth Portal"

# Категорії молодіжних можливостей
CATEGORY_URLS = [
    f"{BASE_URL}/go-abroad_en",
    f"{BASE_URL}/volunteering_en",
    f"{BASE_URL}/learning-training_en",
    f"{BASE_URL}/participate_en",
]


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
    if "volunteer" in text or "solidarity" in text:
        return "volunteer"
    if "exchange" in text or "mobility" in text or "abroad" in text:
        return "exchange"
    if "internship" in text or "traineeship" in text:
        return "internship"
    if "scholarship" in text:
        return "scholarship"
    if "discovereu" in text or "discover eu" in text:
        return "exchange"
    if "training" in text or "course" in text:
        return "course"
    if "grant" in text or "funding" in text:
        return "grant"
    return "exchange"


def is_youth_eligible(title, summary):
    """ЄС Youth Portal — все для молоді 13-30. Фільтруємо для 14-17."""
    text = (title + " " + summary).lower()
    # Тільки для молоді старше 18 — виключаємо
    exclude_18_plus_only = [
        "only 18+", "18 and over", "must be 18", "must be of age",
        "adults only", "university students only"
    ]
    if any(m in text for m in exclude_18_plus_only):
        return False
    return True


def parse_list_page(url):
    print(f"  📄 {url}")
    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # EU Youth Portal (drupal) структура
    selectors = [
        "article.node",
        "div.views-row",
        "div.card",
        "li.list-item",
    ]

    articles = []
    for sel in selectors:
        articles = soup.select(sel)
        if articles:
            break

    # Fallback: шукаємо посилання всередині контенту
    if not articles:
        main = soup.find("main") or soup.find("div", class_=re.compile(r"content|main"))
        if main:
            for link in main.find_all("a", href=True):
                href = link.get("href", "")
                text = clean_text(link.get_text(strip=True), 200)
                # Тільки посилання зі змістовними заголовками
                if len(text) > 20 and not text.startswith("http"):
                    articles.append(link)

    for article in articles[:15]:
        if article.name == "a":
            title = clean_text(article.get_text(strip=True), 200)
            href = article.get("href", "")
            excerpt = ""
        else:
            title_el = article.find(["h2", "h3", "h4", "a"])
            if not title_el:
                continue
            link_el = title_el.find("a") if title_el.name != "a" else title_el
            if not link_el:
                link_el = article.find("a", href=True)
            if not link_el:
                continue
            title = clean_text(title_el.get_text(strip=True), 200)
            href = link_el.get("href", "")

            excerpt_el = article.find("p") or article.find(class_=re.compile(r"desc|summary|teaser"))
            excerpt = clean_text(excerpt_el.get_text(strip=True), 400) if excerpt_el else ""

        if not href or len(title) < 15:
            continue
        # Відкидаємо навігаційні посилання (контакти, про, тощо)
        if any(s in href.lower() for s in ["contact", "about", "privacy", "legal", "cookie"]):
            continue

        full_url = urljoin(BASE_URL, href)

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
        soup.find("main") or
        soup.find("article") or
        soup.find("div", class_=re.compile(r"content|main-content|node"))
    )

    summary = ""
    if content_el:
        first_p = content_el.find("p")
        if first_p:
            summary = clean_text(first_p.get_text(strip=True), 400)

    full_text = content_el.get_text(" ", strip=True) if content_el else ""
    deadline = extract_deadline(full_text)
    return summary, deadline


def scrape_eu_youth():
    print(f"🔍 Скрапінг {BASE_URL}")

    all_items = []
    for cat_url in CATEGORY_URLS:
        items = parse_list_page(cat_url)
        all_items.extend(items)
        time.sleep(1.5)

    seen = set()
    unique = []
    for it in all_items:
        if it["url"] in seen:
            continue
        # Відсікаємо те що не на youth.europa.eu
        if BASE_URL not in it["url"] and not it["url"].startswith("/"):
            continue
        seen.add(it["url"])
        unique.append(it)

    print(f"🔗 Знайдено {len(unique)} унікальних записів")

    opportunities = []
    for i, item in enumerate(unique[:25], 1):
        if not is_youth_eligible(item["title"], item["excerpt"]):
            continue

        print(f"  [{i}/{len(unique)}] {item['title'][:60]}")
        summary, deadline = parse_detail(item["url"])
        time.sleep(1.2)

        opp_type = detect_type(item["title"], item["excerpt"] + " " + (summary or ""))

        record = {
            "title": item["title"][:200],
            "slug": make_slug(item["title"], SOURCE_NAME),
            "summary": (summary or item["excerpt"] or
                       "Молодіжна можливість від Європейського Союзу. Детальніше за посиланням.")[:400],
            "age_from": 14,
            "age_to": 17,
            "opportunity_type": opp_type,
            "categories": "{leadership,languages,education}",
            "child_needs": "{}",
            "format": "За кордоном ЄС + онлайн",
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
    opps = scrape_eu_youth()

    if not opps:
        print("\n⚠️  Нічого не зібрано")
    else:
        print(f"\n📤 Завантажую {len(opps)} записів у Supabase...")
        client.upsert_opportunities(opps)
