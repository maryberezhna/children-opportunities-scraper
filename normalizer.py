"""AI-нормалізація через Claude Haiku."""
import os
import hashlib
import re
import logging
from typing import Optional
import anthropic
from slugify import slugify

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """Ти аналізуєш тексти про можливості для ДІТЕЙ 0-18 років в Україні.

ТВОЇ ЗАВДАННЯ:
1. Визначити чи це можливість для дитини 0-18
2. Витягнути ТОЧНИЙ вік (age_from, age_to) з тексту
3. Класифікувати opportunity_type
4. Визначити cost_type
5. Витягнути child_needs якщо є (ВПО, сироти, інвалідність тощо)

ПРАВИЛА:
- "для школярів" → age_from=7, age_to=17
- "старшокласники" → age_from=15, age_to=17
- "8-10 класи" → age_from=14, age_to=16
- "дошкільнята" → age_from=3, age_to=6
- Якщо для дорослих/студентів університету → confidence=0.1

Типи opportunity_type:
course, olympiad, competition, club, exchange, camp, scholarship,
allowance, grant, festival, medical_aid, psychology, rehabilitation,
humanitarian, internship, volunteer

Типи cost_type:
free, partially_free, paid_affordable, paid_premium, subsidized

child_needs (тільки якщо прямо вказано):
gifted, disability, orphan, idp, veteran_family, oncology, low_income

Поверни JSON через extract_opportunity."""


EXTRACT_TOOL = {
    "name": "extract_opportunity",
    "description": "Витягує дані про можливість для дитини",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "summary": {"type": "string", "maxLength": 400},
            "age_from": {"type": "integer", "minimum": 0, "maximum": 18},
            "age_to": {"type": "integer", "minimum": 0, "maximum": 18},
            "opportunity_type": {"type": "string"},
            "categories": {"type": "array", "items": {"type": "string"}},
            "child_needs": {"type": "array", "items": {"type": "string"}},
            "format": {"type": "string"},
            "cost_type": {"type": "string"},
            "deadline": {"type": ["string", "null"]},
            "confidence": {"type": "number"},
        },
        "required": ["title", "summary", "age_from", "age_to",
                     "opportunity_type", "cost_type", "confidence"],
    },
}


class Normalizer:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        self.model = "claude-haiku-4-5-20251001"

    def normalize(self, raw_text: str, source: str, source_url: str,
                  raw_title: Optional[str] = None) -> Optional[dict]:
        try:
            user_msg = f"""Джерело: {source}
URL: {source_url}
Заголовок: {raw_title or '(немає)'}

ТЕКСТ:
{raw_text[:6000]}

Витягни дані через extract_opportunity."""

            response = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                system=SYSTEM_PROMPT,
                tools=[EXTRACT_TOOL],
                tool_choice={"type": "tool", "name": "extract_opportunity"},
                messages=[{"role": "user", "content": user_msg}],
            )

            tool_use = next(
                (b for b in response.content
                 if b.type == "tool_use" and b.name == "extract_opportunity"),
                None
            )
            if not tool_use:
                return None

            data = tool_use.input
            if data.get("confidence", 0) < 0.5:
                logger.info(f"Low confidence, skipping: {source_url}")
                return None

            title = data["title"]
            data["slug"] = self._make_slug(title, source)
            data["content_hash"] = self._make_hash(title, source_url)
            data["source"] = source
            data["source_url"] = source_url
            data.pop("confidence", None)

            return data

        except Exception as e:
            logger.error(f"Normalize failed for {source_url}: {e}")
            return None

    @staticmethod
    def _make_slug(title: str, source: str) -> str:
        base = slugify(title, max_length=80, word_boundary=True)
        short_hash = hashlib.md5(f"{title}{source}".encode()).hexdigest()[:6]
        return f"{base}-{short_hash}"

    @staticmethod
    def _make_hash(title: str, url: str) -> str:
        normalized = re.sub(r"[^\w\s]", "", title.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return hashlib.sha256(f"{normalized}|{url}".encode()).hexdigest()[:16]
