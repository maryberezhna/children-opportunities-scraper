"""Підключення до Supabase."""
import os
import logging
from typing import Optional
from supabase import create_client, Client

logger = logging.getLogger(__name__)


def get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def upsert_opportunity(client: Client, data: dict) -> Optional[dict]:
    try:
        result = client.table("opportunities").upsert(
            data,
            on_conflict="content_hash"
        ).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(f"Supabase upsert failed: {e}")
        return None


def archive_expired(client: Client) -> int:
    from datetime import date
    today = date.today().isoformat()
    result = client.table("opportunities") \
        .update({"cost_type": "closed"}) \
        .lt("deadline", today) \
        .neq("cost_type", "closed") \
        .execute()
    return len(result.data) if result.data else 0
