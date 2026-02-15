from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp
import aiosqlite

from bot.db.database import DB_PATH

log = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
CACHE_TTL = 300  # 5 minutes — prices change faster than sports odds


class KalshiAPI:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Caching layer (reuses games_cache table) ──────────────────────

    async def _cached_request(self, url: str, params: dict, ttl: int | None = None) -> list | dict | None:
        cache_key = f"kalshi:{url}:{json.dumps(params, sort_keys=True)}"
        effective_ttl = ttl if ttl is not None else CACHE_TTL

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                fetched_at = row[1]
                try:
                    from datetime import datetime, timezone
                    fetched_dt = datetime.fromisoformat(fetched_at)
                    if fetched_dt.tzinfo is None:
                        fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
                    if age < effective_ttl:
                        return json.loads(row[0])
                except (ValueError, TypeError):
                    pass

        # Cache miss or stale — fetch from API
        session = await self._get_session()
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    log.warning("Kalshi API %s returned %s", url, resp.status)
                    # Try stale cache as fallback
                    if row:
                        return json.loads(row[0])
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Kalshi API request failed: %s", e)
            if row:
                return json.loads(row[0])
            return None

        # Store in cache
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO games_cache (game_id, sport, data, fetched_at)"
                " VALUES (?, 'kalshi', ?, datetime('now'))",
                (cache_key, json.dumps(data)),
            )
            await db.commit()

        return data

    # ── Public API methods ────────────────────────────────────────────

    async def get_events(self, status: str = "open", limit: int = 100, cursor: str | None = None) -> dict | None:
        """Fetch events list. Returns raw API response with 'events' and 'cursor' keys."""
        params: dict = {"status": status, "limit": limit, "with_nested_markets": "true"}
        if cursor:
            params["cursor"] = cursor
        return await self._cached_request(f"{BASE_URL}/events", params)

    async def get_event(self, event_ticker: str) -> dict | None:
        """Fetch a single event with its markets."""
        data = await self._cached_request(
            f"{BASE_URL}/events/{event_ticker}",
            {},
        )
        if data and "event" in data:
            return data["event"]
        return data

    async def get_markets(self, event_ticker: str | None = None, status: str = "open", limit: int = 100) -> dict | None:
        """Fetch markets list."""
        params: dict = {"status": status, "limit": limit}
        if event_ticker:
            params["event_ticker"] = event_ticker
        return await self._cached_request(f"{BASE_URL}/markets", params)

    async def get_market(self, ticker: str) -> dict | None:
        """Fetch a single market by ticker (for resolution checking). Uses short TTL."""
        data = await self._cached_request(
            f"{BASE_URL}/markets/{ticker}",
            {},
            ttl=60,  # 1 minute for resolution checks
        )
        if data and "market" in data:
            return data["market"]
        return data

    async def get_categories(self) -> list[str]:
        """Get unique categories from open events."""
        data = await self.get_events(limit=200)
        if not data or "events" not in data:
            return []
        categories: dict[str, bool] = {}
        for event in data["events"]:
            cat = event.get("category", "")
            if cat and cat not in categories:
                categories[cat] = True
        return list(categories.keys())

    async def get_events_by_category(self, category: str) -> list[dict]:
        """Get open events filtered by category."""
        data = await self.get_events(limit=200)
        if not data or "events" not in data:
            return []
        return [e for e in data["events"] if e.get("category") == category]


# Singleton instance
kalshi_api = KalshiAPI()
