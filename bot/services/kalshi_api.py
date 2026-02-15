from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiohttp
import aiosqlite

from bot.db.database import DB_PATH

log = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
CACHE_TTL = 300  # 5 minutes

# ── Sports series tickers ─────────────────────────────────────────────
# Organized by sport → bet type. Each entry maps to a Kalshi series_ticker
# that returns individual game markets via GET /markets?series_ticker=X

SPORTS = {
    "NBA": {
        "label": "NBA Basketball",
        "series": {
            "Game": "KXNBAGAME",
            "Spread": "KXNBASPREAD",
            "Total Points": "KXNBATOTAL",
        },
    },
    "NFL": {
        "label": "NFL Football",
        "series": {
            "Game": "KXNFLGAME",
            "Spread": "KXNFLSPREAD",
            "Total Points": "KXNFLTOTAL",
        },
    },
    "MLB": {
        "label": "MLB Baseball",
        "series": {
            "Game": "KXMLBGAME",
            "Spread": "KXMLBSPREAD",
            "Total Runs": "KXMLBTOTAL",
        },
    },
    "NHL": {
        "label": "NHL Hockey",
        "series": {
            "Game": "KXNHLGAME",
            "Spread": "KXNHLSPREAD",
            "Total Goals": "KXNHLTOTAL",
        },
    },
    "NCAAMB": {
        "label": "College Basketball (M)",
        "series": {
            "Game": "KXNCAAMBGAME",
            "Spread": "KXNCAAMBSPREAD",
            "Total Points": "KXNCAAMBTOTAL",
        },
    },
    "NCAAWB": {
        "label": "College Basketball (W)",
        "series": {
            "Game": "KXNCAAWBGAME",
            "Spread": "KXNCAAWBSPREAD",
            "Total Points": "KXNCAAWBTOTAL",
        },
    },
    "NCAAF": {
        "label": "College Football",
        "series": {
            "Game": "KXNCAAFGAME",
            "Spread": "KXNCAAFSPREAD",
            "Total Points": "KXNCAAFTOTAL",
        },
    },
    "EPL": {
        "label": "English Premier League",
        "series": {
            "Game": "KXEPLGAME",
            "Spread": "KXEPLSPREAD",
            "Total Goals": "KXEPLTOTAL",
        },
    },
    "La Liga": {
        "label": "La Liga",
        "series": {
            "Game": "KXLALIGAGAME",
            "Spread": "KXLALIGASPREAD",
            "Total Goals": "KXLALIGATOTAL",
        },
    },
    "Bundesliga": {
        "label": "Bundesliga",
        "series": {
            "Game": "KXBUNDESLIGAGAME",
            "Spread": "KXBUNDESLIGASPREAD",
            "Total Goals": "KXBUNDESLIGATOTAL",
        },
    },
    "Serie A": {
        "label": "Serie A",
        "series": {
            "Game": "KXSERIEAGAME",
            "Spread": "KXSERIEASPREAD",
            "Total Goals": "KXSERIEATOTAL",
        },
    },
    "Ligue 1": {
        "label": "Ligue 1",
        "series": {
            "Game": "KXLIGUE1GAME",
            "Spread": "KXLIGUE1SPREAD",
            "Total Goals": "KXLIGUE1TOTAL",
        },
    },
    "UCL": {
        "label": "Champions League",
        "series": {
            "Game": "KXUCLGAME",
            "Spread": "KXUCLSPREAD",
            "Total Goals": "KXUCLTOTAL",
        },
    },
    "MLS": {
        "label": "MLS",
        "series": {
            "Game": "KXMLSGAME",
            "Spread": "KXMLSSPREAD",
            "Total Goals": "KXMLSTOTAL",
        },
    },
    "UFC": {
        "label": "UFC / MMA",
        "series": {
            "Fight Winner": "KXUFCFIGHT",
        },
    },
}


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

        stale_data = None
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                fetched_at = row[1]
                try:
                    fetched_dt = datetime.fromisoformat(fetched_at)
                    if fetched_dt.tzinfo is None:
                        fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
                    if age < effective_ttl:
                        return json.loads(row[0])
                except (ValueError, TypeError):
                    pass
                stale_data = row[0]

        # Cache miss or stale — fetch from API
        session = await self._get_session()
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    log.warning("Kalshi API %s returned %s", url, resp.status)
                    if stale_data:
                        return json.loads(stale_data)
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Kalshi API request failed: %s", e)
            if stale_data:
                return json.loads(stale_data)
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

    async def get_markets_by_series(self, series_ticker: str, status: str = "open", limit: int = 100) -> list[dict]:
        """Fetch open markets for a given series ticker."""
        data = await self._cached_request(
            f"{BASE_URL}/markets",
            {"series_ticker": series_ticker, "status": status, "limit": limit},
        )
        if not data or "markets" not in data:
            return []
        return data["markets"]

    async def get_market(self, ticker: str) -> dict | None:
        """Fetch a single market by ticker (for resolution checking). Uses short TTL."""
        data = await self._cached_request(
            f"{BASE_URL}/markets/{ticker}",
            {},
            ttl=60,
        )
        if data and "market" in data:
            return data["market"]
        return data

    async def get_sport_markets(self, sport_key: str, bet_type: str) -> list[dict]:
        """Get open markets for a sport + bet type combo.

        sport_key: key from SPORTS dict (e.g. "NBA")
        bet_type: key from series dict (e.g. "Game", "Spread", "Total Points")
        """
        sport = SPORTS.get(sport_key)
        if not sport:
            return []
        series_ticker = sport["series"].get(bet_type)
        if not series_ticker:
            return []
        return await self.get_markets_by_series(series_ticker)

    async def get_available_sports(self) -> list[str]:
        """Return sport keys that currently have open markets.

        Checks the first bet type (usually 'Game') for each sport.
        """
        available = []
        # Check all sports concurrently
        tasks = {}
        for key, sport in SPORTS.items():
            first_series = next(iter(sport["series"].values()))
            tasks[key] = self.get_markets_by_series(first_series, limit=1)

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for key, result in zip(tasks.keys(), results):
            if isinstance(result, list) and len(result) > 0:
                available.append(key)

        return available


# Singleton instance
kalshi_api = KalshiAPI()
