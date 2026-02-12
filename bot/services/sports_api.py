from __future__ import annotations

import json
import time

import aiohttp
import aiosqlite

from bot.config import API_SPORTS_KEY
from bot.db.database import DB_PATH

BASE_URL = "https://v3.football.api-sports.io"
CACHE_TTL = 900  # 15 minutes


class SportsAPI:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"x-apisports-key": API_SPORTS_KEY}
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _cached_request(self, endpoint: str, params: dict) -> dict | None:
        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                fetched_at = row[1]
                # Simple TTL check (fetched_at is an ISO timestamp string)
                import datetime
                fetched = datetime.datetime.fromisoformat(fetched_at)
                age = (datetime.datetime.now() - fetched).total_seconds()
                if age < CACHE_TTL:
                    return json.loads(row[0])

        session = await self._get_session()
        async with session.get(f"{BASE_URL}/{endpoint}", params=params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO games_cache (game_id, sport, data) VALUES (?, ?, ?)",
                (cache_key, "football", json.dumps(data)),
            )
            await db.commit()

        return data

    async def get_upcoming_games(self, league_id: int = 39, season: int = 2025) -> list[dict]:
        data = await self._cached_request(
            "fixtures", {"league": league_id, "season": season, "next": 10}
        )
        if not data:
            return []
        return data.get("response", [])

    async def get_game_result(self, game_id: int) -> dict | None:
        data = await self._cached_request("fixtures", {"id": game_id})
        if not data:
            return None
        results = data.get("response", [])
        return results[0] if results else None

    async def get_odds(self, game_id: int) -> list[dict]:
        data = await self._cached_request("odds", {"fixture": game_id})
        if not data:
            return []
        return data.get("response", [])
