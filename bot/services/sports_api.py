from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import aiohttp
import aiosqlite

from bot.config import ODDS_API_KEY
from bot.db.database import DB_PATH

BASE_URL = "https://api.the-odds-api.com/v4"
CACHE_TTL = 900  # 15 minutes
DEFAULT_SPORT = "upcoming"  # special key: returns all in-season sports


class SportsAPI:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Caching layer ────────────────────────────────────────────────────

    async def _cached_request(self, url: str, params: dict) -> list | dict | None:
        cache_key = f"{url}:{json.dumps(params, sort_keys=True)}"

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                fetched_at = row[1]
                fetched = datetime.fromisoformat(fetched_at)
                now = datetime.now()
                if (now - fetched).total_seconds() < CACHE_TTL:
                    return json.loads(row[0])

        session = await self._get_session()
        full_params = {**params, "apiKey": ODDS_API_KEY}
        async with session.get(url, params=full_params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO games_cache (game_id, sport, data) VALUES (?, ?, ?)",
                (cache_key, "odds_api", json.dumps(data)),
            )
            await db.commit()

        return data

    # ── Public methods ───────────────────────────────────────────────────

    async def get_sports(self) -> list[dict]:
        """Return list of in-season sports."""
        data = await self._cached_request(f"{BASE_URL}/sports", {})
        return data if isinstance(data, list) else []

    async def get_upcoming_games(self, sport: str = DEFAULT_SPORT) -> list[dict]:
        """Fetch upcoming games with h2h odds.

        Each item has: id, sport_key, commence_time, home_team, away_team, bookmakers.
        """
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {"regions": "us", "markets": "h2h", "oddsFormat": "american"},
        )
        if not isinstance(data, list):
            return []

        # Filter to games that haven't started yet
        now = datetime.now(timezone.utc)
        upcoming = []
        for game in data:
            commence = game.get("commence_time", "")
            try:
                ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                if ct > now:
                    upcoming.append(game)
            except (ValueError, TypeError):
                upcoming.append(game)

        upcoming.sort(key=lambda g: g.get("commence_time", ""))
        return upcoming

    async def get_game_by_id(self, event_id: str, sport: str | None = None) -> dict | None:
        """Fetch a single game by event ID. Tries scores endpoint first."""
        if sport:
            sports_to_check = [sport]
        else:
            # Check scores across all sports — use upcoming to find it
            sports_to_check = await self._get_sport_keys_for_event(event_id)
            if not sports_to_check:
                return None

        for sk in sports_to_check:
            data = await self._cached_request(
                f"{BASE_URL}/sports/{sk}/scores",
                {"eventIds": event_id, "daysFrom": 3},
            )
            if isinstance(data, list) and data:
                return data[0]
        return None

    async def get_game_odds(self, event_id: str, sport: str) -> dict[str, float] | None:
        """Fetch h2h odds for a specific event. Returns {home: x, away: y, draw: z}."""
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american",
                "eventIds": event_id,
            },
        )
        if not isinstance(data, list) or not data:
            return None

        game = data[0]
        return self.parse_odds(game)

    async def get_scores(self, sport: str, days_from: int = 3) -> list[dict]:
        """Get completed/live scores for a sport."""
        params = {}
        if days_from:
            params["daysFrom"] = days_from
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/scores", params
        )
        return data if isinstance(data, list) else []

    async def get_fixture_status(self, event_id: str, sport: str | None = None) -> dict | None:
        """Return status info for a game: {started, completed, home_team, away_team, home_score, away_score}."""
        game = await self.get_game_by_id(event_id, sport)
        if not game:
            return None

        now = datetime.now(timezone.utc)
        commence = game.get("commence_time", "")
        try:
            ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            started = ct <= now
        except (ValueError, TypeError):
            started = False

        completed = game.get("completed", False)

        scores = game.get("scores")
        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")
        home_score = None
        away_score = None

        if scores:
            for s in scores:
                if s.get("name") == home_team:
                    home_score = int(s["score"]) if s.get("score") is not None else None
                elif s.get("name") == away_team:
                    away_score = int(s["score"]) if s.get("score") is not None else None

        return {
            "started": started,
            "completed": completed,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
        }

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def american_to_decimal(american: int | float) -> float:
        """Convert American odds to decimal odds for payout calculation."""
        american = float(american)
        if american >= 0:
            return (american / 100) + 1
        else:
            return (100 / abs(american)) + 1

    @staticmethod
    def format_american(odds: int | float) -> str:
        """Format American odds with +/- sign."""
        odds = int(odds)
        return f"+{odds}" if odds > 0 else str(odds)

    def parse_odds(self, game: dict) -> dict[str, dict] | None:
        """Parse a game dict into {home: {american, decimal}, away: {...}, draw: {...}}."""
        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            return None

        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")

        # Use first bookmaker
        for bookmaker in bookmakers:
            for market in bookmaker.get("markets", []):
                if market.get("key") != "h2h":
                    continue
                result = {}
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name", "")
                    price = outcome.get("price")
                    if price is None:
                        continue
                    entry = {
                        "american": int(price),
                        "decimal": self.american_to_decimal(price),
                    }
                    if name == home_team:
                        result["home"] = entry
                    elif name == away_team:
                        result["away"] = entry
                    elif name.lower() == "draw":
                        result["draw"] = entry
                if result:
                    return result
        return None

    async def _get_sport_keys_for_event(self, event_id: str) -> list[str]:
        """Try to find which sport an event belongs to by checking cached data."""
        # Check if we have it cached in any sport's scores
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data FROM games_cache WHERE sport = 'odds_api'"
            )
            rows = await cursor.fetchall()
            for row in rows:
                try:
                    cached = json.loads(row[0])
                    if isinstance(cached, list):
                        for game in cached:
                            if game.get("id") == event_id:
                                return [game.get("sport_key")]
                except (json.JSONDecodeError, TypeError):
                    continue

        # Fallback: get all active sports and return them
        sports = await self.get_sports()
        return [s["key"] for s in sports if s.get("active")]
