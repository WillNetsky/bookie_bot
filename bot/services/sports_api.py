from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

import aiohttp
import aiosqlite

from bot.config import ODDS_API_KEY
from bot.db.database import DB_PATH, get_connection

BASE_URL = "https://api.the-odds-api.com/v4"
CACHE_TTL = 7200  # 2 hours — odds don't change fast enough to matter
SCORES_CACHE_TTL = 1800  # 30 minutes — matches check_results loop interval
DEFAULT_SPORT = "upcoming"  # special key: returns all in-season sports


class SportsAPI:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._last_request_time: float = 0.0  # monotonic timestamp of last API call
        self._request_lock: asyncio.Lock = asyncio.Lock()
        # Quota tracking from API response headers
        self.requests_used: int | None = None
        self.requests_remaining: int | None = None
        self.requests_last: str | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _update_quota(self, resp: aiohttp.ClientResponse) -> None:
        """Read quota headers from an API response."""
        used = resp.headers.get("x-requests-used")
        remaining = resp.headers.get("x-requests-remaining")
        last = resp.headers.get("x-requests-last")
        if used is not None:
            self.requests_used = int(used)
        if remaining is not None:
            self.requests_remaining = int(remaining)
        if last is not None:
            self.requests_last = last
        log.info(
            "API quota — used: %s, remaining: %s, this_call: %s",
            self.requests_used, self.requests_remaining, last,
        )

    def get_quota(self) -> dict:
        """Return current quota info."""
        return {
            "used": self.requests_used,
            "remaining": self.requests_remaining,
            "last": self.requests_last,
        }

    # ── Pruning helpers ──────────────────────────────────────────────────

    def _prune_events(self, data: list) -> list:
        if not isinstance(data, list): return data
        return [
            {
                "id": e.get("id"),
                "sport_key": e.get("sport_key"),
                "sport_title": e.get("sport_title"),
                "commence_time": e.get("commence_time"),
                "home_team": e.get("home_team"),
                "away_team": e.get("away_team"),
            }
            for e in data
        ]

    def _prune_odds(self, data: list) -> list:
        if not isinstance(data, list): return data
        pruned = []
        for e in data:
            item = {
                "id": e.get("id"),
                "sport_key": e.get("sport_key"),
                "sport_title": e.get("sport_title"),
                "commence_time": e.get("commence_time"),
                "home_team": e.get("home_team"),
                "away_team": e.get("away_team"),
                "bookmakers": []
            }
            # Only keep the first bookmaker's markets to save space
            if e.get("bookmakers"):
                bm = e["bookmakers"][0]
                item["bookmakers"].append({
                    "key": bm.get("key"),
                    "title": bm.get("title"),
                    "markets": bm.get("markets", [])
                })
            pruned.append(item)
        return pruned

    def _prune_scores(self, data: list) -> list:
        if not isinstance(data, list): return data
        return [
            {
                "id": e.get("id"),
                "sport_key": e.get("sport_key"),
                "commence_time": e.get("commence_time"),
                "completed": e.get("completed"),
                "home_team": e.get("home_team"),
                "away_team": e.get("away_team"),
                "scores": e.get("scores"),
            }
            for e in data
        ]

    # ── Caching layer ────────────────────────────────────────────────────

    async def _cached_request(
        self, url: str, params: dict, ttl: int | None = None, prune_func: callable | None = None
    ) -> list | dict | None:
        cache_key = f"{url}:{json.dumps(params, sort_keys=True)}"
        effective_ttl = ttl if ttl is not None else CACHE_TTL

        stale_data = None
        db = await get_connection()
        try:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                fetched_at = row[1]
                fetched = datetime.fromisoformat(fetched_at).replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                if (now - fetched).total_seconds() < effective_ttl:
                    data = json.loads(row[0])
                    return prune_func(data) if prune_func else data
                # Keep stale data as fallback in case the API call fails
                stale_data = row[0]
        finally:
            await db.close()

        # Rate limit: at least 1 second between API calls to avoid 429s.
        # Lock ensures concurrent gather() calls serialize properly.
        async with self._request_lock:
            now_mono = time.monotonic()
            elapsed = now_mono - self._last_request_time
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
            self._last_request_time = time.monotonic()

        if not ODDS_API_KEY:
            log.warning("No ODDS_API_KEY configured — cannot fetch fresh data for %s", url)
            if stale_data:
                data = json.loads(stale_data)
                return prune_func(data) if prune_func else data
            return None

        session = await self._get_session()
        full_params = {**params, "apiKey": ODDS_API_KEY}
        log.debug("API call: %s", url)
        try:
            async with session.get(url, params=full_params) as resp:
                self._update_quota(resp)
                if resp.status != 200:
                    log.warning("API returned %s for %s — using stale cache", resp.status, url)
                    if stale_data:
                        data = json.loads(stale_data)
                        return prune_func(data) if prune_func else data
                    return None
                data = await resp.json()
        except aiohttp.ClientError:
            log.warning("API request failed for %s — using stale cache", url)
            if stale_data:
                data = json.loads(stale_data)
                return prune_func(data) if prune_func else data
            return None

        # Store in cache with retries for locked database
        if data:
            pruned_data = prune_func(data) if prune_func else data
            data_str = json.dumps(pruned_data)
            
            for attempt in range(3):
                db = await get_connection()
                try:
                    await db.execute(
                        "INSERT OR REPLACE INTO games_cache (game_id, sport, data) VALUES (?, ?, ?)",
                        (cache_key, "odds_api", data_str),
                    )
                    await db.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    log.warning("Failed to update cache for %s: %s", url, e)
                finally:
                    await db.close()

        return data

    # ── Public methods ───────────────────────────────────────────────────

    async def get_sports(self) -> list[dict]:
        """Return list of in-season sports."""
        data = await self._cached_request(f"{BASE_URL}/sports", {})
        return data if isinstance(data, list) else []

    async def get_events(self, sport: str = DEFAULT_SPORT, hours: int | None = None) -> list[dict]:
        """Fetch upcoming events (FREE — no quota cost)."""
        if sport == "upcoming":
            active_sports = await self.get_sports()
            sport_keys = [
                sp["key"] for sp in active_sports
                if sp.get("key") and sp.get("active")
            ]

            async def _fetch_sport(sk: str) -> list[dict]:
                result = await self._cached_request(
                    f"{BASE_URL}/sports/{sk}/events", {},
                    prune_func=self._prune_events
                )
                return result if isinstance(result, list) else []

            results = await asyncio.gather(*[_fetch_sport(sk) for sk in sport_keys])
            data = [game for batch in results for game in batch]
        else:
            data = await self._cached_request(
                f"{BASE_URL}/sports/{sport}/events", {},
                prune_func=self._prune_events
            )
            if not isinstance(data, list):
                return []

        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=hours) if hours is not None else None
        upcoming = []
        for game in data:
            commence = game.get("commence_time", "")
            try:
                ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                if ct <= now:
                    continue
                if cutoff and ct > cutoff:
                    continue
                upcoming.append(game)
            except (ValueError, TypeError):
                upcoming.append(game)

        upcoming.sort(key=lambda g: g.get("commence_time", ""))
        return upcoming

    async def get_odds_for_sport(self, sport: str) -> list[dict]:
        """Fetch odds for all games in a sport (costs markets × regions = 3 credits)."""
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {"regions": "us", "markets": "h2h,spreads,totals", "oddsFormat": "american"},
            prune_func=self._prune_odds
        )
        return data if isinstance(data, list) else []

    async def get_odds_for_event(self, sport: str, event_id: str) -> dict | None:
        """Fetch odds for a single event. Costs 3 credits."""
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {
                "regions": "us",
                "markets": "h2h,spreads,totals",
                "oddsFormat": "american",
                "eventIds": event_id,
            },
            prune_func=self._prune_odds
        )
        if isinstance(data, list) and data:
            return data[0]
        return None

    async def get_upcoming_games(self, sport: str = DEFAULT_SPORT, hours: int | None = None) -> list[dict]:
        """Fetch upcoming games with odds (DEPRECATED)."""
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {"regions": "us", "markets": "h2h,spreads,totals", "oddsFormat": "american"},
            prune_func=self._prune_odds
        )
        if not isinstance(data, list):
            return []

        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=hours) if hours is not None else None
        upcoming = []
        for game in data:
            commence = game.get("commence_time", "")
            try:
                ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                if ct <= now:
                    continue
                if cutoff and ct > cutoff:
                    continue
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
                {"eventIds": event_id},
                ttl=SCORES_CACHE_TTL,
                prune_func=self._prune_scores
            )
            if isinstance(data, list) and data:
                return data[0]
        return None

    async def get_game_odds(self, event_id: str, sport: str) -> dict[str, float] | None:
        """Fetch odds for a specific event including h2h, spreads, and totals."""
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {
                "regions": "us",
                "markets": "h2h,spreads,totals",
                "oddsFormat": "american",
                "eventIds": event_id,
            },
            prune_func=self._prune_odds
        )
        if not isinstance(data, list) or not data:
            return None

        game = data[0]
        return self.parse_odds(game)

    async def get_scores(self, sport: str, days_from: int | None = None) -> list[dict]:
        """Get completed/live scores for a sport."""
        params = {}
        if days_from:
            params["daysFrom"] = days_from
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/scores", params,
            ttl=SCORES_CACHE_TTL,
            prune_func=self._prune_scores
        )
        return data if isinstance(data, list) else []

    async def get_fixture_status(self, event_id: str, sport: str | None = None) -> dict | None:
        """Return status info for a game: {started, completed, home_team, away_team, home_score, away_score}."""
        game = await self.get_game_by_id(event_id, sport)
        if not game:
            return None
        return self._parse_fixture_status(game)

    def _parse_fixture_status(self, game: dict) -> dict:
        """Extract status info from a game/scores dict."""
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
                    home_score = self._parse_score(s.get("score"))
                elif s.get("name") == away_team:
                    away_score = self._parse_score(s.get("score"))

        return {
            "started": started,
            "completed": completed,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
        }

    async def get_scores_by_sport(self, sport: str, days_from: int = 3) -> dict[str, dict]:
        """Fetch all scores for a sport and return a dict keyed by event ID.

        Each value is the parsed fixture status dict. Uses the scores cache TTL.
        Uses daysFrom to catch games completed between check cycles (costs 2 credits).
        """
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/scores",
            {"daysFrom": days_from},
            ttl=SCORES_CACHE_TTL,
            prune_func=self._prune_scores
        )
        if not isinstance(data, list):
            return {}
        result = {}
        for game in data:
            eid = game.get("id")
            if eid:
                result[eid] = self._parse_fixture_status(game)
        return result

    async def get_cached_scores(self, sport: str) -> list[dict]:
        """Read scores from cache without making API calls. Returns [] if no cached data."""
        cache_key = f"{BASE_URL}/sports/{sport}/scores:{{}}"
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                data = json.loads(row[0])
                return data if isinstance(data, list) else []
        return []

    async def find_outright_in_cache(self, event_id: str) -> tuple[dict, str] | None:
        """Search cached outright/odds data for an event ID without making API calls.

        Returns (event_dict, sport_key) if found, else None.
        """
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT game_id, data FROM games_cache WHERE sport = 'odds_api' AND game_id LIKE '%/odds:%'"
            )
            rows = await cursor.fetchall()
            for row in rows:
                try:
                    cached = json.loads(row[1])
                    if not isinstance(cached, list):
                        continue
                    for ev in cached:
                        if ev.get("id") == event_id:
                            sport_key = ev.get("sport_key", "")
                            return ev, sport_key
                except (json.JSONDecodeError, TypeError):
                    continue
        return None

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _parse_score(raw: str | None) -> int | None:
        """Parse a score string into an integer.

        Handles cricket-style scores like '234/5' by taking the runs portion.
        """
        if raw is None:
            return None
        try:
            # Cricket scores: "234/5" → 234 (runs before slash)
            return int(raw.split("/")[0])
        except (ValueError, IndexError):
            return None

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
        """Parse a game dict into odds for h2h, spreads, and totals.

        Returns keys: home, away, draw (h2h), spread_home, spread_away,
        over, under — each with american, decimal, and point (for spreads/totals).
        """
        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            return None

        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")

        result = {}

        # Use first bookmaker that has markets
        for bookmaker in bookmakers:
            for market in bookmaker.get("markets", []):
                market_key = market.get("key")

                if market_key == "h2h" and "home" not in result:
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

                elif market_key == "spreads" and "spread_home" not in result:
                    for outcome in market.get("outcomes", []):
                        name = outcome.get("name", "")
                        price = outcome.get("price")
                        point = outcome.get("point")
                        if price is None or point is None:
                            continue
                        entry = {
                            "american": int(price),
                            "decimal": self.american_to_decimal(price),
                            "point": float(point),
                        }
                        if name == home_team:
                            result["spread_home"] = entry
                        elif name == away_team:
                            result["spread_away"] = entry

                elif market_key == "totals" and "over" not in result:
                    for outcome in market.get("outcomes", []):
                        name = outcome.get("name", "")
                        price = outcome.get("price")
                        point = outcome.get("point")
                        if price is None or point is None:
                            continue
                        entry = {
                            "american": int(price),
                            "decimal": self.american_to_decimal(price),
                            "point": float(point),
                        }
                        if name.lower() == "over":
                            result["over"] = entry
                        elif name.lower() == "under":
                            result["under"] = entry

            if result:
                break

        return result if result else None

    async def get_outrights(self, sport: str) -> list[dict]:
        """Fetch outright (futures) odds for a sport.

        Returns the raw list of outright events, each with bookmakers → markets → outcomes.
        """
        data = await self._cached_request(
            f"{BASE_URL}/sports/{sport}/odds",
            {"regions": "us", "markets": "outrights", "oddsFormat": "american"},
        )
        if not isinstance(data, list):
            return []
        return data

    def parse_outright_odds(self, game: dict) -> list[dict] | None:
        """Parse an outright event into a sorted list of {name, american, decimal}.

        Returns outcomes sorted by decimal odds (favorites first), or None if no data.
        """
        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            return None

        outcomes = []
        for bookmaker in bookmakers:
            for market in bookmaker.get("markets", []):
                if market.get("key") != "outrights":
                    continue
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name", "")
                    price = outcome.get("price")
                    if price is None:
                        continue
                    outcomes.append({
                        "name": name,
                        "american": int(price),
                        "decimal": self.american_to_decimal(price),
                    })
                if outcomes:
                    break
            if outcomes:
                break

        if not outcomes:
            return None

        # Sort by decimal odds ascending (favorites first)
        outcomes.sort(key=lambda o: o["decimal"])
        return outcomes

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
