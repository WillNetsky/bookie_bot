from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiosqlite

from bot.config import KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH
from bot.db.database import DB_PATH

log = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
CACHE_TTL = 300  # 5 minutes
DISCOVERY_TTL = 600  # 10 minutes for availability checks

# ── Kalshi → Odds-API sport key mapping (for live scores) ─────────────
KALSHI_TO_ODDS_API = {
    # Maps Kalshi sport_key (game ticker) → odds-api sport key for live scores
    "KXNBAGAME": "basketball_nba",
    "KXNFLGAME": "americanfootball_nfl",
    "KXMLBGAME": "baseball_mlb",
    "KXNHLGAME": "icehockey_nhl",
    "KXNCAAMBGAME": "basketball_ncaab",
    "KXNCAAWBGAME": "basketball_wncaab",
    "KXNCAAFGAME": "americanfootball_ncaaf",
    "KXEPLGAME": "soccer_epl",
    "KXLALIGAGAME": "soccer_spain_la_liga",
    "KXBUNDESLIGAGAME": "soccer_germany_bundesliga",
    "KXSERIEAGAME": "soccer_italy_serie_a",
    "KXLIGUE1GAME": "soccer_france_ligue_one",
    "KXUCLGAME": "soccer_uefa_champs_league",
    "KXMLSGAME": "soccer_usa_mls",
    "KXUFCFIGHT": "mma_mixed_martial_arts",
}

# ── Sports series tickers ─────────────────────────────────────────────
# Dynamically discovered from Kalshi /series endpoint (cached 24h).
# SPORTS is populated at startup via kalshi_api.refresh_sports().
# Each entry: {sport_key: {"label": str, "series": {"Game": ticker, ...}}}

SPORTS: dict[str, dict] = {}

# Series tickers to exclude (novelty, duplicates, esports, one-offs)
_EXCLUDED_TICKERS = {
    "KXBEASTGAMES", "KXCHESSGAME", "KXCOLLEGEGAMEDAYGUEST",
    "KXFANATICSGAMESFIRSTPLACE", "KXFANATICSGAMESSECONDPLACE",
    "KXFANATICSGAMESTHIRDPLACE", "KXFIFAUSPULLGAME",
    "KXMVENBASINGLEGAME", "KXMVENFLMULTIGAME", "KXMVENFLSINGLEGAME",
    "KXMVESPORTSMULTIGAMEEXTENDED", "KXMVENFLMULTIGAMEEXTENDED",
    "KXMVENBAMULTIGAMEEXTENDED", "KXNBAFINALSVIEWERGAME7",
    "KXNBACELEBRITYGAME", "KXNFLCELEBRITYGAME", "KXPICKLEBALLGAMES",
    "KXPPLGAMES", "KXTTELITEGAME", "KXVALORANTGAMETEAMVSMIBR",
    "KXNBAGAMES", "KXCS2GAMES", "KXLOLGAMES",
}

SERIES_CACHE_TTL = 86400  # 24 hours — series list rarely changes

# ── Futures / props / specials ────────────────────────────────────────
# Multi-outcome markets (pick YES on one option from N choices).
# Maps sport → named market → series_ticker.

FUTURES = {
    "NBA": {
        "label": "NBA",
        "markets": {
            "Championship": "KXNBA",
            "MVP": "KXNBAMVP",
            "All-Star Tournament": "KXNBAALLSTARGAME",
            "All-Star MVP": "KXNBAALLSTARMVP",
        },
    },
    "NFL": {
        "label": "NFL",
        "markets": {
            "Super Bowl": "KXSB",
            "MVP": "KXNFLMVP",
            "AFC Champion": "KXNFLAFCCHAMP",
            "NFC Champion": "KXNFLNFCCHAMP",
        },
    },
    "NHL": {
        "label": "NHL",
        "markets": {
            "Stanley Cup": "KXNHL",
        },
    },
    "MLB": {
        "label": "MLB",
        "markets": {
            "World Series": "KXMLB",
            "MVP": "KXMLBMVP",
        },
    },
    "NCAAMB": {
        "label": "College Basketball (M)",
        "markets": {
            "Championship": "KXNCAAMB",
        },
    },
    "NCAAF": {
        "label": "College Football",
        "markets": {
            "Championship": "KXNCAAF",
        },
    },
    "Boxing": {
        "label": "Boxing",
        "markets": {
            "Fights": "KXBOXING",
        },
    },
    "EPL": {
        "label": "English Premier League",
        "markets": {
            "Winner": "KXEPL",
        },
    },
    "UCL": {
        "label": "Champions League",
        "markets": {
            "Winner": "KXUCL",
        },
    },
}

# Map FUTURES key → corresponding SPORTS key (e.g. "NBA" → "KXNBAGAME")
# Populated by refresh_sports() after SPORTS is built.
FUTURES_TO_SPORTS: dict[str, str] = {}

# Reverse: SPORTS key → FUTURES key (e.g. "KXNBAGAME" → "NBA")
SPORTS_TO_FUTURES: dict[str, str] = {}

# Map sport keys to their parent sport for grouping futures with games
FUTURES_SPORT_MAP = {}
for _fk in FUTURES:
    FUTURES_SPORT_MAP[_fk] = _fk


def _team_matches(name1: str, name2: str) -> bool:
    """Check if two team names refer to the same team.

    Handles abbreviation differences like 'Iowa St.' vs 'Iowa State'.
    """
    n1 = name1.lower().strip().rstrip(".")
    n2 = name2.lower().strip().rstrip(".")
    if n1 == n2:
        return True
    # Check if one contains the other (e.g. "Iowa St" in "Iowa State")
    if n1 in n2 or n2 in n1:
        return True
    # Normalize common abbreviations
    for short, long in [("st", "state"), ("u", "university")]:
        n1_norm = n1.replace(short, long)
        n2_norm = n2.replace(short, long)
        if n1_norm == n2_norm:
            return True
    return False


def _extract_event_suffix(event_ticker: str) -> str:
    """Extract the date+teams suffix from an event ticker for cross-series matching.

    e.g. "KXNBAGAME-26FEB14-LAL-BOS" → "26FEB14-LAL-BOS"
    The first segment (before first hyphen) is the series prefix.
    """
    parts = event_ticker.split("-", 1)
    return parts[1] if len(parts) > 1 else event_ticker


def _parse_event_ticker_date(event_ticker: str) -> datetime | None:
    """Parse game date from event ticker.

    e.g. "KXNBAGAME-26FEB14-LAL-BOS" → 2026-02-14 (date only, midnight UTC).
         "KXMLBGAME-25APR18ATHMIL" → 2025-04-18
    Format is YYMMMDD where MMM is 3-letter month abbreviation.
    The date part may have team codes appended (e.g. "25APR18ATHMIL"),
    so we only parse the first 7 characters.
    """
    parts = event_ticker.split("-")
    if len(parts) < 2:
        return None
    date_part = parts[1][:7]  # e.g. "26FEB14" or "25APR18" (trim team codes)
    if len(date_part) < 7:
        return None
    try:
        dt = datetime.strptime(date_part, "%y%b%d")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _decimal_to_american(decimal_odds: float) -> int:
    """Convert decimal odds to American odds."""
    if decimal_odds <= 1.0:
        return -10000  # Extreme favorite / essentially no odds
    if decimal_odds >= 2.0:
        return round((decimal_odds - 1) * 100)
    else:
        return round(-100 / (decimal_odds - 1))


def _parse_game_from_markets(
    markets: list[dict], event_ticker: str, sport_key: str, sport_label: str, is_soccer: bool
) -> dict | None:
    """Parse a group of game markets into a game object."""
    if not markets:
        return None

    # Determine home/away from title and yes_sub_title
    # Basketball/Football: "Away at Home Winner?"
    # Soccer: "Home vs Away Winner?"
    home_team = None
    away_team = None
    home_market = None
    away_market = None

    first = markets[0]
    title = first.get("title", "")

    if is_soccer and " vs " in title:
        # "Home vs Away Winner?"
        parts = title.replace(" Winner?", "").replace(" winner?", "").split(" vs ", 1)
        if len(parts) == 2:
            home_team = parts[0].strip()
            away_team = parts[1].strip()
    elif " at " in title:
        # "Away at Home Winner?"
        parts = title.replace(" Winner?", "").replace(" winner?", "").split(" at ", 1)
        if len(parts) == 2:
            away_team = parts[0].strip()
            home_team = parts[1].strip()

    # Match markets to sides using yes_sub_title
    for m in markets:
        sub = m.get("yes_sub_title", "").strip()
        if not sub:
            continue
        if home_team and sub.lower() == home_team.lower():
            home_market = m
            m["_side"] = "home"
        elif away_team and sub.lower() == away_team.lower():
            away_market = m
            m["_side"] = "away"

    # Fallback: if we have exactly 2 markets but couldn't match by title parsing,
    # use yes_sub_title directly with ordering from title
    if len(markets) == 2 and (not home_market or not away_market):
        m1, m2 = markets[0], markets[1]
        sub1 = m1.get("yes_sub_title", "").strip()
        sub2 = m2.get("yes_sub_title", "").strip()
        if sub1 and sub2:
            if is_soccer:
                home_team, away_team = sub1, sub2
                home_market, away_market = m1, m2
            else:
                # For "at" format: first mentioned in title is away
                if sub1 in title and sub2 in title:
                    idx1 = title.index(sub1)
                    idx2 = title.index(sub2)
                    if idx1 < idx2:
                        away_team, home_team = sub1, sub2
                        away_market, home_market = m1, m2
                    else:
                        home_team, away_team = sub1, sub2
                        home_market, away_market = m1, m2
                else:
                    away_team, home_team = sub1, sub2
                    away_market, home_market = m1, m2
            if home_market:
                home_market["_side"] = "home"
            if away_market:
                away_market["_side"] = "away"

    if not home_team or not away_team:
        return None

    # Skip stale markets — if the event ticker date is more than 2 days in the past,
    # this is an old/future-season market (e.g. 2025 MLB games still listed as open).
    ticker_date = _parse_event_ticker_date(event_ticker)
    if ticker_date:
        now = datetime.now(timezone.utc)
        days_old = (now - ticker_date).total_seconds() / 86400
        if days_old > 2:
            return None

    # Estimate game start time.
    # close_time = when betting closes (during/end of game, NOT game start)
    # expected_expiration_time = when market settles (after game ends)
    # Best approach: estimate from expected_expiration_time minus game duration.
    # Validate against event ticker date (e.g. 26FEB14 = Feb 14, 2026).
    expire_str = first.get("expected_expiration_time") or first.get("close_time", "")
    commence_time = _estimate_commence_time(expire_str, sport_key, event_ticker)

    # Store close/expiration time for display
    close_time = first.get("close_time", "")
    expiration_time = first.get("expected_expiration_time", "")

    return {
        "id": event_ticker,
        "home_team": home_team,
        "away_team": away_team,
        "sport_key": sport_key,
        "sport_title": sport_label,
        "commence_time": commence_time,
        "close_time": close_time,
        "expiration_time": expiration_time,
        "_kalshi_markets": {
            "home": home_market,
            "away": away_market,
        },
    }


def _estimate_commence_time(expire_str: str, sport_key: str, event_ticker: str = "") -> str:
    """Estimate game start time from Kalshi's expected_expiration_time.

    Uses the event ticker date as a sanity check — if the estimated time
    lands on a different day than the ticker date, use the ticker date
    with the estimated time-of-day.
    """
    if not expire_str:
        return ""
    try:
        expire = datetime.fromisoformat(expire_str.replace("Z", "+00:00"))
        # Approximate game duration offset (match by ticker substring)
        sk = sport_key.upper()
        if "NFL" in sk or "NCAAF" in sk:
            hours = 4
        elif "MLB" in sk:
            hours = 4
        elif "NBA" in sk or "NCAAM" in sk or "NCAAW" in sk:
            hours = 3
        elif "NHL" in sk:
            hours = 3
        elif "UFC" in sk or "MMA" in sk or "BOXING" in sk or "FIGHT" in sk:
            hours = 1
        elif "SOCCER" in sk or "EPL" in sk or "LIGA" in sk or "BUNDESLIGA" in sk or "SERIE" in sk or "LIGUE" in sk or "UCL" in sk or "MLS" in sk or "FACUP" in sk or "EREDIVISIE" in sk:
            hours = 2.5
        else:
            hours = 3
        commence = expire - timedelta(hours=hours)

        # Validate against event ticker date if available
        if event_ticker:
            ticker_date = _parse_event_ticker_date(event_ticker)
            if ticker_date and commence.date() != ticker_date.date():
                # Estimated time landed on wrong day — use ticker date with estimated time
                commence = commence.replace(
                    year=ticker_date.year,
                    month=ticker_date.month,
                    day=ticker_date.day,
                )

        return commence.isoformat().replace("+00:00", "Z")
    except (ValueError, TypeError):
        return expire_str


def _extract_spread_team(yes_sub_title: str) -> str | None:
    """Extract team name from spread yes_sub_title.

    e.g. "Seattle wins by over 9.5 Points" → "Seattle"
    """
    if " wins by" in yes_sub_title:
        return yes_sub_title.split(" wins by")[0].strip()
    return None


def _pick_best_spread(markets: list[dict]) -> dict | None:
    """From a list of spread markets for one game, pick the main line.

    Spread markets are one-sided: "Team wins by over X Points"
    YES = that team covers (-X), NO = opposing team covers (+X).
    Find the market closest to 50/50 and return both sides.
    """
    if not markets:
        return None

    # Find the single market closest to even (yes_ask ≈ 0.50)
    best = None
    best_diff = 999.0

    for m in markets:
        yes_price = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or "0.5")
        diff = abs(yes_price - 0.5)
        if diff < best_diff:
            best_diff = diff
            best = m

    if not best:
        return None

    strike = float(best.get("floor_strike") or 0)
    yes_price = float(best.get("yes_ask_dollars") or best.get("last_price_dollars") or "0.5")
    ticker = best.get("ticker")

    # YES side: the named team covers at -strike
    yes_team = _extract_spread_team(best.get("yes_sub_title", ""))
    if not yes_team:
        return None

    yes_decimal = round(1.0 / yes_price, 3) if yes_price > 0 else 2.0
    yes_american = _decimal_to_american(yes_decimal)

    # NO side: opposing team covers at +strike
    no_price = 1.0 - yes_price
    no_decimal = round(1.0 / no_price, 3) if no_price > 0 else 2.0
    no_american = _decimal_to_american(no_decimal)

    # Find the opposing team name from other markets in this game
    other_team = None
    for m in markets:
        t = _extract_spread_team(m.get("yes_sub_title", ""))
        if t and t != yes_team:
            other_team = t
            break

    if not other_team:
        # Can't find opposing team — still return with placeholder
        other_team = "Opponent"

    return {
        yes_team: {
            "decimal": yes_decimal,
            "american": yes_american,
            "point": -strike,  # negative = favorite
            "_market_ticker": ticker,
            "_kalshi_pick": "yes",
        },
        other_team: {
            "decimal": no_decimal,
            "american": no_american,
            "point": strike,  # positive = underdog
            "_market_ticker": ticker,
            "_kalshi_pick": "no",
        },
    }


def _pick_best_total(markets: list[dict]) -> dict | None:
    """From a list of total markets for one game, pick the main line."""
    if not markets:
        return None

    # Pick the total line where yes_ask is closest to 0.50
    best = None
    best_diff = 999.0

    for m in markets:
        yes_price = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or "0.5")
        diff = abs(yes_price - 0.5)
        if diff < best_diff:
            best_diff = diff
            best = m

    if not best:
        return None

    strike = float(best.get("floor_strike") or 0)
    yes_price = float(best.get("yes_ask_dollars") or best.get("last_price_dollars") or "0.5")
    over_decimal = round(1.0 / yes_price, 3) if yes_price > 0 else 2.0
    over_american = _decimal_to_american(over_decimal)

    no_price = 1.0 - yes_price
    under_decimal = round(1.0 / no_price, 3) if no_price > 0 else 2.0
    under_american = _decimal_to_american(under_decimal)

    ticker = best.get("ticker")
    return {
        "over": {"decimal": over_decimal, "american": over_american, "point": strike,
                 "_market_ticker": ticker, "_kalshi_pick": "yes"},
        "under": {"decimal": under_decimal, "american": under_american, "point": strike,
                   "_market_ticker": ticker, "_kalshi_pick": "no"},
    }


MAX_CONCURRENT = 5  # Max simultaneous Kalshi API requests
MAX_RETRIES = 2
RETRY_BACKOFF = 1.0  # seconds, doubles each retry

# ── RSA signing for authenticated requests ────────────────────────────

_private_key = None


def _load_private_key():
    """Load the RSA private key from the configured PEM file."""
    global _private_key
    if _private_key is not None:
        return _private_key
    if not KALSHI_PRIVATE_KEY_PATH:
        return None
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        pem_data = Path(KALSHI_PRIVATE_KEY_PATH).read_bytes()
        _private_key = load_pem_private_key(pem_data, password=None)
        log.info("Kalshi RSA private key loaded successfully")
        return _private_key
    except Exception as e:
        log.warning("Failed to load Kalshi private key: %s", e)
        return None


def _sign_request(method: str, path: str, timestamp_ms: int) -> str | None:
    """Sign a Kalshi API request using RSA-PSS with SHA256."""
    key = _load_private_key()
    if not key:
        return None
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        message = f"{timestamp_ms}{method}{path}".encode()
        signature = key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()
    except Exception as e:
        log.warning("Failed to sign Kalshi request: %s", e)
        return None


class KalshiAPI:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _auth_headers(self, method: str, url: str) -> dict[str, str]:
        """Generate Kalshi authentication headers if key is configured."""
        if not KALSHI_API_KEY_ID:
            return {}
        timestamp_ms = int(time.time() * 1000)
        path = urlparse(url).path
        signature = _sign_request(method.upper(), path, timestamp_ms)
        if not signature:
            return {}
        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

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

        # Cache miss or stale — fetch from API with rate limiting + retry
        async with self._semaphore:
            session = await self._get_session()
            for attempt in range(MAX_RETRIES + 1):
                try:
                    headers = self._auth_headers("GET", url)
                    async with session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 429:
                            if attempt < MAX_RETRIES:
                                wait = RETRY_BACKOFF * (2 ** attempt)
                                log.info("Kalshi 429 rate limited, retrying in %.1fs...", wait)
                                await asyncio.sleep(wait)
                                continue
                            log.warning("Kalshi API %s returned 429 after %d retries", url, MAX_RETRIES)
                            if stale_data:
                                return json.loads(stale_data)
                            return None
                        if resp.status != 200:
                            log.warning("Kalshi API %s returned %s", url, resp.status)
                            if stale_data:
                                return json.loads(stale_data)
                            return None
                        data = await resp.json()
                        break
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

    # ── Sports discovery ────────────────────────────────────────────────

    async def refresh_sports(self) -> None:
        """Fetch all sports series from Kalshi and populate the SPORTS dict.

        Calls GET /series once, caches for 24h. Builds SPORTS dynamically
        by finding GAME/FIGHT series and matching SPREAD/TOTAL variants.
        """
        data = await self._cached_request(
            f"{BASE_URL}/series",
            {},
            ttl=SERIES_CACHE_TTL,
        )
        if not data or "series" not in data:
            log.warning("Failed to fetch Kalshi series list")
            return

        all_series = data["series"]
        game_tickers: dict[str, dict] = {}   # prefix → {ticker, title}
        spread_tickers: dict[str, str] = {}  # prefix → ticker
        total_tickers: dict[str, str] = {}   # prefix → ticker

        for item in all_series:
            ticker = item.get("ticker", "")
            title = item.get("title", "")
            cat = item.get("category", "")
            if cat != "Sports":
                continue
            if ticker in _EXCLUDED_TICKERS:
                continue

            t = ticker.upper()
            if t.endswith("GAME"):
                prefix = ticker[:-4]
                game_tickers[prefix] = {"ticker": ticker, "title": title}
            elif t.endswith("FIGHT"):
                prefix = ticker[:-5]
                game_tickers[prefix] = {"ticker": ticker, "title": title}
            elif t.endswith("SPREAD"):
                prefix = ticker[:-6]
                spread_tickers[prefix] = ticker
            elif t.endswith("TOTAL"):
                prefix = ticker[:-5]
                total_tickers[prefix] = ticker

        # Build SPORTS dict
        new_sports: dict[str, dict] = {}
        for prefix, info in game_tickers.items():
            game_ticker = info["ticker"]
            label = info["title"]
            # Clean up label: remove "Game", "Winner", "Fight" suffix noise
            for suffix in (" Game", " Games", " Winner", " winner", " fight winner"):
                if label.endswith(suffix):
                    label = label[:-len(suffix)]
            label = label.strip()
            if not label:
                label = prefix.replace("KX", "")

            # Use the ticker as the sport key (unique, stable)
            sport_key = game_ticker
            series = {"Game": game_ticker}
            if prefix in spread_tickers:
                series["Spread"] = spread_tickers[prefix]
            if prefix in total_tickers:
                series["Total"] = total_tickers[prefix]

            new_sports[sport_key] = {"label": label, "series": series}

        SPORTS.clear()
        SPORTS.update(new_sports)

        # Build FUTURES ↔ SPORTS cross-references
        # Match by checking if the FUTURES key appears in the SPORTS ticker
        # e.g. FUTURES key "NBA" matches SPORTS key "KXNBAGAME"
        FUTURES_TO_SPORTS.clear()
        SPORTS_TO_FUTURES.clear()
        for fut_key in FUTURES:
            fut_upper = fut_key.upper()
            for sport_key in SPORTS:
                # Check if ticker contains the futures key (e.g. "KXNBAGAME" contains "NBA")
                sk_upper = sport_key.upper().replace("KX", "")
                if sk_upper.startswith(fut_upper) and sk_upper[len(fut_upper):] in ("GAME", "FIGHT", ""):
                    FUTURES_TO_SPORTS[fut_key] = sport_key
                    SPORTS_TO_FUTURES[sport_key] = fut_key
                    break

        log.info("Loaded %d sports from Kalshi series API", len(SPORTS))

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

    async def get_sport_games(self, sport_key: str) -> list[dict]:
        """Fetch game markets for a sport and group into game objects.

        Returns a list of game dicts with home/away teams, moneyline odds,
        and commence_time — matching the format used by /odds.
        """
        sport = SPORTS.get(sport_key)
        if not sport:
            return []

        # Get first series key (usually "Game" or "Fight Winner")
        first_key = next(iter(sport["series"].keys()))
        series_ticker = sport["series"][first_key]
        markets = await self.get_markets_by_series(series_ticker)
        if not markets:
            return []

        # Group markets by event_ticker
        event_groups: dict[str, list[dict]] = {}
        for m in markets:
            et = m.get("event_ticker", "")
            if et not in event_groups:
                event_groups[et] = []
            event_groups[et].append(m)

        # Auto-detect title format from first market:
        # Soccer: "Home vs Away Winner?" / US sports: "Away at Home Winner?"
        first_title = markets[0].get("title", "")
        is_soccer = " vs " in first_title and " at " not in first_title

        games = []
        for event_ticker, group in event_groups.items():
            game = _parse_game_from_markets(group, event_ticker, sport_key, sport["label"], is_soccer)
            if game:
                games.append(game)

        # Sort by commence_time
        games.sort(key=lambda g: g.get("commence_time", "9999"))
        return games

    async def get_game_odds(self, sport_key: str, game: dict) -> dict:
        """Fetch full odds (moneyline + spread + total) for a specific game.

        game: a game dict from get_sport_games() with home_team, away_team, _kalshi_markets.
        Returns a parsed odds dict matching the format from sports_api.parse_odds().
        """
        sport = SPORTS.get(sport_key)
        if not sport:
            return {}

        series = sport["series"]
        parsed = {}
        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")
        event_ticker = game.get("id", "")

        # Parse moneyline from the game's cached markets
        kalshi_markets = game.get("_kalshi_markets", {})
        for side in ("home", "away"):
            m = kalshi_markets.get(side)
            if not m:
                continue
            yes_price = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or "0.5")
            decimal_odds = round(1.0 / yes_price, 3) if yes_price > 0 else 2.0
            american = _decimal_to_american(decimal_odds)
            parsed[side] = {"decimal": decimal_odds, "american": american, "point": None}

        # Fetch spread and total markets concurrently
        spread_key = None
        total_key = None
        for k in series:
            kl = k.lower()
            if "spread" in kl:
                spread_key = k
            elif "total" in kl:
                total_key = k

        coros = []
        fetch_keys = []
        if spread_key:
            coros.append(self.get_markets_by_series(series[spread_key]))
            fetch_keys.append("spread")
        if total_key:
            coros.append(self.get_markets_by_series(series[total_key]))
            fetch_keys.append("total")

        if coros:
            # Extract game suffix (date+teams) from event_ticker for cross-series matching
            # e.g. "KXNBAGAME-26FEB14-LAL-BOS" → "-26FEB14-LAL-BOS"
            game_suffix = _extract_event_suffix(event_ticker)

            results = await asyncio.gather(*coros, return_exceptions=True)
            for key, result in zip(fetch_keys, results):
                if isinstance(result, Exception) or not result:
                    continue
                # Filter to markets matching this game's suffix
                event_markets = [
                    m for m in result
                    if _extract_event_suffix(m.get("event_ticker", "")) == game_suffix
                ]

                if key == "spread":
                    spread_raw = _pick_best_spread(event_markets)
                    if spread_raw:
                        # Map team names to home/away
                        for team_name, odds_data in spread_raw.items():
                            entry = {
                                "decimal": odds_data["decimal"],
                                "american": odds_data["american"],
                                "point": odds_data["point"],
                                "_market_ticker": odds_data.get("_market_ticker"),
                                "_kalshi_pick": odds_data.get("_kalshi_pick", "yes"),
                            }
                            if _team_matches(team_name, home_team):
                                parsed["spread_home"] = entry
                            elif _team_matches(team_name, away_team):
                                parsed["spread_away"] = entry
                elif key == "total":
                    total_odds = _pick_best_total(event_markets)
                    if total_odds:
                        parsed.update(total_odds)

        return parsed

    async def get_all_games(self) -> list[dict]:
        """Fetch games across all sports concurrently.

        Returns a flat list of game dicts sorted by commence_time.
        """
        tasks = {}
        for key in SPORTS:
            tasks[key] = self.get_sport_games(key)

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        all_games = []
        for key, result in zip(tasks.keys(), results):
            if isinstance(result, list):
                all_games.extend(result)

        all_games.sort(key=lambda g: g.get("commence_time", "9999"))
        return all_games

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

    async def get_futures_markets(self, series_ticker: str) -> list[dict]:
        """Fetch open markets for a futures/props series, sorted by probability.

        Returns list of dicts with: ticker, title, yes_price, american_odds, subtitle.
        """
        markets = await self.get_markets_by_series(series_ticker, limit=200)
        if not markets:
            return []

        options = []
        for m in markets:
            yes_price = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or "0")
            if yes_price <= 0:
                continue
            decimal_odds = round(1.0 / yes_price, 3)
            american = _decimal_to_american(decimal_odds)
            options.append({
                "ticker": m.get("ticker", ""),
                "title": m.get("yes_sub_title") or m.get("title", ""),
                "yes_price": yes_price,
                "decimal_odds": decimal_odds,
                "american_odds": american,
                "close_time": m.get("close_time") or m.get("expected_expiration_time"),
                "event_ticker": m.get("event_ticker", ""),
            })

        # Sort by probability descending (favorites first)
        options.sort(key=lambda o: o["yes_price"], reverse=True)
        return options

    async def discover_available(self) -> dict:
        """Discover all available markets — games and futures.

        Returns:
            {
                "games": {sport_key: {"count": N, "next_time": "ISO str" | None}, ...},
                "futures": {sport_key: {name: True, ...}, ...},
            }

        Results are cached as a single entry for DISCOVERY_TTL (10 min)
        to avoid bursting 31 API requests on each /kalshi call.
        """
        # Check for cached discovery result first
        cache_key = "kalshi:discovery:all"
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (cache_key,),
            )
            row = await cursor.fetchone()
            if row:
                try:
                    fetched_dt = datetime.fromisoformat(row[1])
                    if fetched_dt.tzinfo is None:
                        fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
                    if age < DISCOVERY_TTL:
                        return json.loads(row[0])
                except (ValueError, TypeError):
                    pass

        # Ensure SPORTS is populated
        if not SPORTS:
            await self.refresh_sports()

        games_tasks = {}
        for key, sport in SPORTS.items():
            first_series = next(iter(sport["series"].values()))
            # limit=1: just check if markets exist. Full data fetched on drill-in.
            games_tasks[key] = self.get_markets_by_series(first_series, limit=1)

        futures_tasks = {}
        for sport_key, sport in FUTURES.items():
            for market_name, series_ticker in sport["markets"].items():
                task_key = f"{sport_key}:{market_name}"
                futures_tasks[task_key] = self.get_markets_by_series(series_ticker, limit=1)

        all_keys = list(games_tasks.keys()) + list(futures_tasks.keys())
        all_coros = list(games_tasks.values()) + list(futures_tasks.values())

        results = await asyncio.gather(*all_coros, return_exceptions=True)

        games_available = {}
        futures_available = {}

        n_games = len(games_tasks)
        for i, key in enumerate(all_keys):
            result = results[i]
            has_markets = isinstance(result, list) and len(result) > 0
            if i < n_games:
                if has_markets:
                    now = datetime.now(timezone.utc)
                    # Check if the sample market is stale
                    m = result[0]
                    et = m.get("event_ticker", "")
                    ticker_date = _parse_event_ticker_date(et) if et else None
                    if ticker_date and (now - ticker_date).total_seconds() / 86400 > 2:
                        continue  # Skip entire sport — sample market is stale

                    # Estimate next game time from the sample
                    exp = m.get("expected_expiration_time") or m.get("close_time", "")
                    est_start = _estimate_commence_time(exp, key, et) if exp else None
                    next_upcoming = None
                    has_live = False
                    if est_start:
                        try:
                            start_dt = datetime.fromisoformat(est_start.replace("Z", "+00:00"))
                            exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                            if start_dt > now:
                                next_upcoming = est_start
                            elif now <= exp_dt:
                                has_live = True
                        except (ValueError, TypeError):
                            pass

                    games_available[key] = {
                        "next_time": next_upcoming,
                        "has_live": has_live,
                    }
            else:
                if has_markets:
                    sport_key, market_name = key.split(":", 1)
                    if sport_key not in futures_available:
                        futures_available[sport_key] = {}
                    futures_available[sport_key][market_name] = True

        result = {"games": games_available, "futures": futures_available}

        # Cache the full discovery result
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO games_cache (game_id, sport, data, fetched_at)"
                " VALUES (?, 'kalshi', ?, datetime('now'))",
                (cache_key, json.dumps(result)),
            )
            await db.commit()

        return result


# Singleton instance
kalshi_api = KalshiAPI()
