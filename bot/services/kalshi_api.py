from __future__ import annotations

import asyncio
import base64
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiosqlite

from bot.config import KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH
from bot.db.database import DB_PATH, get_connection
from bot.utils import decimal_to_american

log = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
CACHE_TTL = 1800  # 30 minutes — matches refresh_discovery loop interval
DISCOVERY_TTL = 1800  # 30 minutes for availability checks
AGG_MARKETS_CACHE_KEY = "kalshi:all_open_markets:aggregated"  # single stable cache key

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

# Series tickers to exclude (only true duplicates / non-bettable products)
_EXCLUDED_TICKERS = {
    # Multi-game parlay / MV products (not individual game markets)
    "KXMVENBASINGLEGAME", "KXMVENFLMULTIGAME", "KXMVENFLSINGLEGAME",
    "KXMVESPORTSMULTIGAMEEXTENDED", "KXMVENFLMULTIGAMEEXTENDED",
    "KXMVENBAMULTIGAMEEXTENDED",
    # Plural duplicates of singular series (KXNBAGAMES duplicates KXNBAGAME)
    "KXNBAGAMES", "KXCS2GAMES", "KXLOLGAMES", "KXPPLGAMES",
    # NFL sub-stat series (duplicate the main KXNFLGAME markets)
    "KXNFLGAMESACK", "KXNFLGAMETD", "KXNFLGAMETO", "KXNFLGAMEFG",
    # Non-game series that happen to end in GAME
    "KXCOLLEGEGAMEDAYGUEST", "KXMLBSERIESGAMETOTAL",
    "KXEWCTEAMFIGHTTACTICS",
}

# Suffixes that are derivative markets (spread/total), not primary game series.
# We include everything in the Sports category EXCEPT these + _EXCLUDED_TICKERS.
_DERIVATIVE_SUFFIXES = ("SPREAD", "TOTAL", "SACK", "TD", "TO", "FG")

SERIES_CACHE_TTL = 86400  # 24 hours — series list rarely changes

# Label overrides — Kalshi titles are often generic ("Professional Basketball")
_LABEL_OVERRIDES: dict[str, str] = {
    "KXNBAGAME": "NBA",
    "KXNFLGAME": "NFL",
    "KXMLBGAME": "MLB",
    "KXNHLGAME": "NHL",
    "KXNCAAMBGAME": "College Basketball (M)",
    "KXNCAAWBGAME": "College Basketball (W)",
    "KXNCAAFGAME": "College Football",
    "KXNCAAFCSGAME": "College Football (FCS)",
    "KXNCAAFD3GAME": "College Football (D3)",
    "KXNCAABGAME": "College Basketball",
    "KXNCAABBGAME": "College Baseball",
    "KXNCAAHOCKEYGAME": "College Hockey",
    "KXNCAALAXGAME": "College Lacrosse (W)",
    "KXNCAAMLAXGAME": "College Lacrosse (M)",
    "KXWNBAGAME": "WNBA",
    "KXMLBASGAME": "MLB All-Star",
    "KXNBAALLSTARGAME": "NBA All-Star",
    "KXWNBAASGAME": "WNBA All-Star",
    "KXBOXINGFIGHT": "Boxing",
    "KXUFCFIGHT": "UFC",
    "KXEPLGAME": "English Premier League",
    "KXLALIGAGAME": "La Liga",
    "KXBUNDESLIGAGAME": "Bundesliga",
    "KXSERIEAGAME": "Serie A",
    "KXLIGUE1GAME": "Ligue 1",
    "KXUCLGAME": "Champions League",
    "KXUELGAME": "Europa League",
    "KXUECLGAME": "Conference League",
    "KXMLSGAME": "MLS",
    "KXFACUPGAME": "FA Cup",
    "KXEREDIVISIEGAME": "Eredivisie",
    "KXLIGAMXGAME": "Liga MX",
    "KXSAUDIPLGAME": "Saudi Pro League",
    "KXUNRIVALEDGAME": "Unrivaled",
    "KXLAXGAME": "PLL Lacrosse",
    "KXIPLGAME": "IPL Cricket",
    "KXWPLGAME": "WPL Cricket",
    "KXAHLGAME": "AHL",
    "KXKHLGAME": "KHL",
    # Esports
    "KXCS2GAME": "Counter-Strike 2",
    "KXCSGOGAME": "CS:GO",
    "KXLOLGAME": "League of Legends",
    "KXVALORANTGAME": "Valorant",
    "KXDOTA2GAME": "Dota 2",
    "KXOWGAME": "Overwatch",
    "KXR6GAME": "Rainbow Six",
    "KXCODGAME": "Call of Duty",
    # Other
    "KXCHESSGAME": "Chess",
    "KXPICKLEBALLGAMES": "Pickleball",
    "KXWOCURLGAME": "Olympic Curling",
    "KXBEASTGAMES": "Beast Games",
    "KXNBACELEBRITYGAME": "NBA Celebrity Game",
    "KXNFLCELEBRITYGAME": "NFL Celebrity Game",
    "KXNBAFINALSVIEWERGAME7": "NBA Finals Game 7 Viewership",
    "KXTTELITEGAME": "TT Elite",
    "KXCRYPTOFIGHTNIGHT": "Crypto Fight Night",
    "KXVALORANTGAMETEAMVSMIBR": "Valorant (Team vs MIBR)",
    "KXFIFAUSPULLGAME": "FIFA US Pull",
    "KXMCGREGORFIGHTNEXT": "McGregor Next Fight",
    "KXFANATICSGAMESFIRSTPLACE": "Fanatics Games (1st)",
    "KXFANATICSGAMESSECONDPLACE": "Fanatics Games (2nd)",
    "KXFANATICSGAMESTHIRDPLACE": "Fanatics Games (3rd)",
    # Match series
    "KXATPMATCH": "ATP Tennis",
    "KXWTAMATCH": "WTA Tennis",
    "KXCHALLENGERMATCH": "ATP Challenger",
    "KXWTACHALLENGERMATCH": "WTA Challenger",
    "KXATPCHALLENGERMATCH": "ATP Challenger (2)",
    "KXDAVISCUPMATCH": "Davis Cup",
    "KXUNITEDCUPMATCH": "United Cup",
    "KXCRICKETODIMATCH": "Cricket ODI",
    "KXCRICKETWOMENODIMATCH": "Cricket Women's ODI",
    "KXCRICKETWOMENT20IMATCH": "Cricket Women's T20I",
    "KXCRICKETT20IMATCH": "Cricket T20I",
    "KXCRICKETTESTMATCH": "Cricket Test",
    "KXCRICKETWOMENTESTMATCH": "Cricket Women's Test",
    "KXT20MATCH": "T20 Cricket",
    "KXSSHIELDMATCH": "Sheffield Shield Cricket",
    "KXSIXNATIONSMATCH": "Six Nations Rugby",
    "KXRUGBYFRA14MATCH": "French Top 14 Rugby",
    "KXRUGBYGPREMMATCH": "Gallagher Premiership Rugby",
    "KXRUGBYNRLMATCH": "NRL Rugby",
    "KXDARTSMATCH": "Darts",
    "KXTGLMATCH": "TGL Golf",
    "KXSIXKINGSSLAMMATCH": "Six Kings Slam Tennis",
    "KXSIXKINGSMATCH": "Six Kings Slam Tennis",
    "KXPGARYDERMATCH": "Ryder Cup Golf",
    "KXATPEXACTMATCH": "ATP Exact Score",
}

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
    """Parse game date (and optional time) from event ticker.

    e.g. "KXNBAGAME-26FEB14-LAL-BOS" → 2026-02-14 (date only, midnight UTC).
         "KXMLBGAME-25APR18ATHMIL" → 2025-04-18
         "KXAHLGAME-26FEB191200IOWCHI" → 2026-02-19 12:00 UTC
    Format is YYMMMDD[HHMM] where MMM is 3-letter month abbreviation.
    The date part may have team codes appended, so we parse carefully.
    """
    parts = event_ticker.split("-")
    if len(parts) < 2:
        return None
    segment = parts[1]
    date_part = segment[:7]  # e.g. "26FEB14" or "25APR18"
    if len(date_part) < 7:
        return None
    try:
        dt = datetime.strptime(date_part, "%y%b%d")
        # Check if a 4-digit time follows (HHMM)
        rest = segment[7:]
        if len(rest) >= 4 and rest[:4].isdigit():
            hh = int(rest[:2])
            mm = int(rest[2:4])
            if 0 <= hh < 24 and 0 <= mm < 60:
                dt = dt.replace(hour=hh, minute=mm)
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


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

    if " vs " in title:
        # "Home vs Away Winner?" — used by soccer and international tournaments (WBC, etc.)
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

    # Use expected_expiration_time as the display time.
    # Kalshi doesn't expose actual game start times — expiration is the
    # closest reliable timestamp (when the market settles, shortly after
    # the game ends).
    expiration_time = first.get("expected_expiration_time") or first.get("close_time", "")
    close_time = first.get("close_time", "")

    return {
        "id": event_ticker,
        "home_team": home_team,
        "away_team": away_team,
        "sport_key": sport_key,
        "sport_title": sport_label,
        "commence_time": expiration_time,
        "close_time": close_time,
        "expiration_time": expiration_time,
        "_kalshi_markets": {
            "home": home_market,
            "away": away_market,
            "all": markets,
        },
    }



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
    yes_american = decimal_to_american(yes_decimal)

    # NO side: opposing team covers at +strike
    no_price = 1.0 - yes_price
    no_decimal = round(1.0 / no_price, 3) if no_price > 0 else 2.0
    no_american = decimal_to_american(no_decimal)

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
    over_american = decimal_to_american(over_decimal)

    no_price = 1.0 - yes_price
    under_decimal = round(1.0 / no_price, 3) if no_price > 0 else 2.0
    under_american = decimal_to_american(under_decimal)

    ticker = best.get("ticker")
    return {
        "over": {"decimal": over_decimal, "american": over_american, "point": strike,
                 "_market_ticker": ticker, "_kalshi_pick": "yes"},
        "under": {"decimal": under_decimal, "american": under_american, "point": strike,
                   "_market_ticker": ticker, "_kalshi_pick": "no"},
    }


MAX_CONCURRENT = 5  # Max simultaneous Kalshi API requests
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # seconds, doubles each retry (2, 4, 8s)

# ── RSA signing for authenticated requests ────────────────────────────

_private_key = None


def _is_market_active(m: dict) -> bool:
    """Return True if the market hasn't passed its expected expiration time.

    Uses expected_expiration_time (when the market resolves/settles), NOT
    close_time (when betting closes, which is often at game start and is
    already past for in-progress games).
    """
    exp = m.get("expected_expiration_time") or ""
    if not exp:
        return True
    try:
        exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        return exp_dt > datetime.now(timezone.utc)
    except (ValueError, TypeError):
        return True


def _is_market_bettable(m: dict) -> bool:
    """Return True if the market's YES price is between 2¢ and 98¢ (2%–98%).

    Filters out near-certain and near-impossible markets where one side
    offers no meaningful payout.
    """
    try:
        price = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)
    except (ValueError, TypeError):
        return False
    return 0.02 <= price <= 0.98


def _earliest_market_time(m: dict) -> str:
    """Return the earlier of close_time / expected_expiration_time as an ISO string."""
    ct = m.get("close_time") or ""
    et = m.get("expected_expiration_time") or ""
    if not ct:
        return et
    if not et:
        return ct
    try:
        ct_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        et_dt = datetime.fromisoformat(et.replace("Z", "+00:00"))
        return ct if ct_dt <= et_dt else et
    except (ValueError, TypeError):
        return ct or et


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
        self._sports_series_cache: set[str] = set()
        self._series_info: dict[str, dict] = {}  # {ticker: {"category": ..., "title": ...}}
        self._prewarm_task: asyncio.Task | None = None
        self._markets_fetch_lock = asyncio.Lock()  # Prevent concurrent full-market fetches
        # In-memory caches — avoid SQLite round-trips + JSON parsing on hot paths
        self._mem_all_markets: tuple[list[dict], float] | None = None
        self._mem_browse_data: tuple[dict[str, list[dict]], float] | None = None
        self._mem_all_games: tuple[list[dict], float] | None = None
        # Log auth config at init
        if KALSHI_API_KEY_ID:
            log.info("Kalshi API key configured: %s...", KALSHI_API_KEY_ID[:8])
        else:
            log.warning("No Kalshi API key configured — requests will be unauthenticated")
        if KALSHI_PRIVATE_KEY_PATH:
            key_path = Path(KALSHI_PRIVATE_KEY_PATH)
            if key_path.exists():
                log.info("Kalshi private key file found: %s", KALSHI_PRIVATE_KEY_PATH)
            else:
                log.warning("Kalshi private key file NOT found: %s", KALSHI_PRIVATE_KEY_PATH)
        else:
            log.warning("No Kalshi private key path configured")

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

    def schedule_markets_prewarm(self) -> None:
        """Kick off a background refresh of the markets cache if it's stale.

        Called when discover_available() returns a cached result so that by the
        time the user navigates to a sport, the markets data is already ready.
        """
        # Don't stack up duplicate tasks
        if self._prewarm_task and not self._prewarm_task.done():
            return
        try:
            self._prewarm_task = asyncio.get_running_loop().create_task(
                self._prewarm_markets()
            )
        except RuntimeError:
            pass  # no running loop (e.g. during testing)

    async def _prewarm_markets(self) -> None:
        try:
            await self.get_all_open_sports_markets()
            log.debug("Background markets pre-warm complete")
        except Exception as e:
            log.debug("Background markets pre-warm error: %s", e)

    # ── Caching layer (reuses games_cache table) ──────────────────────

    async def _cached_request(
        self, url: str, params: dict, ttl: int | None = None, 
        prune_func: callable | None = None, sport: str = "kalshi"
    ) -> list | dict | None:
        cache_key = f"kalshi:{url}:{json.dumps(params, sort_keys=True)}"
        effective_ttl = ttl if ttl is not None else CACHE_TTL
        short_url = url.replace(BASE_URL, "")

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
                try:
                    fetched_dt = datetime.fromisoformat(fetched_at)
                    if fetched_dt.tzinfo is None:
                        fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
                    if age < effective_ttl:
                        log.debug("Cache HIT %s (age %.0fs / ttl %ds)", short_url, age, effective_ttl)
                        data = json.loads(row[0])
                        return prune_func(data) if prune_func else data
                    log.debug("Cache STALE %s (age %.0fs / ttl %ds)", short_url, age, effective_ttl)
                except (ValueError, TypeError):
                    pass
                stale_data = row[0]
        finally:
            await db.close()

        # Cache miss or stale — fetch from API with rate limiting + retry
        log.debug("Cache MISS %s — fetching from API (auth=%s)", short_url, bool(KALSHI_API_KEY_ID))
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
                                data = json.loads(stale_data)
                                return prune_func(data) if prune_func else data
                            return None
                        if resp.status != 200:
                            log.warning("Kalshi API %s returned %s", url, resp.status)
                            if stale_data:
                                data = json.loads(stale_data)
                                return prune_func(data) if prune_func else data
                            return None
                        data = await resp.json()
                        break
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    log.warning("Kalshi API request failed: %s", e)
                    if stale_data:
                        data = json.loads(stale_data)
                        return prune_func(data) if prune_func else data
                    return None

        # Store in cache with retries for locked database
        if data:
            # Prune BEFORE storing to save disk space
            pruned_data = prune_func(data) if prune_func else data
            data_str = json.dumps(pruned_data)
            
            for attempt in range(3):
                db = await get_connection()
                try:
                    await db.execute(
                        "INSERT OR REPLACE INTO games_cache (game_id, sport, data, fetched_at)"
                        " VALUES (?, ?, ?, datetime('now'))",
                        (cache_key, sport, data_str),
                    )
                    await db.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    log.warning("Failed to update cache for %s: %s", short_url, e)
                finally:
                    await db.close()

        return data

    # ── Sports discovery ────────────────────────────────────────────────

    async def refresh_sports(self) -> None:
        """Fetch all sports series from Kalshi and populate the SPORTS dict.

        Calls GET /series once, caches for 24h. Builds SPORTS dynamically
        by finding GAME/FIGHT series and matching SPREAD/TOTAL variants.
        """
        data = await self._cached_request(
            f"{BASE_URL}/series",
            {"limit": 10000},
            ttl=SERIES_CACHE_TTL,
            prune_func=self._prune_series_list
        )
        if not data or "series" not in data:
            log.warning("Failed to fetch Kalshi series list")
            return

        all_series = data["series"]

        # Populate _series_info for ALL series (used by get_browse_data)
        for item in all_series:
            t = item.get("ticker", "")
            if t:
                self._series_info[t] = {
                    "category": item.get("category", ""),
                    "title": item.get("title", ""),
                }

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

            # Classify as derivative (spread/total) or primary (game) series.
            # Use blacklist approach: anything that doesn't end with a known
            # derivative suffix is treated as a primary game series.
            is_derivative = False
            for suffix in _DERIVATIVE_SUFFIXES:
                if t.endswith(suffix):
                    is_derivative = True
                    prefix = ticker[:-len(suffix)]
                    if suffix == "SPREAD":
                        spread_tickers[prefix] = ticker
                    elif suffix == "TOTAL":
                        total_tickers[prefix] = ticker
                    # Other derivative suffixes (SACK, TD, etc.) are just skipped
                    break

            if not is_derivative:
                # Determine prefix by stripping known game suffixes
                if t.endswith("GAMES"):
                    prefix = ticker[:-5]
                elif t.endswith("GAME"):
                    prefix = ticker[:-4]
                elif t.endswith("FIGHT"):
                    prefix = ticker[:-5]
                elif t.endswith("MATCH"):
                    prefix = ticker[:-5]
                elif t.endswith("BOUT"):
                    prefix = ticker[:-4]
                elif t.endswith("RACE"):
                    prefix = ticker[:-4]
                elif t.endswith("ROUND"):
                    prefix = ticker[:-5]
                elif t.endswith("SET"):
                    prefix = ticker[:-3]
                else:
                    # Unknown suffix — still include it, use ticker as its own prefix
                    prefix = ticker
                    log.info("New sports series with unknown suffix: %s (%s)", ticker, title)
                game_tickers[prefix] = {"ticker": ticker, "title": title}

        # Build SPORTS dict
        new_sports: dict[str, dict] = {}
        for prefix, info in game_tickers.items():
            game_ticker = info["ticker"]

            # Use override label if available, otherwise clean up API title
            if game_ticker in _LABEL_OVERRIDES:
                label = _LABEL_OVERRIDES[game_ticker]
            else:
                label = info["title"]
                for suffix in (" Game", " Games", " Winner", " winner",
                               " fight winner", " Fight", " fight",
                               " Match", " match"):
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

        # Update sports series cache for fast filtering
        self._sports_series_cache = {
            ticker for info in SPORTS.values()
            for ticker in info["series"].values()
        }

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
                remainder = sk_upper[len(fut_upper):]
                if sk_upper.startswith(fut_upper) and remainder in ("GAME", "GAMES", "FIGHT", "MATCH", "BOUT", "RACE", "ROUND", "SET", ""):
                    FUTURES_TO_SPORTS[fut_key] = sport_key
                    SPORTS_TO_FUTURES[sport_key] = fut_key
                    break

        log.info(
            "Loaded %d sports from Kalshi (%d with spread, %d with totals). "
            "Futures cross-refs: %s",
            len(SPORTS),
            sum(1 for s in SPORTS.values() if "Spread" in s["series"]),
            sum(1 for s in SPORTS.values() if "Total" in s["series"]),
            {k: v for k, v in FUTURES_TO_SPORTS.items()},
        )

    def _prune_market(self, m: dict) -> dict:
        """Keep only fields we actually use to save RAM."""
        ticker = m.get("ticker", "")
        event_ticker = m.get("event_ticker", "")
        series_ticker = m.get("series_ticker")
        
        # Fallback for missing series_ticker in v2 API
        if not series_ticker and event_ticker:
            if "-" in event_ticker:
                series_ticker = event_ticker.split("-")[0]
            else:
                series_ticker = event_ticker

        return {
            "ticker": ticker,
            "event_ticker": event_ticker,
            "series_ticker": series_ticker,
            "title": m.get("title"),
            "yes_sub_title": m.get("yes_sub_title"),
            "yes_ask_dollars": m.get("yes_ask_dollars"),
            "last_price_dollars": m.get("last_price_dollars"),
            "expected_expiration_time": m.get("expected_expiration_time"),
            "close_time": m.get("close_time"),
            "floor_strike": m.get("floor_strike"),
            # For resolution
            "status": m.get("status"),
            "settlement_value_dollars": m.get("settlement_value_dollars"),
            "result": m.get("result"),
        }

    def _prune_markets_list(self, data: dict) -> dict:
        """Pruning function for market list responses."""
        if not data or "markets" not in data:
            return data
        data["markets"] = [self._prune_market(m) for m in data["markets"]]
        return data

    def _prune_series_list(self, data: dict) -> dict:
        """Pruning function for series list responses."""
        if not data or "series" not in data:
            return data
        data["series"] = [
            {
                "ticker": s.get("ticker"),
                "title": s.get("title"),
                "category": s.get("category"),
            }
            for s in data["series"]
        ]
        return data

    async def get_active_series_tickers(self) -> set[str]:
        """Fetch tickers of all 'Sports' series that currently have open markets.
        
        This is much more efficient than fetching all open markets and filtering.
        """
        # GET /series with status=open is not supported, but we can filter by category
        # and then check which ones actually have markets.
        # However, the most efficient way to see what is ACTIVE is to fetch
        # all open markets but only keep their series_ticker.
        # But wait, if we fetch all open markets to find active series, we haven't saved anything.
        
        # New strategy: Kalshi /series endpoint returns ALL series.
        # We already have them in SPORTS.
        return set(SPORTS.keys())

    async def get_all_open_sports_markets(self) -> list[dict]:
        """Fetch open markets only for known sports series."""
        if not SPORTS:
            await self.refresh_sports()
        return await self.get_all_open_markets()

    async def get_all_open_markets(self) -> list[dict]:
        """Fetch all open sports markets from Kalshi, cached as a single aggregated entry.

        Aggregates all pagination pages under one stable cache key, eliminating
        cursor-keyed entry accumulation that caused database bloat.
        """
        # In-memory fast path — skip DB round-trip while data is fresh
        if self._mem_all_markets is not None:
            data, ts = self._mem_all_markets
            if time.monotonic() - ts < CACHE_TTL:
                log.debug("Cache HIT all_open_markets (in-memory)")
                return data

        # Check single stable cache key
        stale_data = None
        db = await get_connection()
        try:
            cursor = await db.execute(
                "SELECT data, fetched_at FROM games_cache WHERE game_id = ?",
                (AGG_MARKETS_CACHE_KEY,),
            )
            row = await cursor.fetchone()
            if row:
                try:
                    fetched_dt = datetime.fromisoformat(row[1])
                    if fetched_dt.tzinfo is None:
                        fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
                    if age < CACHE_TTL:
                        log.debug("Cache HIT all_open_markets (age %.0fs)", age)
                        result = json.loads(row[0])
                        self._mem_all_markets = (result, time.monotonic())
                        return result
                    log.debug("Cache STALE all_open_markets (age %.0fs)", age)
                except (ValueError, TypeError):
                    pass
                stale_data = row[0]
        finally:
            await db.close()

        # Cache miss/stale — paginate all open Kalshi markets in one pass and
        # filter to sports series client-side.  This is 1-2 API calls instead of
        # one call per series (60+), which avoids 429 storms on cold start.
        log.debug("Cache MISS all_open_markets — fetching via pagination")

        # Lock prevents two concurrent callers from both starting a full fetch.
        async with self._markets_fetch_lock:
            # Re-check after acquiring lock in case previous holder already filled it.
            if self._mem_all_markets is not None:
                data, ts = self._mem_all_markets
                if time.monotonic() - ts < CACHE_TTL:
                    return data

            if not SPORTS:
                await self.refresh_sports()

            sports_series = self._sports_series_cache
            if not sports_series:
                log.warning("No sports series known — cannot fetch markets")
                if stale_data:
                    result = [m for m in json.loads(stale_data) if _is_market_active(m) and _is_market_bettable(m)]
                    self._mem_all_markets = (result, time.monotonic())
                    return result
                return []

            session = await self._get_session()
            url = f"{BASE_URL}/markets"
            all_markets: list[dict] = []
            page_cursor = None

            while True:
                params: dict = {"status": "open", "limit": 1000}
                if page_cursor:
                    params["cursor"] = page_cursor

                page_data = None
                async with self._semaphore:
                    for attempt in range(MAX_RETRIES + 1):
                        try:
                            headers = self._auth_headers("GET", url)
                            async with session.get(url, params=params, headers=headers) as resp:
                                if resp.status == 429:
                                    if attempt < MAX_RETRIES:
                                        wait = RETRY_BACKOFF * (2 ** attempt)
                                        log.info("Kalshi 429 (markets page), retrying in %.1fs...", wait)
                                        await asyncio.sleep(wait)
                                        continue
                                    log.warning("Kalshi markets 429 after %d retries", MAX_RETRIES)
                                    break
                                if resp.status != 200:
                                    log.warning("Kalshi markets returned %s", resp.status)
                                    break
                                page_data = await resp.json()
                                break
                        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                            log.warning("Kalshi markets request failed: %s", e)
                            break

                if not page_data or "markets" not in page_data:
                    break

                for m in page_data["markets"]:
                    if m.get("series_ticker") in sports_series:
                        all_markets.append(self._prune_market(m))

                page_cursor = page_data.get("cursor")
                if not page_cursor:
                    break
                await asyncio.sleep(0.2)

            log.info("Fetched %d open sports markets from Kalshi (%d series)", len(all_markets), len(sports_series))
            all_markets = [m for m in all_markets if _is_market_active(m) and _is_market_bettable(m)]
            log.debug("After expiry filter: %d sports markets", len(all_markets))

            if not all_markets:
                if stale_data:
                    log.warning("Kalshi markets fetch returned nothing, using stale cache")
                    result = [m for m in json.loads(stale_data) if _is_market_active(m) and _is_market_bettable(m)]
                    self._mem_all_markets = (result, time.monotonic())
                    return result
                return []

            # Store as single aggregated entry (replaces any previous entry)
            self._mem_all_markets = (all_markets, time.monotonic())
            data_str = json.dumps(all_markets)
            for attempt in range(3):
                db = await get_connection()
                try:
                    await db.execute(
                        "INSERT OR REPLACE INTO games_cache (game_id, sport, data, fetched_at)"
                        " VALUES (?, 'kalshi', ?, datetime('now'))",
                        (AGG_MARKETS_CACHE_KEY, data_str),
                    )
                    await db.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    log.warning("Failed to update aggregated markets cache: %s", e)
                finally:
                    await db.close()

            return all_markets

    async def get_browse_data(self) -> dict[str, list[dict]]:
        """Sports markets grouped by series for /bet browse. Cached 15 min.

        Reuses the sports-only markets cache (get_all_open_markets) — no separate
        full-Kalshi-market fetch. Returns a single "Sports" category containing
        all active sports series, sorted by soonest closing market.

        Returns: {"Sports": [{ticker, label, market_count, markets: [...]}, ...]}
        """
        if self._mem_browse_data is not None:
            data, ts = self._mem_browse_data
            if time.monotonic() - ts < CACHE_TTL:
                log.debug("Cache HIT browse_data (in-memory)")
                return data

        all_markets = await self.get_all_open_sports_markets()

        # Group markets by series_ticker
        series_markets: dict[str, list[dict]] = {}
        for m in all_markets:
            st = m.get("series_ticker", "")
            if not st:
                continue
            if st not in series_markets:
                series_markets[st] = []
            series_markets[st].append(m)

        # Build series list
        series_list: list[dict] = []
        for series_ticker, markets in series_markets.items():
            raw_title = self._series_info.get(series_ticker, {}).get("title") or series_ticker
            label = _LABEL_OVERRIDES.get(series_ticker) or raw_title

            markets_sorted = sorted(
                markets,
                key=lambda m: _earliest_market_time(m) or "9999"
            )

            series_list.append({
                "ticker": series_ticker,
                "label": label,
                "market_count": len(markets),
                "markets": markets_sorted,
            })

        # Sort by soonest closing market
        series_list.sort(key=lambda s: _earliest_market_time(s["markets"][0]) if s["markets"] else "9999")

        category_data: dict[str, list[dict]] = {"Sports": series_list}
        self._mem_browse_data = (category_data, time.monotonic())
        return category_data

    # ── Public API methods ────────────────────────────────────────────

    async def get_markets_by_series(self, series_ticker: str, status: str = "open", limit: int = 100) -> list[dict]:
        """Fetch open markets for a given series ticker."""
        data = await self._cached_request(
            f"{BASE_URL}/markets",
            {"series_ticker": series_ticker, "status": status, "limit": limit},
            prune_func=self._prune_markets_list
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
            prune_func=self._prune_market
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
        """Fetch game markets for a sport and group into game objects efficiently."""
        sport = SPORTS.get(sport_key)
        if not sport:
            return []

        # Fetch ALL open sports markets once (pruned and cached)
        all_markets = await self.get_all_open_sports_markets()
        
        # Get primary series tickers for this sport
        target_series = set(sport["series"].values())
        
        # Filter to markets belonging to this sport
        markets = [m for m in all_markets if m.get("series_ticker") in target_series]
        
        if not markets:
            return []

        # Group markets by event_ticker
        event_groups: dict[str, list[dict]] = {}
        for m in markets:
            et = m.get("event_ticker", "")
            if et not in event_groups:
                event_groups[et] = []
            event_groups[et].append(m)

        # Detect soccer for title parsing
        is_soccer = "soccer" in sport["label"].lower() or "soccer" in sport_key.lower()

        games = []
        now = datetime.now(timezone.utc)
        for event_ticker, group in event_groups.items():
            game = _parse_game_from_markets(group, event_ticker, sport_key, sport["label"], is_soccer)
            if not game:
                continue
            # Skip games whose expected_expiration_time has passed
            exp = game.get("expiration_time", "")
            if exp:
                try:
                    exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                    if now > exp_dt:
                        continue
                except (ValueError, TypeError):
                    pass
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
            american = decimal_to_american(decimal_odds)
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
        """Fetch games across all sports efficiently.

        Uses get_all_open_sports_markets to fetch everything once, then filters and
        groups by event locally. This replaces 140+ individual API calls with 1-2.
        """
        if self._mem_all_games is not None:
            data, ts = self._mem_all_games
            if time.monotonic() - ts < CACHE_TTL:
                return data

        all_markets = await self.get_all_open_sports_markets()
        
        # Build mapping of series_ticker -> (sport_key, sport_label, is_soccer)
        series_to_sport = {}
        for sk, info in SPORTS.items():
            label = info["label"]
            # We need to know if it's soccer for title parsing
            # (Matches logic in get_sport_games)
            # Find a representative market to check title format?
            # Instead, just check if "Soccer" is in label or sport_key
            is_soccer = "soccer" in label.lower() or "soccer" in sk.lower()
            
            for ticker in info["series"].values():
                series_to_sport[ticker] = (sk, label, is_soccer)

        # Group markets by event_ticker
        event_groups: dict[str, list[dict]] = {}
        for m in all_markets:
            st = m.get("series_ticker", "")
            if st in series_to_sport:
                et = m.get("event_ticker", "")
                if et not in event_groups:
                    event_groups[et] = []
                event_groups[et].append(m)

        all_games = []
        now = datetime.now(timezone.utc)
        for event_ticker, group in event_groups.items():
            # Get sport info from the first market in group
            st = group[0].get("series_ticker", "")
            sk, label, is_soccer = series_to_sport[st]
            
            game = _parse_game_from_markets(group, event_ticker, sk, label, is_soccer)
            if not game:
                continue
                
            # Skip games whose expected_expiration_time has passed
            exp = game.get("expiration_time", "")
            if exp:
                try:
                    exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                    if now > exp_dt:
                        continue
                except (ValueError, TypeError):
                    pass
            all_games.append(game)

        all_games.sort(key=lambda g: g.get("commence_time", "9999"))
        self._mem_all_games = (all_games, time.monotonic())
        return all_games

    async def get_available_sports(self) -> list[str]:
        """Return sport keys that currently have open markets."""
        all_markets = await self.get_all_open_sports_markets()
        
        # Get set of all active series tickers
        active_series = {m.get("series_ticker") for m in all_markets}
        
        available = []
        for sk, info in SPORTS.items():
            # If ANY of this sport's series are active, include it
            if any(ticker in active_series for ticker in info["series"].values()):
                available.append(sk)

        return available

    async def get_futures_markets(self, series_ticker: str) -> list[dict]:
        """Fetch open markets for a futures/props series, sorted by probability."""
        data = await self._cached_request(
            f"{BASE_URL}/markets",
            {"series_ticker": series_ticker, "status": "open", "limit": 200},
        )
        if not data or "markets" not in data:
            return []

        options = []
        for m in data["markets"]:
            yes_price = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or "0")
            if not (0.02 <= yes_price <= 0.98):
                continue
            decimal_odds = round(1.0 / yes_price, 3)
            american = decimal_to_american(decimal_odds)
            options.append({
                "ticker": m.get("ticker"),
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

    async def discover_available(self, force: bool = False) -> dict:
        """Discover all available markets — games and futures.

        Returns:
            {
                "games": {sport_key: {"count": N, "next_time": "ISO str" | None}, ...},
                "futures": {sport_key: {name: True, ...}, ...},
            }

        Results are cached as a single entry for DISCOVERY_TTL (10 min).
        """
        # Check for cached discovery result first
        cache_key = "kalshi:discovery:all"
        if not force:
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
                            cached = json.loads(row[0])
                            log.debug(
                                "Discovery cache HIT (age %.0fs): %d games, %d futures",
                                age, len(cached.get("games", {})), len(cached.get("futures", {})),
                            )
                            # Markets cache has a shorter TTL — pre-warm in background
                            # so games load instantly when the user picks a sport.
                            self.schedule_markets_prewarm()
                            return cached
                    except (ValueError, TypeError):
                        pass

        # Ensure SPORTS is populated
        if not SPORTS:
            log.info("SPORTS empty — calling refresh_sports()")
            await self.refresh_sports()

        log.info("Discovery cache miss — fetching open sports markets")
        all_markets = await self.get_all_open_sports_markets()

        # Build reverse mappings for fast lookup
        game_series_to_sport = {}
        for sk, info in SPORTS.items():
            for ticker in info["series"].values():
                game_series_to_sport[ticker] = sk

        futures_series_to_cat = {}
        for sk, info in FUTURES.items():
            for ticker in info["markets"].values():
                futures_series_to_cat[ticker] = sk

        games_available = {}
        futures_available = {}
        now = datetime.now(timezone.utc)

        for m in all_markets:
            st = m.get("series_ticker", "")
            
            # Check if it's a game market (could be Game, Spread, or Total)
            if st in game_series_to_sport:
                sk = game_series_to_sport[st]
                exp = m.get("expected_expiration_time") or m.get("close_time", "")
                
                # Only count markets whose expiration is still in the future —
                # mirrors the `if now > exp_dt: continue` check in get_sport_games
                # so discovery never shows a sport whose games have all ended.
                if exp:
                    try:
                        exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                        if exp_dt > now:
                            if sk not in games_available:
                                games_available[sk] = {"next_time": None}
                            current_next = games_available[sk]["next_time"]
                            if not current_next or exp < current_next:
                                games_available[sk]["next_time"] = exp
                    except (ValueError, TypeError):
                        # Unparseable expiry — include the sport conservatively
                        if sk not in games_available:
                            games_available[sk] = {"next_time": None}

            # Check if it's a futures market
            elif st in futures_series_to_cat:
                cat_key = futures_series_to_cat[st]
                if cat_key not in futures_available:
                    futures_available[cat_key] = {}
                # Match market name from FUTURES dict
                for name, ticker in FUTURES[cat_key]["markets"].items():
                    if ticker == st:
                        futures_available[cat_key][name] = True
                        break

        result = {"games": games_available, "futures": futures_available}
        game_names = [SPORTS.get(k, {}).get("label", k) for k in games_available]
        log.info(
            "Discovery complete: %d game sports (%s), %d futures sports",
            len(games_available),
            ", ".join(game_names[:10]) + ("..." if len(game_names) > 10 else ""),
            len(futures_available),
        )

        # Cache the full discovery result (best-effort — don't crash discovery on lock)
        data_str = json.dumps(result)
        for attempt in range(3):
            db = await get_connection()
            try:
                await db.execute(
                    "INSERT OR REPLACE INTO games_cache (game_id, sport, data, fetched_at)"
                    " VALUES (?, 'kalshi', ?, datetime('now'))",
                    (cache_key, data_str),
                )
                await db.commit()
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                log.warning("Failed to cache discovery result: %s", e)
                break
            finally:
                await db.close()

        return result


# Singleton instance
kalshi_api = KalshiAPI()
