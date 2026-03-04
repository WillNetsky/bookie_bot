import asyncio
import logging
import re
from datetime import datetime, time, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.services import betting_service, leaderboard_notifier
from bot.services.kalshi_api import (
    kalshi_api, SPORTS, FUTURES,
    _parse_event_ticker_date
)
from bot.constants import PICK_EMOJI, PICK_LABELS
from bot.utils import (
    format_matchup, format_game_time, format_pick_label,
    format_american, decimal_to_american
)
from bot.db.database import cleanup_cache, vacuum_db

log = logging.getLogger(__name__)


async def _safe_defer(interaction: discord.Interaction, *, edit: bool = False) -> bool:
    """Defer an interaction, returning False if it cannot be deferred."""
    try:
        await interaction.response.defer()
        return True
    except discord.errors.NotFound:
        # 10062 — interaction expired before we could defer (took > 3s to get here)
        cmd = interaction.command
        log.warning("Interaction expired before defer (command: %s)", cmd.name if cmd else "unknown")
        return False
    except discord.errors.HTTPException as e:
        # 40060 — already acknowledged, usually means two bot instances running
        cmd = interaction.command
        log.warning("Interaction already acknowledged (command: %s, code: %s)", cmd.name if cmd else "unknown", e.code)
        return False


def _sport_emoji(sport_key: str) -> str:
    """Return an appropriate emoji for a sport based on its ticker."""
    sk = sport_key.upper()
    if "MLB" in sk or "WBC" in sk or "NCAABB" in sk or "COLLEGEBASEBALL" in sk:
        return "\u26be"  # ⚾ — must be before NCAAB (basketball) check
    if "NBA" in sk or "WNBA" in sk or "NCAAMB" in sk or "NCAAWB" in sk or "NCAAB" in sk or "NBL" in sk or "EUROLEAGUE" in sk or "EUROCUP" in sk or "ACB" in sk or "BSL" in sk or "KBL" in sk or "BBL" in sk or "FIBA" in sk or "ABA" in sk or "GBL" in sk or "VTB" in sk or "CBA" in sk or "UNRIVALED" in sk or "ARGLNB" in sk or "JBLEAGUE" in sk or "BBSERIEA" in sk or "LNBELITE" in sk:
        return "\U0001f3c0"  # 🏀
    if "NFL" in sk or "NCAAF" in sk:
        return "\U0001f3c8"  # 🏈
    if "NHL" in sk or "AHL" in sk or "KHL" in sk or "IIHF" in sk or "SHL" in sk or "DEL" in sk or "LIIGA" in sk or "ELH" in sk or "NCAAHOCKEY" in sk or "SWISSLEAGUE" in sk or "SWISSNL" in sk or "HOCKEYALLSVENSKAN" in sk or "NLA" in sk or "NLB" in sk or "KXNL" in sk:
        return "\U0001f3d2"  # 🏒
    if "UFC" in sk or "BOXING" in sk or "MMA" in sk or "FIGHT" in sk or "MCGREGOR" in sk:
        return "\U0001f94a"  # 🥊
    if "LAX" in sk or "LACROSSE" in sk:
        return "\U0001f94d"  # 🥍
    if "CRICKET" in sk or "IPL" in sk or "WPL" in sk:
        return "\U0001f3cf"  # 🏏
    if "TENNIS" in sk or "ATP" in sk or "WTA" in sk or "DAVISCUP" in sk or "UNITEDCUP" in sk or "SIXKINGS" in sk or "CHALLENGER" in sk:
        return "\U0001f3be"  # 🎾
    if "CURL" in sk:
        return "\U0001f94c"  # 🥌
    if "CHESS" in sk:
        return "\u265f\ufe0f"  # ♟️
    if "PICKLE" in sk:
        return "\U0001f3d3"  # 🏓
    if "RUGBY" in sk or "SIXNATIONS" in sk or "NRL" in sk:
        return "\U0001f3c9"  # 🏉
    if "DARTS" in sk:
        return "\U0001f3af"  # 🎯
    if "NASCAR" in sk or "RACE" in sk:
        return "\U0001f3ce\ufe0f"  # 🏎️
    if "GOLF" in sk or "TGL" in sk or "PGA" in sk or "RYDER" in sk:
        return "\u26f3"  # ⛳
    if "CRICKET" in sk or "IPL" in sk or "WPL" in sk or "T20" in sk or "ODI" in sk or "SSHIELD" in sk:
        return "\U0001f3cf"  # 🏏
    if "CS2" in sk or "CSGO" in sk or "VALORANT" in sk or "LOL" in sk or "DOTA" in sk or "OW" in sk or "R6" in sk or "COD" in sk or "ESPORT" in sk:
        return "\U0001f3ae"  # 🎮
    if "BEAST" in sk or "FANATICS" in sk:
        return "\U0001f3ac"  # 🎬
    # Soccer — explicit leagues + generic "LEAGUE"/"CUP" after all non-soccer sports caught
    if (
        "EPL" in sk or "PREMIER" in sk
        or "LALIGA" in sk or "BUNDESLIGA" in sk
        or "SERIEA" in sk or "SERIEB" in sk
        or "LIGUE" in sk
        or "MLS" in sk or "NWSL" in sk
        or "UCL" in sk or "UEL" in sk or "UECL" in sk
        or "UEFA" in sk or "CONCACAF" in sk
        or "WORLDCUP" in sk or "EUROCUP" in sk
        or "EREDIVISIE" in sk or "EREDIV" in sk
        or "CHAMPIONSHIP" in sk
        or "ALEAGUE" in sk or "JLEAGUE" in sk or "KLEAGUE" in sk
        or "AFC" in sk   # AFC Champions League (AFC ≠ NFL conference at this point)
        or "ACL" in sk   # Asian Champions League (alternate ticker)
        or "COUPE" in sk # Coupe de France, Coupe de la Ligue, etc.
        or "COPPA" in sk # Coppa Italia
        or "PRIMERA" in sk or "DIVISION" in sk  # Argentina Primera, various divisions
        or "SOCCER" in sk
        or "LEAGUE" in sk  # catches most leagues not listed above
        or "CUP" in sk     # FA Cup, KNVB Cup, etc. — all specific cup sports caught above
        or "COPA" in sk or "COPALIB" in sk
        or "SUPERLIGA" in sk or "SUPERLEAGUE" in sk or "SUPERLIG" in sk
        or "DIMAYOR" in sk  # Liga DIMAYOR (Colombian soccer)
        or "LIGA" in sk  # LALIGA already above, also catches DIMAYOR, BBVA, etc.
        or "HNL" in sk      # Croatia HNL
        or "SCOTTISH" in sk or "SCOTPREM" in sk  # Scottish Premiership / Cup
        or "GREECE" in sk or "GREEK" in sk or "GRC" in sk  # Super League Greece
        or "ARGENT" in sk   # Argentina Primera Division
    ):
        return "\u26bd"  # ⚽
    return "\U0001f3c6"  # 🏆 — Other / truly unrecognized


# Maps sport aliases (text or emoji) → the canonical emoji returned by _sport_emoji.
# Used so `/bet baseball` or `/bet ⚾` returns every baseball market regardless of
# which series ticker it comes from.
_SPORT_ALIASES: dict[str, str] = {
    # ── text aliases ───────────────────────────────────────────────────
    "basketball":       "🏀",
    "nba":              "🏀",
    "wnba":             "🏀",
    "college basketball": "🏀",
    "ncaa basketball":  "🏀",
    "football":         "🏈",
    "nfl":              "🏈",
    "college football": "🏈",
    "ncaa football":    "🏈",
    "baseball":         "⚾",
    "mlb":              "⚾",
    "hockey":           "🏒",
    "nhl":              "🏒",
    "ice hockey":       "🏒",
    "ufc":              "🥊",
    "boxing":           "🥊",
    "mma":              "🥊",
    "fighting":         "🥊",
    "soccer":           "⚽",
    "lacrosse":         "🥍",
    "cricket":          "🏏",
    "tennis":           "🎾",
    "curling":          "🥌",
    "chess":            "♟️",
    "pickleball":       "🏓",
    "rugby":            "🏉",
    "darts":            "🎯",
    "nascar":           "🏎️",
    "racing":           "🏎️",
    "golf":             "⛳",
    "esports":          "🎮",
    "gaming":           "🎮",
    # ── emoji aliases (direct emoji input) ─────────────────────────────
    "🏀": "🏀",
    "🏈": "🏈",
    "⚾": "⚾",
    "🏒": "🏒",
    "🥊": "🥊",
    "🥍": "🥍",
    "🏏": "🏏",
    "🎾": "🎾",
    "🥌": "🥌",
    "♟️": "♟️",
    "🏓": "🏓",
    "🏉": "🏉",
    "🎯": "🎯",
    "🏎️": "🏎️",
    "⛳": "⛳",
    "🎮": "🎮",
    "⚽": "⚽",
}

# Human-readable sport labels keyed by sport emoji (used for sport-selector display).
_SPORT_EMOJI_LABEL: dict[str, str] = {
    "🏀": "Pro Basketball",
    "🏈": "Football",
    "⚾": "Baseball",
    "🏒": "Hockey",
    "🥊": "Combat Sports",
    "⚽": "Soccer",
    "🎾": "Tennis",
    "🥍": "Lacrosse",
    "🏏": "Cricket",
    "🥌": "Curling",
    "♟️": "Chess",
    "🏓": "Pickleball",
    "🏉": "Rugby",
    "🎯": "Darts",
    "🏎️": "Racing",
    "⛳": "Golf",
    "🎮": "Esports",
    "🎬": "Entertainment",
    "🏆": "Other",
}


def _is_ended(expiration_time: str) -> bool:
    """Check if a game has ended based on expected_expiration_time."""
    if not expiration_time:
        return False
    try:
        et = datetime.fromisoformat(expiration_time.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) > et
    except (ValueError, TypeError):
        return False


def _parse_iso_dt(s: str | None) -> "datetime | None":
    """Parse an ISO datetime string (with or without Z) into a UTC-aware datetime."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _fmt_eta(dt: "datetime | None", now: datetime, verb: str = "closes") -> str:
    """Return a human-readable ETA string like 'closes in 2h 15m' or 'LIVE'."""
    if dt is None:
        return ""
    diff = (dt - now).total_seconds()
    if diff <= 0:
        return "LIVE"
    elif diff < 3600:
        m = int(diff // 60)
        return f"{verb} in {m}m"
    elif diff < 86400:
        h = int(diff // 3600)
        m = int((diff % 3600) // 60)
        return f"{verb} in {h}h {m}m" if m else f"{verb} in {h}h"
    else:
        d = int(diff // 86400)
        h = int((diff % 86400) // 3600)
        return f"{verb} in {d}d {h}h" if h else f"{verb} in {d}d"


def _format_game_time(commence_time: str) -> str:
    """Format game time for display. Shows the event date from the ticker."""
    if not commence_time:
        return "TBD"
    return format_game_time(commence_time)


def _earliest_market_time(m: dict) -> str:
    """Return the earlier of close_time / expected_expiration_time as an ISO string.

    Different market types use these fields differently — some sports set
    close_time to today (game start) but expected_expiration_time to a
    tournament end date weeks away, and others do the reverse. The sooner
    of the two is always the actionable deadline for the user.
    """
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


def _calc_cashout(bet: dict, market: dict) -> float | None:
    """Calculate the current cash-out value for a Kalshi bet.

    When selling a YES position you receive the BID price (what buyers offer),
    not the ASK price (what sellers want).  For a NO position the exit price is
    1 - YES_ask, which equals the NO bid.

    Returns None if market price data is unavailable or out of valid range.
    """
    pick = (bet.get("pick") or "").lower()

    if pick == "yes":
        # Prefer bid (what you actually receive when selling YES)
        raw = (
            market.get("yes_bid_dollars")
            or market.get("yes_ask_dollars")
            or market.get("last_price_dollars")
        )
    else:
        # NO exit price = 1 − YES_ask  (= NO bid)
        raw = market.get("yes_ask_dollars") or market.get("last_price_dollars")

    if raw is None:
        return None
    try:
        price = float(raw)
    except (ValueError, TypeError):
        return None

    if not (0.01 <= price <= 0.99):
        return None

    current_price = price if pick == "yes" else (1.0 - price)
    # n_contracts = amount ÷ entry_price = amount × entry_decimal_odds
    n_contracts = bet["amount"] * bet["odds"]
    return max(round(n_contracts * current_price, 2), 0)



_NUMBER_RE = re.compile(r'\b\d+\.?\d*\b')


def _teams_from_event_ticker(event_ticker: str) -> tuple[str, str] | None:
    """Extract (away, home) team codes from a hyphen-delimited event ticker.

    Works for tickers like "KXNHLGAMETOTAL-26FEB28-COL-ANA" where the last
    two segments are uppercase team abbreviations.  Length limit is 10 to
    accommodate esports team names (e.g. OXUJI=5, PHANTOM=7).
    """
    parts = event_ticker.split("-")
    if len(parts) >= 4:
        away, home = parts[-2], parts[-1]
        if away.isalpha() and home.isalpha() and 2 <= len(away) <= 10 and 2 <= len(home) <= 10:
            return away, home
    return None


_DATE_SEGMENT_RE = re.compile(r'^\d{2}[A-Za-z]{3}\d+$')


def _teams_from_event_ticker_flexible(event_ticker: str) -> tuple[str, str] | None:
    """Like _teams_from_event_ticker but handles player-prop suffixes.

    For tickers like KXNBA-26MAR5-LAL-GSW-LEBRONPOINTS the standard function
    fails because parts[-1] is not a team code. This version scans forward past
    the date segment and picks the first two 2-4 char alpha team codes found.
    """
    teams = _teams_from_event_ticker(event_ticker)
    if teams:
        return teams
    parts = event_ticker.split("-")
    found: list[str] = []
    past_date = False
    for p in parts[1:]:  # skip series prefix
        if not past_date:
            if _DATE_SEGMENT_RE.match(p):
                past_date = True
            continue
        if p.isalpha() and 2 <= len(p) <= 4 and p.isupper():
            found.append(p)
            if len(found) == 2:
                return (found[0], found[1])
    return None


# Esports per-map/set qualifier segments: M1, MAP2, SET3, GAME1, ROUND2, etc.
# Deliberately excludes single-letter G/R/Q so team codes like G2 and R6 are kept.
_MAP_QUAL_RE = re.compile(r'^(?:MAP|SET|GAME|ROUND|M)\d+$', re.IGNORECASE)


def _extract_game_fingerprint(event_ticker: str) -> str | None:
    """Return a stable game key by stripping the series prefix and map qualifiers.

    e.g. KXNBA-26MAR04-LAL-GSW and KXNBAGAMETOTAL-26MAR04-LAL-GSW → '26MAR04-LAL-GSW'
         KXNBA-26MAR04-LAL-GSW-LEBRONPOINTS → '26MAR04-LAL-GSW'  (player prop trimmed)
         KXLOL-26MAR04-M1-BLG-JDG (map between date and teams) → '26MAR04-BLG-JDG'
         KXASOCCER-20250304-MACARTHUR-CENTRALCOAST → '20250304-MACARTHUR-CENTRALCOAST'

    Returns None only if the ticker has no '-' at all.
    """
    if "-" not in event_ticker:
        return None
    suffix = event_ticker.split("-", 1)[1]      # drop series prefix
    parts = suffix.split("-")
    # Remove map/set/game qualifier segments from anywhere in the suffix
    # (M1, MAP2, SET3, GAME1, ROUND1, etc.) so per-map markets group with the match.
    # G2/T1/R6 are intentionally NOT matched — they could be esports team codes.
    parts = [p for p in parts if not _MAP_QUAL_RE.match(p)]
    return "-".join(parts[:3]) if parts else None


# Matches "Set 1", "Map 2", "Round 3" inside market titles
_QUALIFIER_RE = re.compile(r'\b(Set|Map|Round)\s+(\d+)\b', re.IGNORECASE)


def _clean_market_title(title: str) -> str:
    """Clean a raw Kalshi market title for compact display.

    - Strips trailing "Winner?" / "winner?"
    - Strips any remaining trailing "?"
    - Moves "Set N" / "Map N" / "Round N" to a "(Set N)" prefix so the
      matchup reads naturally.

    Examples:
      "Djokovic vs Medvedev Winner?"      → "Djokovic vs Medvedev"
      "Djokovic vs Medvedev Set 1 Winner?"→ "(Set 1) Djokovic vs Medvedev"
      "Team A vs Team B Map 2 Winner?"    → "(Map 2) Team A vs Team B"
      "Team A wins?"                      → "Team A wins"
    """
    if not title:
        return title
    t = title.strip()
    # Strip trailing "Winner?" (case-insensitive, "?" optional)
    t = re.sub(r'\s*[Ww]inner\s*\??\s*$', '', t).strip()
    # Strip trailing " Game" / " Match" / " Fight" (series name bleed-through)
    t = re.sub(r'\s+(Game|Match|Fight)$', '', t, flags=re.IGNORECASE).strip()
    # Strip any leftover trailing "?"
    t = re.sub(r'\?+$', '', t).strip()
    # Hoist set/map/round qualifier to a "(X N)" prefix
    qm = _QUALIFIER_RE.search(t)
    if qm:
        qualifier = f"({qm.group(1).capitalize()} {qm.group(2)})"
        rest = (t[:qm.start()] + t[qm.end():]).strip()
        t = f"{qualifier} {rest}".strip() if rest else qualifier
    return t or title


def _group_markets_by_prop(markets: list[dict]) -> list[dict]:
    """Group markets that belong to the same game/event into a single entry.

    Primary key is event_ticker — all outcomes for the same game (e.g.
    home win / away win / tie, or all spread lines) share one event_ticker.
    Falls back to ticker (always unique) so ungroupable markets stay single.

    Returns a list of items, each either:
      {"type": "single", "market": {...}}
      {"type": "group", "label": str, "subtitle": str, "markets": [...], "count": int}
    """
    from collections import OrderedDict
    event_map: dict[str, list[dict]] = OrderedDict()
    for m in markets:
        key = m.get("event_ticker") or m.get("ticker", "")
        event_map.setdefault(key, []).append(m)

    result: list[dict] = []
    for key, group in event_map.items():
        if len(group) == 1:
            result.append({"type": "single", "market": group[0]})
        else:
            group_sorted = sorted(
                group,
                key=lambda m: _earliest_market_time(m) or "9999"
            )
            # Game title (shared across the group)
            game_title = group_sorted[0].get("title") or key
            # Collect sub-titles to distinguish threshold vs outcome groups
            sub_titles = [m.get("yes_sub_title") or "" for m in group_sorted]
            has_numbers = any(_NUMBER_RE.search(s) for s in sub_titles if s)
            if has_numbers:
                # Threshold variants — show numeric range (e.g. "Points Over 10.5–15.5")
                nums_per = [_NUMBER_RE.findall(s) for s in sub_titles if s]
                all_nums = [float(n[0]) for n in nums_per if n]
                if all_nums:
                    lo, hi = min(all_nums), max(all_nums)
                    first_sub = next((s for s in sub_titles if _NUMBER_RE.search(s)), "")
                    threshold_label = _NUMBER_RE.sub("#", first_sub, count=1).replace("#", f"{lo}–{hi}", 1)
                else:
                    threshold_label = game_title
                teams = _teams_from_event_ticker(key)
                label = f"{teams[0]} @ {teams[1]} · {threshold_label}" if teams else threshold_label
                subtitle = f"{len(group_sorted)} thresholds"
            else:
                # Game outcomes (win/loss/tie, team names, etc.)
                label = _clean_market_title(game_title)
                # For player props / non-outcome groups, add team context if not already present
                if " at " not in label.lower() and " vs " not in label.lower() and " @ " not in label:
                    teams = _teams_from_event_ticker(key)
                    if teams:
                        label = f"{teams[0]} @ {teams[1]} · {label}"
                subs = [s for s in sub_titles[:3] if s]
                subtitle = " · ".join(subs) if subs else f"{len(group_sorted)} options"
            result.append({
                "type": "group",
                "label": label,
                "subtitle": subtitle,
                "markets": group_sorted,
                "count": len(group_sorted),
            })
    return result


def _is_nba_market(m: dict) -> bool:
    sk = _market_series_ticker(m).upper()
    return "NBA" in sk or "WNBA" in sk


def _group_markets_by_sport(markets: list[dict]) -> list[dict]:
    """Group markets by sport (same emoji key), sorted by most games first.

    NBA and WNBA are split out from the rest of basketball and shown as their
    own "NBA" group; remaining basketball leagues appear as "International Basketball".
    """
    sport_map: dict[str, list[dict]] = {}
    for m in markets:
        emoji = _sport_emoji(_market_series_ticker(m).upper())
        sport_map.setdefault(emoji, []).append(m)
    result = []
    for emoji, sport_markets in sport_map.items():
        if emoji == "🏀":
            nba_markets = [m for m in sport_markets if _is_nba_market(m)]
            intl_markets = [m for m in sport_markets if not _is_nba_market(m)]
            for sub_markets, sub_label in (
                (nba_markets, "NBA"),
                (intl_markets, "International Basketball"),
            ):
                if not sub_markets:
                    continue
                game_keys = {
                    _extract_game_fingerprint(m.get("event_ticker", "")) or m.get("event_ticker", "")
                    for m in sub_markets
                }
                result.append({
                    "emoji": emoji,
                    "label": sub_label,
                    "markets": sub_markets,
                    "game_count": len(game_keys),
                })
        else:
            game_keys = {
                _extract_game_fingerprint(m.get("event_ticker", "")) or m.get("event_ticker", "")
                for m in sport_markets
            }
            label = _SPORT_EMOJI_LABEL.get(emoji, emoji)
            result.append({
                "emoji": emoji,
                "label": label,
                "markets": sport_markets,
                "game_count": len(game_keys),
            })
    result.sort(key=lambda x: x["game_count"], reverse=True)
    return result


def _group_markets_by_game(markets: list[dict]) -> list[dict]:
    """Group markets by game (same matchup across different series), sorted by start time."""
    from collections import OrderedDict
    game_map: dict[str, list[dict]] = OrderedDict()
    for m in markets:
        et = m.get("event_ticker", "")
        key = _extract_game_fingerprint(et) or et
        game_map.setdefault(key, []).append(m)
    result = []
    for key, game_markets in game_map.items():
        sorted_markets = sorted(game_markets, key=lambda m: _earliest_market_time(m) or "9999")
        first = sorted_markets[0]
        teams = _teams_from_event_ticker_flexible(first.get("event_ticker", ""))
        if teams:
            label = f"{teams[0]} @ {teams[1]}"
        else:
            label = _best_game_label(sorted_markets)
        exp = _earliest_market_time(first)
        result.append({
            "key": key,
            "label": label,
            "time": exp,
            "time_str": _format_game_time(exp) if exp else "",
            "markets": sorted_markets,
            "market_count": len(sorted_markets),
        })
    result.sort(key=lambda g: g["time"] or "9999")
    return result


def _best_game_label(markets: list[dict]) -> str:
    """Build the cleanest possible game label from a group of related markets.

    Strategy (in order):
    1. Use yes_sub_title values as team names (most reliable — e.g. "JD Gaming", "BLG")
    2. Extract the "X vs. Y" portion out of verbose market titles
    3. Fall back to the shortest title, stripped of leading map/set qualifiers
    """
    # 1. Collect unique non-numeric yes_sub_title values as team names
    seen: set[str] = set()
    teams: list[str] = []
    for m in markets:
        s = (m.get("yes_sub_title") or "").strip()
        if s and not re.search(r'\d', s) and s.lower() not in ("tie", "draw", "yes", "no") and s not in seen:
            seen.add(s)
            teams.append(s)
        if len(teams) == 2:
            break
    if len(teams) == 2:
        return f"{teams[0]} vs. {teams[1]}"

    # 2. Find "X vs. Y" or "X at Y" in market titles and extract just the matchup
    titles = sorted([m.get("title") or "" for m in markets if m.get("title")], key=len)
    for t in titles:
        for sep in (" vs. ", " vs ", " at "):
            if sep not in t:
                continue
            idx = t.index(sep)
            before = t[:idx]
            after = t[idx + len(sep):]
            # Strip leading filler ("Will X win the", "in the", etc.)
            for filler in (" in the ", " in ", " the "):
                pos = before.rfind(filler)
                if pos != -1:
                    before = before[pos + len(filler):]
                    break
            # Strip trailing game-name / map noise from team_b
            after = re.sub(r'\s+(?:match|game|series|Map\s+\d+|CS2|League of Legends|Dota\s*2?|Valorant|R6|COD).*$', '', after, flags=re.IGNORECASE).strip()
            if before.strip() and after:
                return f"{before.strip()}{sep}{after}"

    # 3. Last resort: shortest title, removing any leading (Map X) / (Set X) qualifier
    if titles:
        cleaned = _clean_market_title(titles[0])
        cleaned = re.sub(r'^\((?:Map|Set|Round|Game)\s+\d+\)\s*', '', cleaned)
        return cleaned or titles[0]
    return "?"


# ── Helper functions ───────────────────────────────────────────────────


def _market_series_ticker(m: dict) -> str:
    """Return the series ticker for a market, falling back to the event_ticker prefix."""
    st = m.get("series_ticker") or ""
    if not st:
        et = m.get("event_ticker") or ""
        st = et.split("-")[0] if "-" in et else et
    return st.lower()


def _is_close_market(m: dict, max_pct: float) -> bool:
    """Return True if the market's implied probability is within [1-max_pct, max_pct].

    E.g. max_pct=0.60 keeps markets priced between 40¢ and 60¢ (≤60/40 odds).
    """
    price_raw = m.get("yes_ask_dollars") or m.get("last_price_dollars")
    if price_raw is None:
        return False
    try:
        p = float(price_raw)
    except (ValueError, TypeError):
        return False
    return (1.0 - max_pct) <= p <= max_pct


def _resolve_kalshi_bet(
    pick_key: str, kalshi_markets: dict, odds_entry: dict
) -> tuple[str | None, str]:
    """Map a sports-style pick to a Kalshi market_ticker and yes/no pick."""
    if pick_key == "home":
        m = kalshi_markets.get("home")
        return (m["ticker"], "yes") if m else (None, "yes")
    elif pick_key == "away":
        m = kalshi_markets.get("away")
        return (m["ticker"], "yes") if m else (None, "yes")
    elif pick_key in ("spread_home", "spread_away", "over", "under"):
        ticker = odds_entry.get("_market_ticker")
        if ticker:
            kalshi_pick = odds_entry.get("_kalshi_pick", "yes")
            return (ticker, kalshi_pick)
        if pick_key == "spread_home":
            m = kalshi_markets.get("home")
        elif pick_key == "spread_away":
            m = kalshi_markets.get("away")
        else:
            m = kalshi_markets.get("home")
        return (m["ticker"], "yes") if m else (None, "yes")
    return (None, "yes")


def _build_pick_display(pick_key: str, home: str, away: str, odds_entry: dict) -> str:
    """Build a human-readable pick display string."""
    point = odds_entry.get("point")
    if pick_key == "home":
        return f"{home} ML"
    elif pick_key == "away":
        return f"{away} ML"
    elif pick_key == "spread_home":
        return f"{home} {point:+g}" if point is not None else f"{home} Spread"
    elif pick_key == "spread_away":
        return f"{away} {point:+g}" if point is not None else f"{away} Spread"
    elif pick_key == "over":
        return f"Over {point:g}" if point is not None else "Over"
    elif pick_key == "under":
        return f"Under {point:g}" if point is not None else "Under"
    return pick_key.capitalize()


# ── Games List View (for /live) ───────────────────────────────────────
# Display-only view showing live game scores with pagination.

GAMES_PER_PAGE = 10


class GamesListView(discord.ui.View):
    """Paginated live game scores display."""

    def __init__(
        self,
        all_games: list[dict],
        title: str,
        is_live: bool = False,
        scores: dict[str, dict] | None = None,
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.all_games = all_games
        self.title = title
        self.is_live_mode = is_live
        self.scores = scores or {}
        self.page = page
        self.message: discord.Message | None = None

        total_pages = max(1, (len(all_games) + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        if total_pages > 1:
            if page > 0:
                self.add_item(GamesPageButton("prev", page - 1, row=0))
            if page < total_pages - 1:
                self.add_item(GamesPageButton("next", page + 1, row=0))

    def _find_score(self, game: dict) -> dict | None:
        if not self.scores:
            return None
        home = game.get("home_team", "").lower()
        away = game.get("away_team", "").lower()
        for score_data in self.scores.values():
            s_home = (score_data.get("home_team") or "").lower()
            s_away = (score_data.get("away_team") or "").lower()
            if (home in s_home or s_home in home) and (away in s_away or s_away in away):
                return score_data
        return None

    def build_embed(self) -> discord.Embed:
        total = len(self.all_games)
        total_pages = max(1, (total + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        start = self.page * GAMES_PER_PAGE
        page_games = self.all_games[start:start + GAMES_PER_PAGE]

        color = discord.Color.red() if self.is_live_mode else discord.Color.blue()
        embed = discord.Embed(title=self.title, color=color)

        if not page_games:
            embed.description = "No live games right now." if self.is_live_mode else "No upcoming games."
            return embed

        lines = []
        for g in page_games:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            sport = g.get("sport_title", "")
            score = self._find_score(g) if self.is_live_mode else None
            if score and score.get("started") and score.get("home_score") is not None:
                score_str = f"**{away}** {score['away_score']} - {score['home_score']} **{home}**"
                if score.get("completed"):
                    score_str += "  (Final)"
                else:
                    score_str = f"\U0001f534 {score_str}"
                time_parts = []
                if g.get("commence_time"):
                    time_parts.append(f"Started {format_game_time(g['commence_time'])}")
                if g.get("expiration_time"):
                    time_parts.append(f"Ends ~{format_game_time(g['expiration_time'])}")
                detail = sport
                if time_parts:
                    detail += f"\n{' · '.join(time_parts)}"
                lines.append(f"{score_str}\n{detail}")
            else:
                time_str = _format_game_time(g.get("commence_time", ""))
                lines.append(f"**{away}** @ **{home}**\n{sport} · {time_str}")

        embed.description = "\n\n".join(lines)
        parts = []
        if total_pages > 1:
            parts.append(f"Page {self.page + 1}/{total_pages}")
        parts.append(f"{total} game{'s' if total != 1 else ''}")
        embed.set_footer(text=" · ".join(parts))
        return embed

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class GamesPageButton(discord.ui.Button["GamesListView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        new_view = GamesListView(
            all_games=view.all_games,
            title=view.title,
            is_live=view.is_live_mode,
            scores=view.scores,
            page=self.target_page,
        )
        embed = new_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=new_view)
        except discord.NotFound:
            pass


# ── Market List View (for /games) ─────────────────────────────────────
# Shows raw Kalshi markets sorted by closest expiry with YES/NO betting.

MARKETS_PER_PAGE = 10
THRESHOLD_PER_PAGE = 20
GAMES_PER_PAGE = 10


def _market_odds_str(m: dict) -> tuple[str, str]:
    """Return (yes_american, no_american) strings for display, or ("?", "?")."""
    yes_ask = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)
    if 0 < yes_ask < 1:
        yes_am = format_american(decimal_to_american(round(1.0 / yes_ask, 3)))
        no_am  = format_american(decimal_to_american(round(1.0 / (1.0 - yes_ask), 3)))
        return yes_am, no_am
    return "?", "?"


def _price_to_american(price: float) -> str:
    """Convert a Kalshi yes price (0–1) to a formatted American odds string."""
    if 0 < price < 1:
        return format_american(decimal_to_american(round(1.0 / price, 3)))
    return "?"


def _series_label(series_ticker: str) -> str:
    """Return a human-readable sport label for a series ticker, or ''."""
    for info in SPORTS.values():
        if series_ticker in info["series"].values():
            return info["label"]
    return ""


class MarketListView(discord.ui.View):
    """Paginated list of raw Kalshi markets sorted by soonest expiry."""

    def __init__(self, markets: list[dict], page: int = 0, timeout: float = 180.0, parlay_legs: list[dict] | None = None, game_back: dict | None = None) -> None:
        super().__init__(timeout=timeout)
        self.markets = markets
        self.page = page
        self.parlay_legs = parlay_legs
        self._game_back = game_back
        self.message: discord.Message | None = None
        self._grouped = _group_markets_by_prop(markets)
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self._grouped) + MARKETS_PER_PAGE - 1) // MARKETS_PER_PAGE)
        start = self.page * MARKETS_PER_PAGE
        page_items = self._grouped[start:start + MARKETS_PER_PAGE]

        if page_items:
            self.add_item(MarketGroupedDropdown(page_items, start, self, row=0))
        if self.page > 0:
            self.add_item(MarketPageButton("prev", self.page - 1, row=1))
        if self.page < total_pages - 1:
            self.add_item(MarketPageButton("next", self.page + 1, row=1))
        parlay_row = 2
        if self._game_back:
            self.add_item(BackToGamesButton(self._game_back, row=2))
            parlay_row = 3
        if self.parlay_legs is not None:
            if self.parlay_legs:
                self.add_item(ParlayViewSlipButton(self.parlay_legs, row=parlay_row))
            if len(self.parlay_legs) >= 2:
                self.add_item(ParlayPlaceFromListButton(self.parlay_legs, row=parlay_row))

    def build_embed(self) -> discord.Embed:
        total = len(self._grouped)
        total_pages = max(1, (total + MARKETS_PER_PAGE - 1) // MARKETS_PER_PAGE)
        start = self.page * MARKETS_PER_PAGE
        page_items = self._grouped[start:start + MARKETS_PER_PAGE]

        embed = discord.Embed(title="\U0001f4ca Open Markets", color=discord.Color.blue())
        if not page_items:
            embed.description = "No open markets right now."
            return embed

        lines = []
        for item in page_items:
            if item["type"] == "single":
                m = item["market"]
                title = _clean_market_title(m.get("title") or "?")
                exp = _earliest_market_time(m)
                sport = _series_label(m.get("series_ticker") or "")
                time_str = _format_game_time(exp) if exp else "TBD"
                header = f"**{title}**"
                if sport:
                    header += f"  _{sport}_"
                yes_am, no_am = _market_odds_str(m)
                odds_str = f"YES {yes_am} / NO {no_am}"
                lines.append(f"{header}\n{time_str} · {odds_str}")
            else:
                label = item["label"]
                count = item["count"]
                first_m = item["markets"][0]
                exp = _earliest_market_time(first_m)
                sport = _series_label(first_m.get("series_ticker") or "")
                time_str = _format_game_time(exp) if exp else "TBD"
                sport_emoji = _sport_emoji(item["markets"][0].get("series_ticker") or "")
                header = f"{sport_emoji} **{label}**"
                if sport:
                    header += f"  _{sport}_"
                # Build per-outcome American odds string (e.g. "Lakers -120 · Warriors +100")
                parts = []
                for mkt in item["markets"][:3]:
                    sub = mkt.get("yes_sub_title") or ""
                    price = mkt.get("yes_ask_dollars") or mkt.get("last_price_dollars")
                    try:
                        am = _price_to_american(float(price)) if price is not None else None
                    except (TypeError, ValueError):
                        am = None
                    if sub and am is not None:
                        parts.append(f"{sub} {am}")
                    elif sub:
                        parts.append(sub)
                subtitle = " · ".join(parts) if parts else item.get("subtitle", f"{count} options")
                lines.append(f"{header}\n{time_str} · {subtitle}")

        embed.description = "\n\n".join(lines)
        if self.parlay_legs is not None:
            n = len(self.parlay_legs)
            embed.set_footer(text=f"Page {self.page + 1}/{total_pages} · Parlay: {n} leg{'s' if n != 1 else ''} · Select a market to add")
        else:
            embed.set_footer(text=f"Page {self.page + 1}/{total_pages} · Select a market below to bet")
        return embed

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class MarketGroupedDropdown(discord.ui.Select["MarketListView"]):
    """Dropdown for MarketListView supporting both singles and grouped prop markets."""

    def __init__(self, page_items: list[dict], start: int, list_view: "MarketListView", row: int = 0) -> None:
        self._list_view = list_view
        # Both maps keyed by the short position-based value string (safe <= 6 chars)
        self._market_map: dict[str, dict] = {}
        self._group_map: dict[str, list[dict]] = {}
        options: list[discord.SelectOption] = []
        for i, item in enumerate(page_items[:25]):
            if item["type"] == "single":
                m = item["market"]
                ticker = m.get("ticker") or ""
                title = m.get("yes_sub_title") or _clean_market_title(m.get("title") or ticker)
                label = title[:100]
                yes_am, no_am = _market_odds_str(m)
                desc = f"YES {yes_am} / NO {no_am}"
                key = f"m:{start + i}"  # position-based; avoids long ticker exceeding 100-char limit
                self._market_map[key] = m
                options.append(discord.SelectOption(label=label, value=key, description=desc[:100]))
            else:
                key = f"g:{start + i}"
                label = item["label"][:100]
                parts = []
                for mkt in item["markets"][:3]:
                    sub = mkt.get("yes_sub_title") or ""
                    price = mkt.get("yes_ask_dollars") or mkt.get("last_price_dollars")
                    try:
                        am = _price_to_american(float(price)) if price is not None else None
                    except (TypeError, ValueError):
                        am = None
                    if sub and am is not None:
                        parts.append(f"{sub} {am}")
                    elif sub:
                        parts.append(sub)
                desc = (" · ".join(parts) if parts else item.get("subtitle", f"{item['count']} options"))[:100]
                self._group_map[key] = item["markets"]
                sport_emoji = _sport_emoji(item["markets"][0].get("series_ticker") or "")
                options.append(discord.SelectOption(label=label, value=key, description=desc, emoji=sport_emoji))
        super().__init__(placeholder="Select a market to bet on...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        value = self.values[0]
        parlay_legs = self._list_view.parlay_legs
        if not await _safe_defer(interaction, edit=True):
            return
        if value.startswith("m:"):
            market = self._market_map.get(value)
            if not market:
                await interaction.followup.send("Market not found.", ephemeral=True)
                return
            if parlay_legs is not None:
                next_view = ParlayMarketDetailView(market, self._list_view)
            else:
                next_view = RawMarketBetView(market, self._list_view)
            embed = next_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=next_view)
        elif value.startswith("g:"):
            group_markets = self._group_map.get(value, [])
            if not group_markets:
                await interaction.followup.send("Group not found.", ephemeral=True)
                return
            threshold_view = LiveThresholdView(group_markets, self._list_view)
            embed = threshold_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=threshold_view)


class LiveThresholdView(discord.ui.View):
    """Shows individual threshold options for a grouped market in the /live flow."""

    def __init__(
        self,
        group_markets: list[dict],
        list_view: "MarketListView",
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.markets = group_markets
        self._all_markets = list_view.markets
        self._list_page = list_view.page
        self._parlay_legs = getattr(list_view, 'parlay_legs', None)
        self._game_back = getattr(list_view, '_game_back', None)
        self.page = page
        self.message: discord.Message | None = None
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self.markets) + THRESHOLD_PER_PAGE - 1) // THRESHOLD_PER_PAGE)
        start = self.page * THRESHOLD_PER_PAGE
        page_markets = self.markets[start:start + THRESHOLD_PER_PAGE]

        market_map: dict[str, dict] = {}
        options: list[discord.SelectOption] = []
        for m in page_markets:
            ticker = m.get("ticker", "")
            title = m.get("yes_sub_title") or m.get("title") or ticker
            label = title[:100]
            yes_am, no_am = _market_odds_str(m)
            desc = f"YES {yes_am} / NO {no_am}"[:100]
            market_map[ticker] = m
            options.append(discord.SelectOption(label=label, value=ticker, description=desc))

        if options:
            self.add_item(LiveThresholdSelect(options, market_map, self._all_markets, self._list_page, row=0, parlay_legs=self._parlay_legs, game_back=self._game_back))

        if total_pages > 1:
            if self.page > 0:
                self.add_item(LiveThresholdPageButton("prev", self.page - 1, row=1))
            if self.page < total_pages - 1:
                self.add_item(LiveThresholdPageButton("next", self.page + 1, row=1))

        self.add_item(RawMarketBackButton(self._all_markets, self._list_page, row=2, parlay_legs=self._parlay_legs, game_back=self._game_back))

    def build_embed(self) -> discord.Embed:
        total_pages = max(1, (len(self.markets) + THRESHOLD_PER_PAGE - 1) // THRESHOLD_PER_PAGE)
        start = self.page * THRESHOLD_PER_PAGE
        page_markets = self.markets[start:start + THRESHOLD_PER_PAGE]

        header = self.markets[0].get("title") or self.markets[0].get("yes_sub_title") or "?"
        embed = discord.Embed(title=header, color=discord.Color.red())

        if not page_markets:
            embed.description = "No thresholds found."
            return embed

        lines = []
        for m in page_markets:
            title = m.get("yes_sub_title") or m.get("title") or "?"
            yes_am, no_am = _market_odds_str(m)
            lines.append(f"**{title}** · YES {yes_am} / NO {no_am}")
        embed.description = "\n".join(lines)

        footer = f"Page {self.page + 1}/{total_pages} · " if total_pages > 1 else ""
        embed.set_footer(text=f"{footer}Select a threshold to bet")
        return embed

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class LiveThresholdSelect(discord.ui.Select["LiveThresholdView"]):
    def __init__(
        self,
        options: list[discord.SelectOption],
        market_map: dict[str, dict],
        all_markets: list[dict],
        list_page: int,
        row: int = 0,
        parlay_legs: list[dict] | None = None,
        game_back: dict | None = None,
    ) -> None:
        super().__init__(placeholder="Select a threshold to bet on...", options=options, row=row)
        self._market_map = market_map
        self._all_markets = all_markets
        self._list_page = list_page
        self._parlay_legs = parlay_legs
        self._game_back = game_back

    async def callback(self, interaction: discord.Interaction) -> None:
        ticker = self.values[0]
        market = self._market_map.get(ticker)
        if not market:
            await interaction.response.send_message("Market not found.", ephemeral=True)
            return
        if not await _safe_defer(interaction, edit=True):
            return
        list_view = MarketListView(self._all_markets, self._list_page, parlay_legs=self._parlay_legs, game_back=self._game_back)
        if self._parlay_legs is not None:
            next_view = ParlayMarketDetailView(market, list_view)
        else:
            next_view = RawMarketBetView(market, list_view)
        embed = next_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=next_view)


class LiveThresholdPageButton(discord.ui.Button["LiveThresholdView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class MarketPageButton(discord.ui.Button["MarketListView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev"  if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class RawMarketBetView(discord.ui.View):
    """YES / NO buttons for a single raw Kalshi market."""

    def __init__(self, market: dict, list_view: MarketListView, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.market   = market

        yes_am, no_am = _market_odds_str(market)
        self.add_item(RawMarketPickButton("yes", f"YES  {yes_am}", market, row=0))
        self.add_item(RawMarketPickButton("no",  f"NO   {no_am}",  market, row=0))
        self.add_item(RawMarketBackButton(list_view.markets, list_view.page, row=1, game_back=getattr(list_view, '_game_back', None)))

    def build_embed(self) -> discord.Embed:
        m       = self.market
        title   = m.get("title") or "?"
        yes_sub = m.get("yes_sub_title") or ""
        exp     = m.get("expected_expiration_time") or m.get("close_time") or ""
        sport   = _series_label(m.get("series_ticker") or "")
        time_str = _format_game_time(exp) if exp else "TBD"
        yes_am, no_am = _market_odds_str(m)

        embed = discord.Embed(title=title, color=discord.Color.blue())
        if sport:
            embed.description = f"_{sport}_"
        if yes_sub:
            embed.add_field(name="YES resolves if", value=yes_sub, inline=False)
        embed.add_field(name="Closes",  value=time_str, inline=True)
        embed.add_field(name="YES",     value=yes_am,   inline=True)
        embed.add_field(name="NO",      value=no_am,    inline=True)
        embed.set_footer(text="Pick YES or NO, then enter your wager")
        return embed


class RawMarketPickButton(discord.ui.Button["RawMarketBetView"]):
    def __init__(self, pick: str, label: str, market: dict, row: int) -> None:
        style = discord.ButtonStyle.success if pick == "yes" else discord.ButtonStyle.danger
        super().__init__(label=label, style=style, row=row)
        self.pick   = pick
        self.market = market

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(RawMarketBetModal(self.market, self.pick))


class RawMarketBackButton(discord.ui.Button["RawMarketBetView"]):
    def __init__(self, markets: list[dict], page: int, row: int, parlay_legs: list[dict] | None = None, game_back: dict | None = None) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary,
                         emoji="\u25c0\ufe0f", row=row)
        self._markets = markets
        self._page = page
        self._parlay_legs = parlay_legs
        self._game_back = game_back

    async def callback(self, interaction: discord.Interaction) -> None:
        list_view = MarketListView(self._markets, self._page, parlay_legs=self._parlay_legs, game_back=self._game_back)
        embed = list_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=list_view)
        except discord.NotFound:
            pass


class BackToGamesButton(discord.ui.Button["MarketListView"]):
    """Navigates from a game's market list back to the sport's game list."""

    def __init__(self, game_back: dict, row: int) -> None:
        super().__init__(label="Games", emoji="\u25c0\ufe0f", style=discord.ButtonStyle.secondary, row=row)
        self._game_back = game_back

    async def callback(self, interaction: discord.Interaction) -> None:
        gb = self._game_back
        game_view = GameListView(
            gb["markets"], gb["emoji"], gb["label"],
            all_markets=gb.get("all_markets"),
            page=gb.get("page", 0),
            parlay_legs=gb.get("parlay_legs"),
        )
        embed = game_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=game_view)
        except discord.NotFound:
            pass


class GameListView(discord.ui.View):
    """Shows games within a sport; selecting one opens MarketListView for that game."""

    def __init__(
        self,
        sport_markets: list[dict],
        sport_emoji: str,
        sport_label: str,
        all_markets: list[dict] | None = None,
        page: int = 0,
        timeout: float = 180.0,
        parlay_legs: list[dict] | None = None,
    ) -> None:
        super().__init__(timeout=timeout)
        self.sport_markets = sport_markets
        self.sport_emoji = sport_emoji
        self.sport_label = sport_label
        self.all_markets = all_markets
        self.page = page
        self.parlay_legs = parlay_legs
        self.message: discord.Message | None = None
        self._games = _group_markets_by_game(sport_markets)
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self._games) + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        start = self.page * GAMES_PER_PAGE
        page_games = self._games[start:start + GAMES_PER_PAGE]
        if page_games:
            self.add_item(GameSelectDropdown(page_games, self, row=0))
        if self.page > 0:
            self.add_item(GamePageButton("prev", self.page - 1, row=1))
        if self.page < total_pages - 1:
            self.add_item(GamePageButton("next", self.page + 1, row=1))
        if self.all_markets is not None:
            self.add_item(BackToSportsButton(self.all_markets, row=2))
        if self.parlay_legs is not None:
            parlay_row = 3 if self.all_markets is not None else 2
            if self.parlay_legs:
                self.add_item(ParlayViewSlipButton(self.parlay_legs, row=parlay_row))
            if len(self.parlay_legs) >= 2:
                self.add_item(ParlayPlaceFromListButton(self.parlay_legs, row=parlay_row))

    def build_embed(self) -> discord.Embed:
        total_pages = max(1, (len(self._games) + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        start = self.page * GAMES_PER_PAGE
        page_games = self._games[start:start + GAMES_PER_PAGE]
        embed = discord.Embed(
            title=f"{self.sport_emoji} {self.sport_label}",
            color=discord.Color.blue(),
        )
        if not page_games:
            embed.description = "No games found."
            return embed
        lines = []
        for g in page_games:
            n = g["market_count"]
            time_part = f" · {g['time_str']}" if g["time_str"] else ""
            lines.append(f"**{g['label']}** · {n} market{'s' if n != 1 else ''}{time_part}")
        embed.description = "\n\n".join(lines)
        footer = f"Page {self.page + 1}/{total_pages} · " if total_pages > 1 else ""
        embed.set_footer(text=f"{footer}Select a game to see betting options")
        return embed

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class GameSelectDropdown(discord.ui.Select["GameListView"]):
    def __init__(self, page_games: list[dict], game_list_view: "GameListView", row: int = 0) -> None:
        self._game_list_view = game_list_view
        self._games_by_key: dict[str, dict] = {}
        options: list[discord.SelectOption] = []
        for i, g in enumerate(page_games[:25]):
            key = f"g:{i}"
            self._games_by_key[key] = g
            n = g["market_count"]
            desc = f"{n} market{'s' if n != 1 else ''}"
            if g["time_str"]:
                desc = f"{g['time_str']} · {desc}"
            options.append(discord.SelectOption(
                label=g["label"][:100],
                value=key,
                description=desc[:100],
            ))
        super().__init__(placeholder="Select a game...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        key = self.values[0]
        game = self._games_by_key.get(key)
        if not game:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return
        if not await _safe_defer(interaction, edit=True):
            return
        glv = self._game_list_view
        game_back = {
            "markets": glv.sport_markets,
            "page": glv.page,
            "emoji": glv.sport_emoji,
            "label": glv.sport_label,
            "all_markets": glv.all_markets,
            "parlay_legs": glv.parlay_legs,
        }
        market_view = MarketListView(
            game["markets"],
            parlay_legs=glv.parlay_legs,
            game_back=game_back,
        )
        embed = market_view.build_embed()
        embed.title = f"{glv.sport_emoji} {game['label']}"
        await interaction.edit_original_response(embed=embed, view=market_view)


class GamePageButton(discord.ui.Button["GameListView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class BackToSportsButton(discord.ui.Button["GameListView"]):
    """Navigates from a sport's game list back to the sport selector."""

    def __init__(self, all_markets: list[dict], row: int) -> None:
        super().__init__(label="Sports", emoji="\u25c0\ufe0f", style=discord.ButtonStyle.secondary, row=row)
        self._all_markets = all_markets

    async def callback(self, interaction: discord.Interaction) -> None:
        sport_view = SportSelectorView(self._all_markets)
        embed = sport_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=sport_view)
        except discord.NotFound:
            pass


class SportSelectorView(discord.ui.View):
    """Top-level sport picker for /bet; selecting a sport opens GameListView."""

    def __init__(self, all_markets: list[dict], page: int = 0, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.all_markets = all_markets
        self.page = page
        self.message: discord.Message | None = None
        self._sports = _group_markets_by_sport(all_markets)
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self._sports) + 25 - 1) // 25)
        start = self.page * 25
        page_sports = self._sports[start:start + 25]
        if page_sports:
            self.add_item(SportSelectDropdown(page_sports, self, row=0))
        if self.page > 0:
            self.add_item(SportPageButton("prev", self.page - 1, row=1))
        if self.page < total_pages - 1:
            self.add_item(SportPageButton("next", self.page + 1, row=1))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="\U0001f4ca Open Markets", color=discord.Color.blue())
        if not self._sports:
            embed.description = "No open markets right now."
            return embed
        lines = []
        for s in self._sports:
            n = s["game_count"]
            lines.append(f"{s['emoji']} **{s['label']}** · {n} game{'s' if n != 1 else ''}")
        embed.description = "\n".join(lines)
        embed.set_footer(text="Select a sport to browse games")
        return embed

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class SportSelectDropdown(discord.ui.Select["SportSelectorView"]):
    def __init__(self, sports: list[dict], sport_view: "SportSelectorView", row: int = 0) -> None:
        self._sport_view = sport_view
        self._sports_by_idx: dict[str, dict] = {}
        options: list[discord.SelectOption] = []
        for i, s in enumerate(sports[:25]):
            key = str(i)
            self._sports_by_idx[key] = s
            n = s["game_count"]
            options.append(discord.SelectOption(
                label=s["label"],
                value=key,
                description=f"{n} game{'s' if n != 1 else ''}",
                emoji=s["emoji"],
            ))
        super().__init__(placeholder="Select a sport...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        sport = self._sports_by_idx.get(self.values[0])
        if not sport:
            await interaction.response.send_message("Sport not found.", ephemeral=True)
            return
        if not await _safe_defer(interaction, edit=True):
            return
        game_view = GameListView(
            sport["markets"], sport["emoji"], sport["label"],
            all_markets=self._sport_view.all_markets,
        )
        embed = game_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=game_view)


class SportPageButton(discord.ui.Button["SportSelectorView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class RawMarketBetModal(discord.ui.Modal, title="Place Bet"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, market: dict, pick: str) -> None:
        super().__init__()
        self.market = market
        self.pick   = pick

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message("Invalid amount — enter a whole number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be positive.", ephemeral=True)
            return

        if not await _safe_defer(interaction):
            return

        m    = self.market
        pick = self.pick
        yes_ask = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)

        if pick == "yes":
            decimal_odds = round(1.0 / yes_ask, 3) if yes_ask > 0 else 2.0
        else:
            no_ask = 1.0 - yes_ask
            decimal_odds = round(1.0 / no_ask, 3) if no_ask > 0 else 2.0

        american = decimal_to_american(decimal_odds)
        close_time = m.get("close_time") or m.get("expected_expiration_time")

        if close_time:
            try:
                ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                if ct <= datetime.now(timezone.utc):
                    await interaction.followup.send("This market has already closed.", ephemeral=True)
                    return
            except (ValueError, TypeError):
                pass

        title    = m.get("title") or "?"
        yes_sub  = m.get("yes_sub_title") or ""
        pick_display = (f"YES — {yes_sub}" if pick == "yes" and yes_sub
                        else ("YES" if pick == "yes" else "NO"))

        bet_id = await betting_service.place_kalshi_bet(
            user_id=interaction.user.id,
            market_ticker=m["ticker"],
            event_ticker=m.get("event_ticker", ""),
            pick=pick,
            amount=amount,
            odds=decimal_odds,
            title=title,
            close_time=close_time,
            pick_display=pick_display,
        )

        if bet_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        payout = round(amount * decimal_odds, 2)
        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID",           value=f"#K{bet_id}",             inline=True)
        embed.add_field(name="Pick",             value=pick_display,               inline=True)
        embed.add_field(name="Wager",            value=f"${amount:.2f}",           inline=True)
        embed.add_field(name="Odds",             value=format_american(american),  inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}",           inline=True)
        embed.add_field(name="Market",           value=title[:1024],               inline=False)
        await interaction.followup.send(embed=embed)


# ── Parlay List-Mode Views ────────────────────────────────────────────
# Used by /parlay when browsing markets via MarketListView.

MAX_PARLAY_LEGS = 10


class ParlayMarketDetailView(discord.ui.View):
    """YES/NO detail view for adding a raw Kalshi market to a parlay."""

    def __init__(self, market: dict, list_view: "MarketListView", timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.market = market
        legs = list_view.parlay_legs or []
        game_back = getattr(list_view, '_game_back', None)
        yes_am, no_am = _market_odds_str(market)
        self.add_item(ParlayPickButton("yes", f"YES  {yes_am}", market, list_view.markets, list_view.page, legs, row=0, game_back=game_back))
        self.add_item(ParlayPickButton("no",  f"NO   {no_am}",  market, list_view.markets, list_view.page, legs, row=0, game_back=game_back))
        self.add_item(RawMarketBackButton(list_view.markets, list_view.page, row=1, parlay_legs=legs, game_back=game_back))

    def build_embed(self) -> discord.Embed:
        m = self.market
        title   = m.get("title") or "?"
        yes_sub = m.get("yes_sub_title") or ""
        exp     = m.get("expected_expiration_time") or m.get("close_time") or ""
        sport   = _series_label(m.get("series_ticker") or "")
        time_str = _format_game_time(exp) if exp else "TBD"
        yes_am, no_am = _market_odds_str(m)
        embed = discord.Embed(title=title, color=discord.Color.purple())
        if sport:
            embed.description = f"_{sport}_"
        if yes_sub:
            embed.add_field(name="YES resolves if", value=yes_sub, inline=False)
        embed.add_field(name="Closes", value=time_str, inline=True)
        embed.add_field(name="YES",    value=yes_am,   inline=True)
        embed.add_field(name="NO",     value=no_am,    inline=True)
        embed.set_footer(text="Pick YES or NO to add to your parlay")
        return embed


class ParlayPickButton(discord.ui.Button):
    """Adds YES or NO for a market to the parlay slip, then returns to market list."""

    def __init__(
        self, pick: str, label: str, market: dict,
        all_markets: list[dict], list_page: int, legs: list[dict], row: int,
        game_back: dict | None = None,
    ) -> None:
        style = discord.ButtonStyle.success if pick == "yes" else discord.ButtonStyle.danger
        super().__init__(label=label, style=style, row=row)
        self.pick = pick
        self.market = market
        self._all_markets = all_markets
        self._list_page = list_page
        self._legs = legs
        self._game_back = game_back

    async def callback(self, interaction: discord.Interaction) -> None:
        m = self.market
        ticker = m.get("ticker", "")

        if any(leg["market_ticker"] == ticker for leg in self._legs):
            await interaction.response.send_message("This market is already in your parlay.", ephemeral=True)
            return
        if len(self._legs) >= MAX_PARLAY_LEGS:
            await interaction.response.send_message(f"Maximum {MAX_PARLAY_LEGS} legs allowed.", ephemeral=True)
            return

        yes_ask = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)
        if self.pick == "yes":
            decimal_odds = round(1.0 / yes_ask, 3) if yes_ask > 0 else 2.0
        else:
            no_ask = 1.0 - yes_ask
            decimal_odds = round(1.0 / no_ask, 3) if no_ask > 0 else 2.0

        american = decimal_to_american(decimal_odds)
        title   = m.get("title") or "?"
        yes_sub = m.get("yes_sub_title") or ""
        pick_display = (f"YES — {yes_sub}" if self.pick == "yes" and yes_sub
                        else ("YES" if self.pick == "yes" else "NO"))
        close_time = m.get("close_time") or m.get("expected_expiration_time")

        leg = {
            "market_ticker": ticker,
            "event_ticker": m.get("event_ticker", ""),
            "pick": self.pick,
            "odds": decimal_odds,
            "american": american,
            "title": title,
            "pick_display": pick_display,
            "close_time": close_time,
        }
        new_legs = self._legs + [leg]
        list_view = MarketListView(self._all_markets, self._list_page, parlay_legs=new_legs, game_back=self._game_back)
        embed = list_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=list_view)
        except discord.NotFound:
            pass


class ParlayViewSlipButton(discord.ui.Button):
    """Opens the parlay slip from the market list."""

    def __init__(self, legs: list[dict], row: int) -> None:
        n = len(legs)
        super().__init__(
            label=f"View Slip ({n})",
            style=discord.ButtonStyle.success,
            emoji="\U0001f4cb",
            row=row,
        )
        self._legs = legs

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        slip_view = ParlaySlipView(self._legs, view.markets, view.page)
        embed = slip_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=slip_view)
        except discord.NotFound:
            pass


class ParlayPlaceFromListButton(discord.ui.Button):
    """Opens wager modal directly from the market list (2+ legs)."""

    def __init__(self, legs: list[dict], row: int) -> None:
        super().__init__(
            label="Place Parlay",
            style=discord.ButtonStyle.primary,
            emoji="\U0001f4b0",
            row=row,
        )
        self._legs = legs

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(ParlayAmountModal(self._legs))


class ParlaySlipView(discord.ui.View):
    """Parlay slip: shows legs, combined odds, remove/place controls."""

    def __init__(self, legs: list[dict], all_markets: list[dict], list_page: int, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.legs = list(legs)
        self._all_markets = all_markets
        self._list_page = list_page
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        if self.legs:
            self.add_item(ParlayRemoveLegSelect(self.legs, self._all_markets, self._list_page, row=0))
        if len(self.legs) >= 2:
            self.add_item(ParlaySlipPlaceButton(self.legs, row=1))
        self.add_item(RawMarketBackButton(self._all_markets, self._list_page, row=2, parlay_legs=self.legs))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Parlay Slip", color=discord.Color.purple())
        if not self.legs:
            embed.description = "No legs yet. Go back to add markets."
            return embed

        total_odds = 1.0
        for i, leg in enumerate(self.legs, 1):
            total_odds *= leg["odds"]
            odds_str = format_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}: {leg['pick_display']}",
                value=f"{leg.get('title', '?')} — {odds_str} ({leg['odds']:.2f}x)",
                inline=False,
            )
        total_odds = round(total_odds, 4)
        embed.add_field(
            name="Combined Odds",
            value=f"**{total_odds:.2f}x** — $100 wins **${round(100 * total_odds):.2f}**",
            inline=False,
        )
        footer_parts = [f"{len(self.legs)} leg{'s' if len(self.legs) != 1 else ''}"]
        if len(self.legs) < 2:
            footer_parts.append("Add at least 2 legs to place")
        embed.set_footer(text=" · ".join(footer_parts))
        return embed


class ParlayRemoveLegSelect(discord.ui.Select):
    """Dropdown to remove a leg from the slip."""

    def __init__(self, legs: list[dict], all_markets: list[dict], list_page: int, row: int) -> None:
        self._all_markets = all_markets
        self._list_page = list_page
        self._legs = legs
        options = []
        for i, leg in enumerate(legs):
            label = leg["pick_display"]
            if len(label) > 94:
                label = label[:91] + "..."
            options.append(discord.SelectOption(label=f"Remove: {label}", value=str(i)))
        super().__init__(placeholder="Remove a leg...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        idx = int(self.values[0])
        view.legs = [leg for i, leg in enumerate(self._legs) if i != idx]
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class ParlaySlipPlaceButton(discord.ui.Button):
    """Opens wager modal from the slip view."""

    def __init__(self, legs: list[dict], row: int) -> None:
        super().__init__(label="Place Parlay", style=discord.ButtonStyle.success, emoji="\U0001f4b0", row=row)
        self._legs = legs

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        legs = view.legs if view is not None else self._legs
        await interaction.response.send_modal(ParlayAmountModal(legs))


class ParlayAmountModal(discord.ui.Modal, title="Place Parlay"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, legs: list[dict]) -> None:
        super().__init__()
        self.legs = legs

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message("Invalid amount — enter a whole number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Bet amount must be positive.", ephemeral=True)
            return

        if not await _safe_defer(interaction):
            return

        if len(self.legs) < 2:
            await interaction.followup.send("Need at least 2 legs for a parlay.", ephemeral=True)
            return

        parlay_id = await betting_service.place_kalshi_parlay(
            user_id=interaction.user.id,
            legs=self.legs,
            amount=amount,
        )

        if parlay_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        total_odds = 1.0
        for leg in self.legs:
            total_odds *= leg["odds"]
        total_odds = round(total_odds, 4)
        payout = round(amount * total_odds)

        embed = discord.Embed(title="Parlay Placed!", color=discord.Color.green())
        embed.add_field(name="Parlay ID",        value=f"#KP{parlay_id}",      inline=True)
        embed.add_field(name="Legs",             value=str(len(self.legs)),     inline=True)
        embed.add_field(name="Wager",            value=f"${amount:.2f}",        inline=True)
        embed.add_field(name="Combined Odds",    value=f"{total_odds:.2f}x",    inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}",        inline=True)
        for i, leg in enumerate(self.legs, 1):
            odds_str = format_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}",
                value=f"{leg['pick_display']} — {odds_str}",
                inline=False,
            )
        await interaction.edit_original_response(embed=embed, view=None)


# ── Legacy Parlay Views (game-centric flow, kept for reference) ────────


class KalshiParlayView(discord.ui.View):
    """Multi-step parlay builder using Kalshi markets."""

    def __init__(
        self,
        games: list[dict],
        timeout: float = 300.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.games = games
        self.legs: list[dict] = []
        self.message: discord.Message | None = None
        self._show_game_select()

    def _available_games(self) -> list[dict]:
        """Games not already in the parlay slip."""
        used_events = {leg["event_ticker"] for leg in self.legs}
        return [g for g in self.games if g["id"] not in used_events]

    def _show_game_select(self) -> None:
        self.clear_items()
        available = self._available_games()
        if available:
            self.add_item(KalshiParlayGameSelect(available[:25], row=0))
        if self.legs:
            self.add_item(KalshiParlayViewSlipButton(row=1))
        self.add_item(KalshiParlayCancelButton(row=1))

    def _show_slip(self) -> None:
        self.clear_items()
        if len(self.legs) < MAX_PARLAY_LEGS and self._available_games():
            self.add_item(KalshiParlayAddLegButton(row=0))
        if self.legs:
            self.add_item(KalshiParlayRemoveLegSelect(self.legs, row=1))
        if len(self.legs) >= 2:
            self.add_item(KalshiParlayPlaceButton(row=2))
        self.add_item(KalshiParlayCancelButton(row=2))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Parlay Builder", color=discord.Color.purple())
        if not self.legs:
            embed.description = "Select a game to add your first leg."
            return embed

        total_odds = 1.0
        for i, leg in enumerate(self.legs, 1):
            total_odds *= leg["odds"]
            odds_str = format_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}: {leg['pick_display']}",
                value=f"{leg.get('title', '?')} — {odds_str} ({leg['odds']:.2f}x)",
                inline=False,
            )

        total_odds = round(total_odds, 4)
        embed.add_field(
            name="Combined Odds",
            value=f"**{total_odds:.2f}x** — $100 wins **${round(100 * total_odds):.2f}**",
            inline=False,
        )
        footer_parts = [f"{len(self.legs)} leg{'s' if len(self.legs) != 1 else ''}"]
        if len(self.legs) < 2:
            footer_parts.append("Add at least 2 legs to place")
        embed.set_footer(text=" · ".join(footer_parts))
        return embed

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class KalshiParlayGameSelect(discord.ui.Select["KalshiParlayView"]):
    """Dropdown to pick a game for a parlay leg."""

    def __init__(self, games: list[dict], row: int = 0) -> None:
        self.games_map: dict[str, dict] = {}
        options = []
        for g in games[:25]:
            game_id = g["id"]
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            label = format_matchup(home, away)
            if len(label) > 100:
                label = label[:97] + "..."
            desc = _format_game_time(g.get("commence_time", ""))
            if len(desc) > 100:
                desc = desc[:100]
            self.games_map[game_id] = g
            options.append(discord.SelectOption(label=label, value=game_id, description=desc))
        super().__init__(placeholder="Select a game to add...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        game_id = self.values[0]
        game = self.games_map.get(game_id)
        if not game:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        view = self.view
        if view is None:
            return

        if not await _safe_defer(interaction):
            return

        sport_key = game.get("sport_key", "")
        parsed = await kalshi_api.get_game_odds(sport_key, game)

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")

        # Build bet type selection for this game
        bet_view = KalshiParlayBetTypeView(game, parsed, view)
        time_str = _format_game_time(game.get("commence_time", ""))
        embed = discord.Embed(
            title="Add Parlay Leg",
            description=f"**{format_matchup(home, away)}**\n{time_str}\n\nPick a bet type:",
            color=discord.Color.purple(),
        )
        await interaction.edit_original_response(embed=embed, view=bet_view)


class KalshiParlayBetTypeView(discord.ui.View):
    """Bet type buttons for adding a parlay leg."""

    def __init__(self, game: dict, parsed: dict, parlay_view: KalshiParlayView, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.game = game
        self.parlay_view = parlay_view

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")
        fmt = format_american

        row = 0
        if "home" in parsed:
            self.add_item(KalshiParlayBetTypeButton(
                "home", f"Home {fmt(parsed['home']['american'])}",
                game, parsed["home"], parlay_view, row=row,
            ))
        if "away" in parsed:
            self.add_item(KalshiParlayBetTypeButton(
                "away", f"Away {fmt(parsed['away']['american'])}",
                game, parsed["away"], parlay_view, row=row,
            ))

        row = 1
        if "spread_home" in parsed:
            sh = parsed["spread_home"]
            self.add_item(KalshiParlayBetTypeButton(
                "spread_home", f"{home} {sh['point']:+g} ({fmt(sh['american'])})",
                game, sh, parlay_view, row=row,
            ))
        if "spread_away" in parsed:
            sa = parsed["spread_away"]
            self.add_item(KalshiParlayBetTypeButton(
                "spread_away", f"{away} {sa['point']:+g} ({fmt(sa['american'])})",
                game, sa, parlay_view, row=row,
            ))

        row = 2
        if "over" in parsed:
            ov = parsed["over"]
            self.add_item(KalshiParlayBetTypeButton(
                "over", f"Over {ov['point']:g} ({fmt(ov['american'])})",
                game, ov, parlay_view, row=row,
            ))
        if "under" in parsed:
            un = parsed["under"]
            self.add_item(KalshiParlayBetTypeButton(
                "under", f"Under {un['point']:g} ({fmt(un['american'])})",
                game, un, parlay_view, row=row,
            ))

        self.add_item(KalshiParlayBackToGamesButton(parlay_view, row=3))


class KalshiParlayBetTypeButton(discord.ui.Button):
    """Adds a leg to the parlay slip when clicked."""

    def __init__(
        self, pick_key: str, label: str, game: dict, odds_entry: dict,
        parlay_view: KalshiParlayView, row: int,
    ) -> None:
        emoji = PICK_EMOJI.get(pick_key)
        super().__init__(label=label, style=discord.ButtonStyle.primary, emoji=emoji, row=row)
        self.pick_key = pick_key
        self.game = game
        self.odds_entry = odds_entry
        self.parlay_view = parlay_view

    async def callback(self, interaction: discord.Interaction) -> None:
        game = self.game
        odds_entry = self.odds_entry
        home = game.get("home_team", "?")
        away = game.get("away_team", "?")

        kalshi_markets = game.get("_kalshi_markets", {})
        market_ticker, kalshi_pick = _resolve_kalshi_bet(self.pick_key, kalshi_markets, odds_entry)

        if not market_ticker:
            await interaction.response.send_message("Could not find market for this bet.", ephemeral=True)
            return

        pick_display = _build_pick_display(self.pick_key, home, away, odds_entry)
        market = kalshi_markets.get("home") or kalshi_markets.get("away") or {}
        close_time = market.get("close_time") or market.get("expected_expiration_time")
        bet_title = f"{format_matchup(home, away)} ({game.get('sport_title', '')})"

        leg = {
            "market_ticker": market_ticker,
            "event_ticker": game.get("id", ""),
            "pick": kalshi_pick,
            "odds": odds_entry["decimal"],
            "american": odds_entry["american"],
            "title": bet_title,
            "pick_display": pick_display,
            "close_time": close_time,
        }

        pv = self.parlay_view
        pv.legs.append(leg)
        pv._show_slip()
        embed = pv.build_embed()
        await interaction.response.edit_message(embed=embed, view=pv)


class KalshiParlayViewSlipButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="View Slip", style=discord.ButtonStyle.success, emoji="\U0001f4cb", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view._show_slip()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayAddLegButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Add Leg", style=discord.ButtonStyle.success, emoji="\u2795", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view._show_game_select()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayRemoveLegSelect(discord.ui.Select["KalshiParlayView"]):
    def __init__(self, legs: list[dict], row: int) -> None:
        options = []
        for i, leg in enumerate(legs):
            label = leg["pick_display"]
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(label=f"Remove: {label}", value=str(i)))
        super().__init__(placeholder="Remove a leg...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        idx = int(self.values[0])
        if 0 <= idx < len(view.legs):
            view.legs.pop(idx)
        view._show_slip()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayPlaceButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Place Parlay", style=discord.ButtonStyle.success, emoji="\U0001f4b0", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        modal = KalshiParlayAmountModal(view)
        await interaction.response.send_modal(modal)


class KalshiParlayCancelButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view:
            view.clear_items()
            view.stop()
        embed = discord.Embed(title="Parlay Cancelled", color=discord.Color.orange())
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayBackToGamesButton(discord.ui.Button):
    def __init__(self, parlay_view: KalshiParlayView, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.parlay_view = parlay_view

    async def callback(self, interaction: discord.Interaction) -> None:
        pv = self.parlay_view
        pv._show_game_select()
        embed = pv.build_embed()
        await interaction.response.edit_message(embed=embed, view=pv)


class KalshiParlayAmountModal(discord.ui.Modal, title="Place Parlay"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, parlay_view: KalshiParlayView) -> None:
        super().__init__()
        self.parlay_view = parlay_view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message("Invalid amount — enter a whole number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Bet amount must be positive.", ephemeral=True)
            return

        if not await _safe_defer(interaction):
            return

        pv = self.parlay_view
        if len(pv.legs) < 2:
            await interaction.followup.send("Need at least 2 legs for a parlay.", ephemeral=True)
            return

        parlay_id = await betting_service.place_kalshi_parlay(
            user_id=interaction.user.id,
            legs=pv.legs,
            amount=amount,
        )

        if parlay_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        total_odds = 1.0
        for leg in pv.legs:
            total_odds *= leg["odds"]
        total_odds = round(total_odds, 4)
        payout = round(amount * total_odds)

        embed = discord.Embed(title="Parlay Placed!", color=discord.Color.green())
        embed.add_field(name="Parlay ID", value=f"#KP{parlay_id}", inline=True)
        embed.add_field(name="Legs", value=str(len(pv.legs)), inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Combined Odds", value=f"{total_odds:.2f}x", inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        for i, leg in enumerate(pv.legs, 1):
            odds_str = format_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}",
                value=f"{leg['pick_display']} — {odds_str}",
                inline=False,
            )

        pv.clear_items()
        pv.stop()
        await interaction.edit_original_response(embed=embed, view=None)


# ── /mybets Cash-Out Views ────────────────────────────────────────────


class _CashOutConfirmView(discord.ui.View):
    """Ephemeral confirm/cancel prompt shown when a user picks a bet to cash out."""

    def __init__(self, bet_id: int, cashout: int, wagered: int) -> None:
        super().__init__(timeout=60)
        self.bet_id = bet_id
        self.cashout = cashout
        self.wagered = wagered

    @discord.ui.button(label="Confirm Cash Out", style=discord.ButtonStyle.success)
    async def confirm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        from bot.db import models as _models

        user_id = await _models.cashout_kalshi_bet(self.bet_id, self.cashout)
        if user_id is None:
            await interaction.response.edit_message(
                content="This bet is no longer pending (already resolved or cashed out).",
                embed=None,
                view=None,
            )
            return

        delta = self.cashout - self.wagered
        sign = "+" if delta >= 0 else ""
        self.stop()
        await interaction.response.edit_message(
            content=f"Cashed out for **${self.cashout:.2f}** ({sign}${delta:.2f}). Balance updated.",
            embed=None,
            view=None,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        self.stop()
        await interaction.response.edit_message(
            content="Cash out cancelled.", embed=None, view=None
        )


class _KalshiCashOutSelect(discord.ui.Select["_KalshiCashOutView"]):
    def __init__(self, user_id: int, bets_with_cashout: list[tuple[dict, int]]) -> None:
        self._user_id = user_id
        self._bets: dict[str, tuple[dict, int]] = {
            str(bet["id"]): (bet, cashout) for bet, cashout in bets_with_cashout
        }
        options = []
        for bet, cashout in bets_with_cashout[:25]:
            pick_label = bet.get("pick_display") or bet["pick"].upper()
            delta = cashout - bet["amount"]
            sign = "+" if delta >= 0 else ""
            label = f"#{bet['id']} · {pick_label} · ${cashout:.2f} ({sign}${delta:.2f})"
            if len(label) > 100:
                label = label[:97] + "..."
            title = bet.get("title") or bet["market_ticker"]
            desc = title[:100] if len(title) <= 100 else title[:97] + "..."
            options.append(
                discord.SelectOption(label=label, value=str(bet["id"]), description=desc)
            )
        super().__init__(placeholder="Select a bet to cash out...", options=options)

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self._user_id:
            await interaction.response.send_message(
                "These aren't your bets.", ephemeral=True
            )
            return

        bet_id_str = self.values[0]
        bet, cashout = self._bets[bet_id_str]
        delta = cashout - bet["amount"]
        sign = "+" if delta >= 0 else ""
        title = bet.get("title") or bet["market_ticker"]
        pick_label = bet.get("pick_display") or bet["pick"].upper()

        embed = discord.Embed(
            title="Cash Out Confirmation",
            description=(
                f"**{title}**\n"
                f"Pick: **{pick_label}** · Wagered: **${bet['amount']:,}**\n\n"
                f"Cash out now for **${cashout:.2f}** ({sign}${delta:.2f})"
            ),
            color=discord.Color.green() if delta >= 0 else discord.Color.red(),
        )
        view = _CashOutConfirmView(bet["id"], cashout, bet["amount"])
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class _KalshiCashOutView(discord.ui.View):
    """Attached to the /mybets response; lets users cash out individual Kalshi bets."""

    def __init__(self, user_id: int, bets_with_cashout: list[tuple[dict, int]]) -> None:
        super().__init__(timeout=300)
        self.message: discord.Message | None = None
        self.add_item(_KalshiCashOutSelect(user_id, bets_with_cashout))

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.delete()
            except Exception:
                pass


# ── Cog ───────────────────────────────────────────────────────────────


class HistoryView(discord.ui.View):
    """Paginated view for /myhistory showing resolved bets with stats."""

    PAGE_SIZE = 10

    def __init__(self, user_id: int, stats: dict, total_items: int, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.stats = stats
        self.total_items = total_items
        self.page = 0
        self.total_pages = max(1, (total_items + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.message: discord.Message | None = None
        self._update_buttons()

    def _update_buttons(self) -> None:
        self.prev_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= self.total_pages - 1

    async def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Bet History", color=discord.Color.blue())

        # Stats header
        stats = self.stats
        total = stats.get("total") or 0
        wins = stats.get("wins") or 0
        losses = stats.get("losses") or 0
        pushes = stats.get("pushes") or 0
        total_wagered = stats.get("total_wagered") or 0
        total_payout = stats.get("total_payout") or 0
        profit = total_payout - total_wagered
        win_rate = (wins / total * 100) if total > 0 else 0
        profit_sign = "+" if profit >= 0 else ""

        embed.description = (
            f"**Record:** {wins}W - {losses}L - {pushes}P ({win_rate:.0f}% win rate)\n"
            f"**Wagered:** ${total_wagered:,.2f} · **Profit:** {profit_sign}${profit:,.2f}"
        )

        # Fetch page data
        items = await betting_service.get_user_history(
            self.user_id, page=self.page, page_size=self.PAGE_SIZE
        )

        if not items:
            embed.add_field(name="No bets", value="No resolved bets found.", inline=False)
        else:
            for item in items:
                if item.get("type") == "parlay":
                    self._add_parlay_field(embed, item)
                elif item.get("type") == "kalshi":
                    self._add_kalshi_field(embed, item)
                elif item.get("type") == "kalshi_parlay":
                    self._add_kalshi_parlay_field(embed, item)
                else:
                    self._add_bet_field(embed, item)

        embed.set_footer(text=f"Page {self.page + 1}/{self.total_pages}")
        return embed

    def _add_bet_field(self, embed: discord.Embed, b: dict) -> None:
        status = b["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${b.get('payout', 0):.2f}**"
        elif status == "push":
            icon = "\U0001f535"
            status_text = f"Push — **${b.get('payout', 0):.2f}** refunded"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        is_outright = (b.get("market") or "") == "outrights"
        home = b.get("home_team")
        away = b.get("away_team")
        sport = b.get("sport_title")

        if is_outright:
            matchup = sport or "Futures"
        elif home and away:
            matchup = format_matchup(home, away)
        else:
            matchup = "Unknown"

        sport_line = f"{sport} · " if sport and not is_outright else ""
        pick_label = b["pick"] if is_outright else format_pick_label(b)

        embed.add_field(
            name=f"{icon} Bet #{b['id']} · {status_text}",
            value=(
                f"{sport_line}**{matchup}**\n"
                f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']}x"
            ),
            inline=False,
        )

    def _add_parlay_field(self, embed: discord.Embed, p: dict) -> None:
        status = p["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${p.get('payout', 0):.2f}**"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        leg_lines = []
        for leg in p.get("legs", []):
            ls = leg["status"]
            if ls == "won":
                leg_icon = "\U0001f7e2"
            elif ls == "lost":
                leg_icon = "\U0001f534"
            elif ls == "push":
                leg_icon = "\U0001f535"
            else:
                leg_icon = "\U0001f7e1"

            home = leg.get("home_team") or "?"
            away = leg.get("away_team") or "?"
            pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
            point = leg.get("point")
            if point is not None:
                if leg["pick"] in ("spread_home", "spread_away"):
                    pick_label += f" {point:+g}"
                else:
                    pick_label += f" {point:g}"
            leg_lines.append(f"{leg_icon} {format_matchup(home, away)} — {pick_label} ({leg['odds']:.2f}x)")

        embed.add_field(
            name=f"{icon} Parlay #{p['id']} · {status_text}",
            value=(
                f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                + "\n".join(leg_lines)
            ),
            inline=False,
        )

    def _add_kalshi_field(self, embed: discord.Embed, b: dict) -> None:
        status = b["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${b.get('payout', 0):.2f}**"
        elif status == "cashed_out":
            icon = "\U0001f4b0"
            status_text = f"Cashed Out — **${b.get('payout', 0):.2f}**"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        title = b.get("title") or b.get("market_ticker", "Unknown")
        pick_label = b.get("pick_display") or b["pick"].upper()

        embed.add_field(
            name=f"{icon} Kalshi #{b['id']} · {status_text}",
            value=(
                f"**{title}**\n"
                f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']:.2f}x"
            ),
            inline=False,
        )

    def _add_kalshi_parlay_field(self, embed: discord.Embed, p: dict) -> None:
        status = p["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${p.get('payout', 0):.2f}**"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        leg_lines = []
        for leg in p.get("legs", []):
            ls = leg["status"]
            if ls == "won":
                leg_icon = "\U0001f7e2"
            elif ls == "lost":
                leg_icon = "\U0001f534"
            else:
                leg_icon = "\U0001f7e1"
            pick_label = leg.get("pick_display") or leg["pick"].upper()
            leg_lines.append(f"{leg_icon} {leg.get('title', '?')} — {pick_label} ({leg['odds']:.2f}x)")

        embed.add_field(
            name=f"{icon} Kalshi Parlay #KP{p['id']} · {status_text}",
            value=(
                f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                + "\n".join(leg_lines)
            ),
            inline=False,
        )

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="\u25c0")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your history.", ephemeral=True)
            return
        self.page = max(0, self.page - 1)
        self._update_buttons()
        embed = await self.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.NotFound:
            pass

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="\u25b6")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your history.", ephemeral=True)
            return
        self.page = min(self.total_pages - 1, self.page + 1)
        self._update_buttons()
        embed = await self.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.NotFound:
            pass

    async def on_timeout(self) -> None:
        if self.message:
            try:
                await self.message.delete()
            except discord.NotFound:
                pass


class KalshiCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        log.info("KalshiCog loading — refreshing sports...")
        await kalshi_api.refresh_sports()
        log.info("KalshiCog loaded — %d sports available, starting loops", len(SPORTS))
        self.check_kalshi_results.start()
        self.refresh_discovery.start()
        self.refresh_sports_loop.start()
        self.db_maintenance.start()

    async def cog_unload(self) -> None:
        self.check_kalshi_results.cancel()
        self.refresh_discovery.cancel()
        self.refresh_sports_loop.cancel()
        self.db_maintenance.cancel()
        await kalshi_api.close()

    # ── Sport autocomplete ────────────────────────────────────────────

    async def sport_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        choices = []
        seen = set()
        current_lower = current.lower()

        # Include game sports
        for key, sport in SPORTS.items():
            label = sport["label"]
            if current_lower in label.lower() or current_lower in key.lower():
                choices.append(app_commands.Choice(name=label, value=key))
                seen.add(key)
            if len(choices) >= 25:
                break

        # Include futures-only sports (e.g. Boxing)
        if len(choices) < 25:
            for key, fut in FUTURES.items():
                if key in seen:
                    continue
                label = fut["label"]
                if current_lower in label.lower() or current_lower in key.lower():
                    choices.append(app_commands.Choice(name=f"{label} (Futures)", value=key))
                if len(choices) >= 25:
                    break

        return choices

    # ── /bet ──────────────────────────────────────────────────────────

    @app_commands.command(name="bet", description="Browse Kalshi markets and bet YES or NO")
    @app_commands.describe(search="Filter by sport or keyword (e.g. 'hockey', '🏒', 'Lakers')")
    async def bet(self, interaction: discord.Interaction, search: str | None = None) -> None:
        if not await _safe_defer(interaction):
            return
        log.info("/bet called by %s (search=%r)", interaction.user, search)

        all_markets = await kalshi_api.get_all_open_markets()

        if not all_markets:
            await interaction.followup.send("No open sports markets on Kalshi right now.")
            return

        if not search:
            # Top-level: show sport picker
            view = SportSelectorView(all_markets)
            embed = view.build_embed()
            msg = await interaction.followup.send(embed=embed, view=view)
            view.message = msg
            return

        # Search provided — filter markets then show game list for that sport/keyword
        search_lower = search.lower().strip()
        series_label_map: dict[str, str] = {}
        for sk, info in SPORTS.items():
            label_lower = info.get("label", "").lower()
            for ticker in info["series"].values():
                series_label_map[ticker] = label_lower

        def _st(m: dict) -> str:
            st = m.get("series_ticker") or ""
            if not st:
                et = m.get("event_ticker") or ""
                st = et.split("-")[0] if "-" in et else et
            return st.lower()

        target_emoji = _SPORT_ALIASES.get(search_lower)
        log.info("/bet search=%r (target_emoji=%r): %d markets", search, target_emoji, len(all_markets))
        markets = [
            m for m in all_markets
            if search_lower in (m.get("title") or "").lower()
            or search_lower in (m.get("yes_sub_title") or "").lower()
            or search_lower in series_label_map.get(_st(m).upper(), "")
            or search_lower in _st(m)
            or (target_emoji is not None and _sport_emoji(_st(m).upper()) == target_emoji)
        ]
        log.info("/bet search=%r: %d filtered results", search, len(markets))
        if not markets:
            await interaction.followup.send(f"No markets found matching **{search}**.")
            return

        sport_label = _SPORT_EMOJI_LABEL.get(target_emoji, search.title()) if target_emoji else search.title()
        sport_emoji_display = target_emoji or "\U0001f50d"
        view = GameListView(markets, sport_emoji_display, sport_label, all_markets=all_markets)
        embed = view.build_embed()
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /close ─────────────────────────────────────────────────────────

    @app_commands.command(name="close", description="Markets with close odds, sorted by soonest close time")
    @app_commands.describe(
        threshold="Max favorite % — e.g. 60 shows markets ≤60/40 (default 60)",
        sport="Filter by sport or keyword (optional)",
    )
    async def close(
        self,
        interaction: discord.Interaction,
        threshold: app_commands.Range[int, 51, 99] = 60,
        sport: str | None = None,
    ) -> None:
        if not await _safe_defer(interaction):
            return

        all_markets = await kalshi_api.get_all_open_markets()
        max_pct = threshold / 100.0

        if sport:
            sport_lower = sport.lower().strip()
            series_label_map: dict[str, str] = {}
            for sk, info in SPORTS.items():
                label_lower = info.get("label", "").lower()
                for ticker in info["series"].values():
                    series_label_map[ticker] = label_lower
            target_emoji = _SPORT_ALIASES.get(sport_lower)
            all_markets = [
                m for m in all_markets
                if sport_lower in (m.get("title") or "").lower()
                or sport_lower in (m.get("yes_sub_title") or "").lower()
                or sport_lower in series_label_map.get(_market_series_ticker(m).upper(), "")
                or sport_lower in _market_series_ticker(m)
                or (target_emoji is not None
                    and _sport_emoji(_market_series_ticker(m).upper()) == target_emoji)
            ]

        markets = [m for m in all_markets if _is_close_market(m, max_pct)]

        if not markets:
            hint = f" Try `/bet {sport}`" if sport else " Try raising the threshold."
            await interaction.followup.send(
                f"No markets found with odds ≤{threshold}/{100 - threshold}.{hint}"
            )
            return

        markets.sort(key=lambda m: _earliest_market_time(m) or "9999")
        sport_label = sport.title() if sport else "All Sports"
        view = MarketListView(markets)
        embed = view.build_embed()
        embed.title = f"\U0001f3af Close Markets \u2264{threshold}/{100 - threshold} \u2014 {sport_label}"
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /parlay ────────────────────────────────────────────────────────

    @app_commands.command(name="parlay", description="Build a parlay bet")
    @app_commands.describe(sport="Filter by sport (leave blank for all)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def parlay(self, interaction: discord.Interaction, sport: str | None = None) -> None:
        if not await _safe_defer(interaction):
            return

        all_markets = await kalshi_api.get_all_open_markets()

        if sport:
            sport_lower = sport.lower().strip()
            series_label_map: dict[str, str] = {}
            for sk, info in SPORTS.items():
                label_lower = info.get("label", "").lower()
                for ticker in info["series"].values():
                    series_label_map[ticker] = label_lower
            target_emoji = _SPORT_ALIASES.get(sport_lower)
            all_markets = [
                m for m in all_markets
                if sport_lower in (m.get("title") or "").lower()
                or sport_lower in (m.get("yes_sub_title") or "").lower()
                or sport_lower in series_label_map.get(_market_series_ticker(m).upper(), "")
                or sport_lower in _market_series_ticker(m)
                or (target_emoji is not None
                    and _sport_emoji(_market_series_ticker(m).upper()) == target_emoji)
            ]

        if not all_markets:
            hint = f" Try `/parlay {sport}`" if not sport else " No markets for that sport right now."
            await interaction.followup.send(f"No markets available right now.{hint}")
            return

        view = MarketListView(all_markets, parlay_legs=[])
        embed = view.build_embed()
        embed.title = "Parlay Builder"
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /myparlays ────────────────────────────────────────────────────

    @app_commands.command(name="myparlays", description="View your parlays")
    async def myparlays(self, interaction: discord.Interaction) -> None:
        if not await _safe_defer(interaction):
            return

        # Fetch both old and kalshi parlays
        old_parlays = await betting_service.get_user_parlays(interaction.user.id)
        kalshi_parlays = await betting_service.get_user_kalshi_parlays(interaction.user.id)

        if not old_parlays and not kalshi_parlays:
            await interaction.followup.send("You have no parlays.")
            return

        embed = discord.Embed(title="Your Parlays", color=discord.Color.blue())

        for p in kalshi_parlays[:10]:
            status = p["status"]
            if status == "won":
                icon = "\U0001f7e2"
                status_text = f"Won — **${p.get('payout', 0):.2f}**"
            elif status == "lost":
                icon = "\U0001f534"
                status_text = "Lost"
            else:
                icon = "\U0001f7e1"
                potential = round(p["amount"] * p["total_odds"], 2)
                status_text = f"Pending — potential **${potential:.2f}**"

            leg_lines = []
            for leg in p.get("legs", []):
                ls = leg["status"]
                if ls == "won":
                    leg_icon = "\U0001f7e2"
                elif ls == "lost":
                    leg_icon = "\U0001f534"
                else:
                    leg_icon = "\U0001f7e1"
                pick_display = leg.get("pick_display") or leg["pick"].upper()
                leg_lines.append(f"{leg_icon} {leg.get('title', '?')} — {pick_display} ({leg['odds']:.2f}x)")

            embed.add_field(
                name=f"{icon} Parlay #KP{p['id']} · {status_text}",
                value=(
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                ),
                inline=False,
            )

        # Show old parlays if any remain
        from bot.cogs.betting import PICK_LABELS
        for p in old_parlays[:5]:
            status = p["status"]
            if status == "won":
                icon = "\U0001f7e2"
                status_text = f"Won — **${p.get('payout', 0):.2f}**"
            elif status == "lost":
                icon = "\U0001f534"
                status_text = "Lost"
            else:
                icon = "\U0001f7e1"
                potential = round(p["amount"] * p["total_odds"], 2)
                status_text = f"Pending — potential **${potential:.2f}**"

            leg_lines = []
            for leg in p.get("legs", []):
                ls = leg["status"]
                if ls == "won":
                    leg_icon = "\U0001f7e2"
                elif ls == "lost":
                    leg_icon = "\U0001f534"
                elif ls == "push":
                    leg_icon = "\U0001f535"
                else:
                    leg_icon = "\U0001f7e1"
                home = leg.get("home_team") or "?"
                away = leg.get("away_team") or "?"
                pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
                point = leg.get("point")
                if point is not None:
                    if leg["pick"] in ("spread_home", "spread_away"):
                        pick_label += f" {point:+g}"
                    else:
                        pick_label += f" {point:g}"
                leg_lines.append(f"{leg_icon} {format_matchup(home, away)} — {pick_label} ({leg['odds']:.2f}x)")

            embed.add_field(
                name=f"{icon} Parlay #{p['id']} (legacy) · {status_text}",
                value=(
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                ),
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # ── /cancelparlay ─────────────────────────────────────────────────

    @app_commands.command(name="cancelparlay", description="Cancel a pending parlay")
    @app_commands.describe(parlay_id="The ID of the parlay to cancel (KP number)")
    async def cancelparlay(self, interaction: discord.Interaction, parlay_id: int) -> None:
        await interaction.response.defer(ephemeral=True)

        result = await betting_service.cancel_kalshi_parlay(parlay_id, interaction.user.id)
        if result is None:
            await interaction.followup.send(
                "Could not cancel parlay. Make sure you own it, it's still pending, and no legs have started settling.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Parlay Cancelled",
            description=f"Parlay #KP{parlay_id} cancelled. **${result['amount']:.2f}** refunded.",
            color=discord.Color.orange(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /myhistory ──────────────────────────────────────────────────────

    @app_commands.command(name="myhistory", description="View your resolved bets with stats")
    async def myhistory(self, interaction: discord.Interaction) -> None:
        if not await _safe_defer(interaction):
            return

        stats = await betting_service.get_user_stats(interaction.user.id)
        total_items = await betting_service.count_user_resolved(interaction.user.id)

        if total_items == 0:
            await interaction.followup.send("You have no resolved bets yet.")
            return

        view = HistoryView(interaction.user.id, stats, total_items)
        embed = await view.build_embed()
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /mybets ──────────────────────────────────────────────────────────

    @app_commands.command(name="mybets", description="View your active/pending bets")
    async def mybets(self, interaction: discord.Interaction) -> None:
        if not await _safe_defer(interaction):
            return

        bets = await betting_service.get_user_bets(interaction.user.id, status="pending")
        parlays = await betting_service.get_user_parlays(interaction.user.id, status="pending")
        kalshi_bets = await betting_service.get_user_kalshi_bets(interaction.user.id, status="pending")

        if not bets and not parlays and not kalshi_bets:
            await interaction.followup.send("You have no pending bets. Use `/myhistory` to view past bets.")
            return

        embed = discord.Embed(title="Your Active Bets", color=discord.Color.blue())
        now = datetime.now(timezone.utc)

        # Collect all fields as (sort_dt, name, value) so we can sort before rendering.
        fields: list[tuple[datetime | None, str, str]] = []

        for b in bets:
            potential = round(b["amount"] * b["odds"], 2)
            icon = "\U0001f7e1"  # yellow circle
            status_text = f"Pending — potential **${potential:.2f}**"

            home = b.get("home_team")
            away = b.get("away_team")
            sport = b.get("sport_title")
            is_outright = (b.get("market") or "") == "outrights"

            if is_outright:
                matchup = sport or "Futures"
            elif home and away:
                matchup = format_matchup(home, away)
            else:
                raw_id = b["game_id"].split("|")[0] if "|" in b["game_id"] else b["game_id"]
                matchup = f"Game `{raw_id}`"

            sport_line = f"{sport} · " if sport and not is_outright else ""
            pick_label = b["pick"] if is_outright else format_pick_label(b)

            sort_dt = _parse_iso_dt(b.get("commence_time"))
            eta = _fmt_eta(sort_dt, now, verb="starts") if sort_dt else ""
            eta_line = f"\n⏱ {eta}" if eta else ""

            fields.append((
                sort_dt,
                f"{icon} Bet #{b['id']} (legacy) · {status_text}",
                (
                    f"{sport_line}**{matchup}**\n"
                    f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {format_american(decimal_to_american(b['odds']))}"
                    f"{eta_line}"
                ),
            ))

        # Pending parlays
        for p in parlays:
            icon = "\U0001f7e1"
            potential = round(p["amount"] * p["total_odds"], 2)
            status_text = f"Pending — potential **${potential:.2f}**"

            leg_lines = []
            leg_dts: list[datetime] = []
            for leg in p.get("legs", []):
                ls = leg["status"]
                if ls == "won":
                    leg_icon = "\U0001f7e2"
                elif ls == "lost":
                    leg_icon = "\U0001f534"
                elif ls == "push":
                    leg_icon = "\U0001f535"
                else:
                    leg_icon = "\U0001f7e1"

                home = leg.get("home_team") or "?"
                away = leg.get("away_team") or "?"
                pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
                point = leg.get("point")
                if point is not None:
                    if leg["pick"] in ("spread_home", "spread_away"):
                        pick_label += f" {point:+g}"
                    else:
                        pick_label += f" {point:g}"
                leg_lines.append(f"{leg_icon} {format_matchup(home, away)} — {pick_label} ({format_american(decimal_to_american(leg['odds']))})")
                leg_dt = _parse_iso_dt(leg.get("commence_time"))
                if leg_dt:
                    leg_dts.append(leg_dt)

            # Sort key = last leg to start (parlay resolves when all games finish)
            sort_dt = max(leg_dts) if leg_dts else None
            eta = _fmt_eta(sort_dt, now, verb="starts") if sort_dt else ""
            eta_line = f"\n⏱ {eta}" if eta else ""

            fields.append((
                sort_dt,
                f"{icon} Parlay #{p['id']} (legacy) · {status_text}",
                (
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                    + eta_line
                ),
            ))

        # Fetch current Kalshi market prices for cash-out calculation.
        cashout_pairs: list[tuple[dict, int]] = []
        if kalshi_bets:
            tickers = list({kb["market_ticker"] for kb in kalshi_bets})
            fetched = await asyncio.gather(*[kalshi_api.get_market(t) for t in tickers], return_exceptions=True)
            market_map: dict[str, dict] = {}
            for ticker, result in zip(tickers, fetched):
                if isinstance(result, dict) and result:
                    market_map[ticker] = result
        else:
            market_map = {}

        # Pending Kalshi bets
        for kb in kalshi_bets:
            title = kb.get("title") or kb["market_ticker"]
            pick_label = kb.get("pick_display") or kb["pick"].upper()

            # Look up market data first (needed for close_time fallback and odds)
            market = market_map.get(kb["market_ticker"])

            # Determine sort/ETA time: stored close_time → live market close_time → ticker date
            close_dt = _parse_iso_dt(kb.get("close_time"))
            if close_dt is None and market:
                close_dt = _parse_iso_dt(_earliest_market_time(market))
            event_ticker = kb.get("event_ticker", "")
            ticker_date = _parse_event_ticker_date(event_ticker) if event_ticker else None
            sort_dt = close_dt or ticker_date

            # Icon and ETA line
            if sort_dt and sort_dt <= now:
                icon = "\U0001f534"  # red — live/closed
                eta_line = "\n\U0001f534 LIVE"
            else:
                icon = "\U0001f7e0" if sort_dt and (sort_dt - now).total_seconds() < 86400 else "\U0001f7e3"
                eta_line = f"\n⏱ {_fmt_eta(sort_dt, now)}" if sort_dt else ""
            display_odds = kb["odds"] if (kb["odds"] or 0) > 0 else None
            if market:
                is_finalized = (market.get("status") or "") == "finalized"
                if is_finalized:
                    yes_ask_raw = market.get("previous_yes_ask_dollars")
                else:
                    yes_ask_raw = market.get("yes_ask_dollars") or market.get("last_price_dollars")
                if yes_ask_raw is not None:
                    try:
                        yes_ask = float(yes_ask_raw)
                        if 0.01 <= yes_ask <= 0.99:
                            pick = (kb.get("pick") or "").lower()
                            current_price = yes_ask if pick == "yes" else (1.0 - yes_ask)
                            if current_price > 0:
                                display_odds = round(1.0 / current_price, 3)
                    except (ValueError, TypeError):
                        pass

            stored_odds = kb["odds"] if (kb.get("odds") or 0) > 0 else None
            if stored_odds:
                potential = round(kb["amount"] * stored_odds, 2)
            else:
                potential = None

            # Cash-out value
            cashout = _calc_cashout(kb, market) if market else None
            if cashout is not None:
                cashout_pairs.append((kb, cashout))
                delta = cashout - kb["amount"]
                sign = "+" if delta >= 0 else ""
                cashout_line = f"\nCash out: **${cashout:.2f}** ({sign}${delta:.2f})"
            else:
                cashout_line = ""

            if potential is not None:
                status_text = f"Pending — potential **${potential:.2f}**"
                odds_str = format_american(decimal_to_american(display_odds))
            else:
                status_text = "Pending — potential **N/A**"
                odds_str = "N/A"

            fields.append((
                sort_dt,
                f"{icon} Kalshi #{kb['id']} · {status_text}",
                (
                    f"**{title}**\n"
                    f"Pick: **{pick_label}** · ${kb['amount']:.2f} @ {odds_str}"
                    f"{eta_line}"
                    f"{cashout_line}"
                ),
            ))

        # Sort soonest first; None (no known time) sorts last.
        _FAR_FUTURE = datetime.max.replace(tzinfo=timezone.utc)
        fields.sort(key=lambda x: x[0] if x[0] is not None else _FAR_FUTURE)

        for _, field_name, field_value in fields:
            embed.add_field(name=field_name, value=field_value, inline=False)

        embed.set_footer(
            text="Use /myhistory to view resolved bets"
            + (" · Use the dropdown to cash out a Kalshi bet early" if cashout_pairs else "")
        )
        if cashout_pairs:
            cashout_view = _KalshiCashOutView(interaction.user.id, cashout_pairs)
            msg = await interaction.followup.send(embed=embed, view=cashout_view)
            cashout_view.message = msg
        else:
            await interaction.followup.send(embed=embed)

    # ── /cancelbet ───────────────────────────────────────────────────────

    @app_commands.command(name="cancelbet", description="Cancel a pending bet")
    @app_commands.describe(bet_id="The ID of the bet to cancel (can be numeric or start with K)")
    async def cancelbet(self, interaction: discord.Interaction, bet_id: str) -> None:
        await interaction.response.defer(ephemeral=True)

        # Handle Kalshi bet IDs (e.g., "K123")
        if bet_id.upper().startswith("K"):
            try:
                numeric_id = int(bet_id[1:])
                result = await betting_service.cancel_kalshi_bet(numeric_id, interaction.user.id)
                if result:
                    embed = discord.Embed(
                        title="Bet Cancelled",
                        description=f"Kalshi bet #K{numeric_id} cancelled. **${result['amount']:.2f}** refunded.",
                        color=discord.Color.orange(),
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except ValueError:
                pass

        # Handle legacy numeric IDs
        try:
            numeric_id = int(bet_id)
            # Look up the bet to check game status
            bets = await betting_service.get_user_bets(interaction.user.id)
            target = next((b for b in bets if b["id"] == numeric_id), None)

            if target:
                # Parse composite game_id
                parts = target["game_id"].split("|")
                event_id = parts[0]
                sport_key = parts[1] if len(parts) > 1 else None

                status = await self.sports_api.get_fixture_status(event_id, sport_key)
                if status and status["started"]:
                    await interaction.followup.send(
                        "Cannot cancel — this game has already started.", ephemeral=True
                    )
                    return

            result = await betting_service.cancel_bet(numeric_id, interaction.user.id)
            if result:
                embed = discord.Embed(
                    title="Bet Cancelled",
                    description=f"Legacy bet #{numeric_id} cancelled. **${result['amount']:.2f}** refunded.",
                    color=discord.Color.orange(),
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        except ValueError:
            pass

        await interaction.followup.send(
            "Could not cancel bet. Make sure the ID is correct, you own it, and it's still pending.",
            ephemeral=True,
        )

    # ── /pendingbets (admin) ─────────────────────────────────────────────

    @app_commands.command(name="pendingbets", description="[Admin] View all pending bets")
    @app_commands.checks.has_permissions(administrator=True)
    async def pendingbets(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        game_ids = await betting_service.get_pending_game_ids()

        embed = discord.Embed(title="Pending Bets", color=discord.Color.orange())

        for composite_id in game_ids[:15]:
            bets = await betting_service.get_bets_by_game(composite_id)
            if not bets:
                continue

            first = bets[0]
            home = first.get("home_team")
            away = first.get("away_team")
            sport = first.get("sport_title") or ""
            is_outright = (first.get("market") or "") == "outrights"

            if is_outright:
                matchup = sport or "Futures"
            elif home and away:
                matchup = format_matchup(home, away)
            else:
                matchup = "Unknown"

            lines = []
            total_wagered = 0
            for b in bets:
                pick_label = b["pick"] if is_outright else format_pick_label(b)
                lines.append(
                    f"<@{b['user_id']}> — {pick_label} · ${b['amount']:.2f} @ {b['odds']}x"
                )
                total_wagered += b["amount"]

            # Show game time from first bet's commence_time
            time_str = ""
            ct = first.get("commence_time")
            if ct:
                try:
                    ct_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                    if ct_dt <= datetime.now(timezone.utc):
                        time_str = " \U0001f534 LIVE"
                    else:
                        time_str = f"\n{format_game_time(ct)}"
                except (ValueError, TypeError):
                    pass

            raw_id = composite_id.split("|")[0] if "|" in composite_id else composite_id
            header = f"{matchup} ({len(bets)} bet{'s' if len(bets) != 1 else ''} · ${total_wagered:.2f}){time_str}"
            value = "\n".join(lines) + f"\nID: `{raw_id}`"

            embed.add_field(name=header, value=value, inline=False)

        if len(game_ids) > 15:
            embed.set_footer(text=f"Showing 15 of {len(game_ids)} games")

        # Also show pending parlays
        from bot.db.database import get_connection as _get_conn
        from bot.db import models as _models
        _db = await _get_conn()
        try:
            _cursor = await _db.execute(
                "SELECT * FROM parlays WHERE status = 'pending' ORDER BY created_at DESC LIMIT 15"
            )
            _rows = await _cursor.fetchall()
            pending_parlays = [dict(r) for r in _rows]
        finally:
            await _db.close()

        if pending_parlays:
            parlay_lines = []
            for p in pending_parlays:
                legs = await _models.get_parlay_legs(p["id"])
                leg_count = len(legs)
                parlay_lines.append(
                    f"<@{p['user_id']}> — Parlay #{p['id']} · {leg_count} legs · "
                    f"${p['amount']:.2f} @ {p['total_odds']:.2f}x"
                )
            embed.add_field(
                name=f"Pending Parlays ({len(pending_parlays)})",
                value="\n".join(parlay_lines),
                inline=False,
            )

        # Kalshi bets
        kalshi_bets = await _models.get_all_pending_kalshi_bets()
        if kalshi_bets:
            kalshi_lines = []
            total_kalshi_wagered = 0
            for kb in kalshi_bets[:20]:
                pick_display = kb.get("pick_display") or kb["pick"]
                title = kb.get("title") or kb["market_ticker"]
                if len(title) > 40:
                    title = title[:37] + "..."
                # Parse game time from event ticker
                time_info = ""
                et = kb.get("event_ticker", "")
                ticker_date = _parse_event_ticker_date(et) if et else None
                if ticker_date:
                    now = datetime.now(timezone.utc)
                    if ticker_date.date() < now.date():
                        time_info = " \U0001f534 LIVE"
                    else:
                        time_info = f" · {ticker_date.strftime('%-m/%-d')}"
                kalshi_lines.append(
                    f"<@{kb['user_id']}> — {pick_display} · "
                    f"${kb['amount']:.2f} @ {kb['odds']:.2f}x{time_info}\n{title}"
                )
                total_kalshi_wagered += kb["amount"]
            header = f"Kalshi Bets ({len(kalshi_bets)} · ${total_kalshi_wagered:.2f})"
            value = "\n".join(kalshi_lines)
            if len(kalshi_bets) > 20:
                value += f"\n*...and {len(kalshi_bets) - 20} more*"
            embed.add_field(name=header, value=value[:1024], inline=False)

        if not game_ids and not pending_parlays and not kalshi_bets:
            embed.description = "No pending bets."

        await interaction.followup.send(embed=embed, ephemeral=True)

    @pendingbets.error
    async def pendingbets_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "You need administrator permissions to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        else:
            log.exception("Error in /pendingbets command", exc_info=error)

    # ── /vacuum (admin) ───────────────────────────────────────────────────

    @app_commands.command(name="vacuum", description="[Admin] Force cleanup of cache and reclaim disk space")
    @app_commands.checks.has_permissions(administrator=True)
    async def vacuum(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            log.info(f"Manual vacuum triggered by {interaction.user}")
            deleted = await cleanup_cache(max_age_days=0)  # Clear all cache
            vacuumed = await vacuum_db()
            vacuum_note = "database vacuumed" if vacuumed else "VACUUM skipped (check logs)"
            await interaction.followup.send(
                f"Maintenance complete. Cleared {deleted} cache entries, {vacuum_note}.",
                ephemeral=True
            )
        except Exception as e:
            log.exception("Vacuum command failed")
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    # ── /resolve (admin) ───────────────────────────────────────────────

    @app_commands.command(name="resolve", description="[Admin] Manually resolve a game")
    @app_commands.describe(
        game_id="Game ID to resolve",
        winner="The winning side: home, away, or draw",
        home_score="Home team final score (needed for spread/total bets)",
        away_score="Away team final score (needed for spread/total bets)",
        winner_name="For futures: the winning team/outcome name",
    )
    @app_commands.choices(
        winner=[
            app_commands.Choice(name="Home", value="home"),
            app_commands.Choice(name="Away", value="away"),
            app_commands.Choice(name="Draw", value="draw"),
        ]
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def resolve(
        self,
        interaction: discord.Interaction,
        game_id: str,
        winner: app_commands.Choice[str],
        home_score: int | None = None,
        away_score: int | None = None,
        winner_name: str | None = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        # Find any pending bets matching this game_id (full or partial match)
        pending_ids = await betting_service.get_pending_game_ids()
        matching = [gid for gid in pending_ids if game_id in gid]

        if not matching:
            await interaction.followup.send(
                f"No pending bets found for game `{game_id}`.", ephemeral=True
            )
            return

        all_resolved: list[dict] = []
        for composite_id in matching:
            resolved = await betting_service.resolve_game(
                composite_id, winner.value,
                home_score=home_score, away_score=away_score,
                winner_name=winner_name,
            )
            all_resolved.extend(resolved)
        total_resolved = len(all_resolved)

        if all_resolved:
            await self._post_resolution_announcement(
                all_resolved, home_score=home_score, away_score=away_score,
            )

        if winner_name:
            desc = (
                f"Futures event `{game_id}` resolved.\n"
                f"Winner: **{winner_name}**\n"
                f"**{total_resolved}** bet(s) settled."
            )
        else:
            score_note = ""
            if home_score is not None and away_score is not None:
                score_note = f"\nScore: {home_score} - {away_score} (spread/total bets resolved)"
            else:
                score_note = "\nNote: Spread/total bets need scores to resolve — provide home_score and away_score."
            desc = (
                f"Game `{game_id}` resolved as **{winner.name}** win.\n"
                f"**{total_resolved}** bet(s) settled."
                f"{score_note}"
            )

        embed = discord.Embed(
            title="Game Resolved",
            description=desc,
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @resolve.error
    async def resolve_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            if interaction.response.is_done():
                await interaction.followup.send(
                    "You need administrator permissions to use this command.", ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "You need administrator permissions to use this command.", ephemeral=True
                )
        else:
            log.exception("Error in /resolve command", exc_info=error)

    # ── Resolution announcement ────────────────────────────────────────

    async def _post_resolution_announcement(
        self,
        resolved_bets: list[dict],
        home_score: int | None = None,
        away_score: int | None = None,
    ) -> None:
        """Post an embed announcing resolved bets to the results channel."""
        if not resolved_bets:
            return

        channel = self.bot.get_channel(BET_RESULTS_CHANNEL_ID)
        if channel is None:
            log.warning("Results channel %s not found", BET_RESULTS_CHANNEL_ID)
            return

        first = resolved_bets[0]
        home = first.get("home_team") or "Home"
        away = first.get("away_team") or "Away"
        is_outright = (first.get("market") or "") == "outrights"

        if is_outright:
            title = f"Result: {first.get('sport_title', 'Futures')}"
            description = ""
        else:
            title = f"Game Result: {format_matchup(home, away)}"
            if home_score is not None and away_score is not None:
                description = f"**{home}** {home_score} - {away_score} **{away}**"
            else:
                description = ""

        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.blue(),
        )

        lines = []
        single_bets = [b for b in resolved_bets if b.get("type") != "parlay"]
        for bet in single_bets:
            result = bet["result"]
            if result == "won":
                icon = "\U0001f7e2"
                result_text = f"Won **${bet['payout']:.2f}**"
            elif result == "push":
                icon = "\U0001f535"
                result_text = f"Push (${bet['payout']:.2f} refunded)"
            else:
                icon = "\U0001f534"
                result_text = "Lost"

            pick_label = format_pick_label(bet) if not is_outright else bet["pick"]
            lines.append(
                f"{icon} <@{bet['user_id']}> — {pick_label} · "
                f"${bet['amount']:.2f} @ {bet['odds']}x → {result_text}"
            )

        if lines:
            embed.add_field(name="Bets", value="\n".join(lines), inline=False)

        # Parlay results
        parlay_entries = [b for b in resolved_bets if b.get("type") == "parlay"]
        if parlay_entries:
            parlay_lines = []
            for p in parlay_entries:
                result = p["result"]
                if result == "won":
                    icon = "\U0001f7e2"
                    result_text = f"Won **${p['payout']:.2f}**"
                else:
                    icon = "\U0001f534"
                    result_text = "Lost"
                # Build leg summary with team names
                leg_parts = []
                for leg in p.get("legs", []):
                    h = leg.get("home_team") or "?"
                    a = leg.get("away_team") or "?"
                    pick_label = format_pick_label(leg)
                    leg_icon = "\u2705" if leg.get("status") == "won" else "\u274c" if leg.get("status") == "lost" else "\u23f3"
                    leg_parts.append(f"  {leg_icon} {format_matchup(h, a)} — {pick_label}")
                legs_text = "\n".join(leg_parts)
                parlay_lines.append(
                    f"{icon} <@{p['user_id']}> — Parlay #{p['id']} · "
                    f"${p['amount']:.2f} @ {p.get('total_odds', 0):.2f}x → {result_text}\n{legs_text}"
                )
            embed.add_field(name="Parlays", value="\n".join(parlay_lines), inline=False)

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            log.exception("Failed to send resolution announcement")

        if any(p["result"] == "won" for p in parlay_entries):
            try:
                await channel.send("oh shit pullin a kecker")
            except discord.HTTPException:
                pass

    # ── /live ─────────────────────────────────────────────────────────

    @app_commands.command(name="live", description="Upcoming sports markets — sorted by soonest close")
    async def live(self, interaction: discord.Interaction) -> None:
        if not await _safe_defer(interaction):
            return

        all_markets = await kalshi_api.get_all_open_markets()
        now = datetime.now(timezone.utc)

        # Keep only markets whose actionable deadline is still in the future, sorted soonest first.
        # Use the earlier of close_time / expected_expiration_time — different market types
        # place the "game time" in different fields.
        upcoming: list[tuple[datetime, dict]] = []
        no_close: list[dict] = []
        for m in all_markets:
            close_str = _earliest_market_time(m)
            if not close_str:
                no_close.append(m)
                continue
            try:
                close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                no_close.append(m)
                continue
            if close_dt > now:
                upcoming.append((close_dt, m))

        upcoming.sort(key=lambda t: t[0])
        closing_soon = [m for _, m in upcoming] + no_close

        if not closing_soon:
            await interaction.followup.send("No open markets right now.")
            return

        view = MarketListView(closing_soon)
        embed = view.build_embed()
        embed.title = "\U0001f534 Open Markets"
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── Periodic discovery (pre-warm cache) ─────────────────────────

    @tasks.loop(minutes=30)
    async def refresh_discovery(self) -> None:
        try:
            result = await kalshi_api.discover_available(force=True)
            games = result.get("games", {})
            futures = result.get("futures", {})
            log.info(
                "Periodic discovery: %d game sports, %d futures sports",
                len(games), len(futures),
            )
            await kalshi_api.get_all_games()
        except Exception:
            log.exception("Error in periodic discovery")

    @refresh_discovery.before_loop
    async def before_refresh_discovery(self) -> None:
        await self.bot.wait_until_ready()

    # ── Periodic sports refresh (pick up new series) ─────────────────

    @tasks.loop(hours=24)
    async def refresh_sports_loop(self) -> None:
        try:
            await kalshi_api.refresh_sports()
            log.info("Periodic sports refresh: %d sports available", len(SPORTS))
        except Exception:
            log.exception("Error in periodic sports refresh")

    @refresh_sports_loop.before_loop
    async def before_refresh_sports_loop(self) -> None:
        await self.bot.wait_until_ready()

    # ── Database maintenance task ────────────────────────────────────

    @tasks.loop(time=time(hour=4, minute=0, tzinfo=timezone.utc))
    async def db_maintenance(self) -> None:
        """Periodic database cleanup to save disk space."""
        try:
            log.info("Starting database maintenance...")
            deleted = await cleanup_cache(max_age_days=1)
            log.info(f"Cleaned up {deleted} stale cache entries.")
            vacuumed = await vacuum_db()
            if vacuumed:
                log.info("Database vacuumed successfully.")
            else:
                log.info("VACUUM skipped (see warning above).")
        except Exception:
            log.exception("Error in db_maintenance loop")

    @db_maintenance.before_loop
    async def before_db_maintenance(self) -> None:
        await self.bot.wait_until_ready()

    # ── Resolution loop ───────────────────────────────────────────────

    @tasks.loop(minutes=3)
    async def check_kalshi_results(self) -> None:
        from bot.db import models

        pending = await models.get_pending_kalshi_tickers_with_close_time()
        parlay_pending = await models.get_pending_kalshi_parlay_tickers_with_close_time()

        # Merge single bet and parlay tickers
        all_pending_map: dict[str, str | None] = {}
        for row in pending:
            all_pending_map[row["market_ticker"]] = row.get("close_time")
        for row in parlay_pending:
            ticker = row["market_ticker"]
            ct = row.get("close_time")
            if ticker not in all_pending_map or (ct and (all_pending_map[ticker] is None or ct < all_pending_map[ticker])):
                all_pending_map[ticker] = ct

        if not all_pending_map:
            return

        now = datetime.now(timezone.utc)

        # Increment counter: full sweep every 5th iteration (~15 min)
        self._kalshi_check_count = getattr(self, "_kalshi_check_count", 0) + 1
        full_sweep = self._kalshi_check_count % 5 == 0

        # Determine which tickers to check this iteration
        tickers: list[str] = []
        for ticker, ct in all_pending_map.items():
            if full_sweep or not ct:
                tickers.append(ticker)
                continue
            try:
                close_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                tickers.append(ticker)
                continue
            if now >= close_dt - timedelta(hours=1):
                tickers.append(ticker)

        if not tickers:
            return

        sweep_label = "full" if full_sweep else "urgent"
        log.info("Checking %d/%d pending Kalshi market(s) [%s]...", len(tickers), len(all_pending_map), sweep_label)

        channel = None
        if BET_RESULTS_CHANNEL_ID:
            channel = self.bot.get_channel(BET_RESULTS_CHANNEL_ID)

        for ticker in tickers:
            try:
                market = await kalshi_api.get_market(ticker)
                if not market:
                    continue

                status = market.get("status", "")
                if status not in ("settled", "finalized"):
                    continue

                settlement = market.get("settlement_value_dollars")
                if settlement is None:
                    result_val = market.get("result", "")
                    if result_val == "yes":
                        winning_side = "yes"
                    elif result_val == "no":
                        winning_side = "no"
                    else:
                        continue
                else:
                    try:
                        sv = float(settlement)
                    except (ValueError, TypeError):
                        continue
                    winning_side = "yes" if sv >= 0.99 else "no"

                bets = await models.get_pending_kalshi_bets_by_market(ticker)
                board_before = await leaderboard_notifier.snapshot()
                for bet in bets:
                    won = bet["pick"] == winning_side
                    payout = round(bet["amount"] * bet["odds"], 2) if won else 0
                    await models.resolve_kalshi_bet(bet["id"], won, payout)
                    if won:
                        await leaderboard_notifier.notify_if_passed(bet["user_id"], round(payout), board_before, "sports betting")

                    if channel:
                        result_text = "won" if won else "lost"
                        emoji = "\U0001f7e2" if won else "\U0001f534"
                        user = self.bot.get_user(bet["user_id"])
                        name = user.display_name if user else f"User {bet['user_id']}"
                        pick_display = bet.get("pick_display") or bet["pick"].upper()
                        bet_title = bet.get("title") or ticker

                        embed = discord.Embed(
                            title=f"{emoji} Bet #K{bet['id']} — {result_text.upper()}",
                            color=discord.Color.green() if won else discord.Color.red(),
                        )
                        embed.add_field(name="Bettor", value=name, inline=True)
                        embed.add_field(name="Market", value=bet_title[:256], inline=False)
                        embed.add_field(name="Pick", value=pick_display, inline=True)
                        embed.add_field(name="Wager", value=f"${bet['amount']:.2f}", inline=True)
                        embed.add_field(name="Payout", value=f"${payout:.2f}", inline=True)
                        embed.add_field(name="Result", value=f"Market settled **{winning_side.upper()}**", inline=False)
                        try:
                            await channel.send(embed=embed)
                        except discord.DiscordException:
                            pass

                # ── Resolve parlay legs for this ticker ──
                parlay_legs = await models.get_pending_kalshi_parlay_legs_by_market(ticker)
                affected_parlay_ids: set[int] = set()

                for leg in parlay_legs:
                    leg_won = leg["pick"] == winning_side
                    leg_status = "won" if leg_won else "lost"
                    await models.update_kalshi_parlay_leg_status(leg["id"], leg_status)
                    affected_parlay_ids.add(leg["parlay_id"])

                for pid in affected_parlay_ids:
                    kp = await models.get_kalshi_parlay_by_id(pid)
                    if not kp or kp["status"] != "pending":
                        continue

                    all_legs = await models.get_kalshi_parlay_legs(pid)
                    statuses = [l["status"] for l in all_legs]

                    if "lost" in statuses:
                        await models.update_kalshi_parlay(pid, "lost", payout=0)
                        if channel:
                            user = self.bot.get_user(kp["user_id"])
                            name = user.display_name if user else f"User {kp['user_id']}"
                            leg_summary = "\n".join(
                                f"{'✅' if l['status'] == 'won' else '❌' if l['status'] == 'lost' else '⏳'} {l.get('pick_display') or l['pick']}"
                                for l in all_legs
                            )
                            embed = discord.Embed(
                                title=f"\U0001f534 Parlay #KP{pid} — LOST",
                                color=discord.Color.red(),
                            )
                            embed.add_field(name="Bettor", value=name, inline=True)
                            embed.add_field(name="Wager", value=f"${kp['amount']:.2f}", inline=True)
                            embed.add_field(name="Legs", value=leg_summary[:1024], inline=False)
                            try:
                                await channel.send(embed=embed)
                            except discord.DiscordException:
                                pass

                    elif all(s in ("won",) for s in statuses):
                        effective_odds = 1.0
                        for l in all_legs:
                            effective_odds *= l["odds"]
                        payout = round(kp["amount"] * effective_odds, 2)
                        await models.update_kalshi_parlay(pid, "won", payout=payout)
                        await leaderboard_notifier.deposit_and_notify(kp["user_id"], int(payout), "sports betting")
                        if channel:
                            user = self.bot.get_user(kp["user_id"])
                            name = user.display_name if user else f"User {kp['user_id']}"
                            leg_summary = "\n".join(
                                f"✅ {l.get('pick_display') or l['pick']}"
                                for l in all_legs
                            )
                            embed = discord.Embed(
                                title=f"\U0001f7e2 Parlay #KP{pid} — WON!",
                                color=discord.Color.green(),
                            )
                            embed.add_field(name="Bettor", value=name, inline=True)
                            embed.add_field(name="Wager", value=f"${kp['amount']:.2f}", inline=True)
                            embed.add_field(name="Payout", value=f"${payout:.2f}", inline=True)
                            embed.add_field(name="Legs", value=leg_summary[:1024], inline=False)
                            try:
                                await channel.send(embed=embed)
                                await channel.send("oh shit pullin a kecker")
                            except discord.DiscordException:
                                pass
                    # else: still pending

                log.info("Resolved Kalshi market %s -> %s", ticker, winning_side)

            except Exception:
                log.exception("Error checking Kalshi market %s", ticker)

    @check_kalshi_results.before_loop
    async def before_check_kalshi(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(KalshiCog(bot))
