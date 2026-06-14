"""Classify any Kalshi series into a bet-type taxonomy.

The /bet menu is organised as  League → bet category → event → markets.
Level 1 (league/sport) is handled by emoji grouping in the cog. This module
owns level 2: given a series ticker (and optionally its title), decide which
*category* a market belongs in so the flat per-sport "Futures" dump can be
broken into navigable buckets.

Output categories (``BetType``):

* ``GAME``    — a single dated matchup: moneyline / spread / total and
                in-game derivatives (halves, quarters, maps, BTTS, corners,
                first scorer). These fold into the game's market list.
* ``PROP``    — player performance markets (points, yards, HR, goals, …),
                whether season-long or for one game.
* ``FUTURES`` — season / tournament outcomes not tied to one game:
                championships, awards, win totals, conference & division
                winners, draft, make-the-playoffs, finishing position.
* ``SPECIAL`` — roster/coaching transactions (next team, coach out, trades)
                and non-sport novelty markets (rank lists, eating contests,
                donations, crypto leaderboards).

The classifier is a priority-ordered rule table over the *uppercased ticker*.
The bet-type tokens (MVP, WINS, DRAFT, PTS, COACHOUT, …) are league-agnostic,
so matching on the ticker alone is reliable; ``title`` is only used as a tie
breaker for a few ambiguous cases.

It is deliberately free of any discord / aiohttp imports so it can be unit
tested in isolation (see ``test_taxonomy.py``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# ── Bet-type constants ────────────────────────────────────────────────
GAME = "game"
PROP = "prop"
FUTURES = "futures"
SPECIAL = "special"


# ── Futures subtypes (sub-buckets shown under "Futures") ──────────────
SUB_CHAMPIONSHIP = "Championship"
SUB_CONFERENCE = "Conference / Division"
SUB_AWARD = "Awards"
SUB_WIN_TOTAL = "Win Totals & Records"
SUB_DRAFT = "Draft"
SUB_PLAYOFF = "Playoffs & Seeding"
SUB_FINISH = "Finishing Position"
SUB_STAT_LEADER = "Stat Leaders"
SUB_OUTRIGHT = "Tournament Winner"

# Prop subtype is the stat name; game subtype is the line type; special subtype
# is "Transactions" or "Novelty".
SUB_TRANSACTION = "Transactions"
SUB_NOVELTY = "Novelty"
SUB_GAME_LINES = "Game Lines"
SUB_PERIOD = "Period / Halves"
SUB_GAME_PROP = "Game Props"


@dataclass(frozen=True)
class SeriesClass:
    bet_type: str          # GAME | PROP | FUTURES | SPECIAL
    subtype: str           # human-readable sub-bucket label
    is_sport: bool = True  # False for non-sport novelty


# A rule is (compiled-regex-over-ticker, SeriesClass). First match wins, so the
# table is ordered most-specific → most-general. Tokens are matched against the
# full uppercased ticker; using anchors (^, $) where a token is a true suffix
# avoids false hits inside longer league prefixes.
def _r(pattern: str, bet_type: str, subtype: str, is_sport: bool = True):
    return (re.compile(pattern), SeriesClass(bet_type, subtype, is_sport))


# Explicit non-sport / novelty tickers and token families. These come FIRST so
# they never get swept into a sports bucket by a generic token later.
_NOVELTY_RULES = [
    _r(r"RANKLIST", SPECIAL, SUB_NOVELTY, False),
    _r(r"DONATE|BEASTWATER|MRBEAST", SPECIAL, SUB_NOVELTY, False),
    _r(r"NATHAN", SPECIAL, SUB_NOVELTY, False),          # Nathan's hot dogs
    _r(r"SPELLINGBEE", SPECIAL, SUB_NOVELTY, False),
    _r(r"IMOCOUNTRY|IMO\b", SPECIAL, SUB_NOVELTY, False),  # math olympiad
    _r(r"YTDAILYTOPVIDEO|YOUTUBE", SPECIAL, SUB_NOVELTY, False),
    _r(r"ALPHAARENA|SYNTHETIX|AICODE|BONK\b|SUI\b", SPECIAL, SUB_NOVELTY, False),
    _r(r"KYLE\d*M|KYLEMILE|BRAD\d*M", SPECIAL, SUB_NOVELTY, False),  # endurance stunts
    _r(r"EUROVISION", SPECIAL, SUB_NOVELTY, False),
    _r(r"INTERNETINVITATIONAL", SPECIAL, SUB_NOVELTY, False),
    _r(r"ATTENDANCE|ATTEND\b|SWIFTATTEND|OLEMISSATTEND", SPECIAL, SUB_NOVELTY, False),
    _r(r"ISRAELBAN|UEFAISRAEL|PORTNOY|TUSHPUSH|REBOOT|JCOLECBA", SPECIAL, SUB_NOVELTY, False),
    _r(r"REDZONEADS|REDZONEBRANDADS|PRIMETIME|SBMENTION", SPECIAL, SUB_NOVELTY, False),
    _r(r"GOLFTENNISMAJORS|BATTLEOFSEXES", SPECIAL, SUB_NOVELTY),
]

# Roster / coaching / front-office transactions.
_TRANSACTION_RULES = [
    _r(r"COACHOUT|NEXTCOACH|NEWCOACH|NEXTNFLCOACH|MANAGEROUT|MANAGERSOUT|NEXTMANAGER|COACH$|COACHONDATE", SPECIAL, SUB_TRANSACTION),
    _r(r"NEXTTEAM|NEXTEAM|NEXTCONTRACT|CONTRACTSIZE|CONTRACT\b|CLUBCHANGE", SPECIAL, SUB_TRANSACTION),
    _r(r"TRADE|TRADEOFF|ACQUIRE|SPORTSIGN|PLAYTOGETHER", SPECIAL, SUB_TRANSACTION),
    _r(r"JOIN|SIGN\b|SELL[A-Z]+|PLAYEROPTION|EXPAND|UNRETIRE|RETIRE|REUNITE|RETURN$|UNC$|CFO$", SPECIAL, SUB_TRANSACTION),
    _r(r"STARTNO|STARTCIN|STARTNYJ|STARTCLE|STARTING?QB|DEPTHPOSITION|ALLSTARROSTER|WCSQUAD|SQUAD$", SPECIAL, SUB_TRANSACTION),
]

# Player performance props. "LEADER" / trailing-stat leader markets are handled
# as FUTURES (stat leaders) below, so they must be excluded here; we put the
# LEADER rule first within this section to claim them as stat leaders.
_PROP_RULES = [
    # League-wide stat leaders → futures bucket
    _r(r"^KXLEADER|LEADER[A-Z]*$|PTSLEADER|SERIESPTSLEADER", FUTURES, SUB_STAT_LEADER),
    # Individual player props (per-game or season). Anchored to ticker end so we
    # don't catch e.g. team tickers. Includes basketball / football / baseball /
    # hockey / soccer player stats.
    _r(r"(PTS|REB|(?<!E)AST|3PT|BLK|STL|FOUL)$", PROP, "Player Stats"),  # (?<!E)AST so "EAST" (conference) isn't read as assists
    _r(r"[234]D$|QUADRUPLEDOUBLE|DOUBLEDOUBLE|TRIPLEDOUBLE", PROP, "Double / Triple-Double"),
    _r(r"(RECYDS|RSHYDS|RUSHYDS|REC|PASSING|RUSHING|RECEIVING|SACK|SAFETY|2PTCONV|4DCONV)$", PROP, "Player Stats"),
    _r(r"MOSTRECYDS|FANTASYMOST|FANTASY|RECYDSRECORD|SACKRECORD", PROP, "Player Stats"),
    _r(r"(HR|RBI|HIT|KS|SAVES|ERA|WAR|DOUBLES|TRIPLES|STEALS|SEASONHR|SEASONSTAT|STAT|STATCOUNT)$", PROP, "Player Stats"),
    _r(r"(ANYGOAL|FIRSTGOAL|GOAL|PTS|(?<!E)AST)$", PROP, "Player Stats"),  # hockey/soccer scorers
    _r(r"FIRSTBASKET|FIRSTGOAL|FIRSTGOALSCORER|GOALSCORER|ANYGOAL", PROP, "Scorers"),
    _r(r"PA$|POINTSASSISTS", PROP, "Player Stats"),
    _r(r"PLAYEROTM|PLAYEROTW|PLAYEROFTHE", PROP, "Player of the Period"),
]

# Awards (individual season honours).
_AWARD_RULES = [
    _r(r"MVP|FINALSMVP|SBMVP|WFINMVP|CUPMVP|ASMVP|ALLSTARMVP", FUTURES, SUB_AWARD),
    _r(r"(^|[A-Z])(CY|OROY|DROY|ROY|ROTY|DPOY|OPOY|COY|COTY|EOTY|MOTY|POY|POTY|SIXTH|SPORTSMANSHIP)([A-Z]|$)", FUTURES, SUB_AWARD),
    _r(r"HEISMAN|NAISMITH|CALDER|NORRIS|ADAMS|RICHARD|CONNSMYTHE|HAARON|HANKAARON|VEZINA|SELKE|HART", FUTURES, SUB_AWARD),
    _r(r"COMEBACK|ALCPOTY|RELOTY|EXECUTIVE|MOP|MOSTOUTSTANDING|PLAYEROFYEAR|PFAPOY|EPLPOY", FUTURES, SUB_AWARD),
    _r(r"FINALIST|HEISMANFINALIST", FUTURES, SUB_AWARD),
]

# Win totals & season records.
_WINTOTAL_RULES = [
    _r(r"EXACTWINS|WINS-|WINS$|WINRECORD|WINSTREAK|RECORD[A-Z]*BEST|WORSTRECORD|BESTRECORD|LSTREAK|500$|UNDEFEATED|UNBEATEN", FUTURES, SUB_WIN_TOTAL),
]

# Draft markets.
_DRAFT_RULES = [
    _r(r"DRAFT|FIRSTPICK|TEAM\dPOS|DRAFTPICK|DRAFTED|DRAFTTOP|DRAFTCAT|DRAFTCONF|DRAFTTRADE|DRAFTOU|DRAFTTEAMPOS", FUTURES, SUB_DRAFT),
]

# Playoffs, seeding, advancing.
_PLAYOFF_RULES = [
    _r(r"PLAYOFF|MAKEPLAYOFF|CFPSEED|CFP|SEED$|SEEDDIF|SEEDWIN|SEEDSUM|HIGHSEED|ADVANCE|TOROUND|RO\d|RO4|RO8|TEAMSIN|JOINCONF", FUTURES, SUB_PLAYOFF),
]

# Conference / division winners & conference tournaments / regular-season titles.
_CONFERENCE_RULES = [
    _r(r"AFCNORTH|AFCSOUTH|AFCEAST|AFCWEST|NFCNORTH|NFCSOUTH|NFCEAST|NFCWEST", FUTURES, SUB_CONFERENCE),
    _r(r"ALEAST|ALWEST|ALCENT|NLEAST|NLWEST|NLCENT|MBLNLEAST|ALCENTRAL|DIVWINNER|DIVISION", FUTURES, SUB_CONFERENCE),
    _r(r"PACIFIC|METROPOLITAN|ATLANTIC|CENTRAL|SOUTHWEST|SOUTHEAST|NORTHWEST|MIDWEST", FUTURES, SUB_CONFERENCE),
    _r(r"EAST$|WEST$|EASTERN|WESTERN|CONF$|CONFERENCE|AFCCHAMP|NFCCHAMP|AFC$|NFC$", FUTURES, SUB_CONFERENCE),
    # Conference-tournament / regular-season titles. The short abbreviations are
    # ticker suffixes, so anchor them with $ — otherwise tokens like CAA/AAC/SEC
    # match inside words such as "NCAA".
    _r(r"REG$|REGSEASON|REGULAR|TOURNAMENT$|TOURNEY|BIGEAST$|BIG12$|BIG10$|BIGTEN$|BIGEAST|ACC$|SEC$|PAC12$|SBELT$|SUNBELT$|MWREG|A10$|AAC$|IVY$|MAA$|SOCON$|SWAC$|MAC$|WCC$|CAA$|NEC$|HOR$|AE$|ASUN$|BSKY$|SLC$|PAT$|MW$|MAMER$|AMER$|BIG\d+$", FUTURES, SUB_CONFERENCE),
]

# Finishing position (top-N, relegation, promotion, pole, fastest lap, cut).
_FINISH_RULES = [
    _r(r"TOP\d+|TOP2|TOP3|TOP4|TOP5|TOP6|TOP10|TOP15|TOP20|TOP25|TOP30", FUTURES, SUB_FINISH),
    _r(r"RELEGATION|PROMO|PROMOTION|MAKECUT|MISSCUT|AGECUT|OLDESTCUT|R1LEAD|R2LEAD|R3LEAD|ROUNDLEAD|R1TOP|R2TOP", FUTURES, SUB_FINISH),
    _r(r"POLE|POLEPOSITION|FASTLAP|FASTESTLAP|QUALIFY|SPRINT|TOPCONSTRUCTOR|TOP10FIN|FINISHERS?", FUTURES, SUB_FINISH),
    _r(r"STROKEMARGIN|WINNINGSCORE|WINNERREGION|WINNERWITHOUT|REGION|CATEGORY|CAT$", FUTURES, SUB_FINISH),
]

# In-game derivatives — still one dated matchup, so bucket as GAME so they sit
# with the matchup rather than scattering into Futures.
_GAME_DERIV_RULES = [
    _r(r"1HWINNER|2HWINNER|1H$|2H$|FIRSTHALF|SECONDHALF", GAME, SUB_PERIOD),
    _r(r"1QWINNER|2QWINNER|3QWINNER|4QWINNER|[1-4]QWINNER|OVERTIME|HIGHSCOREQ|NOSCOREQ", GAME, SUB_PERIOD),
    _r(r"\bF5\b|FIRST5|FIRST6|FIRST10|FIRST15|RFI|RUNINFIRST", GAME, SUB_PERIOD),
    _r(r"BTTS|CORNERS|TCORNERS|SIXES|SIX$|FOUR$|TOTALMAPS|TOTALSETS|EXACTSETS", GAME, SUB_GAME_PROP),
    _r(r"MAP$|MAPWINNER|ROUND$|SET$|DISTANCE|GOTHEDISTANCE|KNOCKOUT|MOV$|METHODOFVICTORY|ROUNDS$|1MIN", GAME, SUB_GAME_PROP),
    _r(r"SERIESSCORE|SERIESEXACT|FINALSEXACT|FINALSEXACTSCORE|EXACTSCORE|SERIESSCORE|EXACTMATCH", GAME, SUB_GAME_PROP),
]

# Outright tournament / championship winners. The bare league ticker (KXNBA,
# KXNFL→KXSB, KXMLB, KXNHL, KXUCL, KXEPL, …) and explicit CHAMP/WINNER outrights.
_CHAMPIONSHIP_RULES = [
    _r(r"CHAMP$|CHAMPION|TITLE$|WORLDCUP|WORLDS$|WORLDSERIES|STANLEYCUP|SUPERBOWL|GREYCUP", FUTURES, SUB_CHAMPIONSHIP),
    _r(r"WINNER$|OUTRIGHT|TOURNAMENT|MAJOR$|MAJORWIN|GRANDSLAM|OPEN$|MASTERS|THEOPEN|RYDERCUP|LAVERCUP", FUTURES, SUB_OUTRIGHT),
]


# Final assembled table. Order is significant.
_RULES: list[tuple[re.Pattern, SeriesClass]] = (
    _NOVELTY_RULES
    + _TRANSACTION_RULES
    + _PROP_RULES
    + _AWARD_RULES
    + _DRAFT_RULES
    + _WINTOTAL_RULES
    + _PLAYOFF_RULES
    + _CONFERENCE_RULES
    + _FINISH_RULES
    + _GAME_DERIV_RULES
    + _CHAMPIONSHIP_RULES
)

# Tokens that mark a *dated head-to-head* series — these are the core game lines
# and should always classify as GAME regardless of other tokens in the ticker.
_GAME_SUFFIXES = ("GAME", "GAMES", "MATCH", "FIGHT", "BOUT", "RACE", "H2H")
_DERIVATIVE_SUFFIXES = ("SPREAD", "TOTAL", "TEAMTOTAL")

# Date fingerprint inside an event ticker, e.g. "26FEB14" — present on real
# games, absent on season-long futures.
_EVENT_DATE_RE = re.compile(r"\d{2}[A-Z]{3}\d{2}")


def classify(series_ticker: str, event_ticker: str = "", title: str = "") -> SeriesClass:
    """Classify a Kalshi series into the bet-type taxonomy.

    Args:
        series_ticker: e.g. "KXNBAMVP", "KXNFLGAME", "KXNFLWINS-TB".
        event_ticker:  optional; used to detect a date (game) vs no date (future).
        title:         optional; rarely needed, used only as a tie breaker.
    """
    t = (series_ticker or "").upper()
    et = (event_ticker or "").upper()

    # 1. Core game lines: ticker ends in a game/derivative suffix. These are the
    #    moneyline/spread/total markets that drive the matchup view.
    for sfx in _GAME_SUFFIXES:
        if t.endswith(sfx):
            return SeriesClass(GAME, SUB_GAME_LINES)
    for sfx in _DERIVATIVE_SUFFIXES:
        if t.endswith(sfx):
            return SeriesClass(GAME, SUB_GAME_LINES)

    # 2. Token rule table (most specific → most general).
    for pattern, klass in _RULES:
        if pattern.search(t):
            return klass

    # 3. Fallbacks based on whether the event is dated.
    #    A dated event with no recognised token is treated as a game-level
    #    market; an undated one is an unclassified future/outright.
    if et and _EVENT_DATE_RE.search(et):
        return SeriesClass(GAME, SUB_GAME_PROP)
    return SeriesClass(FUTURES, SUB_OUTRIGHT)


# Ordered list of futures subtypes for stable menu display.
FUTURES_SUBTYPE_ORDER = [
    SUB_CHAMPIONSHIP,
    SUB_OUTRIGHT,
    SUB_CONFERENCE,
    SUB_PLAYOFF,
    SUB_AWARD,
    SUB_STAT_LEADER,
    SUB_WIN_TOTAL,
    SUB_DRAFT,
    SUB_FINISH,
]

BET_TYPE_ORDER = [GAME, PROP, FUTURES, SPECIAL]

BET_TYPE_LABEL = {
    GAME: "Games",
    PROP: "Player Props",
    FUTURES: "Futures",
    SPECIAL: "Specials",
}

BET_TYPE_EMOJI = {
    GAME: "\U0001f3df️",   # 🏟️
    PROP: "\U0001f464",          # 👤
    FUTURES: "\U0001f3c6",       # 🏆
    SPECIAL: "\U0001f3b2",       # 🎲
}
