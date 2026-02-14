import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

# Voice reward settings — $20/hour, paid every 6 minutes
VOICE_PAYOUT_INTERVAL = 6  # minutes between payouts
VOICE_PAYOUT_AMOUNT = 2  # coins per payout (2 × 10 payouts/hr = $20/hr)

# Starting balance for new users
STARTING_BALANCE = 100

# Guild ID for instant slash command sync
GUILD_ID = int(os.environ.get("GUILD_ID", "0")) or None

# Channel ID for posting bet resolution announcements
BET_RESULTS_CHANNEL_ID = 1315299663464890449

# Sports blocked from betting (no score coverage on the-odds-api)
# Prefixes for sport families that have no scores endpoint support
BLOCKED_SPORT_PREFIXES: tuple[str, ...] = (
    "tennis_",
    "cricket_",
    "golf_",
    "boxing_",
    "mma_",
    "politics_",
    "lacrosse_",
    "baseball_mlb_preseason",
    "baseball_milb",
    "baseball_npb",
    "baseball_kbo",
    "baseball_ncaa",
    "basketball_wncaab",
    "icehockey_ahl",
    "icehockey_liiga",
    "icehockey_mestis",
)

# Additional exact-match blocks from env (comma-separated sport keys)
_blocked_raw = os.environ.get("BLOCKED_SPORTS", "")
_BLOCKED_SPORTS_ENV: set[str] = {s.strip() for s in _blocked_raw.split(",") if s.strip()}


def is_sport_blocked(sport_key: str) -> bool:
    """Check if a sport is blocked due to missing score coverage."""
    if sport_key in _BLOCKED_SPORTS_ENV:
        return True
    if sport_key.startswith(BLOCKED_SPORT_PREFIXES):
        return True
    # Block outright/futures markets (championship winner, etc.)
    if "_winner" in sport_key or "_championship" in sport_key:
        return True
    return False
