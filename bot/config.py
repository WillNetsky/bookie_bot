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

# Sports blocked from betting (no score coverage on the-odds-api)
# Comma-separated sport keys, e.g. "cricket_icc_world_t20,cricket_big_bash"
_blocked_raw = os.environ.get("BLOCKED_SPORTS", "")
BLOCKED_SPORTS: set[str] = {s.strip() for s in _blocked_raw.split(",") if s.strip()}
