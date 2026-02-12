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
