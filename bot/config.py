import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

# Kalshi API authentication (key ID + path to PEM private key file)
KALSHI_API_KEY_ID = os.environ.get("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")

# Voice reward settings — $2/hour, paid every 30 minutes
VOICE_PAYOUT_INTERVAL = 30  # minutes between payouts
VOICE_PAYOUT_AMOUNT = 1  # coins per payout (1 × 2 payouts/hr = $2/hr)

# Starting balance for new users
STARTING_BALANCE = 100

# Guild ID for instant slash command sync
GUILD_ID = int(os.environ.get("GUILD_ID", "0")) or None

# Channel ID for posting bet resolution announcements
BET_RESULTS_CHANNEL_ID = 1315299663464890449
