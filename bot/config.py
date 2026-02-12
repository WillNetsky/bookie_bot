import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

# Voice reward settings
COINS_PER_MINUTE = 1

# Starting balance for new users
STARTING_BALANCE = 100
