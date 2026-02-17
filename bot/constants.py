from datetime import timedelta, timezone

# Emoji prefixes for bet type buttons
PICK_EMOJI = {
    "home": "\U0001f3e0",       # 🏠
    "away": "\U0001f4cd",       # 📍
    "draw": "\U0001f91d",       # 🤝
    "spread_home": "\U0001f4ca",  # 📊
    "spread_away": "\U0001f4ca",  # 📊
    "over": "\U0001f4c8",       # 📈
    "under": "\U0001f4c9",       # 📉
}

# Map pick values to their market type and display labels
PICK_MARKET = {
    "home": "h2h",
    "away": "h2h",
    "draw": "h2h",
    "spread_home": "spreads",
    "spread_away": "spreads",
    "over": "totals",
    "under": "totals",
}

PICK_LABELS = {
    "home": "Home",
    "away": "Away",
    "draw": "Draw",
    "spread_home": "Home Spread",
    "spread_away": "Away Spread",
    "over": "Over",
    "under": "Under",
}

# Timezone helpers
TZ_ET = timezone(timedelta(hours=-5))
TZ_PT = timezone(timedelta(hours=-8))
