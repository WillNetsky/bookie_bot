# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bookie Bot is a Discord bot for placing wagers on sporting events using fictional currency. Currency is earned by spending time in voice channels. The project uses Kalshi for primary betting markets and the-odds-api for live scores.

## Key Requirements

- Discord bot for wagering on sporting events with fictional currency
- Currency accrual via voice channel activity
- Free hosting on LemonHost.me
- Kalshi API integration for markets and settlement
- Live scores via the-odds-api (legacy integration)

## Tech Stack

- Python 3.11+ with `discord.py` (slash commands)
- SQLite via `aiosqlite` for persistence
- `aiohttp` for HTTP calls to API-Sports
- `python-dotenv` for configuration

## Build / Run Commands

```bash
pip install -r requirements.txt    # Install dependencies
cp .env.example .env               # Create env file, then fill in tokens
python -m bot.main                 # Run the bot
```

## Project Structure

```
bot/
├── main.py              # Bot entrypoint, cog loading
├── config.py            # Environment variable loading
├── constants.py         # Shared constants (emojis, labels)
├── utils.py             # Shared utility functions (formatting)
├── cogs/                # Slash command handlers (Discord-facing)
│   ├── wallet.py        # /balance, /leaderboard
│   ├── kalshi.py        # /kalshi, /games, /live, /myhistory, /mybets
│   └── voice_rewards.py # Voice channel currency accrual
├── services/            # Business logic layer
│   ├── kalshi_api.py    # Kalshi API client with dynamic sport discovery
│   ├── sports_api.py    # Legacy Odds API client (kept for live scores)
│   ├── betting_service.py # Bet placement and resolution
│   └── wallet_service.py  # Currency management
└── db/                  # Data layer
    ├── database.py      # Connection management, schema init
    └── models.py        # CRUD data access functions
```

## Development Conventions

- **Cogs pattern**: All Discord commands go in `bot/cogs/`. Each cog file has an `async def setup(bot)` function.
- **Services layer**: Cogs call into `bot/services/` for business logic. Cogs should not contain business logic directly.
- **Shared code**: Constants go in `bot/constants.py`, helpers in `bot/utils.py`.
- **Data access**: All SQL queries live in `bot/db/models.py`. Services call models, never raw SQL.
- **Async everywhere**: All I/O (DB, HTTP, Discord) uses async/await.
- **Config via env vars**: All secrets and configuration loaded in `bot/config.py` from `.env`.
