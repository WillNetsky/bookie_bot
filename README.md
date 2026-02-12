## Bookie Bot

A Discord bot for placing wagers on real sporting events using fictional currency earned by hanging out in voice channels.

### Features

- **Real odds** — pulls live moneyline odds from [The Odds API](https://the-odds-api.com) across 70+ sports (NFL, NBA, NHL, MLB, EPL, MMA, and more)
- **Voice channel rewards** — earn $20/hour in fictional currency just by being in a voice channel (paid every 6 minutes)
- **Full betting flow** — browse odds, place bets, track your bets, cancel before game start
- **Auto-resolution** — background task checks scores every 5 minutes and settles bets automatically
- **Sport filtering** — autocomplete search to browse odds by specific sport

### Commands

| Command | Description |
|---------|-------------|
| `/odds [sport]` | View upcoming games with American odds. Optional sport filter with autocomplete. |
| `/bet <game_id> <pick> <amount>` | Place a bet (home/away/draw). Validates game status, odds, and balance. |
| `/mybets` | View your bets with color-coded status (pending/won/lost). |
| `/cancelbet <bet_id>` | Cancel a pending bet and get your wager refunded (only before game starts). |
| `/balance` | Check your current coin balance. |
| `/leaderboard` | See the top balances on the server. |

### Tech Stack

- Python 3.11+ with `discord.py` (slash commands)
- SQLite via `aiosqlite` for persistence
- `aiohttp` for HTTP calls to The Odds API
- `python-dotenv` for configuration

### Setup

```bash
pip install -r requirements.txt
cp .env.example .env    # Fill in your tokens
python -m bot.main
```

#### Environment Variables

| Variable | Description |
|----------|-------------|
| `DISCORD_TOKEN` | Your Discord bot token from the [Developer Portal](https://discord.com/developers/applications/) |
| `ODDS_API_KEY` | Free API key from [The Odds API](https://the-odds-api.com) (500 requests/month) |

#### Discord Bot Setup

1. Create a bot in the [Discord Developer Portal](https://discord.com/developers/applications/)
2. Enable **Server Members Intent** and **Message Content Intent** under Bot settings
3. Invite with: `https://discord.com/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=274878221376&scope=bot%20applications.commands`

### Project Structure

```
bot/
├── main.py              # Bot entrypoint, cog loading
├── config.py            # Environment variable loading
├── cogs/
│   ├── betting.py       # /bet, /odds, /mybets, /cancelbet + auto-resolution task
│   ├── wallet.py        # /balance, /leaderboard
│   └── voice_rewards.py # Voice channel currency accrual ($20/hr)
├── services/
│   ├── sports_api.py    # The Odds API client with SQLite caching
│   ├── betting_service.py # Bet placement, cancellation, and resolution
│   └── wallet_service.py  # Currency management
└── db/
    ├── database.py      # Connection management, schema init
    └── models.py        # CRUD data access functions
```
