## Bookie Bot

A Discord bot for placing wagers on real sporting events using fictional currency earned by hanging out in voice channels.

### Features

- **Real odds** — pulls live moneyline, spread, and totals odds from [The Odds API](https://the-odds-api.com) across 70+ sports (NFL, NBA, NHL, MLB, EPL, MMA, and more)
- **Voice channel rewards** — earn $2/hour in fictional currency just by being in a voice channel (paid every 30 minutes)
- **Interactive betting UI** — browse games, view odds, and place bets through Discord buttons, selects, and modals
- **Parlay builder** — multi-step flow to build parlays: select games, pick bet types, review slip, confirm wager
- **Auto-resolution** — background task checks scores every 30 minutes with smart sport-duration filtering and settles bets automatically
- **Live scores** — check scores for your active bets in real-time
- **Result announcements** — resolved bets and parlays posted to a dedicated channel with full leg details
- **Kalshi prediction markets** — browse and bet on real prediction markets (politics, economics, weather, etc.) from [Kalshi](https://kalshi.com) with yes/no bets and automatic settlement
- **Quota-conscious** — aggressive caching, free endpoint usage, and smart polling to stay within API limits

### Commands

| Command | Description |
|---------|-------------|
| `/games [sport] [hours]` | Browse upcoming games (default: next 6 hours). Free, no API quota cost. |
| `/odds [sport]` | View upcoming games with American odds. Interactive UI to place bets. |
| `/parlay [sport]` | Build a parlay bet through an interactive multi-step flow. |
| `/kalshi` | Browse Kalshi prediction markets and place yes/no bets. |
| `/mybets` | View your pending bets (sports + Kalshi) with color-coded status. |
| `/cancelbet <bet_id>` | Cancel a pending bet before game start for a full refund. |
| `/myhistory` | View resolved bet history with win/loss stats and pagination. |
| `/livescores` | View live scores for your active bets. |
| `/balance` | Check your current coin balance. |
| `/leaderboard` | See the top balances on the server. |
| `/resolve <game_id> <winner>` | [Admin] Manually resolve a game. |
| `/resetbalances <confirm> [amount]` | [Admin] Reset all balances and clear all bets. |
| `/devalue <percent> <confirm>` | [Admin] Devalue everyone's currency by a percentage. |

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
| `BET_RESULTS_CHANNEL_ID` | Channel ID where resolved bet announcements are posted |
| `BLOCKED_SPORTS` | Comma-separated sport keys to hide (e.g. `cricket_ipl,cricket_test_match`) |

#### Discord Bot Setup

1. Create a bot in the [Discord Developer Portal](https://discord.com/developers/applications/)
2. Enable **Server Members Intent** and **Message Content Intent** under Bot settings
3. Invite with: `https://discord.com/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=274878221376&scope=bot%20applications.commands`

### Project Structure

```
bot/
├── main.py              # Bot entrypoint, cog loading
├── config.py            # Environment variable loading, blocked sports
├── cogs/
│   ├── betting.py       # All betting commands, interactive UI views, auto-resolution loop
│   ├── wallet.py        # /balance, /leaderboard
│   ├── kalshi.py        # /kalshi command, prediction market UI, settlement loop
│   └── voice_rewards.py # Voice channel currency accrual ($2/hr)
├── services/
│   ├── sports_api.py    # The Odds API client with SQLite caching + stale fallback
│   ├── kalshi_api.py    # Kalshi prediction market API client with caching
│   ├── betting_service.py # Bet placement, cancellation, and resolution logic
│   └── wallet_service.py  # Currency management
└── db/
    ├── database.py      # Connection management, schema + migrations
    └── models.py        # CRUD data access functions
```

### TODO

- [ ] **Daily free bet** — give users a free bet each day to keep engagement up even when balances are low
- [x] **Bet history / stats** — `/myhistory` command showing win rate, total wagered, profit/loss with pagination
- [ ] **Head-to-head challenges** — let two users bet against each other on a game at even odds (or custom)
- [ ] **Prop bets / custom bets** — admin-created bets on arbitrary events (e.g. "Will X happen during the game?")
- [ ] **Streak bonuses** — bonus payouts for consecutive winning bets (e.g. 3-win streak = 1.5x next payout)
- [ ] **Bankrupt recovery** — small trickle income or daily login bonus so broke users can get back in the game
- [ ] **Bet sharing** — shareable embed when placing a bet so others can tail it with one click
- [ ] **League standings / favorites** — let users follow specific leagues and get notified when new games are available
- [x] **Pagination for /mybets** — paginated view via `/myhistory` for resolved bets
- [ ] **Multi-server support** — per-guild wallets and leaderboards for running the bot across multiple servers
