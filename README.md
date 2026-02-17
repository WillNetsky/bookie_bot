## Bookie Bot

A Discord bot for placing wagers on real sporting events using fictional currency earned by hanging out in voice channels.

### Features

- **100+ sports** — dynamically discovers all available sports from [Kalshi](https://kalshi.com) including NBA, NFL, MLB, NHL, college sports, soccer leagues worldwide, UFC, boxing, cricket, tennis, and more
- **Real odds** — pulls live moneyline, spread, and totals odds with automatic settlement when markets close
- **Voice channel rewards** — earn $2/hour in fictional currency just by being in a voice channel (paid every 30 minutes)
- **Interactive betting UI** — browse games, view odds, and place bets through Discord buttons, selects, and modals
- **Futures & props** — bet on championship winners, MVPs, and other multi-outcome futures markets with paginated option lists
- **Auto-resolution** — background loop checks Kalshi market settlement and pays out automatically
- **Live game tracker** — `/live` shows in-progress games with cached scores, current odds, and estimated end times
- **Result announcements** — resolved bets posted to a dedicated channel with full details
- **Economy controls** — admin commands to reset balances and devalue currency to keep the economy healthy

### Commands



| Command | Description |

|---------|-------------|

| `/kalshi [sport]` | Browse all sports markets. Pick a sport to see games + futures. |

| `/games [sport]` | Browse upcoming games across all sports with odds and start times. |

| `/live [sport]` | View live games with scores, current odds, and estimated end times. |

| `/parlay [sport]` | Build a multi-leg parlay bet with combined odds. |

| `/mybets` | View your pending bets with color-coded status and live indicators. |

| `/myparlays` | View your active and resolved parlay bets. |

| `/livescores` | See a quick summary of scores for all games you have active bets on. |

| `/cancelbet <id>` | Cancel a pending single bet (legacy or Kalshi) for a full refund. |

| `/cancelparlay <id>` | Cancel a pending parlay bet for a full refund. |

| `/myhistory` | View resolved bet history with win/loss stats and pagination. |

| `/balance` | Check your current coin balance. |

| `/leaderboard` | See the top balances on the server. |

| `/pendingbets` | [Admin] View all users' pending bets. |

| `/resolve <game_id> <winner>` | [Admin] Manually resolve a legacy game. |

| `/quota` | [Admin] Check Kalshi API usage and remaining quota. |

| `/resetbalances <confirm> [amount]` | [Admin] Reset all balances and clear all bets. |

| `/devalue <percent> <confirm>` | [Admin] Devalue everyone's currency by a percentage. |



### Tech Stack



- Python 3.11+ with `discord.py` (slash commands)

- SQLite via `aiosqlite` for persistence

- `aiohttp` for HTTP calls to Kalshi API

- `cryptography` for Kalshi API authentication (RSA-PSS signing)

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

| `KALSHI_API_KEY_ID` | API key ID from [Kalshi](https://kalshi.com) (optional, improves rate limits) |

| `KALSHI_PRIVATE_KEY_PATH` | Path to PEM private key file for Kalshi API auth |

| `GUILD_ID` | Guild ID for instant slash command sync |

| `BET_RESULTS_CHANNEL_ID` | Channel ID where resolved bet announcements are posted |



### Project Structure



```

bot/

├── main.py              # Bot entrypoint, cog loading

├── config.py            # Environment variable loading

├── constants.py         # Shared emojis, labels, and market mappings

├── utils.py             # Shared formatting and math helpers

├── cogs/

│   ├── wallet.py        # /balance, /leaderboard, /resetbalances, /devalue

│   ├── kalshi.py        # All betting commands, parlay builder, settlement loops

│   └── voice_rewards.py # Voice channel currency accrual ($2/hr)

├── services/

│   ├── kalshi_api.py    # Kalshi API client — dynamic discovery, auth, rate limiting

│   ├── sports_api.py    # The Odds API client (used for live scores in /live)

│   ├── betting_service.py # Bet placement, cancellation, and resolution logic

│   └── wallet_service.py  # Currency management

└── db/

    ├── database.py      # Connection management, schema + migrations

    └── models.py        # CRUD data access functions

```



### TODO



#### Done

- [x] **Bet history / stats** — `/myhistory` command showing win rate, total wagered, profit/loss

- [x] **Kalshi sports betting** — full sports betting UX via Kalshi (moneyline, spreads, totals)

- [x] **Dynamic sport discovery** — automatically discovers all available sports from Kalshi

- [x] **Futures & props** — championship winners, MVPs, and other multi-outcome markets

- [x] **Parlay Builder** — interactive multi-leg parlay support for Kalshi markets

- [x] **Live game tracker** — `/live` with cached scores, current odds, start/end times

- [x] **Economy controls** — admin commands for managing the money supply

- [x] **Migrate off the-odds-api** — all primary betting migrated to Kalshi, legacy integration removed



#### Up Next

- [ ] **More ways to earn** — posting rewards, `/play` mini-games, daily login bonus, etc.

- [ ] **Head-to-head challenges** — let two users bet against each other on a game at even odds

- [ ] **Prop bets / custom bets** — admin-created bets on arbitrary events

- [ ] **Streak bonuses** — bonus payouts for consecutive winning bets

- [ ] **League favorites** — let users follow specific leagues and get notified when new games are available
