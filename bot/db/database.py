import aiosqlite
import sqlite3
import asyncio
import functools
import logging
import os
import json
import shutil

log = logging.getLogger(__name__)

DB_PATH = "bookie_bot.db"
INJECTION_FILE = "injection.json"
USED_DIR = "used"

def db_retry(max_attempts=3, initial_delay=0.5):
    """Decorator to retry database operations if they fail due to locking."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_err = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    last_err = e
                    if "locked" in str(e).lower() and attempt < max_attempts - 1:
                        delay = initial_delay * (attempt + 1)
                        log.debug(f"Database locked, retrying {func.__name__} (attempt {attempt + 1})...")
                        await asyncio.sleep(delay)
                        continue
                    raise
            raise last_err
        return wrapper
    return decorator

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    discord_id INTEGER PRIMARY KEY,
    balance    INTEGER NOT NULL DEFAULT 100,
    voice_minutes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bets (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(discord_id),
    game_id    TEXT    NOT NULL,
    pick       TEXT    NOT NULL,
    amount     INTEGER NOT NULL,
    odds       REAL    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    payout     INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS games_cache (
    game_id    TEXT PRIMARY KEY,
    sport      TEXT NOT NULL,
    data       TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS parlays (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(discord_id),
    amount     INTEGER NOT NULL,
    total_odds REAL    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    payout     INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS parlay_legs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    parlay_id  INTEGER NOT NULL REFERENCES parlays(id),
    game_id    TEXT    NOT NULL,
    pick       TEXT    NOT NULL,
    odds       REAL    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    home_team  TEXT,
    away_team  TEXT,
    sport_title TEXT,
    market     TEXT DEFAULT 'h2h',
    point      REAL
);

CREATE TABLE IF NOT EXISTS kalshi_bets (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       INTEGER NOT NULL REFERENCES users(discord_id),
    market_ticker TEXT NOT NULL,
    event_ticker  TEXT NOT NULL,
    pick          TEXT NOT NULL,
    amount        INTEGER NOT NULL,
    odds          REAL NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    payout        INTEGER,
    title         TEXT,
    close_time    TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kalshi_parlays (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(discord_id),
    amount     INTEGER NOT NULL,
    total_odds REAL NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending',
    payout     INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kalshi_parlay_legs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    parlay_id       INTEGER NOT NULL REFERENCES kalshi_parlays(id),
    market_ticker   TEXT NOT NULL,
    event_ticker    TEXT NOT NULL,
    pick            TEXT NOT NULL,
    odds            REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    title           TEXT,
    pick_display    TEXT,
    close_time      TEXT
);

CREATE TABLE IF NOT EXISTS kalshi_series (
    ticker          TEXT PRIMARY KEY,
    kalshi_title    TEXT,
    label           TEXT,
    category        TEXT,
    subcategory     TEXT,
    is_excluded     INTEGER NOT NULL DEFAULT 0,
    is_derivative   INTEGER NOT NULL DEFAULT 0,
    parent_ticker   TEXT,
    notes           TEXT,
    last_seen       TEXT
);

CREATE TABLE IF NOT EXISTS twitch_watches (
    stream_name TEXT    NOT NULL,
    guild_id    INTEGER NOT NULL,
    channel_id  INTEGER NOT NULL,
    PRIMARY KEY (stream_name, guild_id)
);
"""


async def get_connection() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("PRAGMA busy_timeout=5000")
    return db


MIGRATIONS = [
    "ALTER TABLE bets ADD COLUMN home_team TEXT",
    "ALTER TABLE bets ADD COLUMN away_team TEXT",
    "ALTER TABLE bets ADD COLUMN sport_title TEXT",
    "ALTER TABLE bets ADD COLUMN market TEXT DEFAULT 'h2h'",
    "ALTER TABLE bets ADD COLUMN point REAL",
    "ALTER TABLE bets ADD COLUMN commence_time TEXT",
    "ALTER TABLE parlay_legs ADD COLUMN commence_time TEXT",
    "ALTER TABLE kalshi_bets ADD COLUMN pick_display TEXT",
    "ALTER TABLE kalshi_series ADD COLUMN subcategory TEXT",
    # kalshi_parlays and kalshi_parlay_legs created in schema above
]


async def init_db() -> None:
    is_new = not os.path.exists(DB_PATH)
    db = await get_connection()
    try:
        await db.executescript(SCHEMA)
        for migration in MIGRATIONS:
            try:
                await db.execute(migration)
            except Exception:
                pass  # column already exists
        await db.commit()
        
        # Automatic injection for fresh databases
        if is_new and os.path.exists(INJECTION_FILE):
            await _handle_injection(db)
            
    finally:
        await db.close()

async def _handle_injection(db: aiosqlite.Connection) -> None:
    """Inject data from injection.json and move files to used/."""
    try:
        log.info("New database detected and %s found. Starting auto-injection...", INJECTION_FILE)
        with open(INJECTION_FILE, 'r') as f:
            data = json.load(f)

        # Inject Users
        for user in data.get('users', []):
            await db.execute(
                "INSERT OR REPLACE INTO users (discord_id, balance) VALUES (?, ?)",
                (user['id'], user['balance'])
            )
        
        # Inject Kalshi Bets
        for bet in data.get('pending_kalshi_bets', []):
            await db.execute(
                """
                INSERT INTO kalshi_bets 
                (user_id, market_ticker, event_ticker, pick, amount, odds, title, pick_display, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    bet['user_id'], bet['market_ticker'], bet['event_ticker'], 
                    bet['pick'], bet['amount'], bet['odds'], 
                    bet['title'], bet['pick_display']
                )
            )

        await db.commit()
        log.info("Auto-injection complete.")

        # Cleanup
        if not os.path.exists(USED_DIR):
            os.makedirs(USED_DIR)
        
        shutil.move(INJECTION_FILE, os.path.join(USED_DIR, INJECTION_FILE))
        log.info("Moved %s to %s/", INJECTION_FILE, USED_DIR)
        
        log_file = "discord_logs_to_inject.txt"
        if os.path.exists(log_file):
            shutil.move(log_file, os.path.join(USED_DIR, log_file))
            log.info("Moved %s to %s/", log_file, USED_DIR)

    except Exception:
        log.exception("Failed to perform auto-injection")

@db_retry()
async def vacuum_db() -> None:
    """Reclaim unused disk space. Requires exclusive access."""
    # Using isolation_level=None ensures we aren't in a transaction
    async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
        try:
            await db.execute("VACUUM")
        except sqlite3.OperationalError as e:
            msg = str(e)
            if "statements in progress" in msg:
                log.warning("VACUUM skipped: database busy with other queries.")
            elif "disk is full" in msg or "database is full" in msg:
                log.warning("VACUUM skipped: insufficient temp disk space (DB needs ~1x its size). Error: %s", e)
            else:
                raise

@db_retry()
async def cleanup_cache(max_age_days: int = 7) -> int:
    """Remove old cache entries to save disk space."""
    db = await get_connection()
    try:
        cursor = await db.execute(
            "DELETE FROM games_cache WHERE fetched_at < datetime('now', '-' || ? || ' days')",
            (max_age_days,),
        )
        await db.commit()
        return cursor.rowcount
    finally:
        await db.close()
