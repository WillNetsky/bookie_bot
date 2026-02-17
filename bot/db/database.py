import aiosqlite
import sqlite3
import asyncio
import functools
import logging

log = logging.getLogger(__name__)

DB_PATH = "bookie_bot.db"

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
    # kalshi_parlays and kalshi_parlay_legs created in schema above
]


async def init_db() -> None:
    db = await get_connection()
    try:
        await db.executescript(SCHEMA)
        for migration in MIGRATIONS:
            try:
                await db.execute(migration)
            except Exception:
                pass  # column already exists
        await db.commit()
    finally:
        await db.close()

@db_retry()
async def vacuum_db() -> None:
    """Reclaim unused disk space."""
    db = await get_connection()
    try:
        await db.execute("VACUUM")
    finally:
        await db.close()

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
