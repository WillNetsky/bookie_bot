import aiosqlite

DB_PATH = "bookie_bot.db"

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
"""


async def get_connection() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    return db


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()
