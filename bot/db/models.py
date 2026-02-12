from __future__ import annotations

from bot.db.database import get_connection
from bot.config import STARTING_BALANCE


async def get_or_create_user(discord_id: int) -> dict:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT discord_id, balance, voice_minutes FROM users WHERE discord_id = ?",
            (discord_id,),
        )
        row = await cursor.fetchone()
        if row:
            return dict(row)
        await db.execute(
            "INSERT INTO users (discord_id, balance) VALUES (?, ?)",
            (discord_id, STARTING_BALANCE),
        )
        await db.commit()
        return {"discord_id": discord_id, "balance": STARTING_BALANCE, "voice_minutes": 0}
    finally:
        await db.close()


async def update_balance(discord_id: int, delta: int) -> int:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE users SET balance = balance + ? WHERE discord_id = ?",
            (delta, discord_id),
        )
        await db.commit()
        cursor = await db.execute(
            "SELECT balance FROM users WHERE discord_id = ?", (discord_id,)
        )
        row = await cursor.fetchone()
        return row["balance"]
    finally:
        await db.close()


async def add_voice_minutes(discord_id: int, minutes: int) -> None:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE users SET voice_minutes = voice_minutes + ? WHERE discord_id = ?",
            (minutes, discord_id),
        )
        await db.commit()
    finally:
        await db.close()


async def create_bet(
    user_id: int,
    game_id: str,
    pick: str,
    amount: int,
    odds: float,
    home_team: str | None = None,
    away_team: str | None = None,
    sport_title: str | None = None,
) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "INSERT INTO bets (user_id, game_id, pick, amount, odds, home_team, away_team, sport_title)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, game_id, pick, amount, odds, home_team, away_team, sport_title),
        )
        await db.commit()
        return cursor.lastrowid
    finally:
        await db.close()


async def get_user_bets(user_id: int, status: str | None = None) -> list[dict]:
    db = await get_connection()
    try:
        if status:
            cursor = await db.execute(
                "SELECT * FROM bets WHERE user_id = ? AND status = ? ORDER BY created_at DESC",
                (user_id, status),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM bets WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_pending_bets_by_game(game_id: str) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM bets WHERE game_id = ? AND status = 'pending'",
            (game_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_pending_game_ids() -> list[str]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT DISTINCT game_id FROM bets WHERE status = 'pending'"
        )
        rows = await cursor.fetchall()
        return [row["game_id"] for row in rows]
    finally:
        await db.close()


async def delete_bet(bet_id: int) -> bool:
    db = await get_connection()
    try:
        cursor = await db.execute("DELETE FROM bets WHERE id = ?", (bet_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


async def get_bet_by_id(bet_id: int) -> dict | None:
    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM bets WHERE id = ?", (bet_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def get_leaderboard(limit: int = 10) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT discord_id, balance, voice_minutes FROM users ORDER BY balance DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()
