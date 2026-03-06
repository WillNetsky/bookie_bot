from __future__ import annotations

from bot.db.database import get_connection, db_retry
from bot.config import STARTING_BALANCE


@db_retry()
@db_retry()
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


@db_retry()
@db_retry()
async def update_balance(discord_id: int, delta: int) -> int:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE users SET balance = balance + ? WHERE discord_id = ?",
            (round(delta, 2), discord_id),
        )
        await db.commit()
        cursor = await db.execute(
            "SELECT balance FROM users WHERE discord_id = ?", (discord_id,)
        )
        row = await cursor.fetchone()
        return row["balance"]
    finally:
        await db.close()


@db_retry()
@db_retry()
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


@db_retry()
@db_retry()
async def create_bet(
    user_id: int,
    game_id: str,
    pick: str,
    amount: int,
    odds: float,
    home_team: str | None = None,
    away_team: str | None = None,
    sport_title: str | None = None,
    market: str = "h2h",
    point: float | None = None,
    commence_time: str | None = None,
) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "INSERT INTO bets (user_id, game_id, pick, amount, odds, home_team, away_team, sport_title, market, point, commence_time)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, game_id, pick, amount, odds, home_team, away_team, sport_title, market, point, commence_time),
        )
        await db.commit()
        return cursor.lastrowid
    finally:
        await db.close()


@db_retry()
@db_retry()
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


@db_retry()
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


@db_retry()
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


@db_retry()
async def get_pending_games_with_commence() -> list[dict]:
    """Return pending game IDs with their commence times from single bets."""
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT game_id, MIN(commence_time) as commence_time"
            " FROM bets WHERE status = 'pending'"
            " GROUP BY game_id"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_pending_parlay_games_with_commence() -> list[dict]:
    """Return pending parlay leg game IDs with their commence times."""
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT game_id, MIN(commence_time) as commence_time"
            " FROM parlay_legs WHERE status = 'pending'"
            " GROUP BY game_id"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def delete_bet(bet_id: int) -> bool:
    db = await get_connection()
    try:
        cursor = await db.execute("DELETE FROM bets WHERE id = ?", (bet_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


@db_retry()
async def get_bet_by_id(bet_id: int) -> dict | None:
    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM bets WHERE id = ?", (bet_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


@db_retry()
async def create_parlay(
    user_id: int, amount: int, total_odds: float, legs: list[dict]
) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "INSERT INTO parlays (user_id, amount, total_odds) VALUES (?, ?, ?)",
            (user_id, amount, total_odds),
        )
        parlay_id = cursor.lastrowid
        for leg in legs:
            await db.execute(
                "INSERT INTO parlay_legs (parlay_id, game_id, pick, odds, home_team, away_team, sport_title, market, point, commence_time)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    parlay_id,
                    leg["game_id"],
                    leg["pick"],
                    leg["odds"],
                    leg.get("home_team"),
                    leg.get("away_team"),
                    leg.get("sport_title"),
                    leg.get("market", "h2h"),
                    leg.get("point"),
                    leg.get("commence_time"),
                ),
            )
        await db.commit()
        return parlay_id
    finally:
        await db.close()


@db_retry()
async def get_parlay_by_id(parlay_id: int) -> dict | None:
    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM parlays WHERE id = ?", (parlay_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


@db_retry()
async def get_parlay_legs(parlay_id: int) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM parlay_legs WHERE parlay_id = ?", (parlay_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_user_parlays(user_id: int, status: str | None = None) -> list[dict]:
    db = await get_connection()
    try:
        if status:
            cursor = await db.execute(
                "SELECT * FROM parlays WHERE user_id = ? AND status = ? ORDER BY created_at DESC",
                (user_id, status),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM parlays WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_pending_parlay_legs_by_game(game_id: str) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM parlay_legs WHERE game_id = ? AND status = 'pending'",
            (game_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def update_parlay_leg_status(leg_id: int, status: str) -> None:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE parlay_legs SET status = ? WHERE id = ?", (status, leg_id)
        )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def update_parlay(parlay_id: int, status: str, payout: int | None = None) -> None:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE parlays SET status = ?, payout = ? WHERE id = ?",
            (status, payout, parlay_id),
        )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def delete_parlay(parlay_id: int) -> bool:
    db = await get_connection()
    try:
        await db.execute("DELETE FROM parlay_legs WHERE parlay_id = ?", (parlay_id,))
        cursor = await db.execute("DELETE FROM parlays WHERE id = ?", (parlay_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


@db_retry()
async def get_pending_parlay_game_ids() -> list[str]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT DISTINCT game_id FROM parlay_legs WHERE status = 'pending'"
        )
        rows = await cursor.fetchall()
        return [row["game_id"] for row in rows]
    finally:
        await db.close()


@db_retry()
async def get_user_resolved_bets(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM bets WHERE user_id = ? AND status != 'pending'"
            " ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_user_resolved_parlays(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM parlays WHERE user_id = ? AND status != 'pending'"
            " ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_user_bet_stats(user_id: int) -> dict:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT"
            " COUNT(*) as total,"
            " SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,"
            " SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as losses,"
            " SUM(CASE WHEN status = 'push' THEN 1 ELSE 0 END) as pushes,"
            " SUM(amount) as total_wagered,"
            " SUM(CASE WHEN status != 'pending' THEN COALESCE(payout, 0) ELSE 0 END) as total_payout"
            " FROM bets WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        stats = dict(row) if row else {
            "total": 0, "wins": 0, "losses": 0, "pushes": 0,
            "total_wagered": 0, "total_payout": 0,
        }

        # Parlay stats
        cursor2 = await db.execute(
            "SELECT"
            " COUNT(*) as total,"
            " SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,"
            " SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as losses,"
            " SUM(amount) as total_wagered,"
            " SUM(CASE WHEN status != 'pending' THEN COALESCE(payout, 0) ELSE 0 END) as total_payout"
            " FROM parlays WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row2 = await cursor2.fetchone()
        if row2:
            p = dict(row2)
            stats["total"] = (stats["total"] or 0) + (p["total"] or 0)
            stats["wins"] = (stats["wins"] or 0) + (p["wins"] or 0)
            stats["losses"] = (stats["losses"] or 0) + (p["losses"] or 0)
            stats["total_wagered"] = (stats["total_wagered"] or 0) + (p["total_wagered"] or 0)
            stats["total_payout"] = (stats["total_payout"] or 0) + (p["total_payout"] or 0)

        return stats
    finally:
        await db.close()


@db_retry()
async def count_user_resolved_bets(user_id: int) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM bets WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
    finally:
        await db.close()


@db_retry()
async def count_user_resolved_parlays(user_id: int) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM parlays WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
    finally:
        await db.close()


@db_retry()
async def get_leaderboard(limit: int = 10) -> list[dict]:
    _PENDING_SQL = """
        COALESCE((SELECT SUM(amount) FROM bets         WHERE user_id = u.discord_id AND status = 'pending'), 0) +
        COALESCE((SELECT SUM(amount) FROM parlays      WHERE user_id = u.discord_id AND status = 'pending'), 0) +
        COALESCE((SELECT SUM(amount) FROM kalshi_bets  WHERE user_id = u.discord_id AND status = 'pending'), 0) +
        COALESCE((SELECT SUM(amount) FROM kalshi_parlays WHERE user_id = u.discord_id AND status = 'pending'), 0)
    """
    db = await get_connection()
    try:
        cursor = await db.execute(
            f"""SELECT discord_id, balance,
                ({_PENDING_SQL}) AS pending_total,
                balance + ({_PENDING_SQL}) AS total_value
            FROM users u
            ORDER BY total_value DESC LIMIT ?""",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def has_pending_bets(user_id: int) -> bool:
    """Return True if the user has any pending bets across all tables."""
    db = await get_connection()
    try:
        for table in ("bets", "parlays", "kalshi_bets", "kalshi_parlays"):
            cursor = await db.execute(
                f"SELECT 1 FROM {table} WHERE user_id = ? AND status = 'pending' LIMIT 1",
                (user_id,),
            )
            if await cursor.fetchone():
                return True
        return False
    finally:
        await db.close()


@db_retry()
async def get_user_pending_total(user_id: int) -> int:
    """Return the total wagered amount of all pending bets for a user across all tables."""
    db = await get_connection()
    try:
        cursor = await db.execute(
            """SELECT
                COALESCE((SELECT SUM(amount) FROM bets          WHERE user_id = ? AND status = 'pending'), 0) +
                COALESCE((SELECT SUM(amount) FROM parlays        WHERE user_id = ? AND status = 'pending'), 0) +
                COALESCE((SELECT SUM(amount) FROM kalshi_bets    WHERE user_id = ? AND status = 'pending'), 0) +
                COALESCE((SELECT SUM(amount) FROM kalshi_parlays WHERE user_id = ? AND status = 'pending'), 0)
                AS total""",
            (user_id, user_id, user_id, user_id),
        )
        row = await cursor.fetchone()
        return int(row["total"]) if row else 0
    finally:
        await db.close()


@db_retry()
async def get_user_rank(user_id: int) -> int | None:
    """Return the user's 1-based leaderboard rank by total value (balance + pending bets).

    Returns None if the user doesn't exist.
    """
    db = await get_connection()
    try:
        cursor = await db.execute(
            """SELECT COUNT(*) + 1 AS rank FROM users u
            WHERE (
                u.balance
                + COALESCE((SELECT SUM(amount) FROM bets          WHERE user_id = u.discord_id AND status = 'pending'), 0)
                + COALESCE((SELECT SUM(amount) FROM parlays        WHERE user_id = u.discord_id AND status = 'pending'), 0)
                + COALESCE((SELECT SUM(amount) FROM kalshi_bets    WHERE user_id = u.discord_id AND status = 'pending'), 0)
                + COALESCE((SELECT SUM(amount) FROM kalshi_parlays WHERE user_id = u.discord_id AND status = 'pending'), 0)
            ) > (
                SELECT u2.balance
                + COALESCE((SELECT SUM(amount) FROM bets          WHERE user_id = u2.discord_id AND status = 'pending'), 0)
                + COALESCE((SELECT SUM(amount) FROM parlays        WHERE user_id = u2.discord_id AND status = 'pending'), 0)
                + COALESCE((SELECT SUM(amount) FROM kalshi_bets    WHERE user_id = u2.discord_id AND status = 'pending'), 0)
                + COALESCE((SELECT SUM(amount) FROM kalshi_parlays WHERE user_id = u2.discord_id AND status = 'pending'), 0)
                FROM users u2 WHERE u2.discord_id = ?
            )""",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row["rank"] if row else None
    finally:
        await db.close()


@db_retry()
async def reset_all_balances(amount: int) -> int:
    """Reset all user balances to the given amount and clear all bets.

    Returns the number of users affected.
    """
    db = await get_connection()
    try:
        # Clear all bets
        await db.execute("DELETE FROM parlay_legs")
        await db.execute("DELETE FROM parlays")
        await db.execute("DELETE FROM bets")
        await db.execute("DELETE FROM kalshi_bets")
        await db.execute("DELETE FROM kalshi_parlay_legs")
        await db.execute("DELETE FROM kalshi_parlays")
        # Reset balances
        cursor = await db.execute(
            "UPDATE users SET balance = ?", (amount,)
        )
        await db.commit()
        return cursor.rowcount
    finally:
        await db.close()


@db_retry()
async def fix_fractional_balances() -> int:
    """Round all user balances to 2 decimal places.

    Returns the number of rows updated.
    """
    db = await get_connection()
    try:
        cursor = await db.execute(
            "UPDATE users SET balance = ROUND(balance, 2)"
        )
        await db.commit()
        return cursor.rowcount
    finally:
        await db.close()


@db_retry()
async def set_user_balance(discord_id: int, amount: int) -> int | None:
    """Set a single user's balance to exactly amount.

    Returns the new balance, or None if the user doesn't exist.
    """
    db = await get_connection()
    try:
        cursor = await db.execute(
            "UPDATE users SET balance = ? WHERE discord_id = ?", (amount, discord_id)
        )
        await db.commit()
        if cursor.rowcount == 0:
            return None
        row = await (await db.execute(
            "SELECT balance FROM users WHERE discord_id = ?", (discord_id,)
        )).fetchone()
        return row["balance"] if row else None
    finally:
        await db.close()


@db_retry()
async def devalue_all_balances(percent: float) -> tuple[int, int]:
    """Reduce all user balances by a percentage.

    Returns (users_affected, total_removed).
    """
    multiplier = 1.0 - (percent / 100.0)
    db = await get_connection()
    try:
        # Get total before
        cursor = await db.execute("SELECT SUM(balance) as total FROM users")
        row = await cursor.fetchone()
        total_before = row["total"] or 0

        cursor = await db.execute(
            "UPDATE users SET balance = MAX(CAST(balance * ? AS INTEGER), 0)",
            (multiplier,),
        )
        await db.commit()

        cursor2 = await db.execute("SELECT SUM(balance) as total FROM users")
        row2 = await cursor2.fetchone()
        total_after = row2["total"] or 0

        return cursor.rowcount, total_before - total_after
    finally:
        await db.close()


# ── Kalshi bets ───────────────────────────────────────────────────────


@db_retry()
async def create_kalshi_bet(
    user_id: int,
    market_ticker: str,
    event_ticker: str,
    pick: str,
    amount: int,
    odds: float,
    title: str | None = None,
    close_time: str | None = None,
    pick_display: str | None = None,
) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "INSERT INTO kalshi_bets (user_id, market_ticker, event_ticker, pick, amount, odds, title, close_time, pick_display)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, market_ticker, event_ticker, pick, amount, odds, title, close_time, pick_display),
        )
        await db.commit()
        return cursor.lastrowid
    finally:
        await db.close()


@db_retry()
async def get_user_kalshi_bets(user_id: int, status: str | None = None) -> list[dict]:
    db = await get_connection()
    try:
        if status:
            cursor = await db.execute(
                "SELECT * FROM kalshi_bets WHERE user_id = ? AND status = ? ORDER BY created_at DESC",
                (user_id, status),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM kalshi_bets WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_kalshi_bet_by_id(bet_id: int) -> dict | None:
    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM kalshi_bets WHERE id = ?", (bet_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


@db_retry()
async def delete_kalshi_bet(bet_id: int) -> bool:
    db = await get_connection()
    try:
        cursor = await db.execute("DELETE FROM kalshi_bets WHERE id = ?", (bet_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


@db_retry()
async def get_pending_kalshi_market_tickers() -> list[str]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT DISTINCT market_ticker FROM kalshi_bets WHERE status = 'pending'"
        )
        rows = await cursor.fetchall()
        return [row["market_ticker"] for row in rows]
    finally:
        await db.close()


@db_retry()
async def get_pending_kalshi_tickers_with_close_time() -> list[dict]:
    """Return pending tickers with their earliest close_time.

    Each dict has 'market_ticker' and 'close_time' (may be None).
    """
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT market_ticker, MIN(close_time) as close_time"
            " FROM kalshi_bets WHERE status = 'pending'"
            " GROUP BY market_ticker"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_all_pending_kalshi_bets() -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM kalshi_bets WHERE status = 'pending' ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_pending_kalshi_bets_by_market(market_ticker: str) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM kalshi_bets WHERE market_ticker = ? AND status = 'pending'",
            (market_ticker,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def resolve_kalshi_bet(bet_id: int, won: bool, payout: int) -> None:
    db = await get_connection()
    try:
        status = "won" if won else "lost"
        await db.execute(
            "UPDATE kalshi_bets SET status = ?, payout = ? WHERE id = ?",
            (status, payout, bet_id),
        )
        if won:
            cursor = await db.execute("SELECT user_id FROM kalshi_bets WHERE id = ?", (bet_id,))
            row = await cursor.fetchone()
            if row:
                await db.execute(
                    "UPDATE users SET balance = balance + ? WHERE discord_id = ?",
                    (payout, row["user_id"]),
                )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def cashout_kalshi_bet(bet_id: int, cashout_amount: float) -> int | None:
    """Mark a pending bet as cashed out, deposit the amount, and return the user_id.

    Returns None if the bet doesn't exist or is not pending.
    """
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT user_id FROM kalshi_bets WHERE id = ? AND status = 'pending'", (bet_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        user_id = row["user_id"]
        await db.execute(
            "UPDATE kalshi_bets SET status = 'cashed_out', payout = ? WHERE id = ?",
            (cashout_amount, bet_id),
        )
        await db.execute(
            "UPDATE users SET balance = balance + ? WHERE discord_id = ?",
            (round(cashout_amount), user_id),
        )
        await db.commit()
        return user_id
    finally:
        await db.close()


@db_retry()
async def get_user_resolved_kalshi_bets(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM kalshi_bets WHERE user_id = ? AND status != 'pending'"
            " ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def count_user_resolved_kalshi_bets(user_id: int) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM kalshi_bets WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
    finally:
        await db.close()


# ── Kalshi parlays ────────────────────────────────────────────────────


@db_retry()
async def create_kalshi_parlay(
    user_id: int, amount: int, total_odds: float, legs: list[dict]
) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "INSERT INTO kalshi_parlays (user_id, amount, total_odds) VALUES (?, ?, ?)",
            (user_id, amount, total_odds),
        )
        parlay_id = cursor.lastrowid
        for leg in legs:
            await db.execute(
                "INSERT INTO kalshi_parlay_legs (parlay_id, market_ticker, event_ticker, pick, odds, title, pick_display, close_time)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    parlay_id,
                    leg["market_ticker"],
                    leg["event_ticker"],
                    leg["pick"],
                    leg["odds"],
                    leg.get("title"),
                    leg.get("pick_display"),
                    leg.get("close_time"),
                ),
            )
        await db.commit()
        return parlay_id
    finally:
        await db.close()


@db_retry()
async def get_kalshi_parlay_by_id(parlay_id: int) -> dict | None:
    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM kalshi_parlays WHERE id = ?", (parlay_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


@db_retry()
async def get_kalshi_parlay_legs(parlay_id: int) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM kalshi_parlay_legs WHERE parlay_id = ?", (parlay_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_user_kalshi_parlays(user_id: int, status: str | None = None) -> list[dict]:
    db = await get_connection()
    try:
        if status:
            cursor = await db.execute(
                "SELECT * FROM kalshi_parlays WHERE user_id = ? AND status = ? ORDER BY created_at DESC",
                (user_id, status),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM kalshi_parlays WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def delete_kalshi_parlay(parlay_id: int) -> bool:
    db = await get_connection()
    try:
        await db.execute("DELETE FROM kalshi_parlay_legs WHERE parlay_id = ?", (parlay_id,))
        cursor = await db.execute("DELETE FROM kalshi_parlays WHERE id = ?", (parlay_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


@db_retry()
async def update_kalshi_parlay(parlay_id: int, status: str, payout: int | None = None) -> None:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE kalshi_parlays SET status = ?, payout = ? WHERE id = ?",
            (status, payout, parlay_id),
        )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def update_kalshi_parlay_leg_status(leg_id: int, status: str) -> None:
    db = await get_connection()
    try:
        await db.execute(
            "UPDATE kalshi_parlay_legs SET status = ? WHERE id = ?", (status, leg_id)
        )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def get_pending_kalshi_parlay_tickers_with_close_time() -> list[dict]:
    """Return pending kalshi parlay leg tickers with their earliest close_time."""
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT market_ticker, MIN(close_time) as close_time"
            " FROM kalshi_parlay_legs WHERE status = 'pending'"
            " GROUP BY market_ticker"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_pending_kalshi_parlay_legs_by_market(market_ticker: str) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM kalshi_parlay_legs WHERE market_ticker = ? AND status = 'pending'",
            (market_ticker,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_user_resolved_kalshi_parlays(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM kalshi_parlays WHERE user_id = ? AND status != 'pending'"
            " ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def count_user_resolved_kalshi_parlays(user_id: int) -> int:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM kalshi_parlays WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
    finally:
        await db.close()


@db_retry()
async def get_user_kalshi_parlay_stats(user_id: int) -> dict:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT"
            " COUNT(*) as total,"
            " SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,"
            " SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as losses,"
            " SUM(amount) as total_wagered,"
            " SUM(CASE WHEN status != 'pending' THEN COALESCE(payout, 0) ELSE 0 END) as total_payout"
            " FROM kalshi_parlays WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else {
            "total": 0, "wins": 0, "losses": 0,
            "total_wagered": 0, "total_payout": 0,
        }
    finally:
        await db.close()


@db_retry()
async def get_user_kalshi_bet_stats(user_id: int) -> dict:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT"
            " COUNT(*) as total,"
            " SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,"
            " SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as losses,"
            " SUM(amount) as total_wagered,"
            " SUM(CASE WHEN status != 'pending' THEN COALESCE(payout, 0) ELSE 0 END) as total_payout"
            " FROM kalshi_bets WHERE user_id = ? AND status != 'pending'",
            (user_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else {
            "total": 0, "wins": 0, "losses": 0,
            "total_wagered": 0, "total_payout": 0,
        }
    finally:
        await db.close()


# ── Casino game stats ─────────────────────────────────────────────────


@db_retry()
async def update_game_stats(
    discord_id: int,
    game: str,
    wagered: float,
    returned: float,
    won: bool,
    pushed: bool = False,
) -> None:
    """Upsert a single game outcome into the per-user, per-game stats table."""
    db = await get_connection()
    try:
        await db.execute(
            """
            INSERT INTO game_stats (discord_id, game, games_played, games_won, games_pushed,
                                    total_wagered, total_returned)
            VALUES (?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(discord_id, game) DO UPDATE SET
                games_played   = games_played   + 1,
                games_won      = games_won      + excluded.games_won,
                games_pushed   = games_pushed   + excluded.games_pushed,
                total_wagered  = total_wagered  + excluded.total_wagered,
                total_returned = total_returned + excluded.total_returned
            """,
            (discord_id, game, int(won), int(pushed), wagered, returned),
        )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def get_game_stats(discord_id: int) -> list[dict]:
    """Return all game stats rows for a user, ordered by game name."""
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT * FROM game_stats WHERE discord_id = ? ORDER BY game",
            (discord_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


# ── Craps roll record ─────────────────────────────────────────────────


@db_retry()
async def get_craps_roll_record() -> dict | None:
    """Return the current roll record, or None if no game has been played."""
    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM craps_roll_record WHERE id = 1")
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


@db_retry()
async def set_craps_roll_record(discord_id: int, display_name: str, roll_count: int) -> None:
    """Insert or replace the craps roll record."""
    db = await get_connection()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO craps_roll_record (id, discord_id, display_name, roll_count)"
            " VALUES (1, ?, ?, ?)",
            (discord_id, display_name, roll_count),
        )
        await db.commit()
    finally:
        await db.close()


# ── Twitch watches ────────────────────────────────────────────────────


@db_retry()
async def add_twitch_watch(stream_name: str, guild_id: int, channel_id: int) -> None:
    db = await get_connection()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO twitch_watches (stream_name, guild_id, channel_id)"
            " VALUES (?, ?, ?)",
            (stream_name.lower(), guild_id, channel_id),
        )
        await db.commit()
    finally:
        await db.close()


@db_retry()
async def remove_twitch_watch(stream_name: str, guild_id: int) -> bool:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "DELETE FROM twitch_watches WHERE stream_name = ? AND guild_id = ?",
            (stream_name.lower(), guild_id),
        )
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


@db_retry()
async def get_twitch_watches(guild_id: int) -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT stream_name, channel_id FROM twitch_watches WHERE guild_id = ?",
            (guild_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


@db_retry()
async def get_all_twitch_watches() -> list[dict]:
    db = await get_connection()
    try:
        cursor = await db.execute(
            "SELECT stream_name, guild_id, channel_id FROM twitch_watches"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()
