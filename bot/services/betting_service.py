from __future__ import annotations

from bot.db import models
from bot.services.wallet_service import withdraw


async def place_bet(
    user_id: int, game_id: str, pick: str, amount: int, odds: float
) -> int | None:
    """Place a bet. Returns bet ID on success, None if insufficient balance."""
    new_balance = await withdraw(user_id, amount)
    if new_balance is None:
        return None
    bet_id = await models.create_bet(user_id, game_id, pick, amount, odds)
    return bet_id


async def get_user_bets(user_id: int, status: str | None = None) -> list[dict]:
    return await models.get_user_bets(user_id, status)


async def resolve_bet(bet_id: int, won: bool) -> None:
    """Resolve a bet as won or lost."""
    from bot.db.database import get_connection

    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM bets WHERE id = ?", (bet_id,))
        bet = await cursor.fetchone()
        if not bet or bet["status"] != "pending":
            return

        if won:
            payout = int(bet["amount"] * bet["odds"])
            await db.execute(
                "UPDATE bets SET status = 'won', payout = ? WHERE id = ?",
                (payout, bet_id),
            )
            await models.update_balance(bet["user_id"], payout)
        else:
            await db.execute(
                "UPDATE bets SET status = 'lost', payout = 0 WHERE id = ?",
                (bet_id,),
            )
        await db.commit()
    finally:
        await db.close()
