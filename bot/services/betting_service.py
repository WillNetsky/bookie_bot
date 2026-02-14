from __future__ import annotations

from bot.db import models
from bot.services.wallet_service import deposit, withdraw


async def place_bet(
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
) -> int | None:
    """Place a bet. Returns bet ID on success, None if insufficient balance."""
    new_balance = await withdraw(user_id, amount)
    if new_balance is None:
        return None
    bet_id = await models.create_bet(
        user_id, game_id, pick, amount, odds,
        home_team=home_team, away_team=away_team, sport_title=sport_title,
        market=market, point=point,
    )
    return bet_id


async def get_user_bets(user_id: int, status: str | None = None) -> list[dict]:
    return await models.get_user_bets(user_id, status)


async def cancel_bet(bet_id: int, user_id: int) -> dict | None:
    """Cancel a pending bet. Returns the bet dict if successful, None otherwise."""
    bet = await models.get_bet_by_id(bet_id)
    if not bet:
        return None
    if bet["user_id"] != user_id:
        return None
    if bet["status"] != "pending":
        return None
    await models.delete_bet(bet_id)
    await deposit(user_id, bet["amount"])
    return bet


async def get_pending_game_ids() -> list[str]:
    return await models.get_pending_game_ids()


async def resolve_game(
    game_id: str,
    winner: str,
    home_score: int | None = None,
    away_score: int | None = None,
    winner_name: str | None = None,
) -> list[dict]:
    """Resolve all pending bets for a game. Returns list of resolved bet dicts.

    Each returned dict contains the original bet fields plus a ``result`` key
    ("won", "lost", or "push") and a ``payout`` value.

    For h2h bets, uses the winner string.
    For spread/total bets, requires home_score and away_score.
    For outrights, uses winner_name to match the pick.
    """
    bets = await models.get_pending_bets_by_game(game_id)
    resolved: list[dict] = []
    have_scores = home_score is not None and away_score is not None

    for bet in bets:
        market = bet.get("market") or "h2h"
        result: str | None = None

        if market == "outrights":
            if winner_name is not None:
                won = bet["pick"].lower() == winner_name.lower()
                await resolve_bet(bet["id"], won)
                result = "won" if won else "lost"
            else:
                continue

        elif market == "h2h":
            won = bet["pick"] == winner
            await resolve_bet(bet["id"], won)
            result = "won" if won else "lost"

        elif market == "spreads" and have_scores:
            point = bet.get("point") or 0.0
            pick = bet["pick"]
            if pick == "spread_home":
                adjusted = home_score + point
                if adjusted > away_score:
                    await resolve_bet(bet["id"], True)
                    result = "won"
                elif adjusted < away_score:
                    await resolve_bet(bet["id"], False)
                    result = "lost"
                else:
                    await refund_bet(bet["id"])
                    result = "push"
            elif pick == "spread_away":
                adjusted = away_score + point
                if adjusted > home_score:
                    await resolve_bet(bet["id"], True)
                    result = "won"
                elif adjusted < home_score:
                    await resolve_bet(bet["id"], False)
                    result = "lost"
                else:
                    await refund_bet(bet["id"])
                    result = "push"

        elif market == "totals" and have_scores:
            point = bet.get("point") or 0.0
            total = home_score + away_score
            pick = bet["pick"]
            if pick == "over":
                if total > point:
                    await resolve_bet(bet["id"], True)
                    result = "won"
                elif total < point:
                    await resolve_bet(bet["id"], False)
                    result = "lost"
                else:
                    await refund_bet(bet["id"])
                    result = "push"
            elif pick == "under":
                if total < point:
                    await resolve_bet(bet["id"], True)
                    result = "won"
                elif total > point:
                    await resolve_bet(bet["id"], False)
                    result = "lost"
                else:
                    await refund_bet(bet["id"])
                    result = "push"

        if result is not None:
            entry = dict(bet)
            entry["result"] = result
            if result == "won":
                entry["payout"] = round(bet["amount"] * bet["odds"], 2)
            elif result == "push":
                entry["payout"] = bet["amount"]
            else:
                entry["payout"] = 0
            resolved.append(entry)

    return resolved


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
            payout = round(bet["amount"] * bet["odds"], 2)
            await db.execute(
                "UPDATE bets SET status = 'won', payout = ? WHERE id = ?",
                (payout, bet_id),
            )
            await db.execute(
                "UPDATE users SET balance = balance + ? WHERE discord_id = ?",
                (payout, bet["user_id"]),
            )
        else:
            await db.execute(
                "UPDATE bets SET status = 'lost', payout = 0 WHERE id = ?",
                (bet_id,),
            )
        await db.commit()
    finally:
        await db.close()


async def refund_bet(bet_id: int) -> None:
    """Refund a bet (push — lands exactly on the line)."""
    from bot.db.database import get_connection

    db = await get_connection()
    try:
        cursor = await db.execute("SELECT * FROM bets WHERE id = ?", (bet_id,))
        bet = await cursor.fetchone()
        if not bet or bet["status"] != "pending":
            return

        await db.execute(
            "UPDATE bets SET status = 'push', payout = ? WHERE id = ?",
            (bet["amount"], bet_id),
        )
        await db.execute(
            "UPDATE users SET balance = balance + ? WHERE discord_id = ?",
            (bet["amount"], bet["user_id"]),
        )
        await db.commit()
    finally:
        await db.close()
