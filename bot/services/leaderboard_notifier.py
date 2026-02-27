"""Leaderboard pass notifications.

Call ``init(bot)`` once at startup.  Then use:

* ``deposit_and_notify(discord_id, amount, source)``
  when you control the deposit call (blackjack, craps, parlays).
* ``snapshot()`` + ``notify_if_passed(discord_id, amount, board_before, source)``
  when the deposit is embedded inside a model/service call you can't intercept.
"""
import logging

import discord

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.db import models
from bot.services import wallet_service

log = logging.getLogger(__name__)

_LIMIT = 20  # how far down the leaderboard to watch
_bot: discord.Client | None = None


def init(bot: discord.Client) -> None:
    global _bot
    _bot = bot


async def snapshot() -> list[dict]:
    """Return current top-_LIMIT leaderboard for use with notify_if_passed."""
    return await models.get_leaderboard(_LIMIT)


def _find_rank(board: list[dict], discord_id: int) -> int:
    """Return 1-based rank, or _LIMIT+1 if not in the snapshot."""
    for i, row in enumerate(board):
        if row["discord_id"] == discord_id:
            return i + 1
    return _LIMIT + 1


def _ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return {1: "1st", 2: "2nd", 3: "3rd"}.get(n % 10, f"{n}th")


async def _check_and_notify(
    discord_id: int,
    amount: int,
    old_rank: int,
    board_before: list[dict],
    source: str,
) -> None:
    if _bot is None:
        return

    board_after = await models.get_leaderboard(_LIMIT)
    new_rank = _find_rank(board_after, discord_id)

    if new_rank >= old_rank:
        return  # didn't move up

    # People who were sitting at the positions the user moved through
    passed = [
        row for row in board_before[new_rank - 1 : old_rank - 1]
        if row["discord_id"] != discord_id
    ]
    if not passed:
        return

    rank_str = _ordinal(new_rank)
    if len(passed) == 1:
        passed_str = f"<@{passed[0]['discord_id']}>"
    elif len(passed) == 2:
        passed_str = f"<@{passed[0]['discord_id']}> and <@{passed[1]['discord_id']}>"
    else:
        others = len(passed) - 2
        top_two = f"<@{passed[0]['discord_id']}> and <@{passed[1]['discord_id']}>"
        passed_str = f"{top_two} and {others} other{'s' if others > 1 else ''}"

    msg = (
        f"<@{discord_id}> earned **${amount:,}** from {source} "
        f"and is now **{rank_str} place**! They passed {passed_str} 📈"
    )

    channel = _bot.get_channel(BET_RESULTS_CHANNEL_ID)
    if channel:
        try:
            await channel.send(msg)
        except Exception:
            log.exception("Failed to post leaderboard pass notification")


async def deposit_and_notify(discord_id: int, amount: int, source: str) -> int:
    """Deposit amount and notify if the user passed anyone on the leaderboard."""
    board_before = await snapshot()
    old_rank = _find_rank(board_before, discord_id)
    new_balance = await wallet_service.deposit(discord_id, amount)
    await _check_and_notify(discord_id, amount, old_rank, board_before, source)
    return new_balance


async def notify_if_passed(
    discord_id: int,
    amount: int,
    board_before: list[dict],
    source: str,
) -> None:
    """Notify if user passed someone, for cases where the deposit was already made.

    board_before must be a snapshot taken BEFORE the deposit occurred.
    """
    old_rank = _find_rank(board_before, discord_id)
    await _check_and_notify(discord_id, amount, old_rank, board_before, source)
