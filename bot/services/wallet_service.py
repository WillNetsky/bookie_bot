from bot.db import models


async def get_balance(discord_id: int) -> int:
    user = await models.get_or_create_user(discord_id)
    return user["balance"]


async def deposit(discord_id: int, amount: int) -> int:
    await models.get_or_create_user(discord_id)
    return await models.update_balance(discord_id, amount)


async def withdraw(discord_id: int, amount: int) -> int | None:
    user = await models.get_or_create_user(discord_id)
    if user["balance"] < amount:
        return None
    return await models.update_balance(discord_id, -amount)


async def add_voice_reward(discord_id: int, minutes: int, coins_per_minute: int) -> int:
    await models.get_or_create_user(discord_id)
    await models.add_voice_minutes(discord_id, minutes)
    reward = minutes * coins_per_minute
    return await models.update_balance(discord_id, reward)
