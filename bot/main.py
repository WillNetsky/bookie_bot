import asyncio
import logging

import discord
from discord.ext import commands

from bot.config import DISCORD_TOKEN
from bot.db.database import init_db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

COGS = [
    "bot.cogs.betting",
    "bot.cogs.wallet",
    "bot.cogs.voice_rewards",
]


class BookieBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.members = True
        intents.voice_states = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        await init_db()
        for cog in COGS:
            await self.load_extension(cog)
            log.info("Loaded cog: %s", cog)
        await self.tree.sync()
        log.info("Slash commands synced.")

    async def on_ready(self) -> None:
        log.info("Logged in as %s (ID: %s)", self.user, self.user.id)


def main() -> None:
    bot = BookieBot()
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
