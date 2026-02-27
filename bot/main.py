import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.config import DISCORD_TOKEN, GUILD_ID
from bot.db.database import init_db
from bot.services import leaderboard_notifier

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

COGS = [
    "bot.cogs.wallet",
    "bot.cogs.voice_rewards",
    "bot.cogs.kalshi",
    "bot.cogs.craps",
    "bot.cogs.blackjack",
    "bot.cogs.twitch",
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
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            log.info("Slash commands synced to guild %s.", GUILD_ID)
            # Clear any stale global commands so they don't show up as duplicates
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            log.info("Cleared global commands to avoid duplicates.")
        else:
            await self.tree.sync()
            log.info("Slash commands synced globally.")

        leaderboard_notifier.init(self)

        @self.tree.error
        async def on_tree_error(
            interaction: discord.Interaction,
            error: app_commands.AppCommandError,
        ) -> None:
            cause = error.original if isinstance(error, app_commands.CommandInvokeError) else error
            if isinstance(cause, discord.HTTPException) and cause.status == 429:
                log.warning(
                    "Global rate limit hit in command '%s' — responding gracefully",
                    interaction.command.name if interaction.command else "unknown",
                )
                msg = "Discord is rate-limiting the bot right now — please try again in a moment."
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message(msg, ephemeral=True)
                    else:
                        await interaction.followup.send(msg, ephemeral=True)
                except Exception:
                    pass
                return
            log.exception("Unhandled app command error in '%s'",
                          interaction.command.name if interaction.command else "unknown",
                          exc_info=error)

    async def on_ready(self) -> None:
        log.info("Logged in as %s (ID: %s)", self.user, self.user.id)


def main() -> None:
    bot = BookieBot()
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
