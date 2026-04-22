"""Dashboard cog: owns the aiohttp web server lifecycle.

Starts the read-only web dashboard on cog load and tears it down on unload.
The cog model gives handlers access to the live discord.py bot instance, so
they can resolve user IDs to display names via the member cache.
"""

from __future__ import annotations

import logging

from discord.ext import commands

from bot.config import DASHBOARD_BIND, DASHBOARD_ENABLED, DASHBOARD_PORT
from bot.web.server import DashboardServer

log = logging.getLogger(__name__)


class Dashboard(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.server: DashboardServer | None = None

    async def cog_load(self) -> None:
        if not DASHBOARD_ENABLED:
            log.info("Dashboard disabled via DASHBOARD_ENABLED=0.")
            return
        self.server = DashboardServer(self.bot, DASHBOARD_BIND, DASHBOARD_PORT)
        try:
            await self.server.start()
        except Exception:
            log.exception("Failed to start dashboard server.")
            self.server = None

    async def cog_unload(self) -> None:
        if self.server is not None:
            try:
                await self.server.stop()
            except Exception:
                log.exception("Error stopping dashboard server.")
            self.server = None


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Dashboard(bot))
