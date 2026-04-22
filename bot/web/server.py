"""aiohttp server lifecycle for the dashboard.

Runs inside the Discord bot's event loop via `AppRunner` + `TCPSite`, so it
shares the same asyncio loop as discord.py and the existing services.
"""

from __future__ import annotations

import logging

from aiohttp import web

from bot.web import fragments, routes

log = logging.getLogger(__name__)


class DashboardServer:
    def __init__(self, bot, host: str, port: int) -> None:
        self.bot = bot
        self.host = host
        self.port = port
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def start(self) -> None:
        app = web.Application()
        app["bot"] = self.bot
        routes.register(app)
        fragments.register(app)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()
        log.info("Dashboard listening on http://%s:%d", self.host, self.port)

    async def stop(self) -> None:
        if self._site is not None:
            await self._site.stop()
            self._site = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        log.info("Dashboard stopped.")
