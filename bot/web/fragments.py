"""HTML fragment endpoints for live dashboard updates.

Each handler returns just the markup for a single section — the same markup
the corresponding full-page route renders inside its `data-poll` container.
The browser swaps the container's innerHTML on each tick.
"""

from __future__ import annotations

from aiohttp import web

from bot.db import models
from bot.services.kalshi_api import kalshi_api
from bot.web import render


def _bot(request: web.Request):
    return request.app["bot"]


def _html(markup: str) -> web.Response:
    return web.Response(text=markup, content_type="text/html")


async def overview_supply(request: web.Request) -> web.Response:
    return _html(render.render_overview_cards(await models.get_money_supply_summary()))


async def overview_activity(request: web.Request) -> web.Response:
    bets = await models.get_all_active_kalshi_bets(limit=200)
    parlays = await models.get_all_active_kalshi_parlays_with_legs(limit=100)
    return _html(render.render_overview_active_summary(bets, parlays))


async def overview_leaderboard(request: web.Request) -> web.Response:
    rows = await models.get_full_leaderboard(limit=10)
    return _html(render.render_leaderboard_table(rows, _bot(request)))


async def wallets_table(request: web.Request) -> web.Response:
    rows = await models.get_full_leaderboard(limit=200)
    return _html(render.render_leaderboard_table(rows, _bot(request)))


async def bets_active(request: web.Request) -> web.Response:
    bets = await models.get_all_active_kalshi_bets(limit=300)
    parlays = await models.get_all_active_kalshi_parlays_with_legs(limit=200)
    return _html(render.render_active_bets_section(bets, parlays, _bot(request)))


async def bets_history(request: web.Request) -> web.Response:
    bets = await models.get_recent_resolved_kalshi_bets(limit=100)
    parlays = await models.get_recent_resolved_kalshi_parlays_with_legs(limit=50)
    return _html(render.render_history_section(bets, parlays, _bot(request)))


async def voice(request: web.Request) -> web.Response:
    rows = await models.get_voice_minutes_leaderboard(limit=200)
    return _html(render.render_voice_section(rows, _bot(request)))


async def markets(request: web.Request) -> web.Response:
    try:
        ms = await kalshi_api.get_all_open_sports_markets()
    except Exception:
        ms = []
    return _html(render.render_markets_section(ms))


def register(app: web.Application) -> None:
    app.router.add_get("/fragments/overview/supply", overview_supply)
    app.router.add_get("/fragments/overview/activity", overview_activity)
    app.router.add_get("/fragments/overview/leaderboard", overview_leaderboard)
    app.router.add_get("/fragments/wallets/table", wallets_table)
    app.router.add_get("/fragments/bets/active", bets_active)
    app.router.add_get("/fragments/bets/history", bets_history)
    app.router.add_get("/fragments/voice", voice)
    app.router.add_get("/fragments/markets", markets)
