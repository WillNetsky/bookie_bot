"""Full-page handlers for the dashboard.

Each page renders an initial server-side snapshot (works without JS) and marks
live sections with `data-poll` so the browser pulls the matching fragment from
`/fragments/...` on an interval. The fragment handlers live in `fragments.py`
and reuse the same `render_*_section` functions, so there is no duplication.
"""

from __future__ import annotations

from aiohttp import web

from bot.db import models
from bot.services.kalshi_api import kalshi_api
from bot.web import render


def _bot(request: web.Request):
    return request.app["bot"]


def _live(container_id: str, url: str, interval_ms: int, initial_html: str) -> str:
    return (
        f'<div id="{container_id}" data-poll="{url}" data-interval="{interval_ms}">'
        f"{initial_html}</div>"
    )


# ── Full pages ────────────────────────────────────────────────────────


async def index(request: web.Request) -> web.Response:
    bot = _bot(request)
    supply = await models.get_money_supply_summary()
    bets = await models.get_all_active_kalshi_bets(limit=200)
    parlays = await models.get_all_active_kalshi_parlays_with_legs(limit=100)
    leaderboard = await models.get_full_leaderboard(limit=10)

    body = (
        "<h2>Money supply</h2>"
        + _live("overview-supply", "/fragments/overview/supply", 5000, render.render_overview_cards(supply))
        + "<h2>Activity</h2>"
        + _live("overview-activity", "/fragments/overview/activity", 3000,
                render.render_overview_active_summary(bets, parlays))
        + "<h2>Top wallets</h2>"
        + _live("overview-leaderboard", "/fragments/overview/leaderboard", 5000,
                render.render_leaderboard_table(leaderboard, bot))
    )
    return web.Response(text=render.page("Overview", "/", body), content_type="text/html")


async def wallets(request: web.Request) -> web.Response:
    bot = _bot(request)
    rows = await models.get_full_leaderboard(limit=200)
    body = (
        "<h2>All wallets</h2>"
        + _live("wallets-table", "/fragments/wallets/table", 5000,
                render.render_leaderboard_table(rows, bot))
    )
    return web.Response(text=render.page("Wallets", "/wallets", body), content_type="text/html")


async def bets_active(request: web.Request) -> web.Response:
    bot = _bot(request)
    bets = await models.get_all_active_kalshi_bets(limit=300)
    parlays = await models.get_all_active_kalshi_parlays_with_legs(limit=200)
    body = _live(
        "active-bets",
        "/fragments/bets/active",
        3000,
        render.render_active_bets_section(bets, parlays, bot),
    )
    return web.Response(
        text=render.page("Active bets", "/bets/active", body), content_type="text/html"
    )


async def bets_history(request: web.Request) -> web.Response:
    bot = _bot(request)
    bets = await models.get_recent_resolved_kalshi_bets(limit=100)
    parlays = await models.get_recent_resolved_kalshi_parlays_with_legs(limit=50)
    body = _live(
        "history",
        "/fragments/bets/history",
        5000,
        render.render_history_section(bets, parlays, bot),
    )
    return web.Response(
        text=render.page("Bet history", "/bets/history", body), content_type="text/html"
    )


async def voice(request: web.Request) -> web.Response:
    bot = _bot(request)
    rows = await models.get_voice_minutes_leaderboard(limit=200)
    body = (
        "<h2>Voice activity</h2>"
        + _live("voice-table", "/fragments/voice", 5000, render.render_voice_section(rows, bot))
    )
    return web.Response(text=render.page("Voice", "/voice", body), content_type="text/html")


async def markets(request: web.Request) -> web.Response:
    try:
        ms = await kalshi_api.get_all_open_sports_markets()
    except Exception:
        ms = []
    body = (
        "<h2>Open markets we know about</h2>"
        + _live("markets", "/fragments/markets", 10000, render.render_markets_section(ms))
    )
    return web.Response(
        text=render.page("Markets", "/markets", body), content_type="text/html"
    )


async def market_detail(request: web.Request) -> web.Response:
    event_ticker = request.match_info["event_ticker"]
    try:
        all_markets = await kalshi_api.get_all_open_sports_markets()
    except Exception:
        all_markets = []
    matching = [m for m in all_markets if m.get("event_ticker") == event_ticker]
    body = render.render_market_detail(event_ticker, matching)
    return web.Response(
        text=render.page(f"Market {event_ticker}", "/markets", body),
        content_type="text/html",
    )


def register(app: web.Application) -> None:
    app.router.add_get("/", index)
    app.router.add_get("/wallets", wallets)
    app.router.add_get("/bets/active", bets_active)
    app.router.add_get("/bets/history", bets_history)
    app.router.add_get("/voice", voice)
    app.router.add_get("/markets", markets)
    app.router.add_get("/markets/{event_ticker}", market_detail)
