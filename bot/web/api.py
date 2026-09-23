"""JSON endpoints for external integrations (e.g. the Homepage start page)."""

from __future__ import annotations

from aiohttp import web

from bot.db import models


async def summary(request: web.Request) -> web.Response:
    active = await models.get_active_kalshi_summary()
    supply = await models.get_money_supply_summary()
    return web.json_response({
        "active_bets": active["bet_count"],
        "active_parlays": active["parlay_count"],
        "active_total": active["bet_count"] + active["parlay_count"],
        "at_risk": active["bet_total"] + active["parlay_total"],
        "money_supply": supply["total"],
        "users": supply["user_count"],
    })


def register(app: web.Application) -> None:
    app.router.add_get("/api/summary", summary)
