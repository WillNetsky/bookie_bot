"""HTML rendering helpers for the dashboard.

Each `render_*_section` function returns the markup for a single live-updating
section. Both the full-page route and the matching `/fragments/...` endpoint
call the same function, so there is one source of truth per section. Pages
simply wrap section calls in the shared layout below.
"""

from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Iterable

from bot.utils import (
    decimal_to_american,
    fmt_money,
    format_american_with_prob,
    format_game_time,
)


def esc(value) -> str:
    """HTML-escape any value, coercing None to empty string."""
    if value is None:
        return ""
    return html.escape(str(value))


def name_for(bot, user_id: int) -> str:
    """Resolve a discord_id to a display name using the bot's caches; fall back to id."""
    if bot is None:
        return str(user_id)
    user = bot.get_user(user_id)
    if user is not None:
        return user.display_name or user.name
    for guild in bot.guilds:
        member = guild.get_member(user_id)
        if member is not None:
            return member.display_name
    return str(user_id)


def fmt_close_time(close_time: str | None) -> str:
    if not close_time:
        return ""
    return format_game_time(close_time)


# ── Layout ────────────────────────────────────────────────────────────


_NAV = [
    ("/", "Overview"),
    ("/wallets", "Wallets"),
    ("/bets/active", "Active bets"),
    ("/bets/history", "History"),
    ("/voice", "Voice"),
    ("/markets", "Markets"),
]


_CSS = """
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
  background: #0f1115;
  color: #e6e6e6;
  font-size: 14px;
  line-height: 1.45;
}
header {
  background: #161922;
  border-bottom: 1px solid #232838;
  padding: 12px 24px;
  display: flex;
  align-items: center;
  gap: 24px;
  position: sticky;
  top: 0;
  z-index: 10;
}
header h1 {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
  color: #f5d76e;
}
nav { display: flex; gap: 4px; flex-wrap: wrap; }
nav a {
  color: #c8c8d0;
  text-decoration: none;
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 13px;
}
nav a:hover { background: #232838; color: #fff; }
nav a.active { background: #2a3142; color: #f5d76e; }
.indicator {
  margin-left: auto;
  font-size: 11px;
  color: #6b7280;
  display: flex;
  align-items: center;
  gap: 6px;
}
.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #4ade80;
  box-shadow: 0 0 6px #4ade80;
}
main {
  padding: 24px;
  max-width: 1400px;
  margin: 0 auto;
}
h2 { font-size: 14px; text-transform: uppercase; letter-spacing: 0.06em; color: #9ca3af; margin: 28px 0 10px; font-weight: 600; }
h2:first-child { margin-top: 0; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 8px; }
.card {
  background: #161922;
  border: 1px solid #232838;
  border-radius: 8px;
  padding: 16px;
}
.card .label { color: #9ca3af; font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; }
.card .value { font-size: 22px; font-weight: 600; color: #f5d76e; margin-top: 4px; }
.card .sub { color: #6b7280; font-size: 12px; margin-top: 2px; }
table {
  width: 100%;
  border-collapse: collapse;
  background: #161922;
  border: 1px solid #232838;
  border-radius: 8px;
  overflow: hidden;
}
th, td {
  text-align: left;
  padding: 8px 12px;
  border-bottom: 1px solid #1f2330;
  font-size: 13px;
  vertical-align: top;
}
th {
  background: #1a1e2a;
  color: #9ca3af;
  font-weight: 600;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  position: sticky;
  top: 49px;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: #1a1e2a; }
.muted { color: #6b7280; }
.pos { color: #4ade80; }
.neg { color: #f87171; }
.warn { color: #fbbf24; }
a { color: #93c5fd; text-decoration: none; }
a:hover { text-decoration: underline; }
.pill {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 11px;
  background: #232838;
  color: #c8c8d0;
}
.pill.win { background: #14532d; color: #86efac; }
.pill.loss { background: #481a1a; color: #fca5a5; }
.pill.void { background: #2a3142; color: #c8c8d0; }
.pill.pending { background: #2a2218; color: #fbbf24; }
details { margin-bottom: 8px; }
details > summary { cursor: pointer; padding: 6px 0; color: #93c5fd; }
details pre {
  background: #0a0b0f;
  border: 1px solid #232838;
  border-radius: 6px;
  padding: 12px;
  overflow-x: auto;
  font-size: 12px;
  color: #c8c8d0;
}
.empty { padding: 24px; text-align: center; color: #6b7280; font-style: italic; }
.legs { padding-left: 18px; color: #9ca3af; font-size: 12px; margin-top: 4px; }
.legs > div { padding: 2px 0; }
"""


_JS = """
(function() {
  const ABORT = new Map();
  function poll(el) {
    const url = el.dataset.poll;
    if (!url) return;
    const interval = parseInt(el.dataset.interval, 10) || 5000;
    const tick = async () => {
      const prev = ABORT.get(el);
      if (prev) prev.abort();
      const ctrl = new AbortController();
      ABORT.set(el, ctrl);
      try {
        const r = await fetch(url, { signal: ctrl.signal, cache: 'no-store' });
        if (!r.ok) return;
        const html = await r.text();
        if (el.innerHTML !== html) el.innerHTML = html;
      } catch (e) { /* aborted or network blip */ }
    };
    el._pollTimer = setInterval(tick, interval);
  }
  function init() {
    document.querySelectorAll('[data-poll]').forEach(poll);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
"""


def page(title: str, current_path: str, body: str) -> str:
    nav_links = "".join(
        f'<a href="{esc(href)}" class="{"active" if href == current_path else ""}">{esc(label)}</a>'
        for href, label in _NAV
    )
    return (
        "<!doctype html><html lang=\"en\"><head>"
        f"<meta charset=\"utf-8\"><title>{esc(title)} · Bookie Bot</title>"
        f"<style>{_CSS}</style>"
        "</head><body>"
        f"<header><h1>Bookie Bot</h1><nav>{nav_links}</nav>"
        "<div class=\"indicator\"><span class=\"dot\"></span>live</div></header>"
        f"<main>{body}</main>"
        f"<script>{_JS}</script>"
        "</body></html>"
    )


# ── Section renderers (also used by /fragments/* endpoints) ───────────


def render_overview_cards(supply: dict) -> str:
    return (
        "<div class=\"cards\">"
        f"<div class=\"card\"><div class=\"label\">Total money supply</div>"
        f"<div class=\"value\">{esc(fmt_money(supply['total']))}</div>"
        f"<div class=\"sub\">{esc(fmt_money(supply['held']))} held · {esc(fmt_money(supply['locked']))} locked in bets</div></div>"
        f"<div class=\"card\"><div class=\"label\">Users</div>"
        f"<div class=\"value\">{supply['user_count']}</div></div>"
        "</div>"
    )


def render_overview_active_summary(active_bets: list[dict], active_parlays: list[dict]) -> str:
    bet_total = sum(int(b["amount"]) for b in active_bets)
    parlay_total = sum(int(p["amount"]) for p in active_parlays)
    return (
        "<div class=\"cards\">"
        f"<div class=\"card\"><div class=\"label\">Active single bets</div>"
        f"<div class=\"value\">{len(active_bets)}</div>"
        f"<div class=\"sub\">{esc(fmt_money(bet_total))} at risk</div></div>"
        f"<div class=\"card\"><div class=\"label\">Active parlays</div>"
        f"<div class=\"value\">{len(active_parlays)}</div>"
        f"<div class=\"sub\">{esc(fmt_money(parlay_total))} at risk</div></div>"
        "</div>"
    )


def render_leaderboard_table(rows: list[dict], bot) -> str:
    if not rows:
        return "<div class=\"empty\">No users yet.</div>"
    body = "".join(
        "<tr>"
        f"<td>{i + 1}</td>"
        f"<td>{esc(name_for(bot, r['discord_id']))}</td>"
        f"<td>{esc(fmt_money(r['total_value']))}</td>"
        f"<td class=\"muted\">{esc(fmt_money(r['balance']))}</td>"
        f"<td class=\"muted\">{esc(fmt_money(r['pending_total']))}</td>"
        f"<td class=\"muted\">{int(r.get('voice_minutes') or 0)} min</td>"
        f"<td class=\"muted\">{int(r.get('bankruptcy_count') or 0)}</td>"
        "</tr>"
        for i, r in enumerate(rows)
    )
    return (
        "<table><thead><tr>"
        "<th>#</th><th>User</th><th>Total value</th><th>Balance</th>"
        "<th>Pending</th><th>Voice</th><th>Bankruptcies</th>"
        "</tr></thead><tbody>"
        f"{body}</tbody></table>"
    )


def _bet_pick_label(bet: dict) -> str:
    pd = bet.get("pick_display")
    if pd:
        return pd
    pick = bet.get("pick", "")
    return pick.upper() if pick else ""


def render_active_bets_section(bets: list[dict], parlays: list[dict], bot) -> str:
    parts: list[str] = []
    parts.append("<h2>Single bets</h2>")
    if not bets:
        parts.append("<div class=\"empty\">No active single bets.</div>")
    else:
        rows = "".join(
            "<tr>"
            f"<td>{esc(name_for(bot, b['user_id']))}</td>"
            f"<td>{esc(b.get('title') or b['market_ticker'])}<div class=\"muted\" style=\"font-size:11px\">{esc(b['market_ticker'])}</div></td>"
            f"<td>{esc(_bet_pick_label(b))}</td>"
            f"<td>{esc(fmt_money(b['amount']))}</td>"
            f"<td>{esc(format_american_with_prob(decimal_to_american(b['odds'])))}</td>"
            f"<td class=\"muted\">{esc(fmt_close_time(b.get('close_time')))}</td>"
            "</tr>"
            for b in bets
        )
        parts.append(
            "<table><thead><tr><th>User</th><th>Market</th><th>Pick</th>"
            "<th>Wager</th><th>Odds</th><th>Closes</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )
    parts.append("<h2>Parlays</h2>")
    if not parlays:
        parts.append("<div class=\"empty\">No active parlays.</div>")
    else:
        rows = []
        for p in parlays:
            legs_html = "".join(
                f"<div>· {esc(leg.get('title') or leg['market_ticker'])} — "
                f"<b>{esc(leg.get('pick_display') or leg['pick'])}</b> "
                f"@ {esc(format_american_with_prob(decimal_to_american(leg['odds'])))}</div>"
                for leg in p.get("legs", [])
            )
            rows.append(
                "<tr>"
                f"<td>{esc(name_for(bot, p['user_id']))}</td>"
                f"<td>{len(p.get('legs', []))} legs<div class=\"legs\">{legs_html}</div></td>"
                f"<td>{esc(fmt_money(p['amount']))}</td>"
                f"<td>{esc(format_american_with_prob(decimal_to_american(p['total_odds'])))}</td>"
                f"<td class=\"muted\">{esc(p.get('created_at') or '')}</td>"
                "</tr>"
            )
        parts.append(
            "<table><thead><tr><th>User</th><th>Legs</th><th>Wager</th>"
            "<th>Combined odds</th><th>Placed</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
        )
    return "".join(parts)


def _status_pill(status: str) -> str:
    cls = {
        "won": "win",
        "lost": "loss",
        "void": "void",
        "pushed": "void",
        "cashed_out": "win",
        "pending": "pending",
    }.get(status, "")
    return f'<span class="pill {cls}">{esc(status)}</span>'


def render_history_section(bets: list[dict], parlays: list[dict], bot) -> str:
    parts: list[str] = []
    parts.append("<h2>Recent settled bets</h2>")
    if not bets:
        parts.append("<div class=\"empty\">No settled bets yet.</div>")
    else:
        rows = "".join(
            "<tr>"
            f"<td>{esc(name_for(bot, b['user_id']))}</td>"
            f"<td>{esc(b.get('title') or b['market_ticker'])}</td>"
            f"<td>{esc(_bet_pick_label(b))}</td>"
            f"<td>{esc(fmt_money(b['amount']))}</td>"
            f"<td class=\"{'pos' if (b.get('payout') or 0) > b['amount'] else 'neg' if (b.get('payout') or 0) == 0 else 'muted'}\">"
            f"{esc(fmt_money(b.get('payout') or 0))}</td>"
            f"<td>{_status_pill(b['status'])}</td>"
            f"<td class=\"muted\">{esc(b.get('created_at') or '')}</td>"
            "</tr>"
            for b in bets
        )
        parts.append(
            "<table><thead><tr><th>User</th><th>Market</th><th>Pick</th>"
            "<th>Wager</th><th>Payout</th><th>Status</th><th>Placed</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )
    parts.append("<h2>Recent settled parlays</h2>")
    if not parlays:
        parts.append("<div class=\"empty\">No settled parlays yet.</div>")
    else:
        rows = []
        for p in parlays:
            legs_html = "".join(
                f"<div>· {esc(leg.get('title') or leg['market_ticker'])} — "
                f"<b>{esc(leg.get('pick_display') or leg['pick'])}</b> "
                f"({_status_pill(leg['status'])})</div>"
                for leg in p.get("legs", [])
            )
            payout = int(p.get("payout") or 0)
            payout_cls = "pos" if payout > p["amount"] else "neg" if payout == 0 else "muted"
            rows.append(
                "<tr>"
                f"<td>{esc(name_for(bot, p['user_id']))}</td>"
                f"<td>{len(p.get('legs', []))} legs<div class=\"legs\">{legs_html}</div></td>"
                f"<td>{esc(fmt_money(p['amount']))}</td>"
                f"<td class=\"{payout_cls}\">{esc(fmt_money(payout))}</td>"
                f"<td>{_status_pill(p['status'])}</td>"
                f"<td class=\"muted\">{esc(p.get('created_at') or '')}</td>"
                "</tr>"
            )
        parts.append(
            "<table><thead><tr><th>User</th><th>Legs</th><th>Wager</th>"
            "<th>Payout</th><th>Status</th><th>Placed</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
        )
    return "".join(parts)


def render_voice_section(rows: list[dict], bot) -> str:
    if not rows:
        return "<div class=\"empty\">No voice activity yet.</div>"
    body = "".join(
        "<tr>"
        f"<td>{i + 1}</td>"
        f"<td>{esc(name_for(bot, r['discord_id']))}</td>"
        f"<td>{int(r['voice_minutes'])} min</td>"
        f"<td class=\"muted\">{_fmt_hours(r['voice_minutes'])}</td>"
        f"<td class=\"muted\">{esc(fmt_money(r['balance']))}</td>"
        f"<td class=\"muted\">{int(r.get('bankruptcy_count') or 0)}</td>"
        "</tr>"
        for i, r in enumerate(rows)
    )
    return (
        "<table><thead><tr><th>#</th><th>User</th><th>Minutes</th>"
        "<th>Hours</th><th>Balance</th><th>Bankruptcies</th></tr></thead>"
        f"<tbody>{body}</tbody></table>"
    )


def _fmt_hours(minutes: int) -> str:
    h = minutes // 60
    m = minutes % 60
    if h == 0:
        return f"{m}m"
    return f"{h}h {m}m"


def _market_price_cells(m: dict) -> tuple[str, str]:
    """Return (yes price, no price) cell HTML."""

    def cents(v) -> str:
        if v is None:
            return "—"
        try:
            return f"{int(v)}¢"
        except (TypeError, ValueError):
            return str(v)

    yes = cents(m.get("yes_ask") or m.get("yes_bid"))
    no = cents(m.get("no_ask") or m.get("no_bid"))
    return yes, no


def render_markets_section(markets: list[dict]) -> str:
    if not markets:
        return "<div class=\"empty\">No open markets cached. Try /games in Discord first.</div>"

    by_event: dict[str, list[dict]] = {}
    for m in markets:
        by_event.setdefault(m.get("event_ticker") or "(unknown)", []).append(m)

    parts: list[str] = []
    parts.append(
        f"<div class=\"muted\" style=\"margin-bottom:12px\">{len(markets)} markets across {len(by_event)} events</div>"
    )
    for event_ticker, ms in sorted(by_event.items()):
        first = ms[0]
        title = first.get("title") or event_ticker
        rows = []
        for m in ms:
            yes, no = _market_price_cells(m)
            rows.append(
                "<tr>"
                f"<td><a href=\"/markets/{esc(m.get('event_ticker'))}\">{esc(m.get('title') or m.get('ticker'))}</a>"
                f"<div class=\"muted\" style=\"font-size:11px\">{esc(m.get('ticker'))}</div></td>"
                f"<td>{esc(yes)}</td>"
                f"<td>{esc(no)}</td>"
                f"<td class=\"muted\">{esc(_fmt_int(m.get('volume')))}</td>"
                f"<td class=\"muted\">{esc(_fmt_int(m.get('liquidity')))}</td>"
                f"<td class=\"muted\">{esc(fmt_close_time(m.get('close_time')))}</td>"
                "</tr>"
            )
        parts.append(
            f"<details open><summary><b>{esc(title)}</b> "
            f"<span class=\"muted\">({event_ticker} · {len(ms)} markets)</span></summary>"
            "<table><thead><tr><th>Market</th><th>Yes</th><th>No</th>"
            "<th>Volume</th><th>Liquidity</th><th>Closes</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></details>"
        )
    return "".join(parts)


def _fmt_int(value) -> str:
    if value is None:
        return ""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def render_market_detail(event_ticker: str, markets: list[dict]) -> str:
    """Render a single event with all its markets and the raw JSON."""
    import json as _json

    if not markets:
        return f"<p>No cached markets for event <code>{esc(event_ticker)}</code>.</p>"

    parts: list[str] = []
    parts.append(f"<p><a href=\"/markets\">← all markets</a></p>")
    parts.append(f"<h2>{esc(markets[0].get('title') or event_ticker)}</h2>")
    parts.append(
        f"<div class=\"muted\" style=\"margin-bottom:12px\">"
        f"Event ticker: <code>{esc(event_ticker)}</code> · {len(markets)} markets</div>"
    )
    rows = []
    for m in markets:
        yes, no = _market_price_cells(m)
        rows.append(
            "<tr>"
            f"<td><b>{esc(m.get('title') or m.get('ticker'))}</b>"
            f"<div class=\"muted\" style=\"font-size:11px\">{esc(m.get('ticker'))}</div></td>"
            f"<td>{esc(yes)}</td>"
            f"<td>{esc(no)}</td>"
            f"<td class=\"muted\">{esc(_fmt_int(m.get('volume')))}</td>"
            f"<td class=\"muted\">{esc(_fmt_int(m.get('liquidity')))}</td>"
            f"<td class=\"muted\">{esc(m.get('status') or '')}</td>"
            f"<td class=\"muted\">{esc(fmt_close_time(m.get('close_time')))}</td>"
            "</tr>"
        )
    parts.append(
        "<table><thead><tr><th>Market</th><th>Yes</th><th>No</th>"
        "<th>Volume</th><th>Liquidity</th><th>Status</th><th>Closes</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )
    parts.append("<h2>Raw market JSON</h2>")
    for m in markets:
        parts.append(
            f"<details><summary>{esc(m.get('ticker'))}</summary>"
            f"<pre>{esc(_json.dumps(m, indent=2, default=str))}</pre></details>"
        )
    return "".join(parts)
