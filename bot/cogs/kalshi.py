import logging
import re
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.services import betting_service
from bot.services.kalshi_api import (
    kalshi_api, SPORTS, FUTURES, KALSHI_TO_ODDS_API,
    _parse_event_ticker_date
)
from bot.services.sports_api import SportsAPI
from bot.constants import PICK_EMOJI, PICK_LABELS
from bot.utils import (
    format_matchup, format_game_time, format_pick_label,
    format_american, decimal_to_american
)
from bot.db.database import cleanup_cache, vacuum_db

log = logging.getLogger(__name__)


def _sport_emoji(sport_key: str) -> str:
    """Return an appropriate emoji for a sport based on its ticker."""
    sk = sport_key.upper()
    if "NBA" in sk or "WNBA" in sk or "NCAAMB" in sk or "NCAAWB" in sk or "NCAAB" in sk or "NBL" in sk or "EUROLEAGUE" in sk or "EUROCUP" in sk or "ACB" in sk or "BSL" in sk or "KBL" in sk or "BBL" in sk or "FIBA" in sk or "ABA" in sk or "GBL" in sk or "VTB" in sk or "CBA" in sk or "UNRIVALED" in sk or "ARGLNB" in sk or "JBLEAGUE" in sk or "BBSERIEA" in sk or "LNBELITE" in sk:
        return "\U0001f3c0"  # 🏀
    if "NFL" in sk or "NCAAF" in sk:
        return "\U0001f3c8"  # 🏈
    if "MLB" in sk or "NCAABB" in sk:
        return "\u26be"  # ⚾
    if "NHL" in sk or "AHL" in sk or "KHL" in sk or "IIHF" in sk or "SHL" in sk or "DEL" in sk or "LIIGA" in sk or "ELH" in sk or "NCAAHOCKEY" in sk or "SWISSLEAGUE" in sk:
        return "\U0001f3d2"  # 🏒
    if "UFC" in sk or "BOXING" in sk or "MMA" in sk or "FIGHT" in sk or "MCGREGOR" in sk:
        return "\U0001f94a"  # 🥊
    if "LAX" in sk or "LACROSSE" in sk:
        return "\U0001f94d"  # 🥍
    if "CRICKET" in sk or "IPL" in sk or "WPL" in sk:
        return "\U0001f3cf"  # 🏏
    if "TENNIS" in sk or "ATP" in sk or "WTA" in sk or "DAVISCUP" in sk or "UNITEDCUP" in sk or "SIXKINGS" in sk or "CHALLENGER" in sk:
        return "\U0001f3be"  # 🎾
    if "CURL" in sk:
        return "\U0001f94c"  # 🥌
    if "CHESS" in sk:
        return "\u265f\ufe0f"  # ♟️
    if "PICKLE" in sk:
        return "\U0001f3d3"  # 🏓
    if "RUGBY" in sk or "SIXNATIONS" in sk or "NRL" in sk:
        return "\U0001f3c9"  # 🏉
    if "DARTS" in sk:
        return "\U0001f3af"  # 🎯
    if "GOLF" in sk or "TGL" in sk or "PGA" in sk or "RYDER" in sk:
        return "\u26f3"  # ⛳
    if "CRICKET" in sk or "IPL" in sk or "WPL" in sk or "T20" in sk or "ODI" in sk or "SSHIELD" in sk:
        return "\U0001f3cf"  # 🏏
    if "CS2" in sk or "CSGO" in sk or "VALORANT" in sk or "LOL" in sk or "DOTA" in sk or "OW" in sk or "R6" in sk or "COD" in sk or "ESPORT" in sk:
        return "\U0001f3ae"  # 🎮
    if "BEAST" in sk or "FANATICS" in sk:
        return "\U0001f3ac"  # 🎬
    # Soccer — anything left with league-like tickers
    return "\u26bd"  # ⚽


def _is_ended(expiration_time: str) -> bool:
    """Check if a game has ended based on expected_expiration_time."""
    if not expiration_time:
        return False
    try:
        et = datetime.fromisoformat(expiration_time.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) > et
    except (ValueError, TypeError):
        return False


def _format_game_time(commence_time: str) -> str:
    """Format game time for display. Shows the event date from the ticker."""
    if not commence_time:
        return "TBD"
    return format_game_time(commence_time)


def _earliest_market_time(m: dict) -> str:
    """Return the earlier of close_time / expected_expiration_time as an ISO string.

    Different market types use these fields differently — some sports set
    close_time to today (game start) but expected_expiration_time to a
    tournament end date weeks away, and others do the reverse. The sooner
    of the two is always the actionable deadline for the user.
    """
    ct = m.get("close_time") or ""
    et = m.get("expected_expiration_time") or ""
    if not ct:
        return et
    if not et:
        return ct
    try:
        ct_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        et_dt = datetime.fromisoformat(et.replace("Z", "+00:00"))
        return ct if ct_dt <= et_dt else et
    except (ValueError, TypeError):
        return ct or et


# ── /kalshi Browse Views (category → series → markets → bet) ──────────

SERIES_PER_PAGE = 25
BROWSE_MARKETS_PER_PAGE = 20


class KalshiTopView(discord.ui.View):
    """Landing page: category dropdown using Kalshi's own category taxonomy."""

    def __init__(self, browse_data: dict[str, list[dict]], timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.browse_data = browse_data
        self.message: discord.Message | None = None

        options: list[discord.SelectOption] = []
        for category in sorted(browse_data.keys()):
            series_list = browse_data[category]
            series_count = len(series_list)
            market_count = sum(s["market_count"] for s in series_list)
            label = category[:100]
            desc = f"{series_count} series · {market_count} markets"[:100]
            options.append(discord.SelectOption(label=label, value=category, description=desc))

        if options:
            self.add_item(KalshiCategorySelect(options[:25]))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Kalshi Markets", color=discord.Color.blue())
        if not self.browse_data:
            embed.description = "No open markets right now."
            return embed
        lines = []
        for category in sorted(self.browse_data.keys()):
            series_list = self.browse_data[category]
            series_count = len(series_list)
            market_count = sum(s["market_count"] for s in series_list)
            lines.append(f"**{category}** — {series_count} series, {market_count} markets")
        embed.description = "\n".join(lines)
        embed.set_footer(text="Select a category below")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiCategorySelect(discord.ui.Select["KalshiTopView"]):
    def __init__(self, options: list[discord.SelectOption]) -> None:
        super().__init__(placeholder="Select a category...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        category = self.values[0]
        view = self.view
        series_list = view.browse_data.get(category, [])
        await interaction.response.defer()
        series_view = KalshiSeriesView(category, series_list, view.browse_data)
        embed = series_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=series_view)


class KalshiSeriesView(discord.ui.View):
    """Shows series within a chosen category."""

    def __init__(
        self,
        category: str,
        series_list: list[dict],
        browse_data: dict[str, list[dict]],
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.category = category
        self.series_list = series_list
        self.browse_data = browse_data
        self.page = page
        self.message: discord.Message | None = None
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self.series_list) + SERIES_PER_PAGE - 1) // SERIES_PER_PAGE)
        start = self.page * SERIES_PER_PAGE
        page_series = self.series_list[start:start + SERIES_PER_PAGE]

        options: list[discord.SelectOption] = []
        for s in page_series:
            label = s["label"][:100]
            count = s["market_count"]
            desc = f"{count} market{'s' if count != 1 else ''}"[:100]
            options.append(discord.SelectOption(label=label, value=s["ticker"], description=desc))

        if options:
            self.add_item(KalshiSeriesSelect(options, self.series_list))

        if total_pages > 1:
            if self.page > 0:
                self.add_item(KalshiSeriesPageButton("prev", self.page - 1, row=1))
            if self.page < total_pages - 1:
                self.add_item(KalshiSeriesPageButton("next", self.page + 1, row=1))

        self.add_item(KalshiBackToCategoriesButton(self.browse_data, row=2))

    def build_embed(self) -> discord.Embed:
        total_pages = max(1, (len(self.series_list) + SERIES_PER_PAGE - 1) // SERIES_PER_PAGE)
        start = self.page * SERIES_PER_PAGE
        page_series = self.series_list[start:start + SERIES_PER_PAGE]

        embed = discord.Embed(title=self.category, color=discord.Color.blue())
        if not page_series:
            embed.description = "No series in this category."
            return embed

        lines = []
        for s in page_series:
            count = s["market_count"]
            lines.append(f"**{s['label']}** — {count} market{'s' if count != 1 else ''}")
        embed.description = "\n".join(lines)

        if total_pages > 1:
            embed.set_footer(text=f"Page {self.page + 1}/{total_pages} · Select a series below")
        else:
            embed.set_footer(text="Select a series below")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiSeriesSelect(discord.ui.Select["KalshiSeriesView"]):
    def __init__(self, options: list[discord.SelectOption], series_list: list[dict]) -> None:
        super().__init__(placeholder="Select a series...", options=options, row=0)
        self._series_map = {s["ticker"]: s for s in series_list}

    async def callback(self, interaction: discord.Interaction) -> None:
        ticker = self.values[0]
        series = self._series_map.get(ticker)
        if not series:
            await interaction.response.send_message("Series not found.", ephemeral=True)
            return
        view = self.view
        await interaction.response.defer()
        markets_view = KalshiMarketsView(series, view.category, view.browse_data, series_page=view.page)
        embed = markets_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=markets_view)


class KalshiSeriesPageButton(discord.ui.Button["KalshiSeriesView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class KalshiBackToCategoriesButton(discord.ui.Button["KalshiSeriesView"]):
    def __init__(self, browse_data: dict, row: int) -> None:
        super().__init__(
            label="Back", style=discord.ButtonStyle.secondary,
            emoji="\u25c0\ufe0f", row=row,
        )
        self._browse_data = browse_data

    async def callback(self, interaction: discord.Interaction) -> None:
        top_view = KalshiTopView(self._browse_data)
        embed = top_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=top_view)


_NUMBER_RE = re.compile(r'\b\d+\.?\d*\b')


def _prop_stem(title: str) -> str:
    return _NUMBER_RE.sub("#", title).strip()


def _group_markets_by_prop(markets: list[dict]) -> list[dict]:
    """Group markets that belong to the same game/event into a single entry.

    Primary key is event_ticker — all outcomes for the same game (e.g.
    home win / away win / tie, or all spread lines) share one event_ticker.
    Falls back to ticker (always unique) so ungroupable markets stay single.

    Returns a list of items, each either:
      {"type": "single", "market": {...}}
      {"type": "group", "label": str, "subtitle": str, "markets": [...], "count": int}
    """
    from collections import OrderedDict
    event_map: dict[str, list[dict]] = OrderedDict()
    for m in markets:
        key = m.get("event_ticker") or m.get("ticker", "")
        event_map.setdefault(key, []).append(m)

    result: list[dict] = []
    for key, group in event_map.items():
        if len(group) == 1:
            result.append({"type": "single", "market": group[0]})
        else:
            group_sorted = sorted(
                group,
                key=lambda m: _earliest_market_time(m) or "9999"
            )
            # Game title (shared across the group)
            game_title = group_sorted[0].get("title") or key
            # Collect sub-titles to distinguish threshold vs outcome groups
            sub_titles = [m.get("yes_sub_title") or "" for m in group_sorted]
            has_numbers = any(_NUMBER_RE.search(s) for s in sub_titles if s)
            if has_numbers:
                # Threshold variants — show numeric range (e.g. "Points Over 10.5–15.5")
                nums_per = [_NUMBER_RE.findall(s) for s in sub_titles if s]
                all_nums = [float(n[0]) for n in nums_per if n]
                if all_nums:
                    lo, hi = min(all_nums), max(all_nums)
                    first_sub = next((s for s in sub_titles if _NUMBER_RE.search(s)), "")
                    label = _NUMBER_RE.sub("#", first_sub, count=1).replace("#", f"{lo}–{hi}", 1)
                else:
                    label = game_title
                subtitle = f"{len(group_sorted)} thresholds"
            else:
                # Game outcomes (win/loss/tie, team names, etc.)
                label = game_title
                subs = [s for s in sub_titles[:3] if s]
                subtitle = " · ".join(subs) if subs else f"{len(group_sorted)} options"
            result.append({
                "type": "group",
                "label": label,
                "subtitle": subtitle,
                "markets": group_sorted,
                "count": len(group_sorted),
            })
    return result


def _get_item_time_key(item: dict) -> str:
    """Get a canonical minute-precision game-time key for a grouped display item."""
    if item["type"] == "single":
        t = _earliest_market_time(item["market"])
    else:
        t = _earliest_market_time(item["markets"][0])
    if not t:
        return "9999"
    try:
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M")
    except (ValueError, TypeError):
        return t[:16]


def _group_for_display(markets: list[dict]) -> list[dict]:
    """Wrap _group_markets_by_prop with a game-level bucket layer.

    If markets span more than one distinct close_time (i.e. multiple games),
    returns game_bucket items so the UI can show one entry per game first.
    Otherwise returns prop-grouped items directly (existing behaviour).

    game_bucket: {"type": "game_bucket", "time_str": str, "markets": [raw...], "count": int}
    """
    prop_grouped = _group_markets_by_prop(markets)

    # Collect ordered unique game-time keys
    time_keys_ordered: list[str] = []
    seen: set[str] = set()
    for item in prop_grouped:
        tk = _get_item_time_key(item)
        if tk not in seen:
            seen.add(tk)
            time_keys_ordered.append(tk)

    if len(time_keys_ordered) <= 1:
        return prop_grouped  # Single game — no extra bucketing needed

    # Group prop items and raw markets by game time
    bucket_prop_count: dict[str, int] = {tk: 0 for tk in time_keys_ordered}
    bucket_raw_markets: dict[str, list[dict]] = {tk: [] for tk in time_keys_ordered}

    for item in prop_grouped:
        tk = _get_item_time_key(item)
        bucket_prop_count[tk] += 1
        if item["type"] == "single":
            bucket_raw_markets[tk].append(item["market"])
        else:
            bucket_raw_markets[tk].extend(item["markets"])

    result = []
    for tk in time_keys_ordered:
        raw = bucket_raw_markets[tk]
        exp = _earliest_market_time(raw[0]) if raw else None
        time_str = _format_game_time(exp) if exp else tk
        result.append({
            "type": "game_bucket",
            "time_str": time_str,
            "markets": raw,
            "count": bucket_prop_count[tk],
        })
    return result


class KalshiMarketsView(discord.ui.View):
    """Shows markets within a chosen series, with prop grouping."""

    def __init__(
        self,
        series: dict,
        category: str,
        browse_data: dict[str, list[dict]],
        series_page: int = 0,
        page: int = 0,
        parent_series: dict | None = None,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.series = series
        self.category = category
        self.browse_data = browse_data
        self.series_page = series_page
        self.page = page
        self.parent_series = parent_series
        self.message: discord.Message | None = None
        self._grouped = _group_for_display(series.get("markets", []))
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        grouped = self._grouped
        total_pages = max(1, (len(grouped) + BROWSE_MARKETS_PER_PAGE - 1) // BROWSE_MARKETS_PER_PAGE)
        start = self.page * BROWSE_MARKETS_PER_PAGE
        page_items = grouped[start:start + BROWSE_MARKETS_PER_PAGE]

        options: list[discord.SelectOption] = []
        for global_idx, item in enumerate(grouped[start:start + BROWSE_MARKETS_PER_PAGE], start=start):
            if item["type"] == "game_bucket":
                label = item["time_str"][:100]
                n = item["count"]
                desc = f"{n} market group{'s' if n != 1 else ''}"[:100]
                options.append(discord.SelectOption(
                    label=label, value=f"gb:{global_idx}",
                    description=desc, emoji="🏟️",
                ))
            elif item["type"] == "single":
                m = item["market"]
                ticker = m.get("ticker", "")
                title = m.get("yes_sub_title") or m.get("title") or ticker
                label = title[:100]
                yes_am, no_am = _market_odds_str(m)
                desc = f"YES {yes_am} / NO {no_am}"[:100]
                options.append(discord.SelectOption(label=label, value=f"m:{ticker}", description=desc))
            else:
                label = item["label"][:100]
                desc = item.get("subtitle", f"{item['count']} options")[:100]
                options.append(discord.SelectOption(
                    label=label, value=f"g:{global_idx}",
                    description=desc, emoji="📊",
                ))

        if options:
            self.add_item(KalshiMarketsSelect(
                options, self._grouped, self.series, self.category,
                self.browse_data, self.series_page, self.page,
            ))

        if total_pages > 1:
            if self.page > 0:
                self.add_item(KalshiMarketsPageButton("prev", self.page - 1, row=1))
            if self.page < total_pages - 1:
                self.add_item(KalshiMarketsPageButton("next", self.page + 1, row=1))

        self.add_item(KalshiBackToSeriesButton(
            self.category, self.browse_data, self.series_page, row=2,
            parent_series=self.parent_series,
        ))

    def build_embed(self) -> discord.Embed:
        grouped = self._grouped
        total_pages = max(1, (len(grouped) + BROWSE_MARKETS_PER_PAGE - 1) // BROWSE_MARKETS_PER_PAGE)
        start = self.page * BROWSE_MARKETS_PER_PAGE
        page_items = grouped[start:start + BROWSE_MARKETS_PER_PAGE]

        embed = discord.Embed(title=self.series["label"], color=discord.Color.blue())
        if not page_items:
            embed.description = "No markets in this series."
            return embed

        lines = []
        for item in page_items:
            if item["type"] == "game_bucket":
                n = item["count"]
                lines.append(f"🏟️ **{item['time_str']}** · {n} market group{'s' if n != 1 else ''}")
            elif item["type"] == "single":
                m = item["market"]
                title = m.get("yes_sub_title") or m.get("title") or "?"
                exp = _earliest_market_time(m)
                time_str = _format_game_time(exp) if exp else "TBD"
                yes_am, no_am = _market_odds_str(m)
                lines.append(f"**{title}**\n{time_str} · YES {yes_am} / NO {no_am}")
            else:
                first_m = item["markets"][0]
                exp = _earliest_market_time(first_m)
                time_str = _format_game_time(exp) if exp else "TBD"
                subtitle = item.get("subtitle", f"{item['count']} options")
                lines.append(f"📊 **{item['label']}** · {time_str} · {subtitle}")

        embed.description = "\n\n".join(lines)
        footer = f"Page {self.page + 1}/{total_pages} · " if total_pages > 1 else ""
        embed.set_footer(text=f"{footer}Select a market to bet")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiMarketsSelect(discord.ui.Select["KalshiMarketsView"]):
    def __init__(
        self,
        options: list[discord.SelectOption],
        grouped: list[dict],
        series: dict,
        category: str,
        browse_data: dict,
        series_page: int,
        markets_page: int,
    ) -> None:
        super().__init__(placeholder="Select a market to bet on...", options=options, row=0)
        self._grouped = grouped
        self._series = series
        self._category = category
        self._browse_data = browse_data
        self._series_page = series_page
        self._markets_page = markets_page

    async def callback(self, interaction: discord.Interaction) -> None:
        value = self.values[0]
        view = self.view
        parent_series = getattr(view, "parent_series", None) if view is not None else None
        await interaction.response.defer()

        if value.startswith("gb:"):
            global_idx = int(value[3:])
            bucket = self._grouped[global_idx]
            game_series = dict(self._series)
            game_series["label"] = f"{self._series.get('label', '')} — {bucket['time_str']}"
            game_series["markets"] = bucket["markets"]
            sub_view = KalshiMarketsView(
                game_series, self._category, self._browse_data,
                series_page=self._series_page, page=0,
                parent_series=self._series,
            )
            embed = sub_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=sub_view)

        elif value.startswith("m:"):
            ticker = value[2:]
            # Find the market in grouped items
            market = None
            for item in self._grouped:
                if item["type"] == "single" and item["market"].get("ticker") == ticker:
                    market = item["market"]
                    break
            if not market:
                await interaction.followup.send("Market not found.", ephemeral=True)
                return
            bet_view = KalshiMarketBetView(
                market, self._series, self._category, self._browse_data,
                series_page=self._series_page, markets_page=self._markets_page,
                parent_series=parent_series,
            )
            embed = bet_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=bet_view)

        elif value.startswith("g:"):
            global_idx = int(value[2:])
            item = self._grouped[global_idx]
            threshold_view = KalshiThresholdView(
                item["markets"], self._series, self._category,
                self._browse_data, self._series_page, self._markets_page,
                parent_series=parent_series,
            )
            embed = threshold_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=threshold_view)


class KalshiMarketsPageButton(discord.ui.Button["KalshiMarketsView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class KalshiBackToSeriesButton(discord.ui.Button["KalshiMarketsView"]):
    def __init__(
        self, category: str, browse_data: dict, series_page: int, row: int,
        parent_series: dict | None = None,
    ) -> None:
        super().__init__(
            label="Back", style=discord.ButtonStyle.secondary,
            emoji="\u25c0\ufe0f", row=row,
        )
        self._category = category
        self._browse_data = browse_data
        self._series_page = series_page
        self._parent_series = parent_series

    async def callback(self, interaction: discord.Interaction) -> None:
        if self._parent_series is not None:
            # We're in a game sub-view — go back to the game list
            parent_view = KalshiMarketsView(
                self._parent_series, self._category, self._browse_data,
                series_page=self._series_page, page=0,
            )
            embed = parent_view.build_embed()
            await interaction.response.edit_message(embed=embed, view=parent_view)
        else:
            series_list = self._browse_data.get(self._category, [])
            series_view = KalshiSeriesView(
                self._category, series_list, self._browse_data, page=self._series_page
            )
            embed = series_view.build_embed()
            await interaction.response.edit_message(embed=embed, view=series_view)


class KalshiMarketBetView(discord.ui.View):
    """YES / NO buttons for a single Kalshi market in the /kalshi browse flow."""

    def __init__(
        self,
        market: dict,
        series: dict,
        category: str,
        browse_data: dict[str, list[dict]],
        series_page: int = 0,
        markets_page: int = 0,
        parent_series: dict | None = None,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.market = market
        yes_am, no_am = _market_odds_str(market)
        self.add_item(RawMarketPickButton("yes", f"YES  {yes_am}", market, row=0))
        self.add_item(RawMarketPickButton("no",  f"NO   {no_am}",  market, row=0))
        self.add_item(KalshiMarketBackButton(
            series, category, browse_data, series_page, markets_page, row=1,
            parent_series=parent_series,
        ))

    def build_embed(self) -> discord.Embed:
        m = self.market
        title = m.get("title") or "?"
        yes_sub = m.get("yes_sub_title") or ""
        exp = _earliest_market_time(m)
        time_str = _format_game_time(exp) if exp else "TBD"
        yes_am, no_am = _market_odds_str(m)
        embed = discord.Embed(title=title, color=discord.Color.blue())
        if yes_sub:
            embed.add_field(name="YES resolves if", value=yes_sub, inline=False)
        embed.add_field(name="Closes", value=time_str, inline=True)
        embed.add_field(name="YES",    value=yes_am,   inline=True)
        embed.add_field(name="NO",     value=no_am,    inline=True)
        embed.set_footer(text="Pick YES or NO, then enter your wager")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if hasattr(self, "message") and self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiMarketBackButton(discord.ui.Button["KalshiMarketBetView"]):
    def __init__(
        self, series: dict, category: str, browse_data: dict,
        series_page: int, markets_page: int, row: int,
        parent_series: dict | None = None,
    ) -> None:
        super().__init__(
            label="Back", style=discord.ButtonStyle.secondary,
            emoji="\u25c0\ufe0f", row=row,
        )
        self._series = series
        self._category = category
        self._browse_data = browse_data
        self._series_page = series_page
        self._markets_page = markets_page
        self._parent_series = parent_series

    async def callback(self, interaction: discord.Interaction) -> None:
        markets_view = KalshiMarketsView(
            self._series, self._category, self._browse_data,
            series_page=self._series_page, page=self._markets_page,
            parent_series=self._parent_series,
        )
        embed = markets_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=markets_view)


# ── Threshold View (grouped prop markets) ─────────────────────────────

THRESHOLD_PER_PAGE = 20


class KalshiThresholdView(discord.ui.View):
    """Shows individual threshold options within a grouped prop market."""

    def __init__(
        self,
        markets: list[dict],
        series: dict,
        category: str,
        browse_data: dict[str, list[dict]],
        series_page: int = 0,
        markets_page: int = 0,
        page: int = 0,
        parent_series: dict | None = None,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.markets = markets
        self.series = series
        self.category = category
        self.browse_data = browse_data
        self.series_page = series_page
        self.markets_page = markets_page
        self.page = page
        self.parent_series = parent_series
        self.message: discord.Message | None = None
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self.markets) + THRESHOLD_PER_PAGE - 1) // THRESHOLD_PER_PAGE)
        start = self.page * THRESHOLD_PER_PAGE
        page_markets = self.markets[start:start + THRESHOLD_PER_PAGE]

        options: list[discord.SelectOption] = []
        for m in page_markets:
            ticker = m.get("ticker", "")
            title = m.get("yes_sub_title") or m.get("title") or ticker
            label = title[:100]
            yes_am, no_am = _market_odds_str(m)
            desc = f"YES {yes_am} / NO {no_am}"[:100]
            options.append(discord.SelectOption(label=label, value=ticker, description=desc))

        if options:
            self.add_item(KalshiThresholdSelect(
                options, self.markets, self.series, self.category,
                self.browse_data, self.series_page, self.markets_page,
                parent_series=self.parent_series,
            ))

        if total_pages > 1:
            if self.page > 0:
                self.add_item(KalshiThresholdPageButton("prev", self.page - 1, row=1))
            if self.page < total_pages - 1:
                self.add_item(KalshiThresholdPageButton("next", self.page + 1, row=1))

        self.add_item(KalshiThresholdBackButton(
            self.series, self.category, self.browse_data,
            self.series_page, self.markets_page, row=2,
            parent_series=self.parent_series,
        ))

    def build_embed(self) -> discord.Embed:
        total_pages = max(1, (len(self.markets) + THRESHOLD_PER_PAGE - 1) // THRESHOLD_PER_PAGE)
        start = self.page * THRESHOLD_PER_PAGE
        page_markets = self.markets[start:start + THRESHOLD_PER_PAGE]

        header = self.markets[0].get("title") or self.markets[0].get("yes_sub_title") or "?"
        embed = discord.Embed(title=header, color=discord.Color.blue())

        if not page_markets:
            embed.description = "No thresholds found."
            return embed

        lines = []
        for m in page_markets:
            title = m.get("yes_sub_title") or m.get("title") or "?"
            yes_am, no_am = _market_odds_str(m)
            lines.append(f"**{title}** · YES {yes_am} / NO {no_am}")
        embed.description = "\n".join(lines)

        footer = f"Page {self.page + 1}/{total_pages} · " if total_pages > 1 else ""
        embed.set_footer(text=f"{footer}Select a threshold to bet")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiThresholdSelect(discord.ui.Select["KalshiThresholdView"]):
    def __init__(
        self,
        options: list[discord.SelectOption],
        markets: list[dict],
        series: dict,
        category: str,
        browse_data: dict,
        series_page: int,
        markets_page: int,
        parent_series: dict | None = None,
    ) -> None:
        super().__init__(placeholder="Select a threshold to bet on...", options=options, row=0)
        self._market_map = {m.get("ticker", ""): m for m in markets}
        self._series = series
        self._category = category
        self._browse_data = browse_data
        self._series_page = series_page
        self._markets_page = markets_page
        self._parent_series = parent_series

    async def callback(self, interaction: discord.Interaction) -> None:
        ticker = self.values[0]
        market = self._market_map.get(ticker)
        if not market:
            await interaction.response.send_message("Market not found.", ephemeral=True)
            return
        await interaction.response.defer()
        bet_view = KalshiMarketBetView(
            market, self._series, self._category, self._browse_data,
            series_page=self._series_page, markets_page=self._markets_page,
            parent_series=self._parent_series,
        )
        embed = bet_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=bet_view)


class KalshiThresholdPageButton(discord.ui.Button["KalshiThresholdView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class KalshiThresholdBackButton(discord.ui.Button["KalshiThresholdView"]):
    def __init__(
        self, series: dict, category: str, browse_data: dict,
        series_page: int, markets_page: int, row: int,
        parent_series: dict | None = None,
    ) -> None:
        super().__init__(
            label="Back", style=discord.ButtonStyle.secondary,
            emoji="\u25c0\ufe0f", row=row,
        )
        self._series = series
        self._category = category
        self._browse_data = browse_data
        self._series_page = series_page
        self._markets_page = markets_page
        self._parent_series = parent_series

    async def callback(self, interaction: discord.Interaction) -> None:
        markets_view = KalshiMarketsView(
            self._series, self._category, self._browse_data,
            series_page=self._series_page, page=self._markets_page,
            parent_series=self._parent_series,
        )
        embed = markets_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=markets_view)


# ── Helper functions ───────────────────────────────────────────────────


def _resolve_kalshi_bet(
    pick_key: str, kalshi_markets: dict, odds_entry: dict
) -> tuple[str | None, str]:
    """Map a sports-style pick to a Kalshi market_ticker and yes/no pick."""
    if pick_key == "home":
        m = kalshi_markets.get("home")
        return (m["ticker"], "yes") if m else (None, "yes")
    elif pick_key == "away":
        m = kalshi_markets.get("away")
        return (m["ticker"], "yes") if m else (None, "yes")
    elif pick_key in ("spread_home", "spread_away", "over", "under"):
        ticker = odds_entry.get("_market_ticker")
        if ticker:
            kalshi_pick = odds_entry.get("_kalshi_pick", "yes")
            return (ticker, kalshi_pick)
        if pick_key == "spread_home":
            m = kalshi_markets.get("home")
        elif pick_key == "spread_away":
            m = kalshi_markets.get("away")
        else:
            m = kalshi_markets.get("home")
        return (m["ticker"], "yes") if m else (None, "yes")
    return (None, "yes")


def _build_pick_display(pick_key: str, home: str, away: str, odds_entry: dict) -> str:
    """Build a human-readable pick display string."""
    point = odds_entry.get("point")
    if pick_key == "home":
        return f"{home} ML"
    elif pick_key == "away":
        return f"{away} ML"
    elif pick_key == "spread_home":
        return f"{home} {point:+g}" if point is not None else f"{home} Spread"
    elif pick_key == "spread_away":
        return f"{away} {point:+g}" if point is not None else f"{away} Spread"
    elif pick_key == "over":
        return f"Over {point:g}" if point is not None else "Over"
    elif pick_key == "under":
        return f"Under {point:g}" if point is not None else "Under"
    return pick_key.capitalize()


# ── Games List View (for /live) ───────────────────────────────────────
# Display-only view showing live game scores with pagination.

GAMES_PER_PAGE = 10


class GamesListView(discord.ui.View):
    """Paginated live game scores display."""

    def __init__(
        self,
        all_games: list[dict],
        title: str,
        is_live: bool = False,
        scores: dict[str, dict] | None = None,
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.all_games = all_games
        self.title = title
        self.is_live_mode = is_live
        self.scores = scores or {}
        self.page = page
        self.message: discord.Message | None = None

        total_pages = max(1, (len(all_games) + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        if total_pages > 1:
            if page > 0:
                self.add_item(GamesPageButton("prev", page - 1, row=0))
            if page < total_pages - 1:
                self.add_item(GamesPageButton("next", page + 1, row=0))

    def _find_score(self, game: dict) -> dict | None:
        if not self.scores:
            return None
        home = game.get("home_team", "").lower()
        away = game.get("away_team", "").lower()
        for score_data in self.scores.values():
            s_home = (score_data.get("home_team") or "").lower()
            s_away = (score_data.get("away_team") or "").lower()
            if (home in s_home or s_home in home) and (away in s_away or s_away in away):
                return score_data
        return None

    def build_embed(self) -> discord.Embed:
        total = len(self.all_games)
        total_pages = max(1, (total + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        start = self.page * GAMES_PER_PAGE
        page_games = self.all_games[start:start + GAMES_PER_PAGE]

        color = discord.Color.red() if self.is_live_mode else discord.Color.blue()
        embed = discord.Embed(title=self.title, color=color)

        if not page_games:
            embed.description = "No live games right now." if self.is_live_mode else "No upcoming games."
            return embed

        lines = []
        for g in page_games:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            sport = g.get("sport_title", "")
            score = self._find_score(g) if self.is_live_mode else None
            if score and score.get("started") and score.get("home_score") is not None:
                score_str = f"**{away}** {score['away_score']} - {score['home_score']} **{home}**"
                if score.get("completed"):
                    score_str += "  (Final)"
                else:
                    score_str = f"\U0001f534 {score_str}"
                time_parts = []
                if g.get("commence_time"):
                    time_parts.append(f"Started {format_game_time(g['commence_time'])}")
                if g.get("expiration_time"):
                    time_parts.append(f"Ends ~{format_game_time(g['expiration_time'])}")
                detail = sport
                if time_parts:
                    detail += f"\n{' · '.join(time_parts)}"
                lines.append(f"{score_str}\n{detail}")
            else:
                time_str = _format_game_time(g.get("commence_time", ""))
                lines.append(f"**{away}** @ **{home}**\n{sport} · {time_str}")

        embed.description = "\n\n".join(lines)
        parts = []
        if total_pages > 1:
            parts.append(f"Page {self.page + 1}/{total_pages}")
        parts.append(f"{total} game{'s' if total != 1 else ''}")
        embed.set_footer(text=" · ".join(parts))
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class GamesPageButton(discord.ui.Button["GamesListView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        new_view = GamesListView(
            all_games=view.all_games,
            title=view.title,
            is_live=view.is_live_mode,
            scores=view.scores,
            page=self.target_page,
        )
        embed = new_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=new_view)
        except discord.NotFound:
            pass


# ── Market List View (for /games) ─────────────────────────────────────
# Shows raw Kalshi markets sorted by closest expiry with YES/NO betting.

MARKETS_PER_PAGE = 10


def _market_odds_str(m: dict) -> tuple[str, str]:
    """Return (yes_american, no_american) strings for display, or ("?", "?")."""
    yes_ask = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)
    if 0 < yes_ask < 1:
        yes_am = format_american(decimal_to_american(round(1.0 / yes_ask, 3)))
        no_am  = format_american(decimal_to_american(round(1.0 / (1.0 - yes_ask), 3)))
        return yes_am, no_am
    return "?", "?"


def _market_yes_pct(m: dict) -> int | None:
    """Return the implied YES win probability as a rounded integer percent, or None."""
    yes_ask = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)
    if 0 < yes_ask < 1:
        return round(yes_ask * 100)
    return None


def _series_label(series_ticker: str) -> str:
    """Return a human-readable sport label for a series ticker, or ''."""
    for info in SPORTS.values():
        if series_ticker in info["series"].values():
            return info["label"]
    return ""


class MarketListView(discord.ui.View):
    """Paginated list of raw Kalshi markets sorted by soonest expiry."""

    def __init__(self, markets: list[dict], page: int = 0, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.markets = markets
        self.page = page
        self.message: discord.Message | None = None
        self._grouped = _group_markets_by_prop(markets)
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self._grouped) + MARKETS_PER_PAGE - 1) // MARKETS_PER_PAGE)
        start = self.page * MARKETS_PER_PAGE
        page_items = self._grouped[start:start + MARKETS_PER_PAGE]

        if page_items:
            self.add_item(MarketGroupedDropdown(page_items, start, self, row=0))
        if self.page > 0:
            self.add_item(MarketPageButton("prev", self.page - 1, row=1))
        if self.page < total_pages - 1:
            self.add_item(MarketPageButton("next", self.page + 1, row=1))

    def build_embed(self) -> discord.Embed:
        total = len(self._grouped)
        total_pages = max(1, (total + MARKETS_PER_PAGE - 1) // MARKETS_PER_PAGE)
        start = self.page * MARKETS_PER_PAGE
        page_items = self._grouped[start:start + MARKETS_PER_PAGE]

        embed = discord.Embed(title="\U0001f4ca Open Markets", color=discord.Color.blue())
        if not page_items:
            embed.description = "No open markets right now."
            return embed

        lines = []
        for item in page_items:
            if item["type"] == "single":
                m = item["market"]
                title = m.get("title") or "?"
                exp = _earliest_market_time(m)
                sport = _series_label(m.get("series_ticker") or "")
                time_str = _format_game_time(exp) if exp else "TBD"
                header = f"**{title}**"
                if sport:
                    header += f"  _{sport}_"
                pct = _market_yes_pct(m)
                if pct is not None:
                    odds_str = f"{pct}% YES / {100 - pct}% NO"
                else:
                    yes_am, no_am = _market_odds_str(m)
                    odds_str = f"YES {yes_am} / NO {no_am}"
                lines.append(f"{header}\n{time_str} · {odds_str}")
            else:
                label = item["label"]
                count = item["count"]
                first_m = item["markets"][0]
                exp = _earliest_market_time(first_m)
                sport = _series_label(first_m.get("series_ticker") or "")
                time_str = _format_game_time(exp) if exp else "TBD"
                header = f"\U0001f4ca **{label}**"
                if sport:
                    header += f"  _{sport}_"
                # Build per-outcome probability string (e.g. "Lakers 65% · Warriors 35%")
                parts = []
                for mkt in item["markets"][:3]:
                    sub = mkt.get("yes_sub_title") or ""
                    pct = _market_yes_pct(mkt)
                    if sub and pct is not None:
                        parts.append(f"{sub} {pct}%")
                    elif sub:
                        parts.append(sub)
                subtitle = " · ".join(parts) if parts else item.get("subtitle", f"{count} options")
                lines.append(f"{header}\n{time_str} · {subtitle}")

        embed.description = "\n\n".join(lines)
        embed.set_footer(text=f"Page {self.page + 1}/{total_pages} · Select a market below to bet")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class MarketGroupedDropdown(discord.ui.Select["MarketListView"]):
    """Dropdown for MarketListView supporting both singles and grouped prop markets."""

    def __init__(self, page_items: list[dict], start: int, list_view: "MarketListView", row: int = 0) -> None:
        self._list_view = list_view
        # Both maps keyed by the short position-based value string (safe <= 6 chars)
        self._market_map: dict[str, dict] = {}
        self._group_map: dict[str, list[dict]] = {}
        options: list[discord.SelectOption] = []
        for i, item in enumerate(page_items[:25]):
            if item["type"] == "single":
                m = item["market"]
                ticker = m.get("ticker") or ""
                title = m.get("yes_sub_title") or m.get("title") or ticker
                label = title[:100]
                pct = _market_yes_pct(m)
                if pct is not None:
                    desc = f"{pct}% YES / {100 - pct}% NO"
                else:
                    yes_am, no_am = _market_odds_str(m)
                    desc = f"YES {yes_am} / NO {no_am}"
                key = f"m:{start + i}"  # position-based; avoids long ticker exceeding 100-char limit
                self._market_map[key] = m
                options.append(discord.SelectOption(label=label, value=key, description=desc[:100]))
            else:
                key = f"g:{start + i}"
                label = item["label"][:100]
                parts = []
                for mkt in item["markets"][:3]:
                    sub = mkt.get("yes_sub_title") or ""
                    pct = _market_yes_pct(mkt)
                    if sub and pct is not None:
                        parts.append(f"{sub} {pct}%")
                    elif sub:
                        parts.append(sub)
                desc = (" · ".join(parts) if parts else item.get("subtitle", f"{item['count']} options"))[:100]
                self._group_map[key] = item["markets"]
                options.append(discord.SelectOption(label=label, value=key, description=desc, emoji="\U0001f4ca"))
        super().__init__(placeholder="Select a market to bet on...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        value = self.values[0]
        await interaction.response.defer()
        if value.startswith("m:"):
            market = self._market_map.get(value)
            if not market:
                await interaction.followup.send("Market not found.", ephemeral=True)
                return
            bet_view = RawMarketBetView(market, self._list_view)
            embed = bet_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=bet_view)
        elif value.startswith("g:"):
            group_markets = self._group_map.get(value, [])
            if not group_markets:
                await interaction.followup.send("Group not found.", ephemeral=True)
                return
            threshold_view = LiveThresholdView(group_markets, self._list_view)
            embed = threshold_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=threshold_view)


class LiveThresholdView(discord.ui.View):
    """Shows individual threshold options for a grouped market in the /live flow."""

    def __init__(
        self,
        group_markets: list[dict],
        list_view: "MarketListView",
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.markets = group_markets
        self._all_markets = list_view.markets
        self._list_page = list_view.page
        self.page = page
        self.message: discord.Message | None = None
        self._rebuild()

    def _rebuild(self) -> None:
        self.clear_items()
        total_pages = max(1, (len(self.markets) + THRESHOLD_PER_PAGE - 1) // THRESHOLD_PER_PAGE)
        start = self.page * THRESHOLD_PER_PAGE
        page_markets = self.markets[start:start + THRESHOLD_PER_PAGE]

        market_map: dict[str, dict] = {}
        options: list[discord.SelectOption] = []
        for m in page_markets:
            ticker = m.get("ticker", "")
            title = m.get("yes_sub_title") or m.get("title") or ticker
            label = title[:100]
            yes_am, no_am = _market_odds_str(m)
            desc = f"YES {yes_am} / NO {no_am}"[:100]
            market_map[ticker] = m
            options.append(discord.SelectOption(label=label, value=ticker, description=desc))

        if options:
            self.add_item(LiveThresholdSelect(options, market_map, self._all_markets, self._list_page, row=0))

        if total_pages > 1:
            if self.page > 0:
                self.add_item(LiveThresholdPageButton("prev", self.page - 1, row=1))
            if self.page < total_pages - 1:
                self.add_item(LiveThresholdPageButton("next", self.page + 1, row=1))

        self.add_item(RawMarketBackButton(self._all_markets, self._list_page, row=2))

    def build_embed(self) -> discord.Embed:
        total_pages = max(1, (len(self.markets) + THRESHOLD_PER_PAGE - 1) // THRESHOLD_PER_PAGE)
        start = self.page * THRESHOLD_PER_PAGE
        page_markets = self.markets[start:start + THRESHOLD_PER_PAGE]

        header = self.markets[0].get("title") or self.markets[0].get("yes_sub_title") or "?"
        embed = discord.Embed(title=header, color=discord.Color.red())

        if not page_markets:
            embed.description = "No thresholds found."
            return embed

        lines = []
        for m in page_markets:
            title = m.get("yes_sub_title") or m.get("title") or "?"
            yes_am, no_am = _market_odds_str(m)
            lines.append(f"**{title}** · YES {yes_am} / NO {no_am}")
        embed.description = "\n".join(lines)

        footer = f"Page {self.page + 1}/{total_pages} · " if total_pages > 1 else ""
        embed.set_footer(text=f"{footer}Select a threshold to bet")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class LiveThresholdSelect(discord.ui.Select["LiveThresholdView"]):
    def __init__(
        self,
        options: list[discord.SelectOption],
        market_map: dict[str, dict],
        all_markets: list[dict],
        list_page: int,
        row: int = 0,
    ) -> None:
        super().__init__(placeholder="Select a threshold to bet on...", options=options, row=row)
        self._market_map = market_map
        self._all_markets = all_markets
        self._list_page = list_page

    async def callback(self, interaction: discord.Interaction) -> None:
        ticker = self.values[0]
        market = self._market_map.get(ticker)
        if not market:
            await interaction.response.send_message("Market not found.", ephemeral=True)
            return
        await interaction.response.defer()
        list_view = MarketListView(self._all_markets, self._list_page)
        bet_view = RawMarketBetView(market, list_view)
        embed = bet_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=bet_view)


class LiveThresholdPageButton(discord.ui.Button["LiveThresholdView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev" if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class MarketPageButton(discord.ui.Button["MarketListView"]):
    def __init__(self, direction: str, target_page: int, row: int) -> None:
        label = "Prev"  if direction == "prev" else "Next"
        emoji = "\u25c0" if direction == "prev" else "\u25b6"
        super().__init__(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=row)
        self.target_page = target_page

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.page = self.target_page
        view._rebuild()
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class RawMarketBetView(discord.ui.View):
    """YES / NO buttons for a single raw Kalshi market."""

    def __init__(self, market: dict, list_view: MarketListView, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.market   = market

        yes_am, no_am = _market_odds_str(market)
        self.add_item(RawMarketPickButton("yes", f"YES  {yes_am}", market, row=0))
        self.add_item(RawMarketPickButton("no",  f"NO   {no_am}",  market, row=0))
        self.add_item(RawMarketBackButton(list_view.markets, list_view.page, row=1))

    def build_embed(self) -> discord.Embed:
        m       = self.market
        title   = m.get("title") or "?"
        yes_sub = m.get("yes_sub_title") or ""
        exp     = m.get("expected_expiration_time") or m.get("close_time") or ""
        sport   = _series_label(m.get("series_ticker") or "")
        time_str = _format_game_time(exp) if exp else "TBD"
        yes_am, no_am = _market_odds_str(m)

        embed = discord.Embed(title=title, color=discord.Color.blue())
        if sport:
            embed.description = f"_{sport}_"
        if yes_sub:
            embed.add_field(name="YES resolves if", value=yes_sub, inline=False)
        embed.add_field(name="Closes",  value=time_str, inline=True)
        embed.add_field(name="YES",     value=yes_am,   inline=True)
        embed.add_field(name="NO",      value=no_am,    inline=True)
        embed.set_footer(text="Pick YES or NO, then enter your wager")
        return embed


class RawMarketPickButton(discord.ui.Button["RawMarketBetView"]):
    def __init__(self, pick: str, label: str, market: dict, row: int) -> None:
        style = discord.ButtonStyle.success if pick == "yes" else discord.ButtonStyle.danger
        super().__init__(label=label, style=style, row=row)
        self.pick   = pick
        self.market = market

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(RawMarketBetModal(self.market, self.pick))


class RawMarketBackButton(discord.ui.Button["RawMarketBetView"]):
    def __init__(self, markets: list[dict], page: int, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary,
                         emoji="\u25c0\ufe0f", row=row)
        self._markets = markets
        self._page = page

    async def callback(self, interaction: discord.Interaction) -> None:
        list_view = MarketListView(self._markets, self._page)
        embed = list_view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=list_view)
        except discord.NotFound:
            pass


class RawMarketBetModal(discord.ui.Modal, title="Place Bet"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, market: dict, pick: str) -> None:
        super().__init__()
        self.market = market
        self.pick   = pick

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message("Invalid amount — enter a whole number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be positive.", ephemeral=True)
            return

        await interaction.response.defer()

        m    = self.market
        pick = self.pick
        yes_ask = float(m.get("yes_ask_dollars") or m.get("last_price_dollars") or 0)

        if pick == "yes":
            decimal_odds = round(1.0 / yes_ask, 3) if yes_ask > 0 else 2.0
        else:
            no_ask = 1.0 - yes_ask
            decimal_odds = round(1.0 / no_ask, 3) if no_ask > 0 else 2.0

        american = decimal_to_american(decimal_odds)
        close_time = m.get("close_time") or m.get("expected_expiration_time")

        if close_time:
            try:
                ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                if ct <= datetime.now(timezone.utc):
                    await interaction.followup.send("This market has already closed.", ephemeral=True)
                    return
            except (ValueError, TypeError):
                pass

        title    = m.get("title") or "?"
        yes_sub  = m.get("yes_sub_title") or ""
        pick_display = (f"YES — {yes_sub}" if pick == "yes" and yes_sub
                        else ("YES" if pick == "yes" else "NO"))

        bet_id = await betting_service.place_kalshi_bet(
            user_id=interaction.user.id,
            market_ticker=m["ticker"],
            event_ticker=m.get("event_ticker", ""),
            pick=pick,
            amount=amount,
            odds=decimal_odds,
            title=title,
            close_time=close_time,
            pick_display=pick_display,
        )

        if bet_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        payout = round(amount * decimal_odds, 2)
        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID",           value=f"#K{bet_id}",             inline=True)
        embed.add_field(name="Pick",             value=pick_display,               inline=True)
        embed.add_field(name="Wager",            value=f"${amount:.2f}",           inline=True)
        embed.add_field(name="Odds",             value=format_american(american),  inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}",           inline=True)
        embed.add_field(name="Market",           value=title[:1024],               inline=False)
        await interaction.followup.send(embed=embed)


# ── Parlay Views ──────────────────────────────────────────────────────

MAX_PARLAY_LEGS = 10


class KalshiParlayView(discord.ui.View):
    """Multi-step parlay builder using Kalshi markets."""

    def __init__(
        self,
        games: list[dict],
        timeout: float = 300.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.games = games
        self.legs: list[dict] = []
        self.message: discord.Message | None = None
        self._show_game_select()

    def _available_games(self) -> list[dict]:
        """Games not already in the parlay slip."""
        used_events = {leg["event_ticker"] for leg in self.legs}
        return [g for g in self.games if g["id"] not in used_events]

    def _show_game_select(self) -> None:
        self.clear_items()
        available = self._available_games()
        if available:
            self.add_item(KalshiParlayGameSelect(available[:25], row=0))
        if self.legs:
            self.add_item(KalshiParlayViewSlipButton(row=1))
        self.add_item(KalshiParlayCancelButton(row=1))

    def _show_slip(self) -> None:
        self.clear_items()
        if len(self.legs) < MAX_PARLAY_LEGS and self._available_games():
            self.add_item(KalshiParlayAddLegButton(row=0))
        if self.legs:
            self.add_item(KalshiParlayRemoveLegSelect(self.legs, row=1))
        if len(self.legs) >= 2:
            self.add_item(KalshiParlayPlaceButton(row=2))
        self.add_item(KalshiParlayCancelButton(row=2))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Parlay Builder", color=discord.Color.purple())
        if not self.legs:
            embed.description = "Select a game to add your first leg."
            return embed

        total_odds = 1.0
        for i, leg in enumerate(self.legs, 1):
            total_odds *= leg["odds"]
            odds_str = format_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}: {leg['pick_display']}",
                value=f"{leg.get('title', '?')} — {odds_str} ({leg['odds']:.2f}x)",
                inline=False,
            )

        total_odds = round(total_odds, 4)
        embed.add_field(
            name="Combined Odds",
            value=f"**{total_odds:.2f}x** — $100 wins **${round(100 * total_odds):.2f}**",
            inline=False,
        )
        footer_parts = [f"{len(self.legs)} leg{'s' if len(self.legs) != 1 else ''}"]
        if len(self.legs) < 2:
            footer_parts.append("Add at least 2 legs to place")
        embed.set_footer(text=" · ".join(footer_parts))
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiParlayGameSelect(discord.ui.Select["KalshiParlayView"]):
    """Dropdown to pick a game for a parlay leg."""

    def __init__(self, games: list[dict], row: int = 0) -> None:
        self.games_map: dict[str, dict] = {}
        options = []
        for g in games[:25]:
            game_id = g["id"]
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            label = format_matchup(home, away)
            if len(label) > 100:
                label = label[:97] + "..."
            desc = _format_game_time(g.get("commence_time", ""))
            if len(desc) > 100:
                desc = desc[:100]
            self.games_map[game_id] = g
            options.append(discord.SelectOption(label=label, value=game_id, description=desc))
        super().__init__(placeholder="Select a game to add...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        game_id = self.values[0]
        game = self.games_map.get(game_id)
        if not game:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        view = self.view
        if view is None:
            return

        await interaction.response.defer()

        sport_key = game.get("sport_key", "")
        parsed = await kalshi_api.get_game_odds(sport_key, game)

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")

        # Build bet type selection for this game
        bet_view = KalshiParlayBetTypeView(game, parsed, view)
        time_str = _format_game_time(game.get("commence_time", ""))
        embed = discord.Embed(
            title="Add Parlay Leg",
            description=f"**{format_matchup(home, away)}**\n{time_str}\n\nPick a bet type:",
            color=discord.Color.purple(),
        )
        await interaction.edit_original_response(embed=embed, view=bet_view)


class KalshiParlayBetTypeView(discord.ui.View):
    """Bet type buttons for adding a parlay leg."""

    def __init__(self, game: dict, parsed: dict, parlay_view: KalshiParlayView, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.game = game
        self.parlay_view = parlay_view

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")
        fmt = format_american

        row = 0
        if "home" in parsed:
            self.add_item(KalshiParlayBetTypeButton(
                "home", f"Home {fmt(parsed['home']['american'])}",
                game, parsed["home"], parlay_view, row=row,
            ))
        if "away" in parsed:
            self.add_item(KalshiParlayBetTypeButton(
                "away", f"Away {fmt(parsed['away']['american'])}",
                game, parsed["away"], parlay_view, row=row,
            ))

        row = 1
        if "spread_home" in parsed:
            sh = parsed["spread_home"]
            self.add_item(KalshiParlayBetTypeButton(
                "spread_home", f"{home} {sh['point']:+g} ({fmt(sh['american'])})",
                game, sh, parlay_view, row=row,
            ))
        if "spread_away" in parsed:
            sa = parsed["spread_away"]
            self.add_item(KalshiParlayBetTypeButton(
                "spread_away", f"{away} {sa['point']:+g} ({fmt(sa['american'])})",
                game, sa, parlay_view, row=row,
            ))

        row = 2
        if "over" in parsed:
            ov = parsed["over"]
            self.add_item(KalshiParlayBetTypeButton(
                "over", f"Over {ov['point']:g} ({fmt(ov['american'])})",
                game, ov, parlay_view, row=row,
            ))
        if "under" in parsed:
            un = parsed["under"]
            self.add_item(KalshiParlayBetTypeButton(
                "under", f"Under {un['point']:g} ({fmt(un['american'])})",
                game, un, parlay_view, row=row,
            ))

        self.add_item(KalshiParlayBackToGamesButton(parlay_view, row=3))


class KalshiParlayBetTypeButton(discord.ui.Button):
    """Adds a leg to the parlay slip when clicked."""

    def __init__(
        self, pick_key: str, label: str, game: dict, odds_entry: dict,
        parlay_view: KalshiParlayView, row: int,
    ) -> None:
        emoji = PICK_EMOJI.get(pick_key)
        super().__init__(label=label, style=discord.ButtonStyle.primary, emoji=emoji, row=row)
        self.pick_key = pick_key
        self.game = game
        self.odds_entry = odds_entry
        self.parlay_view = parlay_view

    async def callback(self, interaction: discord.Interaction) -> None:
        game = self.game
        odds_entry = self.odds_entry
        home = game.get("home_team", "?")
        away = game.get("away_team", "?")

        kalshi_markets = game.get("_kalshi_markets", {})
        market_ticker, kalshi_pick = _resolve_kalshi_bet(self.pick_key, kalshi_markets, odds_entry)

        if not market_ticker:
            await interaction.response.send_message("Could not find market for this bet.", ephemeral=True)
            return

        pick_display = _build_pick_display(self.pick_key, home, away, odds_entry)
        market = kalshi_markets.get("home") or kalshi_markets.get("away") or {}
        close_time = market.get("close_time") or market.get("expected_expiration_time")
        bet_title = f"{format_matchup(home, away)} ({game.get('sport_title', '')})"

        leg = {
            "market_ticker": market_ticker,
            "event_ticker": game.get("id", ""),
            "pick": kalshi_pick,
            "odds": odds_entry["decimal"],
            "american": odds_entry["american"],
            "title": bet_title,
            "pick_display": pick_display,
            "close_time": close_time,
        }

        pv = self.parlay_view
        pv.legs.append(leg)
        pv._show_slip()
        embed = pv.build_embed()
        await interaction.response.edit_message(embed=embed, view=pv)


class KalshiParlayViewSlipButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="View Slip", style=discord.ButtonStyle.success, emoji="\U0001f4cb", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view._show_slip()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayAddLegButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Add Leg", style=discord.ButtonStyle.success, emoji="\u2795", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view._show_game_select()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayRemoveLegSelect(discord.ui.Select["KalshiParlayView"]):
    def __init__(self, legs: list[dict], row: int) -> None:
        options = []
        for i, leg in enumerate(legs):
            label = leg["pick_display"]
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(label=f"Remove: {label}", value=str(i)))
        super().__init__(placeholder="Remove a leg...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        idx = int(self.values[0])
        if 0 <= idx < len(view.legs):
            view.legs.pop(idx)
        view._show_slip()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayPlaceButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Place Parlay", style=discord.ButtonStyle.success, emoji="\U0001f4b0", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        modal = KalshiParlayAmountModal(view)
        await interaction.response.send_modal(modal)


class KalshiParlayCancelButton(discord.ui.Button["KalshiParlayView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view:
            view.clear_items()
            view.stop()
        embed = discord.Embed(title="Parlay Cancelled", color=discord.Color.orange())
        await interaction.response.edit_message(embed=embed, view=view)


class KalshiParlayBackToGamesButton(discord.ui.Button):
    def __init__(self, parlay_view: KalshiParlayView, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.parlay_view = parlay_view

    async def callback(self, interaction: discord.Interaction) -> None:
        pv = self.parlay_view
        pv._show_game_select()
        embed = pv.build_embed()
        await interaction.response.edit_message(embed=embed, view=pv)


class KalshiParlayAmountModal(discord.ui.Modal, title="Place Parlay"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, parlay_view: KalshiParlayView) -> None:
        super().__init__()
        self.parlay_view = parlay_view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message("Invalid amount — enter a whole number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Bet amount must be positive.", ephemeral=True)
            return

        await interaction.response.defer()

        pv = self.parlay_view
        if len(pv.legs) < 2:
            await interaction.followup.send("Need at least 2 legs for a parlay.", ephemeral=True)
            return

        parlay_id = await betting_service.place_kalshi_parlay(
            user_id=interaction.user.id,
            legs=pv.legs,
            amount=amount,
        )

        if parlay_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        total_odds = 1.0
        for leg in pv.legs:
            total_odds *= leg["odds"]
        total_odds = round(total_odds, 4)
        payout = round(amount * total_odds)

        embed = discord.Embed(title="Parlay Placed!", color=discord.Color.green())
        embed.add_field(name="Parlay ID", value=f"#KP{parlay_id}", inline=True)
        embed.add_field(name="Legs", value=str(len(pv.legs)), inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Combined Odds", value=f"{total_odds:.2f}x", inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        for i, leg in enumerate(pv.legs, 1):
            odds_str = format_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}",
                value=f"{leg['pick_display']} — {odds_str}",
                inline=False,
            )

        pv.clear_items()
        pv.stop()
        await interaction.edit_original_response(embed=embed, view=None)


# ── Cog ───────────────────────────────────────────────────────────────


class HistoryView(discord.ui.View):
    """Paginated view for /myhistory showing resolved bets with stats."""

    PAGE_SIZE = 10

    def __init__(self, user_id: int, stats: dict, total_items: int, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.stats = stats
        self.total_items = total_items
        self.page = 0
        self.total_pages = max(1, (total_items + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.message: discord.Message | None = None
        self._update_buttons()

    def _update_buttons(self) -> None:
        self.prev_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= self.total_pages - 1

    async def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Bet History", color=discord.Color.blue())

        # Stats header
        stats = self.stats
        total = stats.get("total") or 0
        wins = stats.get("wins") or 0
        losses = stats.get("losses") or 0
        pushes = stats.get("pushes") or 0
        total_wagered = stats.get("total_wagered") or 0
        total_payout = stats.get("total_payout") or 0
        profit = total_payout - total_wagered
        win_rate = (wins / total * 100) if total > 0 else 0
        profit_sign = "+" if profit >= 0 else ""

        embed.description = (
            f"**Record:** {wins}W - {losses}L - {pushes}P ({win_rate:.0f}% win rate)\n"
            f"**Wagered:** ${total_wagered:,.2f} · **Profit:** {profit_sign}${profit:,.2f}"
        )

        # Fetch page data
        items = await betting_service.get_user_history(
            self.user_id, page=self.page, page_size=self.PAGE_SIZE
        )

        if not items:
            embed.add_field(name="No bets", value="No resolved bets found.", inline=False)
        else:
            for item in items:
                if item.get("type") == "parlay":
                    self._add_parlay_field(embed, item)
                elif item.get("type") == "kalshi":
                    self._add_kalshi_field(embed, item)
                elif item.get("type") == "kalshi_parlay":
                    self._add_kalshi_parlay_field(embed, item)
                else:
                    self._add_bet_field(embed, item)

        embed.set_footer(text=f"Page {self.page + 1}/{self.total_pages}")
        return embed

    def _add_bet_field(self, embed: discord.Embed, b: dict) -> None:
        status = b["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${b.get('payout', 0):.2f}**"
        elif status == "push":
            icon = "\U0001f535"
            status_text = f"Push — **${b.get('payout', 0):.2f}** refunded"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        is_outright = (b.get("market") or "") == "outrights"
        home = b.get("home_team")
        away = b.get("away_team")
        sport = b.get("sport_title")

        if is_outright:
            matchup = sport or "Futures"
        elif home and away:
            matchup = format_matchup(home, away)
        else:
            matchup = "Unknown"

        sport_line = f"{sport} · " if sport and not is_outright else ""
        pick_label = b["pick"] if is_outright else format_pick_label(b)

        embed.add_field(
            name=f"{icon} Bet #{b['id']} · {status_text}",
            value=(
                f"{sport_line}**{matchup}**\n"
                f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']}x"
            ),
            inline=False,
        )

    def _add_parlay_field(self, embed: discord.Embed, p: dict) -> None:
        status = p["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${p.get('payout', 0):.2f}**"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        leg_lines = []
        for leg in p.get("legs", []):
            ls = leg["status"]
            if ls == "won":
                leg_icon = "\U0001f7e2"
            elif ls == "lost":
                leg_icon = "\U0001f534"
            elif ls == "push":
                leg_icon = "\U0001f535"
            else:
                leg_icon = "\U0001f7e1"

            home = leg.get("home_team") or "?"
            away = leg.get("away_team") or "?"
            pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
            point = leg.get("point")
            if point is not None:
                if leg["pick"] in ("spread_home", "spread_away"):
                    pick_label += f" {point:+g}"
                else:
                    pick_label += f" {point:g}"
            leg_lines.append(f"{leg_icon} {format_matchup(home, away)} — {pick_label} ({leg['odds']:.2f}x)")

        embed.add_field(
            name=f"{icon} Parlay #{p['id']} · {status_text}",
            value=(
                f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                + "\n".join(leg_lines)
            ),
            inline=False,
        )

    def _add_kalshi_field(self, embed: discord.Embed, b: dict) -> None:
        status = b["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${b.get('payout', 0):.2f}**"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        title = b.get("title") or b.get("market_ticker", "Unknown")
        pick_label = b.get("pick_display") or b["pick"].upper()

        embed.add_field(
            name=f"{icon} Kalshi #{b['id']} · {status_text}",
            value=(
                f"**{title}**\n"
                f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']:.2f}x"
            ),
            inline=False,
        )

    def _add_kalshi_parlay_field(self, embed: discord.Embed, p: dict) -> None:
        status = p["status"]
        if status == "won":
            icon = "\U0001f7e2"
            status_text = f"Won — **${p.get('payout', 0):.2f}**"
        else:
            icon = "\U0001f534"
            status_text = "Lost"

        leg_lines = []
        for leg in p.get("legs", []):
            ls = leg["status"]
            if ls == "won":
                leg_icon = "\U0001f7e2"
            elif ls == "lost":
                leg_icon = "\U0001f534"
            else:
                leg_icon = "\U0001f7e1"
            pick_label = leg.get("pick_display") or leg["pick"].upper()
            leg_lines.append(f"{leg_icon} {leg.get('title', '?')} — {pick_label} ({leg['odds']:.2f}x)")

        embed.add_field(
            name=f"{icon} Kalshi Parlay #KP{p['id']} · {status_text}",
            value=(
                f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                + "\n".join(leg_lines)
            ),
            inline=False,
        )

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="\u25c0")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your history.", ephemeral=True)
            return
        self.page = max(0, self.page - 1)
        self._update_buttons()
        embed = await self.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.NotFound:
            pass

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="\u25b6")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your history.", ephemeral=True)
            return
        self.page = min(self.total_pages - 1, self.page + 1)
        self._update_buttons()
        embed = await self.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.NotFound:
            pass

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class KalshiCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.sports_api = SportsAPI()

    async def cog_load(self) -> None:
        log.info("KalshiCog loading — refreshing sports...")
        await kalshi_api.refresh_sports()
        log.info("KalshiCog loaded — %d sports available, starting loops", len(SPORTS))
        self.check_kalshi_results.start()
        self.refresh_discovery.start()
        self.refresh_sports_loop.start()
        self.check_legacy_results.start()
        self.db_maintenance.start()

    async def cog_unload(self) -> None:
        self.check_kalshi_results.cancel()
        self.refresh_discovery.cancel()
        self.refresh_sports_loop.cancel()
        self.check_legacy_results.cancel()
        self.db_maintenance.cancel()
        await kalshi_api.close()

    # ── Sport autocomplete ────────────────────────────────────────────

    async def sport_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        choices = []
        seen = set()
        current_lower = current.lower()

        # Include game sports
        for key, sport in SPORTS.items():
            label = sport["label"]
            if current_lower in label.lower() or current_lower in key.lower():
                choices.append(app_commands.Choice(name=label, value=key))
                seen.add(key)
            if len(choices) >= 25:
                break

        # Include futures-only sports (e.g. Boxing)
        if len(choices) < 25:
            for key, fut in FUTURES.items():
                if key in seen:
                    continue
                label = fut["label"]
                if current_lower in label.lower() or current_lower in key.lower():
                    choices.append(app_commands.Choice(name=f"{label} (Futures)", value=key))
                if len(choices) >= 25:
                    break

        return choices

    # ── /bet ──────────────────────────────────────────────────────────

    @app_commands.command(name="bet", description="Browse Kalshi markets and bet YES or NO")
    @app_commands.describe(search="Search markets by keyword (leave blank to browse categories)")
    async def bet(self, interaction: discord.Interaction, search: str | None = None) -> None:
        await interaction.response.defer()
        log.info("/bet called by %s (search=%r)", interaction.user, search)

        if search:
            all_markets = await kalshi_api.get_all_open_markets()
            search_lower = search.lower()
            filtered = [
                m for m in all_markets
                if search_lower in (m.get("title") or "").lower()
                or search_lower in (m.get("yes_sub_title") or "").lower()
            ]
            if not filtered:
                await interaction.followup.send(f"No markets found matching **{search}**.")
                return
            filtered.sort(key=lambda m: _earliest_market_time(m) or "9999")
            view = MarketListView(filtered)
            embed = view.build_embed()
            embed.title = f"Search: {search}"
            msg = await interaction.followup.send(embed=embed, view=view)
            view.message = msg
            return

        browse_data = await kalshi_api.get_browse_data()
        if not browse_data:
            await interaction.followup.send("No open markets on Kalshi right now.")
            return

        view = KalshiTopView(browse_data)
        embed = view.build_embed()
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /parlay ────────────────────────────────────────────────────────

    @app_commands.command(name="parlay", description="Build a parlay bet")
    @app_commands.describe(sport="Filter by sport (leave blank for all)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def parlay(self, interaction: discord.Interaction, sport: str | None = None) -> None:
        await interaction.response.defer()

        if sport and sport in SPORTS:
            games = await kalshi_api.get_sport_games(sport)
        else:
            games = await kalshi_api.get_all_games()

        if not games:
            await interaction.followup.send("No games available right now.")
            return

        # Sort by commence_time (soonest first)
        games.sort(key=lambda g: g.get("commence_time", "9999"))

        view = KalshiParlayView(games)
        embed = view.build_embed()
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /myparlays ────────────────────────────────────────────────────

    @app_commands.command(name="myparlays", description="View your parlays")
    async def myparlays(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        # Fetch both old and kalshi parlays
        old_parlays = await betting_service.get_user_parlays(interaction.user.id)
        kalshi_parlays = await betting_service.get_user_kalshi_parlays(interaction.user.id)

        if not old_parlays and not kalshi_parlays:
            await interaction.followup.send("You have no parlays.")
            return

        embed = discord.Embed(title="Your Parlays", color=discord.Color.blue())

        for p in kalshi_parlays[:10]:
            status = p["status"]
            if status == "won":
                icon = "\U0001f7e2"
                status_text = f"Won — **${p.get('payout', 0):.2f}**"
            elif status == "lost":
                icon = "\U0001f534"
                status_text = "Lost"
            else:
                icon = "\U0001f7e1"
                potential = round(p["amount"] * p["total_odds"], 2)
                status_text = f"Pending — potential **${potential:.2f}**"

            leg_lines = []
            for leg in p.get("legs", []):
                ls = leg["status"]
                if ls == "won":
                    leg_icon = "\U0001f7e2"
                elif ls == "lost":
                    leg_icon = "\U0001f534"
                else:
                    leg_icon = "\U0001f7e1"
                pick_display = leg.get("pick_display") or leg["pick"].upper()
                leg_lines.append(f"{leg_icon} {leg.get('title', '?')} — {pick_display} ({leg['odds']:.2f}x)")

            embed.add_field(
                name=f"{icon} Parlay #KP{p['id']} · {status_text}",
                value=(
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                ),
                inline=False,
            )

        # Show old parlays if any remain
        from bot.cogs.betting import PICK_LABELS
        for p in old_parlays[:5]:
            status = p["status"]
            if status == "won":
                icon = "\U0001f7e2"
                status_text = f"Won — **${p.get('payout', 0):.2f}**"
            elif status == "lost":
                icon = "\U0001f534"
                status_text = "Lost"
            else:
                icon = "\U0001f7e1"
                potential = round(p["amount"] * p["total_odds"], 2)
                status_text = f"Pending — potential **${potential:.2f}**"

            leg_lines = []
            for leg in p.get("legs", []):
                ls = leg["status"]
                if ls == "won":
                    leg_icon = "\U0001f7e2"
                elif ls == "lost":
                    leg_icon = "\U0001f534"
                elif ls == "push":
                    leg_icon = "\U0001f535"
                else:
                    leg_icon = "\U0001f7e1"
                home = leg.get("home_team") or "?"
                away = leg.get("away_team") or "?"
                pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
                point = leg.get("point")
                if point is not None:
                    if leg["pick"] in ("spread_home", "spread_away"):
                        pick_label += f" {point:+g}"
                    else:
                        pick_label += f" {point:g}"
                leg_lines.append(f"{leg_icon} {format_matchup(home, away)} — {pick_label} ({leg['odds']:.2f}x)")

            embed.add_field(
                name=f"{icon} Parlay #{p['id']} (legacy) · {status_text}",
                value=(
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                ),
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # ── /cancelparlay ─────────────────────────────────────────────────

    @app_commands.command(name="cancelparlay", description="Cancel a pending parlay")
    @app_commands.describe(parlay_id="The ID of the parlay to cancel (KP number)")
    async def cancelparlay(self, interaction: discord.Interaction, parlay_id: int) -> None:
        await interaction.response.defer(ephemeral=True)

        result = await betting_service.cancel_kalshi_parlay(parlay_id, interaction.user.id)
        if result is None:
            await interaction.followup.send(
                "Could not cancel parlay. Make sure you own it, it's still pending, and no legs have started settling.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Parlay Cancelled",
            description=f"Parlay #KP{parlay_id} cancelled. **${result['amount']:.2f}** refunded.",
            color=discord.Color.orange(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /myhistory ──────────────────────────────────────────────────────

    @app_commands.command(name="myhistory", description="View your resolved bets with stats")
    async def myhistory(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        stats = await betting_service.get_user_stats(interaction.user.id)
        total_items = await betting_service.count_user_resolved(interaction.user.id)

        if total_items == 0:
            await interaction.followup.send("You have no resolved bets yet.")
            return

        view = HistoryView(interaction.user.id, stats, total_items)
        embed = await view.build_embed()
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /mybets ──────────────────────────────────────────────────────────

    @app_commands.command(name="mybets", description="View your active/pending bets")
    async def mybets(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        bets = await betting_service.get_user_bets(interaction.user.id, status="pending")
        parlays = await betting_service.get_user_parlays(interaction.user.id, status="pending")
        kalshi_bets = await betting_service.get_user_kalshi_bets(interaction.user.id, status="pending")

        if not bets and not parlays and not kalshi_bets:
            await interaction.followup.send("You have no pending bets. Use `/myhistory` to view past bets.")
            return

        # Fetch live scores for pending bets (uses cached data, skip outrights)
        live_scores: dict[str, dict] = {}
        for b in bets:
            if (b.get("market") or "") == "outrights":
                continue
            composite = b["game_id"]
            if composite in live_scores:
                continue
            parts = composite.split("|")
            event_id = parts[0]
            sport_key = parts[1] if len(parts) > 1 else None
            status = await self.sports_api.get_fixture_status(event_id, sport_key)
            if status:
                live_scores[composite] = status

        embed = discord.Embed(title="Your Active Bets", color=discord.Color.blue())

        for b in bets:
            potential = round(b["amount"] * b["odds"], 2)
            icon = "\U0001f7e1"  # yellow circle
            status_text = f"Pending — potential **${potential:.2f}**"

            home = b.get("home_team")
            away = b.get("away_team")
            sport = b.get("sport_title")
            live = live_scores.get(b["game_id"])

            if not home and live:
                home = live.get("home_team")
            if not away and live:
                away = live.get("away_team")

            is_outright = (b.get("market") or "") == "outrights"

            if is_outright:
                matchup = sport or "Futures"
            elif home and away:
                matchup = format_matchup(home, away)
            else:
                raw_id = b["game_id"].split("|")[0] if "|" in b["game_id"] else b["game_id"]
                matchup = f"Game `{raw_id}`"

            score_line = ""
            if not is_outright and live and live["started"] and live["home_score"] is not None and live["away_score"] is not None:
                score_line = f"\nScore: **{home}** {live['home_score']} - {live['away_score']} **{away}**"

            sport_line = f"{sport} · " if sport and not is_outright else ""
            pick_label = b["pick"] if is_outright else format_pick_label(b)

            embed.add_field(
                name=f"{icon} Bet #{b['id']} (legacy) · {status_text}",
                value=(
                    f"{sport_line}**{matchup}**\n"
                    f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']}x"
                    f"{score_line}"
                ),
                inline=False,
            )

        # Show pending parlays inline
        for p in parlays:
            icon = "\U0001f7e1"
            potential = round(p["amount"] * p["total_odds"], 2)
            status_text = f"Pending — potential **${potential:.2f}**"

            leg_lines = []
            for leg in p.get("legs", []):
                ls = leg["status"]
                if ls == "won":
                    leg_icon = "\U0001f7e2"
                elif ls == "lost":
                    leg_icon = "\U0001f534"
                elif ls == "push":
                    leg_icon = "\U0001f535"
                else:
                    leg_icon = "\U0001f7e1"

                home = leg.get("home_team") or "?"
                away = leg.get("away_team") or "?"
                pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
                point = leg.get("point")
                if point is not None:
                    if leg["pick"] in ("spread_home", "spread_away"):
                        pick_label += f" {point:+g}"
                    else:
                        pick_label += f" {point:g}"
                leg_lines.append(f"{leg_icon} {format_matchup(home, away)} — {pick_label} ({leg['odds']:.2f}x)")

            embed.add_field(
                name=f"{icon} Parlay #{p['id']} (legacy) · {status_text}",
                value=(
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                ),
                inline=False,
            )

        # Show pending Kalshi bets
        for kb in kalshi_bets:
            potential = round(kb["amount"] * kb["odds"], 2)
            title = kb.get("title") or kb["market_ticker"]
            pick_label = kb.get("pick_display") or kb["pick"].upper()

            # Determine live/upcoming status from event_ticker date
            event_ticker = kb.get("event_ticker", "")
            ticker_date = _parse_event_ticker_date(event_ticker) if event_ticker else None
            now = datetime.now(timezone.utc)

            if ticker_date and ticker_date.date() < now.date():
                icon = "\U0001f534"  # red — likely live/completed
                time_line = "\n\U0001f534 LIVE"
            elif ticker_date and ticker_date.date() == now.date():
                icon = "\U0001f7e0"  # orange — today
                time_line = "\nToday"
            else:
                icon = "\U0001f7e3"  # purple — future
                time_line = ""
                if ticker_date:
                    time_line = f"\n{ticker_date.strftime('%-m/%-d')}"

            status_text = f"Pending — potential **${potential:.2f}**"

            embed.add_field(
                name=f"{icon} Kalshi #{kb['id']} · {status_text}",
                value=(
                    f"**{title}**\n"
                    f"Pick: **{pick_label}** · ${kb['amount']:.2f} @ {kb['odds']:.2f}x"
                    f"{time_line}"
                ),
                inline=False,
            )

        embed.set_footer(text="Use /myhistory to view resolved bets")
        await interaction.followup.send(embed=embed)

    # ── /livescores ────────────────────────────────────────────────────

    @app_commands.command(name="livescores", description="View live scores for all pending bets")
    async def livescores(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        from bot.db import models as _models
        from bot.services.kalshi_api import _parse_event_ticker_date

        embed = discord.Embed(title="\U0001f534 Live Scores", color=discord.Color.red())
        found_any = False
        now = datetime.now(timezone.utc)

        # ── Odds-API bets with live scores ──
        game_ids = await betting_service.get_pending_game_ids()
        for composite_id in game_ids[:15]:
            bets_for_game = await betting_service.get_bets_by_game(composite_id)
            if not bets_for_game:
                continue

            # Skip outrights
            if (bets_for_game[0].get("market") or "") == "outrights":
                continue

            parts = composite_id.split("|")
            event_id = parts[0]
            sport_key = parts[1] if len(parts) > 1 else None
            status = await self.sports_api.get_fixture_status(event_id, sport_key)
            if not status or not status["started"]:
                continue

            home = status.get("home_team") or bets_for_game[0].get("home_team", "Home")
            away = status.get("away_team") or bets_for_game[0].get("away_team", "Away")

            if status["home_score"] is not None and status["away_score"] is not None:
                score_text = f"**{home}** {status['home_score']} - {status['away_score']} **{away}**"
                if status["completed"]:
                    score_text += "  (Final)"
            else:
                score_text = f"**{away}** @ **{home}**"

            bet_lines = []
            for b in bets_for_game:
                pick_label = format_pick_label(b)
                potential = round(b["amount"] * b["odds"], 2)
                bet_lines.append(
                    f"<@{b['user_id']}> — {pick_label} · ${b['amount']:.2f} → ${potential:.2f}"
                )

            sport = bets_for_game[0].get("sport_title", "")
            embed.add_field(
                name=f"{sport} · {score_text}",
                value="\n".join(bet_lines),
                inline=False,
            )
            found_any = True

        # ── Kalshi bets on live games ──
        kalshi_bets = await _models.get_all_pending_kalshi_bets()
        live_kalshi = []
        for kb in kalshi_bets:
            event_ticker = kb.get("event_ticker", "")
            ticker_date = _parse_event_ticker_date(event_ticker) if event_ticker else None
            if ticker_date and ticker_date.date() <= now.date():
                live_kalshi.append(kb)

        if live_kalshi:
            kalshi_lines = []
            for kb in live_kalshi[:15]:
                title = kb.get("title") or kb["market_ticker"]
                if len(title) > 50:
                    title = title[:47] + "..."
                pick_label = kb.get("pick_display") or kb["pick"].upper()
                potential = round(kb["amount"] * kb["odds"], 2)
                kalshi_lines.append(
                    f"<@{kb['user_id']}> — {pick_label} · ${kb['amount']:.2f} → ${potential:.2f}\n{title}"
                )

            embed.add_field(
                name=f"\U0001f534 Kalshi — Live ({len(live_kalshi)})",
                value="\n".join(kalshi_lines),
                inline=False,
            )
            found_any = True

        if not found_any:
            embed.description = "No pending bets have games in progress right now."

        await interaction.followup.send(embed=embed)

    # ── /cancelbet ───────────────────────────────────────────────────────

    @app_commands.command(name="cancelbet", description="Cancel a pending bet")
    @app_commands.describe(bet_id="The ID of the bet to cancel (can be numeric or start with K)")
    async def cancelbet(self, interaction: discord.Interaction, bet_id: str) -> None:
        await interaction.response.defer(ephemeral=True)

        # Handle Kalshi bet IDs (e.g., "K123")
        if bet_id.upper().startswith("K"):
            try:
                numeric_id = int(bet_id[1:])
                result = await betting_service.cancel_kalshi_bet(numeric_id, interaction.user.id)
                if result:
                    embed = discord.Embed(
                        title="Bet Cancelled",
                        description=f"Kalshi bet #K{numeric_id} cancelled. **${result['amount']:.2f}** refunded.",
                        color=discord.Color.orange(),
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except ValueError:
                pass

        # Handle legacy numeric IDs
        try:
            numeric_id = int(bet_id)
            # Look up the bet to check game status
            bets = await betting_service.get_user_bets(interaction.user.id)
            target = next((b for b in bets if b["id"] == numeric_id), None)

            if target:
                # Parse composite game_id
                parts = target["game_id"].split("|")
                event_id = parts[0]
                sport_key = parts[1] if len(parts) > 1 else None

                status = await self.sports_api.get_fixture_status(event_id, sport_key)
                if status and status["started"]:
                    await interaction.followup.send(
                        "Cannot cancel — this game has already started.", ephemeral=True
                    )
                    return

            result = await betting_service.cancel_bet(numeric_id, interaction.user.id)
            if result:
                embed = discord.Embed(
                    title="Bet Cancelled",
                    description=f"Legacy bet #{numeric_id} cancelled. **${result['amount']:.2f}** refunded.",
                    color=discord.Color.orange(),
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        except ValueError:
            pass

        await interaction.followup.send(
            "Could not cancel bet. Make sure the ID is correct, you own it, and it's still pending.",
            ephemeral=True,
        )

    # ── /pendingbets (admin) ─────────────────────────────────────────────

    @app_commands.command(name="pendingbets", description="[Admin] View all pending bets")
    @app_commands.checks.has_permissions(administrator=True)
    async def pendingbets(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        game_ids = await betting_service.get_pending_game_ids()

        embed = discord.Embed(title="Pending Bets", color=discord.Color.orange())

        for composite_id in game_ids[:15]:
            bets = await betting_service.get_bets_by_game(composite_id)
            if not bets:
                continue

            first = bets[0]
            home = first.get("home_team")
            away = first.get("away_team")
            sport = first.get("sport_title") or ""
            is_outright = (first.get("market") or "") == "outrights"

            if is_outright:
                matchup = sport or "Futures"
            elif home and away:
                matchup = format_matchup(home, away)
            else:
                matchup = "Unknown"

            lines = []
            total_wagered = 0
            for b in bets:
                pick_label = b["pick"] if is_outright else format_pick_label(b)
                lines.append(
                    f"<@{b['user_id']}> — {pick_label} · ${b['amount']:.2f} @ {b['odds']}x"
                )
                total_wagered += b["amount"]

            # Show game time from first bet's commence_time
            time_str = ""
            ct = first.get("commence_time")
            if ct:
                try:
                    ct_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                    if ct_dt <= datetime.now(timezone.utc):
                        time_str = " \U0001f534 LIVE"
                    else:
                        time_str = f"\n{format_game_time(ct)}"
                except (ValueError, TypeError):
                    pass

            raw_id = composite_id.split("|")[0] if "|" in composite_id else composite_id
            header = f"{matchup} ({len(bets)} bet{'s' if len(bets) != 1 else ''} · ${total_wagered:.2f}){time_str}"
            value = "\n".join(lines) + f"\nID: `{raw_id}`"

            embed.add_field(name=header, value=value, inline=False)

        if len(game_ids) > 15:
            embed.set_footer(text=f"Showing 15 of {len(game_ids)} games")

        # Also show pending parlays
        from bot.db.database import get_connection as _get_conn
        from bot.db import models as _models
        _db = await _get_conn()
        try:
            _cursor = await _db.execute(
                "SELECT * FROM parlays WHERE status = 'pending' ORDER BY created_at DESC LIMIT 15"
            )
            _rows = await _cursor.fetchall()
            pending_parlays = [dict(r) for r in _rows]
        finally:
            await _db.close()

        if pending_parlays:
            parlay_lines = []
            for p in pending_parlays:
                legs = await _models.get_parlay_legs(p["id"])
                leg_count = len(legs)
                parlay_lines.append(
                    f"<@{p['user_id']}> — Parlay #{p['id']} · {leg_count} legs · "
                    f"${p['amount']:.2f} @ {p['total_odds']:.2f}x"
                )
            embed.add_field(
                name=f"Pending Parlays ({len(pending_parlays)})",
                value="\n".join(parlay_lines),
                inline=False,
            )

        # Kalshi bets
        kalshi_bets = await _models.get_all_pending_kalshi_bets()
        if kalshi_bets:
            kalshi_lines = []
            total_kalshi_wagered = 0
            for kb in kalshi_bets[:20]:
                pick_display = kb.get("pick_display") or kb["pick"]
                title = kb.get("title") or kb["market_ticker"]
                if len(title) > 40:
                    title = title[:37] + "..."
                # Parse game time from event ticker
                time_info = ""
                et = kb.get("event_ticker", "")
                ticker_date = _parse_event_ticker_date(et) if et else None
                if ticker_date:
                    now = datetime.now(timezone.utc)
                    if ticker_date.date() < now.date():
                        time_info = " \U0001f534 LIVE"
                    else:
                        time_info = f" · {ticker_date.strftime('%-m/%-d')}"
                kalshi_lines.append(
                    f"<@{kb['user_id']}> — {pick_display} · "
                    f"${kb['amount']:.2f} @ {kb['odds']:.2f}x{time_info}\n{title}"
                )
                total_kalshi_wagered += kb["amount"]
            header = f"Kalshi Bets ({len(kalshi_bets)} · ${total_kalshi_wagered:.2f})"
            value = "\n".join(kalshi_lines)
            if len(kalshi_bets) > 20:
                value += f"\n*...and {len(kalshi_bets) - 20} more*"
            embed.add_field(name=header, value=value[:1024], inline=False)

        if not game_ids and not pending_parlays and not kalshi_bets:
            embed.description = "No pending bets."

        await interaction.followup.send(embed=embed, ephemeral=True)

    @pendingbets.error
    async def pendingbets_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "You need administrator permissions to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        else:
            log.exception("Error in /pendingbets command", exc_info=error)

    # ── /quota (admin) ────────────────────────────────────────────────────

    @app_commands.command(name="quota", description="[Admin] Check API quota usage")
    @app_commands.checks.has_permissions(administrator=True)
    async def quota(self, interaction: discord.Interaction) -> None:
        q = self.sports_api.get_quota()
        used = q["used"]
        remaining = q["remaining"]
        last = q["last"]

        if used is None and remaining is None:
            embed = discord.Embed(
                title="API Quota",
                description="No API calls made yet this session — quota unknown.",
                color=discord.Color.greyple(),
            )
        else:
            total = (used or 0) + (remaining or 0)
            embed = discord.Embed(title="API Quota", color=discord.Color.blue())
            embed.add_field(name="Used", value=str(used or 0), inline=True)
            embed.add_field(name="Remaining", value=str(remaining or 0), inline=True)
            embed.add_field(name="Total", value=str(total), inline=True)
            if last:
                embed.set_footer(text=f"Last request: {last}")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @quota.error
    async def quota_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "You need administrator permissions to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)

    # ── /vacuum (admin) ───────────────────────────────────────────────────

    @app_commands.command(name="vacuum", description="[Admin] Force cleanup of cache and reclaim disk space")
    @app_commands.checks.has_permissions(administrator=True)
    async def vacuum(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            log.info(f"Manual vacuum triggered by {interaction.user}")
            deleted = await cleanup_cache(max_age_days=0)  # Clear all cache
            vacuumed = await vacuum_db()
            vacuum_note = "database vacuumed" if vacuumed else "VACUUM skipped (check logs)"
            await interaction.followup.send(
                f"Maintenance complete. Cleared {deleted} cache entries, {vacuum_note}.",
                ephemeral=True
            )
        except Exception as e:
            log.exception("Vacuum command failed")
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    # ── /resolve (admin) ───────────────────────────────────────────────

    @app_commands.command(name="resolve", description="[Admin] Manually resolve a game")
    @app_commands.describe(
        game_id="Game ID to resolve",
        winner="The winning side: home, away, or draw",
        home_score="Home team final score (needed for spread/total bets)",
        away_score="Away team final score (needed for spread/total bets)",
        winner_name="For futures: the winning team/outcome name",
    )
    @app_commands.choices(
        winner=[
            app_commands.Choice(name="Home", value="home"),
            app_commands.Choice(name="Away", value="away"),
            app_commands.Choice(name="Draw", value="draw"),
        ]
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def resolve(
        self,
        interaction: discord.Interaction,
        game_id: str,
        winner: app_commands.Choice[str],
        home_score: int | None = None,
        away_score: int | None = None,
        winner_name: str | None = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        # Find any pending bets matching this game_id (full or partial match)
        pending_ids = await betting_service.get_pending_game_ids()
        matching = [gid for gid in pending_ids if game_id in gid]

        if not matching:
            await interaction.followup.send(
                f"No pending bets found for game `{game_id}`.", ephemeral=True
            )
            return

        all_resolved: list[dict] = []
        for composite_id in matching:
            resolved = await betting_service.resolve_game(
                composite_id, winner.value,
                home_score=home_score, away_score=away_score,
                winner_name=winner_name,
            )
            all_resolved.extend(resolved)
        total_resolved = len(all_resolved)

        if all_resolved:
            await self._post_resolution_announcement(
                all_resolved, home_score=home_score, away_score=away_score,
            )

        if winner_name:
            desc = (
                f"Futures event `{game_id}` resolved.\n"
                f"Winner: **{winner_name}**\n"
                f"**{total_resolved}** bet(s) settled."
            )
        else:
            score_note = ""
            if home_score is not None and away_score is not None:
                score_note = f"\nScore: {home_score} - {away_score} (spread/total bets resolved)"
            else:
                score_note = "\nNote: Spread/total bets need scores to resolve — provide home_score and away_score."
            desc = (
                f"Game `{game_id}` resolved as **{winner.name}** win.\n"
                f"**{total_resolved}** bet(s) settled."
                f"{score_note}"
            )

        embed = discord.Embed(
            title="Game Resolved",
            description=desc,
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @resolve.error
    async def resolve_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            if interaction.response.is_done():
                await interaction.followup.send(
                    "You need administrator permissions to use this command.", ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "You need administrator permissions to use this command.", ephemeral=True
                )
        else:
            log.exception("Error in /resolve command", exc_info=error)

    # ── Resolution announcement ────────────────────────────────────────

    async def _post_resolution_announcement(
        self,
        resolved_bets: list[dict],
        home_score: int | None = None,
        away_score: int | None = None,
    ) -> None:
        """Post an embed announcing resolved bets to the results channel."""
        if not resolved_bets:
            return

        channel = self.bot.get_channel(BET_RESULTS_CHANNEL_ID)
        if channel is None:
            log.warning("Results channel %s not found", BET_RESULTS_CHANNEL_ID)
            return

        first = resolved_bets[0]
        home = first.get("home_team") or "Home"
        away = first.get("away_team") or "Away"
        is_outright = (first.get("market") or "") == "outrights"

        if is_outright:
            title = f"Result: {first.get('sport_title', 'Futures')}"
            description = ""
        else:
            title = f"Game Result: {format_matchup(home, away)}"
            if home_score is not None and away_score is not None:
                description = f"**{home}** {home_score} - {away_score} **{away}**"
            else:
                description = ""

        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.blue(),
        )

        lines = []
        single_bets = [b for b in resolved_bets if b.get("type") != "parlay"]
        for bet in single_bets:
            result = bet["result"]
            if result == "won":
                icon = "\U0001f7e2"
                result_text = f"Won **${bet['payout']:.2f}**"
            elif result == "push":
                icon = "\U0001f535"
                result_text = f"Push (${bet['payout']:.2f} refunded)"
            else:
                icon = "\U0001f534"
                result_text = "Lost"

            pick_label = format_pick_label(bet) if not is_outright else bet["pick"]
            lines.append(
                f"{icon} <@{bet['user_id']}> — {pick_label} · "
                f"${bet['amount']:.2f} @ {bet['odds']}x → {result_text}"
            )

        if lines:
            embed.add_field(name="Bets", value="\n".join(lines), inline=False)

        # Parlay results
        parlay_entries = [b for b in resolved_bets if b.get("type") == "parlay"]
        if parlay_entries:
            parlay_lines = []
            for p in parlay_entries:
                result = p["result"]
                if result == "won":
                    icon = "\U0001f7e2"
                    result_text = f"Won **${p['payout']:.2f}**"
                else:
                    icon = "\U0001f534"
                    result_text = "Lost"
                # Build leg summary with team names
                leg_parts = []
                for leg in p.get("legs", []):
                    h = leg.get("home_team") or "?"
                    a = leg.get("away_team") or "?"
                    pick_label = format_pick_label(leg)
                    leg_icon = "\u2705" if leg.get("status") == "won" else "\u274c" if leg.get("status") == "lost" else "\u23f3"
                    leg_parts.append(f"  {leg_icon} {format_matchup(h, a)} — {pick_label}")
                legs_text = "\n".join(leg_parts)
                parlay_lines.append(
                    f"{icon} <@{p['user_id']}> — Parlay #{p['id']} · "
                    f"${p['amount']:.2f} @ {p.get('total_odds', 0):.2f}x → {result_text}\n{legs_text}"
                )
            embed.add_field(name="Parlays", value="\n".join(parlay_lines), inline=False)

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            log.exception("Failed to send resolution announcement")

    # ── /live ─────────────────────────────────────────────────────────

    @app_commands.command(name="live", description="Upcoming sports markets — sorted by soonest close")
    async def live(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        all_markets = await kalshi_api.get_all_open_markets()
        now = datetime.now(timezone.utc)

        # Keep only markets whose actionable deadline is still in the future, sorted soonest first.
        # Use the earlier of close_time / expected_expiration_time — different market types
        # place the "game time" in different fields.
        upcoming: list[tuple[datetime, dict]] = []
        no_close: list[dict] = []
        for m in all_markets:
            close_str = _earliest_market_time(m)
            if not close_str:
                no_close.append(m)
                continue
            try:
                close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                no_close.append(m)
                continue
            if close_dt > now:
                upcoming.append((close_dt, m))

        upcoming.sort(key=lambda t: t[0])
        closing_soon = [m for _, m in upcoming] + no_close

        if not closing_soon:
            await interaction.followup.send("No open markets right now.")
            return

        view = MarketListView(closing_soon)
        embed = view.build_embed()
        embed.title = "\U0001f534 Open Markets"
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── Periodic discovery (pre-warm cache) ─────────────────────────

    @tasks.loop(minutes=30)
    async def refresh_discovery(self) -> None:
        try:
            result = await kalshi_api.discover_available(force=True)
            games = result.get("games", {})
            futures = result.get("futures", {})
            log.info(
                "Periodic discovery: %d game sports, %d futures sports",
                len(games), len(futures),
            )
            await kalshi_api.get_browse_data()
        except Exception:
            log.exception("Error in periodic discovery")

    @refresh_discovery.before_loop
    async def before_refresh_discovery(self) -> None:
        await self.bot.wait_until_ready()

    # ── Periodic sports refresh (pick up new series) ─────────────────

    @tasks.loop(hours=24)
    async def refresh_sports_loop(self) -> None:
        try:
            await kalshi_api.refresh_sports()
            log.info("Periodic sports refresh: %d sports available", len(SPORTS))
        except Exception:
            log.exception("Error in periodic sports refresh")

    @refresh_sports_loop.before_loop
    async def before_refresh_sports_loop(self) -> None:
        await self.bot.wait_until_ready()

    # ── Database maintenance task ────────────────────────────────────

    @tasks.loop(hours=24)
    async def db_maintenance(self) -> None:
        """Periodic database cleanup to save disk space."""
        try:
            log.info("Starting database maintenance...")
            deleted = await cleanup_cache(max_age_days=1)
            log.info(f"Cleaned up {deleted} stale cache entries.")
            vacuumed = await vacuum_db()
            if vacuumed:
                log.info("Database vacuumed successfully.")
            else:
                log.info("VACUUM skipped (see warning above).")
        except Exception:
            log.exception("Error in db_maintenance loop")

    @db_maintenance.before_loop
    async def before_db_maintenance(self) -> None:
        await self.bot.wait_until_ready()

    # ── Legacy check_results loop (the-odds-api) ──────────────────────

    # Estimated game durations by sport prefix (hours).
    SPORT_DURATION: dict[str, float] = {
        "americanfootball": 3.5,
        "baseball": 3.0,
        "basketball": 2.5,
        "icehockey": 2.5,
        "soccer": 2.0,
        "mma": 1.5,
        "boxing": 1.5,
    }
    DEFAULT_SPORT_DURATION = 2.0  # fallback

    @tasks.loop(minutes=30)
    async def check_legacy_results(self) -> None:
        """Check scores for legacy games likely near completion."""
        try:
            started_games = await betting_service.get_started_pending_games()
            if not started_games:
                from bot.db import models
                pending_single = await models.get_pending_games_with_commence()
                pending_parlay = await models.get_pending_parlay_games_with_commence()
                if not pending_single and not pending_parlay:
                    log.debug("No pending legacy bets remain.")
                    # self.check_legacy_results.stop() # Could stop it
                return

            # Skip if quota is critically low (< 50 remaining)
            quota = self.sports_api.get_quota()
            if quota["remaining"] is not None and quota["remaining"] < 50:
                log.warning("API quota low (%s remaining), skipping score check", quota["remaining"])
                return

            now = datetime.now(timezone.utc)

            # Filter to games likely near completion based on sport duration
            ready_games: dict[str, str | None] = {}
            for composite_id, commence_time in started_games.items():
                parts = composite_id.split("|")
                sport_key = parts[1] if len(parts) > 1 else None

                if commence_time is None:
                    ready_games[composite_id] = commence_time
                    continue

                try:
                    game_start = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ready_games[composite_id] = commence_time
                    continue

                duration = self.DEFAULT_SPORT_DURATION
                if sport_key:
                    for prefix, dur in self.SPORT_DURATION.items():
                        if sport_key.startswith(prefix):
                            duration = dur
                            break

                min_elapsed = min(duration * 0.75, 1.5)
                elapsed_hours = (now - game_start).total_seconds() / 3600

                if elapsed_hours >= min_elapsed:
                    ready_games[composite_id] = commence_time

            if not ready_games:
                return

            sport_games: dict[str, list[str]] = {}
            for composite_id in ready_games:
                parts = composite_id.split("|")
                sport_key = parts[1] if len(parts) > 1 else None
                if not sport_key:
                    continue
                sport_games.setdefault(sport_key, []).append(composite_id)

            all_sport_keys = list(sport_games.keys())
            if len(all_sport_keys) > 8:
                all_sport_keys.sort()
                offset = (now.minute // 30) % len(all_sport_keys)
                rotated = all_sport_keys[offset:] + all_sport_keys[:offset]
                sport_keys = rotated[:8]
            else:
                sport_keys = all_sport_keys

            for sport_key in sport_keys:
                composites = sport_games[sport_key]
                scores_map = await self.sports_api.get_scores_by_sport(sport_key)
                if not scores_map:
                    continue

                for composite_id in composites:
                    event_id = composite_id.split("|")[0]
                    status = scores_map.get(event_id)
                    if not status or not status["completed"]:
                        continue

                    home_score = status["home_score"]
                    away_score = status["away_score"]

                    if home_score is None or away_score is None:
                        continue

                    winner = "home" if home_score > away_score else "away" if away_score > home_score else "draw"

                    resolved = await betting_service.resolve_game(
                        composite_id, winner,
                        home_score=home_score, away_score=away_score,
                    )
                    if resolved:
                        log.info("Resolved %d legacy bets for game %s", len(resolved), event_id)
                        await self._post_resolution_announcement(
                            resolved, home_score=home_score, away_score=away_score,
                        )
        except Exception:
            log.exception("Error in check_legacy_results loop")

    @check_legacy_results.before_loop
    async def before_check_legacy_results(self) -> None:
        await self.bot.wait_until_ready()

    # ── Resolution loop ───────────────────────────────────────────────

    @tasks.loop(minutes=3)
    async def check_kalshi_results(self) -> None:
        from bot.db import models
        from bot.services.wallet_service import deposit

        pending = await models.get_pending_kalshi_tickers_with_close_time()
        parlay_pending = await models.get_pending_kalshi_parlay_tickers_with_close_time()

        # Merge single bet and parlay tickers
        all_pending_map: dict[str, str | None] = {}
        for row in pending:
            all_pending_map[row["market_ticker"]] = row.get("close_time")
        for row in parlay_pending:
            ticker = row["market_ticker"]
            ct = row.get("close_time")
            if ticker not in all_pending_map or (ct and (all_pending_map[ticker] is None or ct < all_pending_map[ticker])):
                all_pending_map[ticker] = ct

        if not all_pending_map:
            return

        now = datetime.now(timezone.utc)

        # Increment counter: full sweep every 5th iteration (~15 min)
        self._kalshi_check_count = getattr(self, "_kalshi_check_count", 0) + 1
        full_sweep = self._kalshi_check_count % 5 == 0

        # Determine which tickers to check this iteration
        tickers: list[str] = []
        for ticker, ct in all_pending_map.items():
            if full_sweep or not ct:
                tickers.append(ticker)
                continue
            try:
                close_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                tickers.append(ticker)
                continue
            if now >= close_dt - timedelta(hours=1):
                tickers.append(ticker)

        if not tickers:
            return

        sweep_label = "full" if full_sweep else "urgent"
        log.info("Checking %d/%d pending Kalshi market(s) [%s]...", len(tickers), len(all_pending_map), sweep_label)

        channel = None
        if BET_RESULTS_CHANNEL_ID:
            channel = self.bot.get_channel(BET_RESULTS_CHANNEL_ID)

        for ticker in tickers:
            try:
                market = await kalshi_api.get_market(ticker)
                if not market:
                    continue

                status = market.get("status", "")
                if status not in ("settled", "finalized"):
                    continue

                settlement = market.get("settlement_value_dollars")
                if settlement is None:
                    result_val = market.get("result", "")
                    if result_val == "yes":
                        winning_side = "yes"
                    elif result_val == "no":
                        winning_side = "no"
                    else:
                        continue
                else:
                    try:
                        sv = float(settlement)
                    except (ValueError, TypeError):
                        continue
                    winning_side = "yes" if sv >= 0.99 else "no"

                bets = await models.get_pending_kalshi_bets_by_market(ticker)
                for bet in bets:
                    won = bet["pick"] == winning_side
                    payout = round(bet["amount"] * bet["odds"]) if won else 0
                    await models.resolve_kalshi_bet(bet["id"], won, payout)

                    if channel:
                        result_text = "won" if won else "lost"
                        emoji = "\U0001f7e2" if won else "\U0001f534"
                        user = self.bot.get_user(bet["user_id"])
                        name = user.display_name if user else f"User {bet['user_id']}"
                        pick_display = bet.get("pick_display") or bet["pick"].upper()
                        bet_title = bet.get("title") or ticker

                        embed = discord.Embed(
                            title=f"{emoji} Bet #K{bet['id']} — {result_text.upper()}",
                            color=discord.Color.green() if won else discord.Color.red(),
                        )
                        embed.add_field(name="Bettor", value=name, inline=True)
                        embed.add_field(name="Market", value=bet_title[:256], inline=False)
                        embed.add_field(name="Pick", value=pick_display, inline=True)
                        embed.add_field(name="Wager", value=f"${bet['amount']:.2f}", inline=True)
                        embed.add_field(name="Payout", value=f"${payout:.2f}", inline=True)
                        embed.add_field(name="Result", value=f"Market settled **{winning_side.upper()}**", inline=False)
                        try:
                            await channel.send(embed=embed)
                        except discord.DiscordException:
                            pass

                # ── Resolve parlay legs for this ticker ──
                parlay_legs = await models.get_pending_kalshi_parlay_legs_by_market(ticker)
                affected_parlay_ids: set[int] = set()

                for leg in parlay_legs:
                    leg_won = leg["pick"] == winning_side
                    leg_status = "won" if leg_won else "lost"
                    await models.update_kalshi_parlay_leg_status(leg["id"], leg_status)
                    affected_parlay_ids.add(leg["parlay_id"])

                for pid in affected_parlay_ids:
                    kp = await models.get_kalshi_parlay_by_id(pid)
                    if not kp or kp["status"] != "pending":
                        continue

                    all_legs = await models.get_kalshi_parlay_legs(pid)
                    statuses = [l["status"] for l in all_legs]

                    if "lost" in statuses:
                        await models.update_kalshi_parlay(pid, "lost", payout=0)
                        if channel:
                            user = self.bot.get_user(kp["user_id"])
                            name = user.display_name if user else f"User {kp['user_id']}"
                            leg_summary = "\n".join(
                                f"{'✅' if l['status'] == 'won' else '❌' if l['status'] == 'lost' else '⏳'} {l.get('pick_display') or l['pick']}"
                                for l in all_legs
                            )
                            embed = discord.Embed(
                                title=f"\U0001f534 Parlay #KP{pid} — LOST",
                                color=discord.Color.red(),
                            )
                            embed.add_field(name="Bettor", value=name, inline=True)
                            embed.add_field(name="Wager", value=f"${kp['amount']:.2f}", inline=True)
                            embed.add_field(name="Legs", value=leg_summary[:1024], inline=False)
                            try:
                                await channel.send(embed=embed)
                            except discord.DiscordException:
                                pass

                    elif all(s in ("won",) for s in statuses):
                        effective_odds = 1.0
                        for l in all_legs:
                            effective_odds *= l["odds"]
                        payout = round(kp["amount"] * effective_odds)
                        await models.update_kalshi_parlay(pid, "won", payout=payout)
                        await deposit(kp["user_id"], payout)
                        if channel:
                            user = self.bot.get_user(kp["user_id"])
                            name = user.display_name if user else f"User {kp['user_id']}"
                            leg_summary = "\n".join(
                                f"✅ {l.get('pick_display') or l['pick']}"
                                for l in all_legs
                            )
                            embed = discord.Embed(
                                title=f"\U0001f7e2 Parlay #KP{pid} — WON!",
                                color=discord.Color.green(),
                            )
                            embed.add_field(name="Bettor", value=name, inline=True)
                            embed.add_field(name="Wager", value=f"${kp['amount']:.2f}", inline=True)
                            embed.add_field(name="Payout", value=f"${payout:.2f}", inline=True)
                            embed.add_field(name="Legs", value=leg_summary[:1024], inline=False)
                            try:
                                await channel.send(embed=embed)
                            except discord.DiscordException:
                                pass
                    # else: still pending

                log.info("Resolved Kalshi market %s -> %s", ticker, winning_side)

            except Exception:
                log.exception("Error checking Kalshi market %s", ticker)

    @check_kalshi_results.before_loop
    async def before_check_kalshi(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(KalshiCog(bot))
