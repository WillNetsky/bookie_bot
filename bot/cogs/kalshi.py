import logging
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.services import betting_service
from bot.services.kalshi_api import (
    kalshi_api, SPORTS, FUTURES, KALSHI_TO_ODDS_API,
    FUTURES_TO_SPORTS, SPORTS_TO_FUTURES, _decimal_to_american,
)
from bot.services.sports_api import SportsAPI
from bot.cogs.betting import (
    format_matchup, format_game_time, PICK_EMOJI,
)

log = logging.getLogger(__name__)

OPTIONS_PER_PAGE = 25


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


def _fmt_american(odds: int) -> str:
    """Format American odds with +/- prefix."""
    return f"{odds:+d}" if odds else "?"


def _is_ended(expiration_time: str) -> bool:
    """Check if a game has ended based on expected_expiration_time."""
    if not expiration_time:
        return False
    try:
        et = datetime.fromisoformat(expiration_time.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) > et
    except (ValueError, TypeError):
        return False


def _is_live(commence_time: str, expiration_time: str = "") -> bool:
    """Check if a game is currently in progress.

    A game is live if it has started (past commence_time) and
    expected_expiration_time hasn't passed yet.
    """
    if not commence_time:
        return False
    if _is_ended(expiration_time):
        return False
    try:
        now = datetime.now(timezone.utc)
        ct = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        return ct <= now
    except (ValueError, TypeError):
        return False


def _format_game_time_with_status(commence_time: str, expiration_time: str = "") -> str:
    """Format game time, showing LIVE/Final if started."""
    if not commence_time:
        return "TBD"
    # Check if game ended (expected_expiration_time passed)
    if _is_ended(expiration_time):
        return "Final"
    try:
        now = datetime.now(timezone.utc)
        ct = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        if ct <= now:
            return "\U0001f534 LIVE"
    except (ValueError, TypeError):
        pass
    return format_game_time(commence_time)


# ── Browse View (landing page) ────────────────────────────────────────


class BrowseView(discord.ui.View):
    """Landing page showing all available Kalshi markets."""

    def __init__(
        self,
        games_available: dict[str, dict],
        futures_available: dict[str, dict[str, bool]],
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.games_available = games_available
        self.futures_available = futures_available
        self.page = page
        self.message: discord.Message | None = None

        # Build all category options sorted properly
        self.all_options: list[discord.SelectOption] = []

        # Game sports — sorted: live first, then by next game time
        game_items = []
        for key, info in games_available.items():
            sport = SPORTS.get(key)
            if sport:
                has_live = info.get("has_live", False)
                next_time = info.get("next_time") or ""
                sort_key = (0 if has_live else 1, next_time or "9999")
                game_items.append((sort_key, key, info, sport))
        game_items.sort(key=lambda x: x[0])

        for _, key, info, sport in game_items:
            next_time = info.get("next_time")
            has_live = info.get("has_live", False)
            if has_live:
                desc = "LIVE now"
            elif next_time:
                desc = f"Next: {format_game_time(next_time)}"
            else:
                desc = "Games available"
            if len(desc) > 100:
                desc = desc[:100]
            label = sport["label"]
            if len(label) > 100:
                label = label[:97] + "..."
            self.all_options.append(discord.SelectOption(
                label=label,
                value=f"games:{key}",
                description=desc,
                emoji=_sport_emoji(key),
            ))

        # Futures sports (that don't already appear as games)
        for key, markets in futures_available.items():
            sports_key = FUTURES_TO_SPORTS.get(key)
            if sports_key and sports_key in games_available:
                continue
            if key in games_available:
                continue
            fut = FUTURES.get(key)
            label = fut["label"] if fut else key
            market_names = ", ".join(markets.keys())
            if len(market_names) > 90:
                market_names = market_names[:87] + "..."
            fut_sport_key = FUTURES_TO_SPORTS.get(key, key)
            self.all_options.append(discord.SelectOption(
                label=label,
                value=f"futures:{key}",
                description=market_names[:100] if market_names else "Futures",
                emoji=_sport_emoji(fut_sport_key),
            ))

        # Paginate dropdown: 25 options per page
        self.total_pages = max(1, (len(self.all_options) + 24) // 25)
        page_start = self.page * 25
        page_options = self.all_options[page_start:page_start + 25]

        if page_options:
            self.add_item(CategorySelect(page_options))

        # Add page buttons if more than one page
        if self.total_pages > 1:
            row = 1
            if self.page > 0:
                prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=row)
                prev_btn.callback = self._prev_page
                self.add_item(prev_btn)
            page_label = discord.ui.Button(
                label=f"Page {self.page + 1}/{self.total_pages}",
                style=discord.ButtonStyle.secondary, disabled=True, row=row,
            )
            self.add_item(page_label)
            if self.page < self.total_pages - 1:
                next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=row)
                next_btn.callback = self._next_page
                self.add_item(next_btn)

    async def _prev_page(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        view = BrowseView(self.games_available, self.futures_available, page=self.page - 1)
        embed = view.build_embed()
        view.message = self.message
        await interaction.edit_original_response(embed=embed, view=view)

    async def _next_page(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        view = BrowseView(self.games_available, self.futures_available, page=self.page + 1)
        embed = view.build_embed()
        view.message = self.message
        await interaction.edit_original_response(embed=embed, view=view)

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Kalshi Markets",
            color=discord.Color.blue(),
        )

        # Games section
        if self.games_available:
            game_lines = []
            # Sort: live first, then by next game time
            sorted_games = sorted(
                self.games_available.items(),
                key=lambda kv: (0 if kv[1].get("has_live") else 1, kv[1].get("next_time") or "9999"),
            )
            for key, info in sorted_games:
                sport = SPORTS.get(key)
                name = sport["label"] if sport else key
                next_time = info.get("next_time")
                has_live = info.get("has_live", False)
                line = f"{_sport_emoji(key)} **{name}**"
                if has_live:
                    line += " \U0001f534 LIVE"
                elif next_time:
                    line += f" — Next: {format_game_time(next_time)}"
                game_lines.append(line)
            total_sports = len(game_lines)
            games_text = "\n".join(game_lines)
            # Discord field limit is 1024 chars — truncate if needed
            if len(games_text) > 1000:
                truncated = []
                length = 0
                for line in game_lines:
                    if length + len(line) + 1 > 950:
                        break
                    truncated.append(line)
                    length += len(line) + 1
                remaining = total_sports - len(truncated)
                games_text = "\n".join(truncated) + f"\n*...and {remaining} more*"
            embed.add_field(
                name=f"Games ({total_sports} sports)",
                value=games_text,
                inline=False,
            )

        # Futures section (only show if not already represented in games)
        if self.futures_available:
            futures_lines = []
            for key, markets in self.futures_available.items():
                sports_key = FUTURES_TO_SPORTS.get(key)
                if sports_key and sports_key in self.games_available:
                    continue  # Already shown in games section
                if key in self.games_available:
                    continue
                fut = FUTURES.get(key)
                name = fut["label"] if fut else key
                market_names = ", ".join(markets.keys())
                fut_sport_key = FUTURES_TO_SPORTS.get(key, key)
                futures_lines.append(f"{_sport_emoji(fut_sport_key)} **{name}** — {market_names}")
            if futures_lines:
                futures_text = "\n".join(futures_lines)
                if len(futures_text) > 1000:
                    truncated = []
                    length = 0
                    for line in futures_lines:
                        if length + len(line) + 1 > 950:
                            break
                        truncated.append(line)
                        length += len(line) + 1
                    remaining = len(futures_lines) - len(truncated)
                    futures_text = "\n".join(truncated) + f"\n*...and {remaining} more*"
                embed.add_field(
                    name="Futures & Props",
                    value=futures_text,
                    inline=False,
                )

        if not self.games_available and not self.futures_available:
            embed.description = "No open markets right now."

        embed.set_footer(text="Select a category below to browse")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class CategorySelect(discord.ui.Select["BrowseView"]):
    """Dropdown to pick a category from the browse view."""

    def __init__(self, options: list[discord.SelectOption]) -> None:
        super().__init__(placeholder="Select a category...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        val = self.values[0]
        cat_type, key = val.split(":", 1)

        await interaction.response.defer()

        if cat_type == "games":
            # Go to sport hub (games + futures for this sport)
            await _show_sport_hub(interaction, key)
        elif cat_type == "futures":
            # Go to futures-only hub
            await _show_sport_hub(interaction, key)


# ── Sport Hub View (games + futures for one sport) ─────────────────────


class SportHubView(discord.ui.View):
    """Sport-specific view showing games dropdown + futures buttons."""

    def __init__(
        self,
        sport_key: str,
        sport_label: str,
        games: list[dict] | None = None,
        futures_markets: dict[str, str] | None = None,
        browse_data: tuple | None = None,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.sport_key = sport_key
        self.sport_label = sport_label
        self.games = games or []
        self.futures_markets = futures_markets or {}
        self.browse_data = browse_data
        self.message: discord.Message | None = None

        row = 0
        if self.games:
            self.add_item(KalshiGameSelect(self.games, row=row))
            row += 1

        # Futures buttons (up to 4 per row, max 2 rows)
        btn_count = 0
        for name, series_ticker in self.futures_markets.items():
            if btn_count >= 8:
                break
            self.add_item(FuturesButton(name, series_ticker, sport_label, row=row))
            btn_count += 1
            if btn_count % 4 == 0:
                row += 1

        # Back to browse button (last row)
        if self.browse_data:
            back_row = min(row + 1, 4)
            self.add_item(BrowseBackButton(self.browse_data, row=back_row))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{self.sport_label} — Kalshi",
            color=discord.Color.blue(),
        )
        if self.games:
            for g in self.games[:15]:
                home = g.get("home_team", "?")
                away = g.get("away_team", "?")
                time_str = _format_game_time_with_status(g.get("commence_time", ""), g.get("expiration_time", ""))
                embed.add_field(
                    name=f"{away} @ {home}",
                    value=time_str,
                    inline=False,
                )
        if self.futures_markets:
            futures_text = " | ".join(f"**{n}**" for n in self.futures_markets)
            embed.add_field(
                name="Futures & Props",
                value=futures_text,
                inline=False,
            )
        if not self.games and not self.futures_markets:
            embed.description = "No open markets for this sport."
        embed.set_footer(text="Select a game or futures market")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class FuturesButton(discord.ui.Button["SportHubView"]):
    """Button that opens a futures market options list."""

    def __init__(self, name: str, series_ticker: str, sport_label: str, row: int) -> None:
        label = name if len(name) <= 80 else name[:77] + "..."
        super().__init__(label=label, style=discord.ButtonStyle.success, emoji="\U0001f3c6", row=row)
        self.market_name = name
        self.series_ticker = series_ticker
        self.sport_label = sport_label

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        options = await kalshi_api.get_futures_markets(self.series_ticker)
        if not options:
            await interaction.followup.send(
                f"No open options for {self.market_name}.", ephemeral=True
            )
            return

        view = self.view
        title = f"{self.sport_label} — {self.market_name}"
        futures_view = FuturesOptionsView(
            options=options,
            title=title,
            series_ticker=self.series_ticker,
            hub_view=view,
        )
        embed = futures_view.build_embed()
        await interaction.edit_original_response(embed=embed, view=futures_view)


class BrowseBackButton(discord.ui.Button):
    """Returns to the browse view."""

    def __init__(self, browse_data: tuple, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.browse_data = browse_data

    async def callback(self, interaction: discord.Interaction) -> None:
        games_available, futures_available = self.browse_data
        view = BrowseView(games_available, futures_available)
        embed = view.build_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


# ── Futures Options View (paginated list of options) ────────────────────


class FuturesOptionsView(discord.ui.View):
    """Paginated dropdown of multi-outcome options (teams/players) for a futures market."""

    def __init__(
        self,
        options: list[dict],
        title: str,
        series_ticker: str,
        hub_view: discord.ui.View | None = None,
        page: int = 0,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.all_options = options
        self.title = title
        self.series_ticker = series_ticker
        self.hub_view = hub_view
        self.page = page
        self.message: discord.Message | None = None

        self._build_page()

    def _build_page(self) -> None:
        start = self.page * OPTIONS_PER_PAGE
        end = start + OPTIONS_PER_PAGE
        page_options = self.all_options[start:end]
        total_pages = max(1, (len(self.all_options) + OPTIONS_PER_PAGE - 1) // OPTIONS_PER_PAGE)

        # Row 0: Options dropdown
        select_options = []
        for i, opt in enumerate(page_options):
            odds_str = _fmt_american(opt["american_odds"])
            prob_pct = f"{opt['yes_price'] * 100:.0f}%"
            label = opt["title"]
            if len(label) > 90:
                label = label[:87] + "..."
            desc = f"{odds_str} ({prob_pct})"
            select_options.append(discord.SelectOption(
                label=label,
                value=str(start + i),
                description=desc,
            ))

        if select_options:
            self.add_item(OptionsSelect(select_options, self.all_options))

        # Row 1: Pagination buttons
        if total_pages > 1:
            if self.page > 0:
                self.add_item(PrevPageButton(row=1))
            if self.page < total_pages - 1:
                self.add_item(NextPageButton(row=1))

        # Back button
        self.add_item(FuturesBackButton(self.hub_view, row=2))

    def build_embed(self) -> discord.Embed:
        total = len(self.all_options)
        total_pages = max(1, (total + OPTIONS_PER_PAGE - 1) // OPTIONS_PER_PAGE)
        start = self.page * OPTIONS_PER_PAGE

        embed = discord.Embed(
            title=self.title,
            color=discord.Color.gold(),
        )

        # Show top options as fields
        page_options = self.all_options[start:start + OPTIONS_PER_PAGE]
        lines = []
        for i, opt in enumerate(page_options, start=start + 1):
            odds_str = _fmt_american(opt["american_odds"])
            prob_pct = f"{opt['yes_price'] * 100:.0f}%"
            lines.append(f"**{i}.** {opt['title']} — {odds_str} ({prob_pct})")

        if lines:
            # Split into chunks of ~10 to avoid field length limits
            for chunk_start in range(0, len(lines), 10):
                chunk = lines[chunk_start:chunk_start + 10]
                name = "Options" if chunk_start == 0 else "\u200b"
                embed.add_field(name=name, value="\n".join(chunk), inline=False)

        footer = f"Page {self.page + 1}/{total_pages} — {total} options"
        embed.set_footer(text=footer)
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


class OptionsSelect(discord.ui.Select["FuturesOptionsView"]):
    """Dropdown of futures options (teams/players)."""

    def __init__(self, options: list[discord.SelectOption], all_options: list[dict]) -> None:
        super().__init__(placeholder="Pick an option to bet on...", options=options, row=0)
        self.all_options = all_options

    async def callback(self, interaction: discord.Interaction) -> None:
        idx = int(self.values[0])
        if idx < 0 or idx >= len(self.all_options):
            await interaction.response.send_message("Invalid option.", ephemeral=True)
            return

        option = self.all_options[idx]
        view = self.view
        title = view.title if view else "Futures"
        modal = FuturesBetModal(option, title)
        await interaction.response.send_modal(modal)


class PrevPageButton(discord.ui.Button["FuturesOptionsView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Prev", style=discord.ButtonStyle.secondary, emoji="\u25c0", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        new_view = FuturesOptionsView(
            options=view.all_options,
            title=view.title,
            series_ticker=view.series_ticker,
            hub_view=view.hub_view,
            page=view.page - 1,
        )
        embed = new_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=new_view)


class NextPageButton(discord.ui.Button["FuturesOptionsView"]):
    def __init__(self, row: int) -> None:
        super().__init__(label="Next", style=discord.ButtonStyle.secondary, emoji="\u25b6", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        new_view = FuturesOptionsView(
            options=view.all_options,
            title=view.title,
            series_ticker=view.series_ticker,
            hub_view=view.hub_view,
            page=view.page + 1,
        )
        embed = new_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=new_view)


class FuturesBackButton(discord.ui.Button["FuturesOptionsView"]):
    """Returns to the sport hub view."""

    def __init__(self, hub_view: discord.ui.View | None, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.hub_view = hub_view

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.hub_view and isinstance(self.hub_view, SportHubView):
            hub = self.hub_view
            # Rebuild the hub view
            new_hub = SportHubView(
                sport_key=hub.sport_key,
                sport_label=hub.sport_label,
                games=hub.games,
                futures_markets=hub.futures_markets,
                browse_data=hub.browse_data,
            )
            embed = new_hub.build_embed()
            try:
                await interaction.response.edit_message(embed=embed, view=new_hub)
            except discord.NotFound:
                pass
        else:
            await interaction.response.defer()


# ── Futures Bet Modal ──────────────────────────────────────────────────


class FuturesBetModal(discord.ui.Modal, title="Place Futures Bet"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, option: dict, market_title: str) -> None:
        super().__init__()
        self.option = option
        self.market_title = market_title

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message(
                "Invalid amount — enter a whole number.", ephemeral=True
            )
            return
        if amount <= 0:
            await interaction.response.send_message(
                "Bet amount must be positive.", ephemeral=True
            )
            return

        await interaction.response.defer()

        option = self.option
        market_ticker = option["ticker"]
        decimal_odds = option["decimal_odds"]
        american_odds = option["american_odds"]
        pick_display = option["title"]
        close_time = option.get("close_time")
        event_ticker = option.get("event_ticker", "")

        if close_time:
            try:
                ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                if ct <= datetime.now(timezone.utc):
                    await interaction.followup.send("This market has already closed.", ephemeral=True)
                    return
            except (ValueError, TypeError):
                pass

        bet_id = await betting_service.place_kalshi_bet(
            user_id=interaction.user.id,
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            pick="yes",
            amount=amount,
            odds=decimal_odds,
            title=self.market_title,
            close_time=close_time,
            pick_display=pick_display,
        )

        if bet_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        payout = round(amount * decimal_odds, 2)

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#K{bet_id}", inline=True)
        embed.add_field(name="Market", value=self.market_title, inline=True)
        embed.add_field(name="Pick", value=pick_display, inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=_fmt_american(american_odds), inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)


# ── Game-level UI components (existing) ───────────────────────────────


class KalshiGameSelect(discord.ui.Select):
    """Dropdown listing games — shows bet type buttons when selected."""

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
            desc = _format_game_time_with_status(g.get("commence_time", ""), g.get("expiration_time", ""))
            if len(desc) > 100:
                desc = desc[:100]
            self.games_map[game_id] = g
            options.append(discord.SelectOption(label=label, value=game_id, description=desc))
        super().__init__(placeholder="Select a game to bet on...", options=options, row=row)

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

        sport_key = game.get("sport_key") or (view.sport_key if hasattr(view, "sport_key") else "NBA")
        parsed = await kalshi_api.get_game_odds(sport_key, game)

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")
        fmt = _fmt_american

        # Build a new view for bet type selection
        bet_view = GameBetTypeView(game, parsed, view)
        time_str = _format_game_time_with_status(game.get("commence_time", ""), game.get("expiration_time", ""))
        embed = discord.Embed(
            title=game.get("sport_title", ""),
            description=f"**{format_matchup(home, away)}**\n{time_str}",
            color=discord.Color.blue(),
        )
        await interaction.edit_original_response(embed=embed, view=bet_view)


class GameBetTypeView(discord.ui.View):
    """View showing bet type buttons for a specific game."""

    def __init__(self, game: dict, parsed: dict, hub_view: discord.ui.View, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.game = game
        self.hub_view = hub_view

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")
        fmt = _fmt_american

        # Row 0: Moneyline
        row = 0
        if "home" in parsed:
            self.add_item(KalshiBetTypeButton(
                "home", f"Home {fmt(parsed['home']['american'])}",
                game, parsed["home"], row=row,
            ))
        if "away" in parsed:
            self.add_item(KalshiBetTypeButton(
                "away", f"Away {fmt(parsed['away']['american'])}",
                game, parsed["away"], row=row,
            ))

        # Row 1: Spreads
        row = 1
        if "spread_home" in parsed:
            sh = parsed["spread_home"]
            self.add_item(KalshiBetTypeButton(
                "spread_home", f"{home} {sh['point']:+g} ({fmt(sh['american'])})",
                game, sh, row=row,
            ))
        if "spread_away" in parsed:
            sa = parsed["spread_away"]
            self.add_item(KalshiBetTypeButton(
                "spread_away", f"{away} {sa['point']:+g} ({fmt(sa['american'])})",
                game, sa, row=row,
            ))

        # Row 2: Totals
        row = 2
        if "over" in parsed:
            ov = parsed["over"]
            self.add_item(KalshiBetTypeButton(
                "over", f"Over {ov['point']:g} ({fmt(ov['american'])})",
                game, ov, row=row,
            ))
        if "under" in parsed:
            un = parsed["under"]
            self.add_item(KalshiBetTypeButton(
                "under", f"Under {un['point']:g} ({fmt(un['american'])})",
                game, un, row=row,
            ))

        # Row 3: Back button
        self.add_item(GameBackButton(hub_view, row=3))


class KalshiBetTypeButton(discord.ui.Button["GameBetTypeView"]):
    """A button representing one bet type (home, away, spread, etc.)."""

    def __init__(self, pick_key: str, label: str, game: dict, odds_entry: dict, row: int) -> None:
        emoji = PICK_EMOJI.get(pick_key)
        super().__init__(label=label, style=discord.ButtonStyle.primary, emoji=emoji, row=row)
        self.pick_key = pick_key
        self.game = game
        self.odds_entry = odds_entry

    async def callback(self, interaction: discord.Interaction) -> None:
        modal = KalshiBetAmountModal(self.game, self.pick_key, self.odds_entry)
        await interaction.response.send_modal(modal)


class GameBackButton(discord.ui.Button["GameBetTypeView"]):
    """Returns to the sport hub view."""

    def __init__(self, hub_view: discord.ui.View, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self._hub_view = hub_view

    async def callback(self, interaction: discord.Interaction) -> None:
        if isinstance(self._hub_view, SportHubView):
            hub = self._hub_view
            new_hub = SportHubView(
                sport_key=hub.sport_key,
                sport_label=hub.sport_label,
                games=hub.games,
                futures_markets=hub.futures_markets,
                browse_data=hub.browse_data,
            )
            embed = new_hub.build_embed()
            try:
                await interaction.response.edit_message(embed=embed, view=new_hub)
            except discord.NotFound:
                pass
        else:
            await interaction.response.defer()


class KalshiBetAmountModal(discord.ui.Modal, title="Place Bet"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, game: dict, pick_key: str, odds_entry: dict) -> None:
        super().__init__()
        self.game = game
        self.pick_key = pick_key
        self.odds_entry = odds_entry

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message(
                "Invalid amount — enter a whole number.", ephemeral=True
            )
            return
        if amount <= 0:
            await interaction.response.send_message(
                "Bet amount must be positive.", ephemeral=True
            )
            return

        await interaction.response.defer()

        game = self.game
        pick_key = self.pick_key
        odds_entry = self.odds_entry
        decimal_odds = odds_entry["decimal"]
        american_odds = odds_entry["american"]

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")

        kalshi_markets = game.get("_kalshi_markets", {})
        market_ticker, kalshi_pick = _resolve_kalshi_bet(
            pick_key, kalshi_markets, odds_entry
        )

        if not market_ticker:
            await interaction.followup.send(
                "Could not find the Kalshi market for this bet.", ephemeral=True
            )
            return

        pick_display = _build_pick_display(pick_key, home, away, odds_entry)

        market = kalshi_markets.get("home") or kalshi_markets.get("away") or {}
        close_time = market.get("close_time") or market.get("expected_expiration_time")

        if close_time:
            try:
                ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                if ct <= datetime.now(timezone.utc):
                    await interaction.followup.send("This market has already closed.", ephemeral=True)
                    return
            except (ValueError, TypeError):
                pass

        bet_title = f"{format_matchup(home, away)} ({game.get('sport_title', '')})"

        bet_id = await betting_service.place_kalshi_bet(
            user_id=interaction.user.id,
            market_ticker=market_ticker,
            event_ticker=game.get("id", ""),
            pick=kalshi_pick,
            amount=amount,
            odds=decimal_odds,
            title=bet_title,
            close_time=close_time,
            pick_display=pick_display,
        )

        if bet_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        payout = round(amount * decimal_odds, 2)

        time_str = _format_game_time_with_status(game.get("commence_time", ""), game.get("expiration_time", ""))

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#K{bet_id}", inline=True)
        embed.add_field(name="Game", value=format_matchup(home, away), inline=True)
        embed.add_field(name="Time", value=time_str, inline=True)
        embed.add_field(name="Pick", value=pick_display, inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=_fmt_american(american_odds), inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)


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


async def _show_sport_hub(interaction: discord.Interaction, sport_key: str) -> None:
    """Navigate to a sport hub view showing games + futures."""
    sport_label = sport_key
    games = []
    futures_markets = {}

    # Resolve cross-references: sport_key could be a SPORTS ticker or FUTURES key
    games_key = sport_key if sport_key in SPORTS else FUTURES_TO_SPORTS.get(sport_key)
    futures_key = sport_key if sport_key in FUTURES else SPORTS_TO_FUTURES.get(sport_key)

    if games_key and games_key in SPORTS:
        sport_label = SPORTS[games_key]["label"]
        games = await kalshi_api.get_sport_games(games_key)
    elif futures_key and futures_key in FUTURES:
        sport_label = FUTURES[futures_key]["label"]

    if futures_key and futures_key in FUTURES:
        futures_markets = FUTURES[futures_key]["markets"]

    view = SportHubView(
        sport_key=sport_key,
        sport_label=sport_label,
        games=games,
        futures_markets=futures_markets,
    )
    embed = view.build_embed()
    await interaction.edit_original_response(embed=embed, view=view)


# ── Games List View (for /games and /livescores) ──────────────────────

GAMES_PER_PAGE = 10


class GamesListView(discord.ui.View):
    """Compact chronological game list with game select for quick betting."""

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
        start = page * GAMES_PER_PAGE
        page_games = all_games[start:start + GAMES_PER_PAGE]

        # Row 0: Game select
        if page_games:
            self.add_item(KalshiGameSelect(page_games, row=0))

        # Row 1: Pagination
        if total_pages > 1:
            if page > 0:
                self.add_item(GamesPageButton("prev", page - 1, row=1))
            if page < total_pages - 1:
                self.add_item(GamesPageButton("next", page + 1, row=1))

    def _find_score(self, game: dict) -> dict | None:
        """Match a Kalshi game to odds-api scores by team name."""
        if not self.scores:
            return None
        home = game.get("home_team", "").lower()
        away = game.get("away_team", "").lower()
        if not home or not away:
            return None
        for score_data in self.scores.values():
            s_home = (score_data.get("home_team") or "").lower()
            s_away = (score_data.get("away_team") or "").lower()
            if not s_home or not s_away:
                continue
            # Match if team names overlap (handles "Celtics" vs "Boston Celtics")
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
            embed.description = "No live games right now." if self.is_live_mode else "No upcoming games right now."
            return embed

        lines = []
        for g in page_games:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            sport = g.get("sport_title", "")

            # Extract moneyline odds from _kalshi_markets
            odds_str = ""
            kalshi_m = g.get("_kalshi_markets", {})
            home_m = kalshi_m.get("home")
            away_m = kalshi_m.get("away")
            if home_m and away_m:
                home_price = float(home_m.get("yes_ask_dollars") or home_m.get("last_price_dollars") or "0")
                away_price = float(away_m.get("yes_ask_dollars") or away_m.get("last_price_dollars") or "0")
                if home_price > 0 and away_price > 0:
                    home_am = _fmt_american(_decimal_to_american(round(1.0 / home_price, 3)))
                    away_am = _fmt_american(_decimal_to_american(round(1.0 / away_price, 3)))
                    odds_str = f"  ({away_am} / {home_am})"

            score = self._find_score(g) if self.is_live_mode else None
            if score and score.get("started") and score.get("home_score") is not None:
                score_str = f"**{away}** {score['away_score']} - {score['home_score']} **{home}**"
                if score.get("completed"):
                    score_str += "  (Final)"
                else:
                    score_str = f"\U0001f534 {score_str}"
                # Time info for live games
                time_parts = []
                commence = g.get("commence_time", "")
                if commence:
                    time_parts.append(f"Started {format_game_time(commence)}")
                exp = g.get("expiration_time", "")
                if exp:
                    time_parts.append(f"Ends ~{format_game_time(exp)}")
                time_line = " · ".join(time_parts) if time_parts else ""
                detail = f"{sport}{odds_str}"
                if time_line:
                    detail += f"\n{time_line}"
                lines.append(f"{score_str}\n{detail}")
            else:
                exp = g.get("expiration_time", "")
                time_str = _format_game_time_with_status(g.get("commence_time", ""), exp)
                exp_str = f" — Ends ~{format_game_time(exp)}" if exp else ""
                lines.append(f"**{away}** @ **{home}**{odds_str}\n{sport} · {time_str}{exp_str}")

        embed.description = "\n\n".join(lines)

        parts = []
        if total_pages > 1:
            parts.append(f"Page {self.page + 1}/{total_pages}")
        parts.append(f"{total} game{'s' if total != 1 else ''}")
        parts.append("Select a game to bet")
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
            odds_str = _fmt_american(leg.get("american", 0))
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
            desc = _format_game_time_with_status(g.get("commence_time", ""), g.get("expiration_time", ""))
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
        time_str = _format_game_time_with_status(game.get("commence_time", ""), game.get("expiration_time", ""))
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
        fmt = _fmt_american

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
            odds_str = _fmt_american(leg.get("american", 0))
            embed.add_field(
                name=f"Leg {i}",
                value=f"{leg['pick_display']} — {odds_str}",
                inline=False,
            )

        pv.clear_items()
        pv.stop()
        await interaction.edit_original_response(embed=embed, view=None)


# ── Cog ───────────────────────────────────────────────────────────────


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

    async def cog_unload(self) -> None:
        self.check_kalshi_results.cancel()
        self.refresh_discovery.cancel()
        self.refresh_sports_loop.cancel()
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

    # ── /kalshi ───────────────────────────────────────────────────────

    @app_commands.command(name="kalshi", description="Browse sports odds and bet via Kalshi")
    @app_commands.describe(sport="Sport to view (leave blank to browse all)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def kalshi(
        self,
        interaction: discord.Interaction,
        sport: str | None = None,
    ) -> None:
        await interaction.response.defer()
        log.info("/kalshi called by %s (sport=%s)", interaction.user, sport)

        if sport is None:
            # Browse mode — discover what's available
            discovery = await kalshi_api.discover_available()
            view = BrowseView(discovery["games"], discovery["futures"])
            embed = view.build_embed()
            msg = await interaction.followup.send(embed=embed, view=view)
            view.message = msg
            return

        sport_key = sport
        valid_sport = sport_key in SPORTS or sport_key in FUTURES
        if not valid_sport:
            await interaction.followup.send(
                "Unknown sport. Use the autocomplete to pick one.", ephemeral=True
            )
            return

        # Sport-specific hub — resolve cross-references
        games_key = sport_key if sport_key in SPORTS else FUTURES_TO_SPORTS.get(sport_key)
        futures_key = sport_key if sport_key in FUTURES else SPORTS_TO_FUTURES.get(sport_key)

        sport_label = sport_key
        if games_key and games_key in SPORTS:
            sport_label = SPORTS[games_key]["label"]
        elif futures_key and futures_key in FUTURES:
            sport_label = FUTURES[futures_key]["label"]

        games = []
        if games_key and games_key in SPORTS:
            games = await kalshi_api.get_sport_games(games_key)

        futures_markets = {}
        if futures_key and futures_key in FUTURES:
            futures_markets = FUTURES[futures_key]["markets"]

        if not games and not futures_markets:
            await interaction.followup.send(
                f"No open {sport_label} markets on Kalshi right now."
            )
            return

        view = SportHubView(
            sport_key=sport_key,
            sport_label=sport_label,
            games=games,
            futures_markets=futures_markets,
        )
        embed = view.build_embed()
        msg = await interaction.followup.send(embed=embed, view=view)
        view.message = msg

    # ── /games ─────────────────────────────────────────────────────────

    @app_commands.command(name="games", description="Browse games to bet on (includes live)")
    @app_commands.describe(sport="Filter by sport (leave blank for all)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def games(self, interaction: discord.Interaction, sport: str | None = None) -> None:
        await interaction.response.defer()

        if sport and sport in SPORTS:
            all_games = await kalshi_api.get_sport_games(sport)
        else:
            all_games = await kalshi_api.get_all_games()

        if not all_games:
            await interaction.followup.send("No games right now.")
            return

        # Sort: live games first, then upcoming by commence_time
        all_games.sort(key=lambda g: (
            0 if _is_live(g.get("commence_time", ""), g.get("expiration_time", "")) else 1,
            g.get("commence_time", "9999"),
        ))

        view = GamesListView(all_games=all_games, title="Games")
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

        # Sort: live first, then upcoming
        games.sort(key=lambda g: (
            0 if _is_live(g.get("commence_time", ""), g.get("expiration_time", "")) else 1,
            g.get("commence_time", "9999"),
        ))

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

    # ── /live ─────────────────────────────────────────────────────────

    @app_commands.command(name="live", description="View live games with scores")
    @app_commands.describe(sport="Filter by sport (leave blank for all)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def live(self, interaction: discord.Interaction, sport: str | None = None) -> None:
        await interaction.response.defer()

        if sport and sport in SPORTS:
            all_games = await kalshi_api.get_sport_games(sport)
        else:
            all_games = await kalshi_api.get_all_games()

        # Filter to live only (started but not finished)
        live_games = [g for g in all_games if _is_live(g.get("commence_time", ""), g.get("expiration_time", ""))]

        if not live_games:
            await interaction.followup.send("No live games right now.")
            return

        # Fetch cached scores (no API calls) for live sports
        scores: dict[str, dict] = {}
        sport_keys_needed = set()
        for g in live_games:
            odds_key = KALSHI_TO_ODDS_API.get(g.get("sport_key", ""))
            if odds_key:
                sport_keys_needed.add(odds_key)

        for odds_key in sport_keys_needed:
            cached = await self.sports_api.get_cached_scores(odds_key)
            for game_data in cached:
                parsed = self.sports_api._parse_fixture_status(game_data)
                eid = game_data.get("id", "")
                if eid:
                    scores[eid] = parsed

        view = GamesListView(
            all_games=live_games, title="\U0001f534 Live Games",
            is_live=True, scores=scores,
        )
        embed = view.build_embed()
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
