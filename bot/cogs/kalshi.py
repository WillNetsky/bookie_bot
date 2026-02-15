import logging
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.services import betting_service
from bot.services.kalshi_api import kalshi_api, SPORTS, FUTURES
from bot.cogs.betting import (
    format_matchup, format_game_time, PICK_EMOJI,
)

log = logging.getLogger(__name__)

OPTIONS_PER_PAGE = 25


def _fmt_american(odds: int) -> str:
    """Format American odds with +/- prefix."""
    return f"{odds:+d}" if odds else "?"


# ── Browse View (landing page) ────────────────────────────────────────


class BrowseView(discord.ui.View):
    """Landing page showing all available Kalshi markets."""

    def __init__(
        self,
        games_available: dict[str, bool],
        futures_available: dict[str, dict[str, bool]],
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.games_available = games_available
        self.futures_available = futures_available
        self.message: discord.Message | None = None

        # Build category options for the dropdown
        options = []

        # Game sports
        for key in games_available:
            sport = SPORTS.get(key)
            if sport:
                options.append(discord.SelectOption(
                    label=sport["label"],
                    value=f"games:{key}",
                    description="Games",
                    emoji="\U0001f3c8",
                ))

        # Futures sports (that don't already appear as games)
        for key, markets in futures_available.items():
            if key not in games_available:
                fut = FUTURES.get(key)
                label = fut["label"] if fut else key
                market_names = ", ".join(markets.keys())
                if len(market_names) > 90:
                    market_names = market_names[:87] + "..."
                options.append(discord.SelectOption(
                    label=label,
                    value=f"futures:{key}",
                    description=market_names[:100] if market_names else "Futures",
                    emoji="\U0001f3c6",
                ))

        if options:
            self.add_item(CategorySelect(options[:25]))

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Kalshi Markets",
            color=discord.Color.blue(),
        )

        # Games section
        if self.games_available:
            game_lines = []
            for key in self.games_available:
                sport = SPORTS.get(key)
                name = sport["label"] if sport else key
                game_lines.append(f"\U0001f3c8 **{name}**")
            embed.add_field(
                name="Games",
                value="\n".join(game_lines),
                inline=False,
            )

        # Futures section
        if self.futures_available:
            futures_lines = []
            for key, markets in self.futures_available.items():
                fut = FUTURES.get(key)
                name = fut["label"] if fut else key
                market_names = ", ".join(markets.keys())
                futures_lines.append(f"\U0001f3c6 **{name}** — {market_names}")
            embed.add_field(
                name="Futures & Props",
                value="\n".join(futures_lines),
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
                display_date = format_game_time(g.get("commence_time", ""))
                embed.add_field(
                    name=f"{away} @ {home}",
                    value=display_date,
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


class KalshiGameSelect(discord.ui.Select["SportHubView"]):
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
            desc = format_game_time(g.get("commence_time", ""))
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

        sport_key = view.sport_key if hasattr(view, "sport_key") else "NBA"
        parsed = await kalshi_api.get_game_odds(sport_key, game)

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")
        fmt = _fmt_american

        # Build a new view for bet type selection
        bet_view = GameBetTypeView(game, parsed, view)
        embed = discord.Embed(
            title=game.get("sport_title", ""),
            description=f"Pick a bet type for **{format_matchup(home, away)}**",
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

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#K{bet_id}", inline=True)
        embed.add_field(name="Game", value=format_matchup(home, away), inline=True)
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

    if sport_key in SPORTS:
        sport_label = SPORTS[sport_key]["label"]
        games = await kalshi_api.get_sport_games(sport_key)
    elif sport_key in FUTURES:
        sport_label = FUTURES[sport_key]["label"]

    if sport_key in FUTURES:
        futures_markets = FUTURES[sport_key]["markets"]

    view = SportHubView(
        sport_key=sport_key,
        sport_label=sport_label,
        games=games,
        futures_markets=futures_markets,
    )
    embed = view.build_embed()
    await interaction.edit_original_response(embed=embed, view=view)


# ── Cog ───────────────────────────────────────────────────────────────


class KalshiCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.check_kalshi_results.start()

    async def cog_unload(self) -> None:
        self.check_kalshi_results.cancel()
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
            all_keys = sorted(set(list(SPORTS.keys()) + list(FUTURES.keys())))
            await interaction.followup.send(
                f"Unknown sport. Available: {', '.join(all_keys)}", ephemeral=True
            )
            return

        # Sport-specific hub
        sport_label = sport_key
        if sport_key in SPORTS:
            sport_label = SPORTS[sport_key]["label"]
        elif sport_key in FUTURES:
            sport_label = FUTURES[sport_key]["label"]

        games = []
        if sport_key in SPORTS:
            games = await kalshi_api.get_sport_games(sport_key)

        futures_markets = {}
        if sport_key in FUTURES:
            futures_markets = FUTURES[sport_key]["markets"]

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

    # ── Resolution loop ───────────────────────────────────────────────

    @tasks.loop(minutes=15)
    async def check_kalshi_results(self) -> None:
        from bot.db import models

        tickers = await models.get_pending_kalshi_market_tickers()
        if not tickers:
            return

        log.info("Checking %d pending Kalshi market(s)...", len(tickers))

        channel = None
        if BET_RESULTS_CHANNEL_ID:
            channel = self.bot.get_channel(BET_RESULTS_CHANNEL_ID)

        for ticker in tickers:
            try:
                market = await kalshi_api.get_market(ticker)
                if not market:
                    continue

                status = market.get("status", "")
                if status != "settled":
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

                log.info("Resolved Kalshi market %s -> %s", ticker, winning_side)

            except Exception:
                log.exception("Error checking Kalshi market %s", ticker)

    @check_kalshi_results.before_loop
    async def before_check_kalshi(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(KalshiCog(bot))
