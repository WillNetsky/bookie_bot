import logging
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.services import betting_service
from bot.services.kalshi_api import kalshi_api, SPORTS
from bot.cogs.betting import (
    format_matchup, format_game_time, PICK_EMOJI, PICK_LABELS,
)

log = logging.getLogger(__name__)


def _fmt_american(odds: int) -> str:
    """Format American odds with +/- prefix."""
    return f"{odds:+d}" if odds else "?"


def _price_to_american(price_dollars: str | None) -> str:
    """Convert Kalshi dollar price to American odds string."""
    if not price_dollars:
        return "?"
    try:
        prob = float(price_dollars)
        if prob <= 0 or prob >= 1:
            return "?"
        if prob >= 0.5:
            american = -(prob / (1 - prob)) * 100
            return f"{american:+.0f}"
        else:
            american = ((1 - prob) / prob) * 100
            return f"+{american:.0f}"
    except (ValueError, TypeError):
        return "?"


def _get_no_price(market: dict) -> str | None:
    """Get the no-side price, deriving from yes if needed."""
    no_price = market.get("no_ask_dollars")
    if no_price:
        return no_price
    yes_price = market.get("yes_ask_dollars") or market.get("last_price_dollars")
    if yes_price:
        try:
            return f"{1.0 - float(yes_price):.2f}"
        except (ValueError, TypeError):
            pass
    return None


# ── Interactive UI components ─────────────────────────────────────────


class KalshiGameSelect(discord.ui.Select["KalshiOddsView"]):
    """Dropdown listing games — shows bet type buttons when selected."""

    def __init__(self, games: list[dict]) -> None:
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
        super().__init__(placeholder="Select a game to bet on...", options=options, row=0)

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

        # Fetch full odds for this game
        parsed = await kalshi_api.get_game_odds(view.sport_key, game)

        home = game.get("home_team", "?")
        away = game.get("away_team", "?")
        fmt = _fmt_american

        view.clear_items()

        # Row 0: Moneyline
        row = 0
        if "home" in parsed:
            view.add_item(KalshiBetTypeButton(
                "home", f"Home {fmt(parsed['home']['american'])}",
                game, parsed["home"], row=row,
            ))
        if "away" in parsed:
            view.add_item(KalshiBetTypeButton(
                "away", f"Away {fmt(parsed['away']['american'])}",
                game, parsed["away"], row=row,
            ))

        # Row 1: Spreads
        row = 1
        if "spread_home" in parsed:
            sh = parsed["spread_home"]
            view.add_item(KalshiBetTypeButton(
                "spread_home", f"{home} {sh['point']:+g} ({fmt(sh['american'])})",
                game, sh, row=row,
            ))
        if "spread_away" in parsed:
            sa = parsed["spread_away"]
            view.add_item(KalshiBetTypeButton(
                "spread_away", f"{away} {sa['point']:+g} ({fmt(sa['american'])})",
                game, sa, row=row,
            ))

        # Row 2: Totals
        row = 2
        if "over" in parsed:
            ov = parsed["over"]
            view.add_item(KalshiBetTypeButton(
                "over", f"Over {ov['point']:g} ({fmt(ov['american'])})",
                game, ov, row=row,
            ))
        if "under" in parsed:
            un = parsed["under"]
            view.add_item(KalshiBetTypeButton(
                "under", f"Under {un['point']:g} ({fmt(un['american'])})",
                game, un, row=row,
            ))

        # Row 3: Back button
        view.add_item(KalshiBackButton(row=3))

        embed = discord.Embed(
            title=f"{game.get('sport_title', '')}",
            description=f"Pick a bet type for **{format_matchup(home, away)}**",
            color=discord.Color.blue(),
        )
        await interaction.edit_original_response(embed=embed, view=view)


class KalshiBetTypeButton(discord.ui.Button["KalshiOddsView"]):
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


class KalshiBackButton(discord.ui.Button["KalshiOddsView"]):
    """Returns to the game select dropdown."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.clear_items()
        view.add_item(KalshiGameSelect(view.games))
        embed = view.build_games_embed()
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.NotFound:
            pass


class KalshiOddsView(discord.ui.View):
    """View attached to /kalshi output — game select + bet type buttons."""

    def __init__(self, games: list[dict], sport_key: str, sport_label: str, timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.games = games
        self.sport_key = sport_key
        self.sport_label = sport_label
        self.message: discord.Message | None = None
        self.add_item(KalshiGameSelect(games))

    def build_games_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{self.sport_label} — Kalshi",
            color=discord.Color.blue(),
        )
        for g in self.games[:25]:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            display_date = format_game_time(g.get("commence_time", ""))
            embed.add_field(
                name=f"{away} @ {home}",
                value=display_date,
                inline=False,
            )
        embed.set_footer(text="Select a game below to bet")
        return embed

    async def on_timeout(self) -> None:
        self.clear_items()
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.NotFound:
                pass


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

        # Determine the underlying Kalshi market and yes/no pick
        kalshi_markets = game.get("_kalshi_markets", {})
        market_ticker, kalshi_pick = _resolve_kalshi_bet(
            pick_key, kalshi_markets, odds_entry
        )

        if not market_ticker:
            await interaction.followup.send(
                "Could not find the Kalshi market for this bet.", ephemeral=True
            )
            return

        # Build pick_display for human-readable display
        pick_display = _build_pick_display(pick_key, home, away, odds_entry)

        # Get close_time from market
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

        # Build a title from the matchup
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


def _resolve_kalshi_bet(
    pick_key: str, kalshi_markets: dict, odds_entry: dict
) -> tuple[str | None, str]:
    """Map a sports-style pick to a Kalshi market_ticker and yes/no pick.

    Returns (market_ticker, "yes" or "no").
    """
    if pick_key == "home":
        m = kalshi_markets.get("home")
        return (m["ticker"], "yes") if m else (None, "yes")
    elif pick_key == "away":
        m = kalshi_markets.get("away")
        return (m["ticker"], "yes") if m else (None, "yes")
    elif pick_key in ("spread_home", "spread_away", "over", "under"):
        # For spread/total, the market_ticker is stored in odds_entry by the API
        ticker = odds_entry.get("_market_ticker")
        if ticker:
            kalshi_pick = odds_entry.get("_kalshi_pick", "yes")
            return (ticker, kalshi_pick)
        # Fallback: use home market for spread_home, away for spread_away
        if pick_key == "spread_home":
            m = kalshi_markets.get("home")
        elif pick_key == "spread_away":
            m = kalshi_markets.get("away")
        else:
            # Over/under — use the market from odds_entry
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
        current_lower = current.lower()
        for key, sport in SPORTS.items():
            label = sport["label"]
            if current_lower in label.lower() or current_lower in key.lower():
                choices.append(app_commands.Choice(name=label, value=key))
            if len(choices) >= 25:
                break
        return choices

    # ── /kalshi ───────────────────────────────────────────────────────

    @app_commands.command(name="kalshi", description="Browse sports odds and bet via Kalshi")
    @app_commands.describe(sport="Sport to view (leave blank for NBA)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def kalshi(
        self,
        interaction: discord.Interaction,
        sport: str | None = None,
    ) -> None:
        await interaction.response.defer()

        sport_key = sport or "NBA"
        if sport_key not in SPORTS:
            await interaction.followup.send(
                f"Unknown sport. Available: {', '.join(SPORTS.keys())}", ephemeral=True
            )
            return

        sport_info = SPORTS[sport_key]

        games = await kalshi_api.get_sport_games(sport_key)
        if not games:
            await interaction.followup.send(
                f"No open {sport_info['label']} games on Kalshi right now."
            )
            return

        view = KalshiOddsView(games, sport_key, sport_info["label"])
        embed = view.build_games_embed()
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

                # Determine winner
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
                        title = bet.get("title") or ticker

                        embed = discord.Embed(
                            title=f"{emoji} Bet #K{bet['id']} — {result_text.upper()}",
                            color=discord.Color.green() if won else discord.Color.red(),
                        )
                        embed.add_field(name="Bettor", value=name, inline=True)
                        embed.add_field(name="Market", value=title[:256], inline=False)
                        embed.add_field(name="Pick", value=pick_display, inline=True)
                        embed.add_field(name="Wager", value=f"${bet['amount']:.2f}", inline=True)
                        embed.add_field(name="Payout", value=f"${payout:.2f}", inline=True)
                        embed.add_field(name="Result", value=f"Market settled **{winning_side.upper()}**", inline=False)
                        try:
                            await channel.send(embed=embed)
                        except discord.DiscordException:
                            pass

                log.info("Resolved Kalshi market %s → %s", ticker, winning_side)

            except Exception:
                log.exception("Error checking Kalshi market %s", ticker)

    @check_kalshi_results.before_loop
    async def before_check_kalshi(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(KalshiCog(bot))
