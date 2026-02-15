import logging
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID
from bot.services import betting_service
from bot.services.kalshi_api import kalshi_api, SPORTS

log = logging.getLogger(__name__)

TZ_ET = timezone(timedelta(hours=-5))
TZ_PT = timezone(timedelta(hours=-8))


def format_close_time(close_str: str) -> str:
    """Format a close_time string into PT / ET display."""
    try:
        ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        pt_dt = ct.astimezone(TZ_PT)
        et_dt = ct.astimezone(TZ_ET)
        pt_str = pt_dt.strftime("%-m/%-d %-I:%M %p PT")
        if pt_dt.date() == et_dt.date():
            et_str = et_dt.strftime("%-I:%M %p ET")
        else:
            et_str = et_dt.strftime("%-m/%-d %-I:%M %p ET")
        return f"{pt_str} / {et_str}"
    except (ValueError, TypeError):
        return "TBD"


def price_to_odds(price_dollars: str | None, fallback_price: str | None = None) -> float | None:
    """Convert Kalshi dollar price string to decimal odds.

    Price is like "0.65" meaning 65 cents. Odds = 1.0 / price.
    """
    raw = price_dollars or fallback_price
    if not raw:
        return None
    try:
        price = float(raw)
        if price <= 0 or price >= 1:
            return None
        return round(1.0 / price, 3)
    except (ValueError, TypeError):
        return None


def price_to_percent(price_dollars: str | None) -> str:
    """Convert dollar price to percentage display."""
    if not price_dollars:
        return "N/A"
    try:
        return f"{float(price_dollars) * 100:.0f}%"
    except (ValueError, TypeError):
        return "N/A"


# ── Interactive UI components ─────────────────────────────────────────


class SportSelect(discord.ui.Select):
    """Step 1: Pick a sport."""

    def __init__(self, available_sports: list[str]) -> None:
        options = []
        for key in available_sports[:25]:
            sport = SPORTS[key]
            options.append(discord.SelectOption(
                label=sport["label"],
                value=key,
            ))
        super().__init__(placeholder="Select a sport...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction) -> None:
        sport_key = self.values[0]
        sport = SPORTS[sport_key]
        bet_types = list(sport["series"].keys())

        # If only one bet type (e.g. UFC), skip to markets
        if len(bet_types) == 1:
            await interaction.response.defer()
            markets = await kalshi_api.get_sport_markets(sport_key, bet_types[0])
            if not markets:
                await interaction.followup.send(f"No open {sport['label']} markets right now.", ephemeral=True)
                return
            view = MarketListView(markets, sport_key, bet_types[0])
            embed = discord.Embed(
                title=f"{sport['label']} — {bet_types[0]}",
                description=f"{len(markets)} open market(s). Select one to bet.",
                color=discord.Color.purple(),
            )
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        else:
            view = BetTypeView(sport_key)
            embed = discord.Embed(
                title=sport["label"],
                description="Select a bet type.",
                color=discord.Color.purple(),
            )
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class SportView(discord.ui.View):
    def __init__(self, available_sports: list[str]) -> None:
        super().__init__(timeout=120)
        self.add_item(SportSelect(available_sports))


class BetTypeSelect(discord.ui.Select):
    """Step 2: Pick bet type (Game/Spread/Total)."""

    def __init__(self, sport_key: str) -> None:
        self.sport_key = sport_key
        sport = SPORTS[sport_key]
        options = [
            discord.SelectOption(label=bt, value=bt)
            for bt in sport["series"].keys()
        ]
        super().__init__(placeholder="Select bet type...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        bet_type = self.values[0]
        sport = SPORTS[self.sport_key]
        markets = await kalshi_api.get_sport_markets(self.sport_key, bet_type)
        if not markets:
            await interaction.followup.send(f"No open {sport['label']} {bet_type} markets right now.", ephemeral=True)
            return

        view = MarketListView(markets, self.sport_key, bet_type)
        embed = discord.Embed(
            title=f"{sport['label']} — {bet_type}",
            description=f"{len(markets)} open market(s). Select one to bet.",
            color=discord.Color.purple(),
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class BetTypeView(discord.ui.View):
    def __init__(self, sport_key: str) -> None:
        super().__init__(timeout=120)
        self.add_item(BetTypeSelect(sport_key))


class MarketSelect(discord.ui.Select):
    """Step 3: Pick a specific market (game)."""

    def __init__(self, markets: list[dict], sport_key: str, bet_type: str) -> None:
        self.markets_map: dict[str, dict] = {}
        self.sport_key = sport_key
        self.bet_type = bet_type
        options = []
        for market in markets[:25]:
            ticker = market["ticker"]
            title = market.get("title", ticker)
            yes_price = market.get("yes_ask_dollars") or market.get("last_price_dollars")
            yes_pct = price_to_percent(yes_price)
            label = title[:95]
            desc = f"Yes: {yes_pct}"
            self.markets_map[ticker] = market
            options.append(discord.SelectOption(label=label, value=ticker, description=desc))
        super().__init__(placeholder="Select a market...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction) -> None:
        ticker = self.values[0]
        market = self.markets_map.get(ticker)
        if not market:
            await interaction.response.send_message("Market not found.", ephemeral=True)
            return

        yes_price = market.get("yes_ask_dollars") or market.get("last_price_dollars")
        no_price = market.get("no_ask_dollars")
        # Derive no price from yes if needed
        if not no_price and yes_price:
            try:
                no_price = f"{1.0 - float(yes_price):.2f}"
            except (ValueError, TypeError):
                pass

        yes_odds = price_to_odds(yes_price)
        no_odds = price_to_odds(no_price)

        close_time = market.get("close_time") or market.get("expected_expiration_time", "")
        close_display = format_close_time(close_time) if close_time else "TBD"

        sport = SPORTS.get(self.sport_key, {})
        sport_label = sport.get("label", self.sport_key)

        embed = discord.Embed(
            title=market.get("title", ticker),
            color=discord.Color.purple(),
        )
        embed.add_field(
            name="Yes",
            value=f"Price: {price_to_percent(yes_price)}\nOdds: {yes_odds:.2f}x" if yes_odds else "Unavailable",
            inline=True,
        )
        embed.add_field(
            name="No",
            value=f"Price: {price_to_percent(no_price)}\nOdds: {no_odds:.2f}x" if no_odds else "Unavailable",
            inline=True,
        )
        embed.add_field(name="Closes", value=close_display, inline=False)
        embed.set_footer(text=f"{sport_label} · {self.bet_type}")

        # Build a fake event dict for bet placement (Kalshi sports markets
        # may not have a real event_ticker, so we use the series ticker)
        event_info = {"event_ticker": market.get("event_ticker", ticker)}

        view = KalshiBetView(market, event_info)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class MarketListView(discord.ui.View):
    def __init__(self, markets: list[dict], sport_key: str, bet_type: str) -> None:
        super().__init__(timeout=120)
        self.add_item(MarketSelect(markets, sport_key, bet_type))


class KalshiBetView(discord.ui.View):
    """Shows Yes/No buttons for placing a bet on a Kalshi market."""

    def __init__(self, market: dict, event: dict) -> None:
        super().__init__(timeout=120)
        self.market = market
        self.event = event

    @discord.ui.button(label="Bet YES", style=discord.ButtonStyle.green, emoji="\U00002705")
    async def bet_yes(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        price = self.market.get("yes_ask_dollars") or self.market.get("last_price_dollars")
        odds = price_to_odds(price)
        if not odds:
            await interaction.response.send_message("Yes price unavailable right now.", ephemeral=True)
            return
        modal = KalshiBetAmountModal(self.market, self.event, "yes", odds)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Bet NO", style=discord.ButtonStyle.red, emoji="\U0000274c")
    async def bet_no(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        no_price = self.market.get("no_ask_dollars")
        if not no_price:
            yes_price = self.market.get("yes_ask_dollars") or self.market.get("last_price_dollars")
            if yes_price:
                try:
                    no_price = f"{1.0 - float(yes_price):.2f}"
                except (ValueError, TypeError):
                    pass
        odds = price_to_odds(no_price)
        if not odds:
            await interaction.response.send_message("No price unavailable right now.", ephemeral=True)
            return
        modal = KalshiBetAmountModal(self.market, self.event, "no", odds)
        await interaction.response.send_modal(modal)


class KalshiBetAmountModal(discord.ui.Modal, title="Place Bet"):
    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, market: dict, event: dict, pick: str, odds: float) -> None:
        super().__init__()
        self.market = market
        self.event = event
        self.pick = pick
        self.odds = odds

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

        market = self.market
        close_time = market.get("close_time") or market.get("expected_expiration_time")

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
            market_ticker=market["ticker"],
            event_ticker=self.event["event_ticker"],
            pick=self.pick,
            amount=amount,
            odds=self.odds,
            title=market.get("title"),
            close_time=close_time,
        )

        if bet_id is None:
            await interaction.followup.send("Insufficient balance!", ephemeral=True)
            return

        payout = round(amount * self.odds, 2)
        pick_display = self.pick.upper()

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#K{bet_id}", inline=True)
        embed.add_field(name="Market", value=market.get("title", market["ticker"])[:256], inline=False)
        embed.add_field(name="Pick", value=pick_display, inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=f"{self.odds:.2f}x", inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)


# ── Cog ───────────────────────────────────────────────────────────────


class KalshiCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.check_kalshi_results.start()

    async def cog_unload(self) -> None:
        self.check_kalshi_results.cancel()
        await kalshi_api.close()

    @app_commands.command(name="kalshi", description="Browse and bet on sports via Kalshi prediction markets")
    async def kalshi(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        available = await kalshi_api.get_available_sports()
        if not available:
            await interaction.followup.send("No open sports markets on Kalshi right now.", ephemeral=True)
            return

        view = SportView(available)
        embed = discord.Embed(
            title="Kalshi Sports Betting",
            description=f"{len(available)} sport(s) with open markets. Pick one.",
            color=discord.Color.purple(),
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

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
                        title = bet.get("title") or ticker

                        embed = discord.Embed(
                            title=f"{emoji} Bet #K{bet['id']} — {result_text.upper()}",
                            color=discord.Color.green() if won else discord.Color.red(),
                        )
                        embed.add_field(name="Bettor", value=name, inline=True)
                        embed.add_field(name="Market", value=title[:256], inline=False)
                        embed.add_field(name="Pick", value=bet["pick"].upper(), inline=True)
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
