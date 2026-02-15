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


def price_to_odds(price_dollars: str | None) -> float | None:
    """Convert Kalshi dollar price string to decimal odds."""
    if not price_dollars:
        return None
    try:
        price = float(price_dollars)
        if price <= 0 or price >= 1:
            return None
        return round(1.0 / price, 3)
    except (ValueError, TypeError):
        return None


def price_to_pct(price_dollars: str | None) -> str:
    """Convert dollar price to percentage display."""
    if not price_dollars:
        return "?"
    try:
        return f"{float(price_dollars) * 100:.0f}%"
    except (ValueError, TypeError):
        return "?"


def price_to_american(price_dollars: str | None) -> str:
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


def get_no_price(market: dict) -> str | None:
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


def get_yes_price(market: dict) -> str | None:
    """Get the yes-side price."""
    return market.get("yes_ask_dollars") or market.get("last_price_dollars")


# ── Interactive UI components ─────────────────────────────────────────


class KalshiMarketSelect(discord.ui.Select["KalshiOddsView"]):
    """Dropdown listing markets — shows Yes/No buttons when selected."""

    def __init__(self, markets: list[dict]) -> None:
        self.markets_map: dict[str, dict] = {}
        options = []
        for m in markets[:25]:
            ticker = m["ticker"]
            title = m.get("title", ticker)
            yes_price = get_yes_price(m)
            no_price = get_no_price(m)
            label = title[:95]
            desc = f"Yes {price_to_pct(yes_price)} / No {price_to_pct(no_price)}"
            self.markets_map[ticker] = m
            options.append(discord.SelectOption(label=label, value=ticker, description=desc))
        super().__init__(placeholder="Select a market to bet on...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        ticker = self.values[0]
        market = self.markets_map.get(ticker)
        if not market:
            await interaction.response.send_message("Market not found.", ephemeral=True)
            return

        view = self.view
        if view is None:
            return

        yes_price = get_yes_price(market)
        no_price = get_no_price(market)
        yes_odds = price_to_odds(yes_price)
        no_odds = price_to_odds(no_price)
        yes_american = price_to_american(yes_price)
        no_american = price_to_american(no_price)

        event_info = {"event_ticker": market.get("event_ticker", ticker)}

        # Replace view with Yes/No buttons
        view.clear_items()

        if yes_odds:
            view.add_item(KalshiBetButton(
                pick="yes",
                label=f"Yes {yes_american}",
                market=market,
                event=event_info,
                odds=yes_odds,
                row=0,
            ))
        if no_odds:
            view.add_item(KalshiBetButton(
                pick="no",
                label=f"No {no_american}",
                market=market,
                event=event_info,
                odds=no_odds,
                row=0,
            ))
        view.add_item(KalshiBackButton(row=1))

        await interaction.response.edit_message(view=view)


class KalshiBetButton(discord.ui.Button["KalshiOddsView"]):
    """Button for placing a Yes or No bet."""

    def __init__(self, pick: str, label: str, market: dict, event: dict, odds: float, row: int) -> None:
        style = discord.ButtonStyle.green if pick == "yes" else discord.ButtonStyle.red
        super().__init__(label=label, style=style, row=row)
        self.pick = pick
        self.market = market
        self.event = event
        self.odds = odds

    async def callback(self, interaction: discord.Interaction) -> None:
        modal = KalshiBetAmountModal(self.market, self.event, self.pick, self.odds)
        await interaction.response.send_modal(modal)


class KalshiBackButton(discord.ui.Button["KalshiOddsView"]):
    """Returns to the market select dropdown."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.clear_items()
        view.add_item(KalshiMarketSelect(view.markets))
        try:
            await interaction.response.edit_message(view=view)
        except discord.NotFound:
            pass


class KalshiOddsView(discord.ui.View):
    """View attached to /kalshi output — mirrors OddsView pattern."""

    def __init__(self, markets: list[dict], timeout: float = 180.0) -> None:
        super().__init__(timeout=timeout)
        self.markets = markets
        self.message: discord.Message | None = None
        self.add_item(KalshiMarketSelect(markets))

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

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#K{bet_id}", inline=True)
        embed.add_field(name="Market", value=market.get("title", market["ticker"])[:256], inline=False)
        embed.add_field(name="Pick", value=self.pick.upper(), inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=price_to_american(
            market.get("yes_ask_dollars") if self.pick == "yes" else get_no_price(market)
        ), inline=True)
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
    @app_commands.describe(
        sport="Sport to view (leave blank for NBA)",
        bet_type="Bet type: game, spread, or total (default: game)",
    )
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def kalshi(
        self,
        interaction: discord.Interaction,
        sport: str | None = None,
        bet_type: str | None = None,
    ) -> None:
        await interaction.response.defer()

        sport_key = sport or "NBA"
        if sport_key not in SPORTS:
            await interaction.followup.send(
                f"Unknown sport. Available: {', '.join(SPORTS.keys())}", ephemeral=True
            )
            return

        sport_info = SPORTS[sport_key]
        series_keys = list(sport_info["series"].keys())

        # Resolve bet_type
        if bet_type:
            bt_lower = bet_type.lower()
            resolved_bt = None
            for sk in series_keys:
                if bt_lower in sk.lower():
                    resolved_bt = sk
                    break
            if not resolved_bt:
                resolved_bt = series_keys[0]
        else:
            resolved_bt = series_keys[0]

        markets = await kalshi_api.get_sport_markets(sport_key, resolved_bt)
        if not markets:
            await interaction.followup.send(
                f"No open {sport_info['label']} {resolved_bt} markets right now."
            )
            return

        # Build embed mirroring /odds style
        display_markets = markets[:25]
        embed = discord.Embed(
            title=f"{sport_info['label']} — {resolved_bt}",
            color=discord.Color.blue(),
        )

        for m in display_markets:
            title = m.get("title", m["ticker"])
            close_time = m.get("close_time") or m.get("expected_expiration_time", "")
            display_date = format_close_time(close_time) if close_time else "TBD"

            yes_price = get_yes_price(m)
            no_price = get_no_price(m)
            yes_am = price_to_american(yes_price)
            no_am = price_to_american(no_price)

            embed.add_field(
                name=f"{title}",
                value=f"Yes **{yes_am}** · No **{no_am}** · {display_date}",
                inline=False,
            )

        view = KalshiOddsView(display_markets)
        embed.set_footer(text="Select a market below to bet")
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
