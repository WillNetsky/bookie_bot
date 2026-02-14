import logging
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import BET_RESULTS_CHANNEL_ID, is_sport_blocked
from bot.services import betting_service, sports_api as sports_api_mod

log = logging.getLogger(__name__)

VALID_PICKS = ("home", "away", "draw", "spread_home", "spread_away", "over", "under")

# Emoji prefixes for bet type buttons
PICK_EMOJI = {
    "home": "\U0001f3e0",       # 🏠
    "away": "\U0001f4cd",       # 📍
    "draw": "\U0001f91d",       # 🤝
    "spread_home": "\U0001f4ca",  # 📊
    "spread_away": "\U0001f4ca",  # 📊
    "over": "\U0001f4c8",       # 📈
    "under": "\U0001f4c9",       # 📉
}

# Map pick values to their market type and display labels
PICK_MARKET = {
    "home": "h2h",
    "away": "h2h",
    "draw": "h2h",
    "spread_home": "spreads",
    "spread_away": "spreads",
    "over": "totals",
    "under": "totals",
}

PICK_LABELS = {
    "home": "Home",
    "away": "Away",
    "draw": "Draw",
    "spread_home": "Home Spread",
    "spread_away": "Away Spread",
    "over": "Over",
    "under": "Under",
}

# Timezone helpers
TZ_ET = timezone(timedelta(hours=-5))
TZ_PT = timezone(timedelta(hours=-8))


def format_game_time(commence_str: str) -> str:
    """Format a commence_time string into PT / ET display."""
    try:
        ct = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
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


def format_pick_label(bet: dict) -> str:
    """Format a bet's pick into a display label including point info."""
    pick = bet.get("pick", "")
    label = PICK_LABELS.get(pick, pick.capitalize())
    point = bet.get("point")
    if point is not None:
        if pick in ("spread_home", "spread_away"):
            label += f" {point:+g}"
        else:
            label += f" {point:g}"
    return label


# ── Interactive betting UI components ─────────────────────────────────


class BetAmountModal(discord.ui.Modal, title="Place Bet"):
    """Modal that asks for the wager amount, then places the bet."""

    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(
        self,
        game: dict,
        pick_key: str,
        odds_entry: dict,
        sports_api,
    ) -> None:
        super().__init__()
        self.game = game
        self.pick_key = pick_key
        self.odds_entry = odds_entry
        self.sports_api = sports_api

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
        pick_value = self.pick_key

        # Validate game hasn't started
        commence = game.get("commence_time", "")
        try:
            ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            if ct <= datetime.now(timezone.utc):
                await interaction.followup.send(
                    "This game has already started.", ephemeral=True
                )
                return
        except (ValueError, TypeError):
            pass

        # Check sport isn't blocked
        if is_sport_blocked(game.get("sport_key", "")):
            await interaction.followup.send(
                "Betting is currently disabled for this sport.", ephemeral=True
            )
            return

        decimal_odds = self.odds_entry["decimal"]
        american_odds = self.odds_entry["american"]
        bet_point = self.odds_entry.get("point")
        market = PICK_MARKET[pick_value]
        home_name = game.get("home_team", "Home")
        away_name = game.get("away_team", "Away")
        sport_title = game.get("sport_title", game.get("sport_key", ""))
        sport_key = game.get("sport_key", "")
        game_id = game.get("id", "")
        composite_id = f"{game_id}|{sport_key}"
        fmt = self.sports_api.format_american

        bet_id = await betting_service.place_bet(
            interaction.user.id, composite_id, pick_value, amount, decimal_odds,
            home_team=home_name, away_team=away_name, sport_title=sport_title,
            market=market, point=bet_point,
        )
        if bet_id is None:
            await interaction.followup.send(
                "Insufficient balance.", ephemeral=True
            )
            return

        payout = round(amount * decimal_odds, 2)
        pick_label = PICK_LABELS.get(pick_value, pick_value)
        if bet_point is not None:
            if pick_value in ("spread_home", "spread_away"):
                pick_label += f" {bet_point:+g}"
            else:
                pick_label += f" {bet_point:g}"

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#{bet_id}", inline=True)
        embed.add_field(name="Game", value=f"{home_name} vs {away_name}", inline=True)
        embed.add_field(name="Pick", value=pick_label, inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=fmt(american_odds), inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)


class BetTypeButton(discord.ui.Button["OddsView"]):
    """A button representing one bet type (home, away, spread, etc.)."""

    def __init__(
        self,
        pick_key: str,
        label: str,
        game: dict,
        odds_entry: dict,
        sports_api,
        row: int,
    ) -> None:
        emoji = PICK_EMOJI.get(pick_key)
        super().__init__(label=label, style=discord.ButtonStyle.primary, emoji=emoji, row=row)
        self.pick_key = pick_key
        self.game = game
        self.odds_entry = odds_entry
        self.sports_api = sports_api

    async def callback(self, interaction: discord.Interaction) -> None:
        modal = BetAmountModal(
            game=self.game,
            pick_key=self.pick_key,
            odds_entry=self.odds_entry,
            sports_api=self.sports_api,
        )
        await interaction.response.send_modal(modal)


class BackButton(discord.ui.Button["OddsView"]):
    """Returns to the game select dropdown."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.clear_items()
        view.add_item(OddsGameSelect(view.games, view.parsed_odds, view.sports_api))
        await interaction.response.edit_message(view=view)


class OddsGameSelect(discord.ui.Select["OddsView"]):
    """Dropdown listing games from the /odds output."""

    def __init__(
        self,
        games: list[dict],
        parsed_odds: dict[str, dict],
        sports_api,
    ) -> None:
        self.games_map: dict[str, dict] = {}
        self.parsed_odds = parsed_odds
        self.sports_api = sports_api

        options = []
        for g in games[:10]:
            game_id = g.get("id", "?")
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            sport_title = g.get("sport_title", g.get("sport_key", ""))
            label = f"{home} vs {away}"
            if len(label) > 100:
                label = label[:97] + "..."
            desc = sport_title[:100] if sport_title else None
            options.append(discord.SelectOption(label=label, value=game_id, description=desc))
            self.games_map[game_id] = g

        super().__init__(placeholder="Select a game to bet on...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        game_id = self.values[0]
        game = self.games_map.get(game_id)
        if not game:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        parsed = self.parsed_odds.get(game_id)
        if not parsed:
            await interaction.response.send_message("No odds available for this game.", ephemeral=True)
            return

        fmt = self.sports_api.format_american
        home = game.get("home_team", "Home")
        away = game.get("away_team", "Away")

        view = self.view
        if view is None:
            return
        view.clear_items()

        # Row 1: moneyline buttons
        row = 0
        if "home" in parsed:
            view.add_item(BetTypeButton(
                "home", f"Home {fmt(parsed['home']['american'])}",
                game, parsed["home"], self.sports_api, row=row,
            ))
        if "away" in parsed:
            view.add_item(BetTypeButton(
                "away", f"Away {fmt(parsed['away']['american'])}",
                game, parsed["away"], self.sports_api, row=row,
            ))
        if "draw" in parsed:
            view.add_item(BetTypeButton(
                "draw", f"Draw {fmt(parsed['draw']['american'])}",
                game, parsed["draw"], self.sports_api, row=row,
            ))

        # Row 2: spread buttons
        row = 1
        if "spread_home" in parsed:
            sh = parsed["spread_home"]
            view.add_item(BetTypeButton(
                "spread_home", f"{home} {sh['point']:+g} ({fmt(sh['american'])})",
                game, sh, self.sports_api, row=row,
            ))
        if "spread_away" in parsed:
            sa = parsed["spread_away"]
            view.add_item(BetTypeButton(
                "spread_away", f"{away} {sa['point']:+g} ({fmt(sa['american'])})",
                game, sa, self.sports_api, row=row,
            ))

        # Row 3: totals buttons
        row = 2
        if "over" in parsed:
            ov = parsed["over"]
            view.add_item(BetTypeButton(
                "over", f"Over {ov['point']:g} ({fmt(ov['american'])})",
                game, ov, self.sports_api, row=row,
            ))
        if "under" in parsed:
            un = parsed["under"]
            view.add_item(BetTypeButton(
                "under", f"Under {un['point']:g} ({fmt(un['american'])})",
                game, un, self.sports_api, row=row,
            ))

        # Row 4: back button
        view.add_item(BackButton(row=3))

        await interaction.response.edit_message(view=view)


class OddsView(discord.ui.View):
    """View attached to /odds output with game select and bet buttons."""

    def __init__(
        self,
        games: list[dict],
        parsed_odds: dict[str, dict],
        sports_api,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.games = games
        self.parsed_odds = parsed_odds
        self.sports_api = sports_api
        self.add_item(OddsGameSelect(games, parsed_odds, sports_api))

    async def on_timeout(self) -> None:
        # Disable all items when the view times out
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]


class Betting(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.sports_api = sports_api_mod.SportsAPI()
        self._sports_cache: list[dict] = []

    async def cog_load(self) -> None:
        self.check_results.start()

    async def cog_unload(self) -> None:
        self.check_results.cancel()
        await self.sports_api.close()

    # ── Sport autocomplete ───────────────────────────────────────────────

    async def sport_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        if not self._sports_cache:
            self._sports_cache = await self.sports_api.get_sports()

        active = [s for s in self._sports_cache if s.get("active")]
        choices = []
        current_lower = current.lower()
        for s in active:
            title = s.get("title", s["key"])
            if current_lower in title.lower() or current_lower in s["key"].lower():
                choices.append(app_commands.Choice(name=title, value=s["key"]))
            if len(choices) >= 25:
                break
        return choices

    # ── /odds ────────────────────────────────────────────────────────────

    @app_commands.command(name="odds", description="View upcoming games and odds")
    @app_commands.describe(
        sport="Sport to view (leave blank for all upcoming)",
        hours="Only show games starting within this many hours (default: 6)",
    )
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def odds(self, interaction: discord.Interaction, sport: str | None = None, hours: int = 6) -> None:
        await interaction.response.defer()

        games = await self.sports_api.get_upcoming_games(sport or "upcoming", hours=hours)
        games = [g for g in games if not is_sport_blocked(g.get("sport_key", ""))]
        if not games:
            await interaction.followup.send("No upcoming games found.")
            return

        display_games = games[:10]
        fmt = self.sports_api.format_american
        embed = discord.Embed(
            title="Upcoming Games" if not sport else f"Upcoming — {games[0].get('sport_title', sport)}",
            color=discord.Color.blue(),
        )

        # Collect parsed odds for the interactive view
        all_parsed: dict[str, dict] = {}

        for g in display_games:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            game_id = g.get("id", "?")
            sport_title = g.get("sport_title", g.get("sport_key", ""))
            display_date = format_game_time(g.get("commence_time", ""))

            parsed = self.sports_api.parse_odds(g)
            if parsed:
                all_parsed[game_id] = parsed

            if parsed:
                home_odds = fmt(parsed["home"]["american"]) if "home" in parsed else "—"
                away_odds = fmt(parsed["away"]["american"]) if "away" in parsed else "—"
                extra_lines = ""
                if "draw" in parsed:
                    extra_lines += f"\n🤝 Draw: {fmt(parsed['draw']['american'])}"
                if "spread_home" in parsed and "spread_away" in parsed:
                    sh = parsed["spread_home"]
                    sa = parsed["spread_away"]
                    sp_home_pt = f"{sh['point']:+g}"
                    sp_away_pt = f"{sa['point']:+g}"
                    extra_lines += (
                        f"\n📊 Spread: {home} {sp_home_pt} ({fmt(sh['american'])}) "
                        f"/ {away} {sp_away_pt} ({fmt(sa['american'])})"
                    )
                if "over" in parsed and "under" in parsed:
                    ov = parsed["over"]
                    un = parsed["under"]
                    extra_lines += (
                        f"\n📈 Total: O {ov['point']:g} ({fmt(ov['american'])}) "
                        f"/ U {un['point']:g} ({fmt(un['american'])})"
                    )

                table = (
                    f"```\n"
                    f"🏠 {home:<25} {home_odds:>6}\n"
                    f"📍 {away:<25} {away_odds:>6}\n"
                    f"```"
                    f"{extra_lines}"
                )
            else:
                table = "```\nOdds unavailable\n```"

            header = f"{sport_title} · {display_date}" if not sport else display_date

            embed.add_field(
                name=header,
                value=f"{table}ID: `{game_id}`",
                inline=False,
            )

        # Only attach interactive view if there are games with odds
        view = None
        if all_parsed:
            # Filter display_games to only those with parsed odds
            games_with_odds = [g for g in display_games if g.get("id") in all_parsed]
            view = OddsView(games_with_odds, all_parsed, self.sports_api)
            embed.set_footer(text="Select a game below to bet, or use /bet <game_id> <pick> <amount>")
        else:
            embed.set_footer(text="Use /bet <game_id> <pick> <amount> to place a wager")

        await interaction.followup.send(embed=embed, view=view)

    # ── /bet ─────────────────────────────────────────────────────────────

    @app_commands.command(name="bet", description="Place a bet on a game")
    @app_commands.describe(
        game_id="Game ID from /odds",
        pick="Your pick",
        amount="Wager amount in dollars",
    )
    @app_commands.choices(
        pick=[
            app_commands.Choice(name="Home (moneyline)", value="home"),
            app_commands.Choice(name="Away (moneyline)", value="away"),
            app_commands.Choice(name="Draw (moneyline)", value="draw"),
            app_commands.Choice(name="Home Spread", value="spread_home"),
            app_commands.Choice(name="Away Spread", value="spread_away"),
            app_commands.Choice(name="Over", value="over"),
            app_commands.Choice(name="Under", value="under"),
        ]
    )
    async def bet(
        self,
        interaction: discord.Interaction,
        game_id: str,
        pick: app_commands.Choice[str],
        amount: int,
    ) -> None:
        pick_value = pick.value

        if amount <= 0:
            await interaction.response.send_message(
                "Bet amount must be positive.", ephemeral=True
            )
            return

        await interaction.response.defer()

        # 1. Find the game in upcoming games (validates it exists and has odds)
        games = await self.sports_api.get_upcoming_games()
        game = next((g for g in games if g.get("id") == game_id), None)

        if game is None:
            await interaction.followup.send(
                "Game not found. Use `/odds` to see available games.", ephemeral=True
            )
            return

        # Block bets on sports without score coverage
        if is_sport_blocked(game.get("sport_key", "")):
            await interaction.followup.send(
                "Betting is currently disabled for this sport (no score coverage).",
                ephemeral=True,
            )
            return

        # 2. Check it hasn't started
        commence = game.get("commence_time", "")
        try:
            ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            if ct <= datetime.now(timezone.utc):
                await interaction.followup.send(
                    "This game has already started.", ephemeral=True
                )
                return
        except (ValueError, TypeError):
            pass

        # 3. Parse odds
        parsed = self.sports_api.parse_odds(game)
        if not parsed or pick_value not in parsed:
            if pick_value == "draw" and "draw" not in (parsed or {}):
                await interaction.followup.send(
                    "Draw is not available for this sport.", ephemeral=True
                )
            elif pick_value in ("spread_home", "spread_away"):
                await interaction.followup.send(
                    "Spread odds are not available for this game.", ephemeral=True
                )
            elif pick_value in ("over", "under"):
                await interaction.followup.send(
                    "Totals odds are not available for this game.", ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "Odds are not available for this game.", ephemeral=True
                )
            return

        odds_entry = parsed[pick_value]
        decimal_odds = odds_entry["decimal"]
        american_odds = odds_entry["american"]
        bet_point = odds_entry.get("point")
        market = PICK_MARKET[pick_value]
        home_name = game.get("home_team", "Home")
        away_name = game.get("away_team", "Away")
        sport_title = game.get("sport_title", game.get("sport_key", ""))
        fmt = self.sports_api.format_american

        # 4. Store sport_key alongside game_id for later resolution
        sport_key = game.get("sport_key", "")
        composite_id = f"{game_id}|{sport_key}"

        # 5. Place the bet (store decimal odds for payout math)
        bet_id = await betting_service.place_bet(
            interaction.user.id, composite_id, pick_value, amount, decimal_odds,
            home_team=home_name, away_team=away_name, sport_title=sport_title,
            market=market, point=bet_point,
        )
        if bet_id is None:
            await interaction.followup.send(
                "Insufficient balance.", ephemeral=True
            )
            return

        payout = round(amount * decimal_odds, 2)

        # Build pick label with line info for spreads/totals
        pick_label = PICK_LABELS.get(pick_value, pick_value)
        if bet_point is not None:
            if pick_value in ("spread_home", "spread_away"):
                pick_label += f" {bet_point:+g}"
            else:
                pick_label += f" {bet_point:g}"

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#{bet_id}", inline=True)
        embed.add_field(name="Game", value=f"{home_name} vs {away_name}", inline=True)
        embed.add_field(name="Pick", value=pick_label, inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=fmt(american_odds), inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)

    # ── /futures ───────────────────────────────────────────────────────────

    @app_commands.command(name="futures", description="View futures/outrights odds (e.g. championship winner)")
    @app_commands.describe(sport="Sport to view futures for")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def futures(self, interaction: discord.Interaction, sport: str) -> None:
        await interaction.response.defer()

        events = await self.sports_api.get_outrights(sport)
        if not events:
            await interaction.followup.send("No futures/outrights found for this sport.")
            return

        fmt = self.sports_api.format_american

        for event in events[:3]:  # limit to 3 outright events
            title = event.get("sport_title", sport)
            event_id = event.get("id", "?")
            outcomes = self.sports_api.parse_outright_odds(event)

            if not outcomes:
                continue

            embed = discord.Embed(
                title=title,
                color=discord.Color.gold(),
            )

            # Show top 15 outcomes in a code block
            lines = []
            for o in outcomes[:15]:
                lines.append(f"{o['name']:<30} {fmt(o['american']):>6}")
            if len(outcomes) > 15:
                lines.append(f"... and {len(outcomes) - 15} more")

            embed.description = f"```\n" + "\n".join(lines) + "\n```"
            embed.set_footer(text=f"ID: {event_id}\nUse /betfuture <game_id> <team> <amount> to bet")

            await interaction.followup.send(embed=embed)

    # ── /betfuture ────────────────────────────────────────────────────────

    @app_commands.command(name="betfuture", description="Place a futures/outrights bet")
    @app_commands.describe(
        game_id="Event ID from /futures",
        team="Team/outcome name to bet on",
        amount="Wager amount in dollars",
    )
    async def betfuture(
        self,
        interaction: discord.Interaction,
        game_id: str,
        team: str,
        amount: int,
    ) -> None:
        if amount <= 0:
            await interaction.response.send_message(
                "Bet amount must be positive.", ephemeral=True
            )
            return

        await interaction.response.defer()

        # Find the outright event — search across sports
        sports = self._sports_cache or await self.sports_api.get_sports()
        found_event = None
        found_sport_key = None
        for s in sports:
            if not s.get("active"):
                continue
            events = await self.sports_api.get_outrights(s["key"])
            for ev in events:
                if ev.get("id") == game_id:
                    found_event = ev
                    found_sport_key = s["key"]
                    break
            if found_event:
                break

        if not found_event:
            await interaction.followup.send(
                "Event not found. Use `/futures` to see available events.", ephemeral=True
            )
            return

        # Parse outcomes and find matching team
        outcomes = self.sports_api.parse_outright_odds(found_event)
        if not outcomes:
            await interaction.followup.send(
                "No odds available for this event.", ephemeral=True
            )
            return

        team_lower = team.lower()
        match = next((o for o in outcomes if o["name"].lower() == team_lower), None)
        # Fuzzy: try substring match if exact fails
        if not match:
            match = next((o for o in outcomes if team_lower in o["name"].lower()), None)

        if not match:
            # Show available teams
            available = ", ".join(o["name"] for o in outcomes[:10])
            await interaction.followup.send(
                f"Team not found. Available: {available}...", ephemeral=True
            )
            return

        sport_title = found_event.get("sport_title", found_sport_key or "")
        composite_id = f"{game_id}|{found_sport_key}"
        fmt = self.sports_api.format_american

        bet_id = await betting_service.place_bet(
            interaction.user.id, composite_id, match["name"], amount, match["decimal"],
            home_team=None, away_team=None, sport_title=sport_title,
            market="outrights", point=None,
        )
        if bet_id is None:
            await interaction.followup.send(
                "Insufficient balance.", ephemeral=True
            )
            return

        payout = round(amount * match["decimal"], 2)

        embed = discord.Embed(title="Futures Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#{bet_id}", inline=True)
        embed.add_field(name="Event", value=sport_title, inline=True)
        embed.add_field(name="Pick", value=match["name"], inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=fmt(match["american"]), inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)

    # ── /mybets ──────────────────────────────────────────────────────────

    @app_commands.command(name="mybets", description="View your active bets")
    async def mybets(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        bets = await betting_service.get_user_bets(interaction.user.id)
        if not bets:
            await interaction.followup.send("You have no bets.")
            return

        # Fetch live scores for pending bets (uses cached data, skip outrights)
        live_scores: dict[str, dict] = {}
        for b in bets[:10]:
            if b["status"] != "pending" or (b.get("market") or "") == "outrights":
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

        embed = discord.Embed(title="Your Bets", color=discord.Color.blue())

        for b in bets[:10]:
            status = b["status"]
            if status == "won":
                icon = "\U0001f7e2"  # green circle
                status_text = f"Won — **${b.get('payout', 0):.2f}**"
            elif status == "lost":
                icon = "\U0001f534"  # red circle
                status_text = "Lost"
            else:
                icon = "\U0001f7e1"  # yellow circle
                potential = round(b["amount"] * b["odds"], 2)
                status_text = f"Pending — potential **${potential:.2f}**"

            # Game info from stored fields, backfill from live data if missing
            home = b.get("home_team")
            away = b.get("away_team")
            sport = b.get("sport_title")
            live = live_scores.get(b["game_id"]) if status == "pending" else None

            if not home and live:
                home = live.get("home_team")
            if not away and live:
                away = live.get("away_team")

            is_outright = (b.get("market") or "") == "outrights"

            if is_outright:
                matchup = sport or "Futures"
            elif home and away:
                matchup = f"{home} vs {away}"
            else:
                raw_id = b["game_id"].split("|")[0] if "|" in b["game_id"] else b["game_id"]
                matchup = f"Game `{raw_id}`"

            # Live score for pending bets (not applicable for outrights)
            score_line = ""
            if not is_outright and live and live["started"] and live["home_score"] is not None and live["away_score"] is not None:
                score_line = f"\nScore: **{home}** {live['home_score']} - {live['away_score']} **{away}**"

            sport_line = f"{sport} · " if sport and not is_outright else ""
            pick_label = b["pick"] if is_outright else format_pick_label(b)

            # Show push status
            if b["status"] == "push":
                icon = "🔵"
                status_text = f"Push — **${b.get('payout', 0):.2f}** refunded"

            embed.add_field(
                name=f"{icon} Bet #{b['id']} · {status_text}",
                value=(
                    f"{sport_line}**{matchup}**\n"
                    f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']}x"
                    f"{score_line}"
                ),
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # ── /livescores ────────────────────────────────────────────────────────

    @app_commands.command(name="livescores", description="View live scores for your active bets")
    async def livescores(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        bets = await betting_service.get_user_bets(interaction.user.id, status="pending")
        if not bets:
            await interaction.followup.send("You have no pending bets.")
            return

        # Filter out outrights — no live scores for futures
        game_bets = [b for b in bets if (b.get("market") or "") != "outrights"]

        # Fetch live status for each unique game
        seen: dict[str, dict] = {}
        for b in game_bets:
            composite = b["game_id"]
            if composite in seen:
                continue
            parts = composite.split("|")
            event_id = parts[0]
            sport_key = parts[1] if len(parts) > 1 else None
            status = await self.sports_api.get_fixture_status(event_id, sport_key)
            if status:
                seen[composite] = status

        embed = discord.Embed(title="Live Scores", color=discord.Color.blue())
        found_any = False

        for b in game_bets:
            live = seen.get(b["game_id"])
            if not live or not live["started"]:
                continue

            home = b.get("home_team") or live.get("home_team", "Home")
            away = b.get("away_team") or live.get("away_team", "Away")
            sport = b.get("sport_title") or ""
            pick_label = format_pick_label(b)
            potential = round(b["amount"] * b["odds"], 2)

            if live["home_score"] is not None and live["away_score"] is not None:
                score_text = f"**{home}** {live['home_score']} - {live['away_score']} **{away}**"
                if live["completed"]:
                    score_text += "  (Final)"
            else:
                score_text = f"**{home}** vs **{away}** — scores unavailable"

            sport_line = f"{sport} · " if sport else ""

            embed.add_field(
                name=f"Bet #{b['id']} · {sport_line}{pick_label} · ${b['amount']:.2f} → ${potential:.2f}",
                value=score_text,
                inline=False,
            )
            found_any = True

        if not found_any:
            embed.description = "None of your bets have started yet."

        await interaction.followup.send(embed=embed)

    # ── /cancelbet ───────────────────────────────────────────────────────

    @app_commands.command(name="cancelbet", description="Cancel a pending bet")
    @app_commands.describe(bet_id="The ID of the bet to cancel")
    async def cancelbet(self, interaction: discord.Interaction, bet_id: int) -> None:
        await interaction.response.defer(ephemeral=True)

        # Look up the bet to check game status
        bets = await betting_service.get_user_bets(interaction.user.id)
        target = next((b for b in bets if b["id"] == bet_id), None)

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

        result = await betting_service.cancel_bet(bet_id, interaction.user.id)
        if result is None:
            await interaction.followup.send(
                "Could not cancel bet. Make sure you own it and it's still pending.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Bet Cancelled",
            description=f"Bet #{bet_id} cancelled. **${result['amount']:.2f}** refunded.",
            color=discord.Color.orange(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

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
            title = f"Game Result: {home} vs {away}"
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
        for bet in resolved_bets:
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

        embed.add_field(name="Bets", value="\n".join(lines), inline=False)

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            log.exception("Failed to send resolution announcement")

    # ── Background task: check results ───────────────────────────────────

    @tasks.loop(minutes=5)
    async def check_results(self) -> None:
        try:
            game_ids = await betting_service.get_pending_game_ids()
            for composite_id in game_ids:
                parts = composite_id.split("|")
                event_id = parts[0]
                sport_key = parts[1] if len(parts) > 1 else None

                status = await self.sports_api.get_fixture_status(event_id, sport_key)
                if not status or not status["completed"]:
                    continue

                home_score = status["home_score"]
                away_score = status["away_score"]

                if home_score is None or away_score is None:
                    continue

                if home_score > away_score:
                    winner = "home"
                elif away_score > home_score:
                    winner = "away"
                else:
                    winner = "draw"

                resolved = await betting_service.resolve_game(
                    composite_id, winner,
                    home_score=home_score, away_score=away_score,
                )
                if resolved:
                    log.info(
                        "Resolved %d bets for game %s (winner: %s)",
                        len(resolved),
                        event_id,
                        winner,
                    )
                    await self._post_resolution_announcement(
                        resolved, home_score=home_score, away_score=away_score,
                    )
        except Exception:
            log.exception("Error in check_results loop")

    @check_results.before_loop
    async def before_check_results(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Betting(bot))
