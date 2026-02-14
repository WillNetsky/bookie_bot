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
            commence_time=game.get("commence_time"),
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
        view.add_item(OddsGameSelect(view.games, view.sports_api))
        await interaction.response.edit_message(view=view)


class OddsGameSelect(discord.ui.Select["OddsView"]):
    """Dropdown listing games — fetches odds on-demand when selected."""

    def __init__(self, games: list[dict], sports_api) -> None:
        self.games_map: dict[str, dict] = {}
        self.sports_api = sports_api

        options = []
        for g in games[:25]:
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

        await interaction.response.defer()

        view = self.view
        if view is None:
            return

        # Fetch odds on-demand for this game's sport (cached 15 min)
        sport_key = game.get("sport_key", "")
        parsed = view.odds_cache.get(game_id)
        if not parsed:
            # Fetch all odds for this sport (one call covers all games in that sport)
            sport_odds = await self.sports_api.get_odds_for_sport(sport_key)
            for g_with_odds in sport_odds:
                gid = g_with_odds.get("id")
                if gid:
                    p = self.sports_api.parse_odds(g_with_odds)
                    if p:
                        view.odds_cache[gid] = p
                    # Merge bookmaker data into the event dict for BetAmountModal
                    if gid in self.games_map:
                        self.games_map[gid].update(g_with_odds)
            parsed = view.odds_cache.get(game_id)

        if not parsed:
            await interaction.followup.send("No odds available for this game.", ephemeral=True)
            return

        # Update the game dict with odds data for the modal
        fmt = self.sports_api.format_american
        home = game.get("home_team", "Home")
        away = game.get("away_team", "Away")

        view.clear_items()

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

        view.add_item(BackButton(row=3))

        await interaction.edit_original_response(view=view)


class OddsView(discord.ui.View):
    """View attached to /odds output with game select and on-demand odds."""

    def __init__(
        self,
        games: list[dict],
        sports_api,
        timeout: float = 180.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.games = games
        self.sports_api = sports_api
        self.odds_cache: dict[str, dict] = {}  # game_id -> parsed odds
        self.add_item(OddsGameSelect(games, sports_api))

    async def on_timeout(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]


# ── Interactive parlay builder UI components ─────────────────────────


class ParlayView(discord.ui.View):
    """Multi-step parlay builder view."""

    def __init__(
        self,
        games: list[dict],
        sports_api,
    ) -> None:
        super().__init__(timeout=300.0)
        self.games = games
        self.sports_api = sports_api
        self.odds_cache: dict[str, dict] = {}  # game_id -> parsed odds
        self.legs: list[dict] = []
        self.show_game_select()

    def _leg_game_ids(self) -> set[str]:
        """Return game IDs already in the slip."""
        return {leg["game_id_raw"] for leg in self.legs}

    def _combined_odds(self) -> float:
        odds = 1.0
        for leg in self.legs:
            odds *= leg["odds"]
        return odds

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Parlay Builder", color=discord.Color.purple())
        if not self.legs:
            embed.description = "Select a game below to start building your parlay."
        else:
            fmt = self.sports_api.format_american
            for i, leg in enumerate(self.legs, 1):
                home = leg.get("home_team") or "?"
                away = leg.get("away_team") or "?"
                pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
                point = leg.get("point")
                if point is not None:
                    if leg["pick"] in ("spread_home", "spread_away"):
                        pick_label += f" {point:+g}"
                    else:
                        pick_label += f" {point:g}"
                embed.add_field(
                    name=f"Leg {i}: {home} vs {away}",
                    value=f"{pick_label} ({leg['odds']:.2f}x)",
                    inline=False,
                )
            combined = self._combined_odds()
            embed.set_footer(text=f"{len(self.legs)} leg{'s' if len(self.legs) != 1 else ''} \u00b7 {combined:.2f}x combined odds")
        return embed

    def show_game_select(self) -> None:
        self.clear_items()
        used_ids = self._leg_game_ids()
        available = [g for g in self.games if g.get("id") not in used_ids][:25]
        if not available:
            # All games used — go straight to slip
            self.show_slip()
            return
        self.add_item(ParlayGameSelect(available, self.sports_api))
        if self.legs:
            self.add_item(ParlayBackToSlipButton(row=1))

    def show_bet_types(self, game: dict) -> None:
        self.clear_items()
        game_id = game.get("id", "")
        parsed = self.odds_cache.get(game_id, {})
        fmt = self.sports_api.format_american
        home = game.get("home_team", "Home")
        away = game.get("away_team", "Away")

        row = 0
        if "home" in parsed:
            self.add_item(ParlayBetTypeButton(
                "home", f"Home {fmt(parsed['home']['american'])}",
                game, parsed["home"], row=row,
            ))
        if "away" in parsed:
            self.add_item(ParlayBetTypeButton(
                "away", f"Away {fmt(parsed['away']['american'])}",
                game, parsed["away"], row=row,
            ))
        if "draw" in parsed:
            self.add_item(ParlayBetTypeButton(
                "draw", f"Draw {fmt(parsed['draw']['american'])}",
                game, parsed["draw"], row=row,
            ))

        row = 1
        if "spread_home" in parsed:
            sh = parsed["spread_home"]
            self.add_item(ParlayBetTypeButton(
                "spread_home", f"{home} {sh['point']:+g} ({fmt(sh['american'])})",
                game, sh, row=row,
            ))
        if "spread_away" in parsed:
            sa = parsed["spread_away"]
            self.add_item(ParlayBetTypeButton(
                "spread_away", f"{away} {sa['point']:+g} ({fmt(sa['american'])})",
                game, sa, row=row,
            ))

        row = 2
        if "over" in parsed:
            ov = parsed["over"]
            self.add_item(ParlayBetTypeButton(
                "over", f"Over {ov['point']:g} ({fmt(ov['american'])})",
                game, ov, row=row,
            ))
        if "under" in parsed:
            un = parsed["under"]
            self.add_item(ParlayBetTypeButton(
                "under", f"Under {un['point']:g} ({fmt(un['american'])})",
                game, un, row=row,
            ))

        self.add_item(ParlayBackButton(row=3))

    def show_slip(self) -> None:
        self.clear_items()
        used_ids = self._leg_game_ids()
        available_count = sum(1 for g in self.games if g.get("id") not in used_ids)

        if available_count > 0 and len(self.legs) < 10:
            self.add_item(ParlayAddLegButton(row=0))
        if self.legs:
            self.add_item(ParlayRemoveLegSelect(self.legs, row=1))
        if len(self.legs) >= 2:
            self.add_item(ParlayPlaceButton(row=2))
        self.add_item(ParlayCancelButton(row=2))

    async def on_timeout(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]


class ParlayGameSelect(discord.ui.Select["ParlayView"]):
    """Game dropdown for parlay builder — fetches odds on-demand."""

    def __init__(self, games: list[dict], sports_api) -> None:
        self.games_map: dict[str, dict] = {}
        self.sports_api = sports_api

        options = []
        for g in games[:25]:
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

        super().__init__(placeholder="Select a game to add...", options=options, row=0)

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

        # Fetch odds on-demand for this game's sport (cached 15 min)
        sport_key = game.get("sport_key", "")
        if game_id not in view.odds_cache:
            sport_odds = await self.sports_api.get_odds_for_sport(sport_key)
            for g_with_odds in sport_odds:
                gid = g_with_odds.get("id")
                if gid:
                    p = self.sports_api.parse_odds(g_with_odds)
                    if p:
                        view.odds_cache[gid] = p

        if game_id not in view.odds_cache:
            await interaction.followup.send("No odds available for this game.", ephemeral=True)
            return

        view.show_bet_types(game)
        embed = view.build_embed()
        embed.description = (
            f"Pick a bet type for **{game.get('home_team', '?')} vs {game.get('away_team', '?')}**"
        )
        await interaction.edit_original_response(embed=embed, view=view)


class ParlayBetTypeButton(discord.ui.Button["ParlayView"]):
    """Button for selecting a bet type to add as a parlay leg."""

    def __init__(self, pick_key: str, label: str, game: dict, odds_entry: dict, row: int) -> None:
        emoji = PICK_EMOJI.get(pick_key)
        super().__init__(label=label, style=discord.ButtonStyle.primary, emoji=emoji, row=row)
        self.pick_key = pick_key
        self.game = game
        self.odds_entry = odds_entry

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return

        game = self.game
        sport_key = game.get("sport_key", "")
        game_id = game.get("id", "")

        view.legs.append({
            "game_id_raw": game_id,
            "game_id": f"{game_id}|{sport_key}",
            "pick": self.pick_key,
            "odds": self.odds_entry["decimal"],
            "home_team": game.get("home_team"),
            "away_team": game.get("away_team"),
            "sport_title": game.get("sport_title", sport_key),
            "market": PICK_MARKET[self.pick_key],
            "point": self.odds_entry.get("point"),
            "commence_time": game.get("commence_time"),
        })

        view.show_slip()
        embed = view.build_embed()
        if len(view.legs) < 2:
            embed.description = "Add at least 2 legs to place a parlay."
        else:
            embed.description = "Ready to place, or add more legs."
        await interaction.response.edit_message(embed=embed, view=view)


class ParlayBackButton(discord.ui.Button["ParlayView"]):
    """Back button to return to game select from bet type view."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.show_game_select()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class ParlayBackToSlipButton(discord.ui.Button["ParlayView"]):
    """Button to go back to the slip view from game select."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Back to Slip", style=discord.ButtonStyle.secondary, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.show_slip()
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class ParlayAddLegButton(discord.ui.Button["ParlayView"]):
    """Button to add another leg (returns to game select)."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Add Leg", style=discord.ButtonStyle.primary, emoji="\u2795", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.show_game_select()
        embed = view.build_embed()
        embed.description = "Select another game to add to your parlay."
        await interaction.response.edit_message(embed=embed, view=view)


class ParlayRemoveLegSelect(discord.ui.Select["ParlayView"]):
    """Dropdown to remove a leg from the slip."""

    def __init__(self, legs: list[dict], row: int) -> None:
        options = []
        for i, leg in enumerate(legs):
            home = leg.get("home_team") or "?"
            away = leg.get("away_team") or "?"
            pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
            label = f"{home} vs {away} — {pick_label}"
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(label=label, value=str(i)))

        super().__init__(placeholder="Remove a leg...", options=options, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        idx = int(self.values[0])
        if 0 <= idx < len(view.legs):
            view.legs.pop(idx)
        view.show_slip()
        embed = view.build_embed()
        if not view.legs:
            embed.description = "All legs removed. Add a leg to start building."
            view.show_game_select()
        elif len(view.legs) < 2:
            embed.description = "Add at least 2 legs to place a parlay."
        await interaction.response.edit_message(embed=embed, view=view)


class ParlayPlaceButton(discord.ui.Button["ParlayView"]):
    """Button to open wager amount modal and place the parlay."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Place Parlay", style=discord.ButtonStyle.success, emoji="\U0001f4b0", row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        modal = ParlayAmountModal(view)
        await interaction.response.send_modal(modal)


class ParlayCancelButton(discord.ui.Button["ParlayView"]):
    """Cancel and abandon the parlay slip."""

    def __init__(self, row: int) -> None:
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger, row=row)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        view.clear_items()
        embed = discord.Embed(
            title="Parlay Cancelled",
            description="Your parlay slip has been abandoned.",
            color=discord.Color.light_grey(),
        )
        view.stop()
        await interaction.response.edit_message(embed=embed, view=view)


class ParlayAmountModal(discord.ui.Modal, title="Place Parlay"):
    """Modal for entering the parlay wager amount."""

    amount_input = discord.ui.TextInput(
        label="Wager amount ($)",
        placeholder="e.g. 50",
        min_length=1,
        max_length=10,
    )

    def __init__(self, parlay_view: ParlayView) -> None:
        super().__init__()
        self.parlay_view = parlay_view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount_input.value.strip().lstrip("$")
        try:
            amount = int(raw)
        except ValueError:
            await interaction.response.send_message(
                "Invalid amount \u2014 enter a whole number.", ephemeral=True
            )
            return
        if amount <= 0:
            await interaction.response.send_message(
                "Bet amount must be positive.", ephemeral=True
            )
            return

        await interaction.response.defer()

        pv = self.parlay_view
        # Re-validate that no games have started
        for leg in pv.legs:
            game = next((g for g in pv.games if g.get("id") == leg["game_id_raw"]), None)
            if game:
                commence = game.get("commence_time", "")
                try:
                    ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                    if ct <= datetime.now(timezone.utc):
                        await interaction.followup.send(
                            f"Game {leg.get('home_team', '?')} vs {leg.get('away_team', '?')} has already started.",
                            ephemeral=True,
                        )
                        return
                except (ValueError, TypeError):
                    pass

        # Build legs for betting service
        service_legs = []
        for leg in pv.legs:
            service_legs.append({
                "game_id": leg["game_id"],
                "pick": leg["pick"],
                "odds": leg["odds"],
                "home_team": leg.get("home_team"),
                "away_team": leg.get("away_team"),
                "sport_title": leg.get("sport_title"),
                "market": leg["market"],
                "point": leg.get("point"),
                "commence_time": leg.get("commence_time"),
            })

        parlay_id = await betting_service.place_parlay(
            interaction.user.id, service_legs, amount
        )
        if parlay_id is None:
            await interaction.followup.send("Insufficient balance.", ephemeral=True)
            return

        total_odds = pv._combined_odds()
        potential_payout = round(amount * total_odds, 2)

        embed = discord.Embed(title="Parlay Placed!", color=discord.Color.green())
        embed.add_field(name="Parlay ID", value=f"#{parlay_id}", inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Combined Odds", value=f"{total_odds:.2f}x", inline=True)
        embed.add_field(name="Potential Payout", value=f"${potential_payout:.2f}", inline=True)

        leg_lines = []
        for i, leg in enumerate(pv.legs, 1):
            home = leg.get("home_team") or "?"
            away = leg.get("away_team") or "?"
            pick_label = PICK_LABELS.get(leg["pick"], leg["pick"])
            point = leg.get("point")
            if point is not None:
                if leg["pick"] in ("spread_home", "spread_away"):
                    pick_label += f" {point:+g}"
                else:
                    pick_label += f" {point:g}"
            leg_lines.append(f"**{i}.** {home} vs {away} \u2014 {pick_label} ({leg['odds']:.2f}x)")

        embed.add_field(name="Legs", value="\n".join(leg_lines), inline=False)

        # Disable the parlay view and update original message
        pv.clear_items()
        pv.stop()
        try:
            await interaction.edit_original_response(
                embed=discord.Embed(
                    title="Parlay Placed",
                    description=f"Parlay #{parlay_id} placed successfully!",
                    color=discord.Color.green(),
                ),
                view=pv,
            )
        except discord.HTTPException:
            pass

        await interaction.followup.send(embed=embed)


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

        # Use free /events endpoint for game listing (0 credits)
        games = await self.sports_api.get_events(sport or "upcoming", hours=hours)
        games = [g for g in games if not is_sport_blocked(g.get("sport_key", ""))]
        if not games:
            await interaction.followup.send("No upcoming games found.")
            return

        display_games = games[:10]
        embed = discord.Embed(
            title="Upcoming Games" if not sport else f"Upcoming — {games[0].get('sport_title', sport)}",
            color=discord.Color.blue(),
        )

        for g in display_games:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            game_id = g.get("id", "?")
            sport_title = g.get("sport_title", g.get("sport_key", ""))
            display_date = format_game_time(g.get("commence_time", ""))

            header = f"{sport_title} · {display_date}" if not sport else display_date
            embed.add_field(
                name=header,
                value=f"🏠 {home} vs 📍 {away}\nID: `{game_id}`",
                inline=False,
            )

        view = OddsView(display_games, self.sports_api)
        embed.set_footer(text="Select a game below to see odds and bet")

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

        # 1. Find the game via free /events endpoint
        events = await self.sports_api.get_events()
        game = next((g for g in events if g.get("id") == game_id), None)

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

        # 3. Fetch odds on-demand for this game's sport (cached 15 min)
        sport_key = game.get("sport_key", "")
        odds_game = await self.sports_api.get_odds_for_event(sport_key, game_id)
        parsed = self.sports_api.parse_odds(odds_game) if odds_game else None
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
            commence_time=game.get("commence_time"),
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

    # ── /parlay ────────────────────────────────────────────────────────────

    @app_commands.command(name="parlay", description="Build a parlay bet")
    @app_commands.describe(
        sport="Sport to view (leave blank for all upcoming)",
        hours="Only show games starting within this many hours (default: 6)",
    )
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def parlay(
        self,
        interaction: discord.Interaction,
        sport: str | None = None,
        hours: int = 6,
    ) -> None:
        await interaction.response.defer()

        # Use free /events endpoint (0 credits)
        games = await self.sports_api.get_events(sport or "upcoming", hours=hours)
        games = [g for g in games if not is_sport_blocked(g.get("sport_key", ""))]
        if not games:
            await interaction.followup.send("No upcoming games found.")
            return

        view = ParlayView(games, self.sports_api)
        embed = view.build_embed()
        await interaction.followup.send(embed=embed, view=view)

    # ── /myparlays ────────────────────────────────────────────────────────

    @app_commands.command(name="myparlays", description="View your parlays")
    async def myparlays(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

        parlays = await betting_service.get_user_parlays(interaction.user.id)
        if not parlays:
            await interaction.followup.send("You have no parlays.")
            return

        embed = discord.Embed(title="Your Parlays", color=discord.Color.blue())

        for p in parlays[:10]:
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
                leg_lines.append(f"{leg_icon} {home} vs {away} — {pick_label} ({leg['odds']:.2f}x)")

            embed.add_field(
                name=f"{icon} Parlay #{p['id']} · {status_text}",
                value=(
                    f"Wager: **${p['amount']:.2f}** · Odds: **{p['total_odds']:.2f}x**\n"
                    + "\n".join(leg_lines)
                ),
                inline=False,
            )

        await interaction.followup.send(embed=embed)

    # ── /cancelparlay ─────────────────────────────────────────────────────

    @app_commands.command(name="cancelparlay", description="Cancel a pending parlay")
    @app_commands.describe(parlay_id="The ID of the parlay to cancel")
    async def cancelparlay(self, interaction: discord.Interaction, parlay_id: int) -> None:
        await interaction.response.defer(ephemeral=True)

        result = await betting_service.cancel_parlay(parlay_id, interaction.user.id)
        if result is None:
            await interaction.followup.send(
                "Could not cancel parlay. Make sure you own it, it's still pending, and no games have started.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Parlay Cancelled",
            description=f"Parlay #{parlay_id} cancelled. **${result['amount']:.2f}** refunded.",
            color=discord.Color.orange(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

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
        sport="Sport (speeds up lookup, optional)",
    )
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def betfuture(
        self,
        interaction: discord.Interaction,
        game_id: str,
        team: str,
        amount: int,
        sport: str | None = None,
    ) -> None:
        if amount <= 0:
            await interaction.response.send_message(
                "Bet amount must be positive.", ephemeral=True
            )
            return

        await interaction.response.defer()

        found_event = None
        found_sport_key = None

        # 1. If sport provided, fetch just that one
        if sport:
            events = await self.sports_api.get_outrights(sport)
            for ev in events:
                if ev.get("id") == game_id:
                    found_event = ev
                    found_sport_key = sport
                    break
        else:
            # 2. Search existing cache first (zero API calls)
            cached = await self.sports_api.find_outright_in_cache(game_id)
            if cached:
                found_event, found_sport_key = cached
            else:
                # 3. Fallback: search across sports (expensive)
                sports_list = self._sports_cache or await self.sports_api.get_sports()
                for s in sports_list:
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

    # ── /pendingbets (admin) ─────────────────────────────────────────────

    @app_commands.command(name="pendingbets", description="[Admin] View all pending bets")
    @app_commands.checks.has_permissions(administrator=True)
    async def pendingbets(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        game_ids = await betting_service.get_pending_game_ids()
        if not game_ids:
            await interaction.followup.send("No pending bets.", ephemeral=True)
            return

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
                matchup = f"{home} vs {away}"
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

            raw_id = composite_id.split("|")[0] if "|" in composite_id else composite_id
            header = f"{matchup} ({len(bets)} bet{'s' if len(bets) != 1 else ''} · ${total_wagered:.2f})"
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
                leg_count = len(p.get("legs", []))
                parlay_lines.append(
                    f"{icon} <@{p['user_id']}> — Parlay #{p['id']} · "
                    f"{leg_count} legs · ${p['amount']:.2f} @ {p.get('total_odds', 0):.2f}x → {result_text}"
                )
            embed.add_field(name="Parlays", value="\n".join(parlay_lines), inline=False)

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            log.exception("Failed to send resolution announcement")

    # ── Background task: check results ───────────────────────────────────

    # Estimated game durations by sport prefix (hours).
    # Only check scores once a game is likely near completion.
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
    async def check_results(self) -> None:
        """Check scores for games likely near completion.

        Budget: 500 credits/month. Each sport check = 1 credit.
        Only checks sports with games that started long enough ago
        to be near completion (based on estimated sport duration).
        Runs every 30 min to conserve quota.
        """
        try:
            # Skip if quota is critically low (< 50 remaining)
            quota = self.sports_api.get_quota()
            if quota["remaining"] is not None and quota["remaining"] < 50:
                log.warning("API quota low (%s remaining), skipping score check", quota["remaining"])
                return

            started_games = await betting_service.get_started_pending_games()
            if not started_games:
                return

            now = datetime.now(timezone.utc)

            # Filter to games likely near completion based on sport duration
            ready_games: dict[str, str | None] = {}
            for composite_id, commence_time in started_games.items():
                parts = composite_id.split("|")
                sport_key = parts[1] if len(parts) > 1 else None

                if commence_time is None:
                    # Legacy bet — always check
                    ready_games[composite_id] = commence_time
                    continue

                try:
                    game_start = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ready_games[composite_id] = commence_time
                    continue

                # Determine minimum elapsed time before checking
                duration = self.DEFAULT_SPORT_DURATION
                if sport_key:
                    for prefix, dur in self.SPORT_DURATION.items():
                        if sport_key.startswith(prefix):
                            duration = dur
                            break

                # Start checking 1.5 hours in, or at 75% of expected duration
                min_elapsed = min(duration * 0.75, 1.5)
                elapsed_hours = (now - game_start).total_seconds() / 3600

                if elapsed_hours >= min_elapsed:
                    ready_games[composite_id] = commence_time

            if not ready_games:
                log.debug("No games ready for score check yet")
                return

            # Group by sport_key for batch fetching
            sport_games: dict[str, list[str]] = {}
            for composite_id in ready_games:
                parts = composite_id.split("|")
                sport_key = parts[1] if len(parts) > 1 else None
                if not sport_key:
                    continue
                sport_games.setdefault(sport_key, []).append(composite_id)

            log.info("Checking scores for %d sport(s): %s", len(sport_games), list(sport_games.keys()))

            # One API call per sport (1 credit each)
            for sport_key, composites in sport_games.items():
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
