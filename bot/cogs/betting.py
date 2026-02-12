import logging
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.services import betting_service, sports_api as sports_api_mod

log = logging.getLogger(__name__)

VALID_PICKS = ("home", "away", "draw")

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
    @app_commands.describe(sport="Sport to view (leave blank for all upcoming)")
    @app_commands.autocomplete(sport=sport_autocomplete)
    async def odds(self, interaction: discord.Interaction, sport: str | None = None) -> None:
        await interaction.response.defer()

        games = await self.sports_api.get_upcoming_games(sport or "upcoming")
        if not games:
            await interaction.followup.send("No upcoming games found.")
            return

        fmt = self.sports_api.format_american
        embed = discord.Embed(
            title="Upcoming Games" if not sport else f"Upcoming — {games[0].get('sport_title', sport)}",
            color=discord.Color.blue(),
        )

        for g in games[:10]:
            home = g.get("home_team", "?")
            away = g.get("away_team", "?")
            game_id = g.get("id", "?")
            sport_title = g.get("sport_title", g.get("sport_key", ""))
            display_date = format_game_time(g.get("commence_time", ""))

            parsed = self.sports_api.parse_odds(g)

            if parsed:
                home_odds = fmt(parsed["home"]["american"]) if "home" in parsed else "—"
                away_odds = fmt(parsed["away"]["american"]) if "away" in parsed else "—"
                draw_line = ""
                if "draw" in parsed:
                    draw_line = f"\n🤝 Draw: {fmt(parsed['draw']['american'])}"

                table = (
                    f"```\n"
                    f"🏠 {home:<25} {home_odds:>6}\n"
                    f"📍 {away:<25} {away_odds:>6}\n"
                    f"```"
                    f"{draw_line}"
                )
            else:
                table = "```\nOdds unavailable\n```"

            header = f"{sport_title} · {display_date}" if not sport else display_date

            embed.add_field(
                name=header,
                value=f"{table}ID: `{game_id}`",
                inline=False,
            )

        embed.set_footer(text="Use /bet <game_id> <pick> <amount> to place a wager")
        await interaction.followup.send(embed=embed)

    # ── /bet ─────────────────────────────────────────────────────────────

    @app_commands.command(name="bet", description="Place a bet on a game")
    @app_commands.describe(
        game_id="Game ID from /odds",
        pick="Your pick: home, away, or draw",
        amount="Wager amount in dollars",
    )
    @app_commands.choices(
        pick=[
            app_commands.Choice(name="Home", value="home"),
            app_commands.Choice(name="Away", value="away"),
            app_commands.Choice(name="Draw", value="draw"),
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
            else:
                await interaction.followup.send(
                    "Odds are not available for this game.", ephemeral=True
                )
            return

        odds_entry = parsed[pick_value]
        decimal_odds = odds_entry["decimal"]
        american_odds = odds_entry["american"]
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
        )
        if bet_id is None:
            await interaction.followup.send(
                "Insufficient balance.", ephemeral=True
            )
            return

        payout = round(amount * decimal_odds, 2)

        embed = discord.Embed(title="Bet Placed!", color=discord.Color.green())
        embed.add_field(name="Bet ID", value=f"#{bet_id}", inline=True)
        embed.add_field(name="Game", value=f"{home_name} vs {away_name}", inline=True)
        embed.add_field(name="Pick", value=pick_value.capitalize(), inline=True)
        embed.add_field(name="Wager", value=f"${amount:.2f}", inline=True)
        embed.add_field(name="Odds", value=fmt(american_odds), inline=True)
        embed.add_field(name="Potential Payout", value=f"${payout:.2f}", inline=True)

        await interaction.followup.send(embed=embed)

    # ── /mybets ──────────────────────────────────────────────────────────

    @app_commands.command(name="mybets", description="View your active bets")
    async def mybets(self, interaction: discord.Interaction) -> None:
        bets = await betting_service.get_user_bets(interaction.user.id)
        if not bets:
            await interaction.response.send_message("You have no bets.", ephemeral=True)
            return

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

            # Game info from stored fields
            home = b.get("home_team")
            away = b.get("away_team")
            sport = b.get("sport_title")

            if home and away:
                matchup = f"{home} vs {away}"
            else:
                raw_id = b["game_id"].split("|")[0] if "|" in b["game_id"] else b["game_id"]
                matchup = f"Game `{raw_id}`"

            sport_line = f"{sport} · " if sport else ""
            pick_label = b["pick"].capitalize()

            embed.add_field(
                name=f"{icon} Bet #{b['id']} · {status_text}",
                value=(
                    f"{sport_line}**{matchup}**\n"
                    f"Pick: **{pick_label}** · ${b['amount']:.2f} @ {b['odds']}x"
                ),
                inline=False,
            )

        await interaction.response.send_message(embed=embed)

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

                resolved = await betting_service.resolve_game(composite_id, winner)
                if resolved:
                    log.info(
                        "Resolved %d bets for game %s (winner: %s)",
                        resolved,
                        event_id,
                        winner,
                    )
        except Exception:
            log.exception("Error in check_results loop")

    @check_results.before_loop
    async def before_check_results(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Betting(bot))
