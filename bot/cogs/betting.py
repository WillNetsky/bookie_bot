import discord
from discord import app_commands
from discord.ext import commands

from bot.services import betting_service, sports_api as sports_api_mod


class Betting(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.sports_api = sports_api_mod.SportsAPI()

    async def cog_unload(self) -> None:
        await self.sports_api.close()

    @app_commands.command(name="odds", description="View upcoming games and odds")
    async def odds(self, interaction: discord.Interaction) -> None:
        games = await self.sports_api.get_upcoming_games()
        if not games:
            await interaction.response.send_message("No upcoming games found.")
            return

        lines = []
        for g in games[:5]:
            fixture = g.get("fixture", {})
            teams = g.get("teams", {})
            home = teams.get("home", {}).get("name", "?")
            away = teams.get("away", {}).get("name", "?")
            lines.append(f"**{home}** vs **{away}** (ID: {fixture.get('id', '?')})")

        await interaction.response.send_message("\n".join(lines))

    @app_commands.command(name="bet", description="Place a bet on a game")
    @app_commands.describe(game_id="Game ID", pick="Your pick (home/away/draw)", amount="Wager amount")
    async def bet(self, interaction: discord.Interaction, game_id: str, pick: str, amount: int) -> None:
        if amount <= 0:
            await interaction.response.send_message("Bet amount must be positive.")
            return

        # TODO: fetch real odds from API
        odds = 2.0
        bet_id = await betting_service.place_bet(
            interaction.user.id, game_id, pick, amount, odds
        )
        if bet_id is None:
            await interaction.response.send_message("Insufficient balance.")
            return

        await interaction.response.send_message(
            f"Bet #{bet_id} placed! {amount} coins on **{pick}** at {odds}x odds."
        )

    @app_commands.command(name="mybets", description="View your active bets")
    async def mybets(self, interaction: discord.Interaction) -> None:
        bets = await betting_service.get_user_bets(interaction.user.id)
        if not bets:
            await interaction.response.send_message("You have no bets.")
            return

        lines = []
        for b in bets[:10]:
            status = b["status"].upper()
            lines.append(
                f"#{b['id']} | Game {b['game_id']} | {b['pick']} | "
                f"{b['amount']} coins @ {b['odds']}x | {status}"
            )
        await interaction.response.send_message("\n".join(lines))


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Betting(bot))
