import discord
from discord import app_commands
from discord.ext import commands

from bot.services import wallet_service
from bot.db import models


class Wallet(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(name="balance", description="Check your balance")
    async def balance(self, interaction: discord.Interaction) -> None:
        bal = await wallet_service.get_balance(interaction.user.id)
        await interaction.response.send_message(f"Your balance: **${bal:.2f}**")

    @app_commands.command(name="leaderboard", description="Top 10 richest users")
    async def leaderboard(self, interaction: discord.Interaction) -> None:
        top = await models.get_leaderboard(10)
        if not top:
            await interaction.response.send_message("No users yet.")
            return

        lines = []
        for i, u in enumerate(top, 1):
            lines.append(f"**{i}.** <@{u['discord_id']}> — ${u['balance']:.2f}")
        await interaction.response.send_message("\n".join(lines))


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Wallet(bot))
