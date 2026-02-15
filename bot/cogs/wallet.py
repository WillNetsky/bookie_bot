import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.config import STARTING_BALANCE
from bot.services import wallet_service
from bot.db import models

log = logging.getLogger(__name__)


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

    # ── /resetbalances (admin) ────────────────────────────────────────

    @app_commands.command(
        name="resetbalances",
        description="[Admin] Reset all balances to starting amount and clear all bets",
    )
    @app_commands.describe(
        amount="Balance to set for all users (default: starting balance)",
        confirm="Type 'yes' to confirm the reset",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def resetbalances(
        self,
        interaction: discord.Interaction,
        confirm: str,
        amount: int | None = None,
    ) -> None:
        if confirm.lower() != "yes":
            await interaction.response.send_message(
                "Reset cancelled. Pass `confirm: yes` to confirm.", ephemeral=True
            )
            return

        reset_amount = amount if amount is not None else STARTING_BALANCE
        await interaction.response.defer(ephemeral=True)

        count = await models.reset_all_balances(reset_amount)
        log.info(
            "Admin %s reset all balances to $%d (%d users affected)",
            interaction.user, reset_amount, count,
        )

        await interaction.followup.send(
            f"Reset **{count}** user(s) to **${reset_amount:.2f}** and cleared all bets.",
            ephemeral=True,
        )

    @resetbalances.error
    async def resetbalances_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "You need administrator permissions to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        else:
            log.exception("Error in /resetbalances command", exc_info=error)

    # ── /devalue (admin) ──────────────────────────────────────────────

    @app_commands.command(
        name="devalue",
        description="[Admin] Devalue everyone's currency by a percentage",
    )
    @app_commands.describe(
        percent="Percentage to cut (e.g. 50 = everyone loses half their balance)",
        confirm="Type 'yes' to confirm",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def devalue(
        self,
        interaction: discord.Interaction,
        percent: int,
        confirm: str,
    ) -> None:
        if confirm.lower() != "yes":
            await interaction.response.send_message(
                "Devaluation cancelled. Pass `confirm: yes` to confirm.", ephemeral=True
            )
            return

        if percent <= 0 or percent > 99:
            await interaction.response.send_message(
                "Percent must be between 1 and 99.", ephemeral=True
            )
            return

        await interaction.response.defer()

        count, total_removed = await models.devalue_all_balances(percent)
        log.info(
            "Admin %s devalued currency by %d%% (%d users, $%d removed)",
            interaction.user, percent, count, total_removed,
        )

        embed = discord.Embed(
            title=f"Currency Devalued by {percent}%",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Users Affected", value=str(count), inline=True)
        embed.add_field(name="Total Removed", value=f"${total_removed:,.2f}", inline=True)
        embed.set_footer(text=f"Issued by {interaction.user.display_name}")

        await interaction.followup.send(embed=embed)

    @devalue.error
    async def devalue_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "You need administrator permissions to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        else:
            log.exception("Error in /devalue command", exc_info=error)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Wallet(bot))
