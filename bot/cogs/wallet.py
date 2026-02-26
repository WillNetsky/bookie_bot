import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.config import STARTING_BALANCE
from bot.services import wallet_service
from bot.db import models

log = logging.getLogger(__name__)

_ADMIN_ROLE = "Mayor"
_ADMIN_MSG  = "You need the Mayor role to use this command."


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

    # ── /setbalance (Mayor only) ───────────────────────────────────────

    @app_commands.command(
        name="setbalance",
        description="[Mayor] Set a specific user's balance",
    )
    @app_commands.describe(
        user="The user whose balance to set",
        amount="New balance amount",
    )
    @app_commands.checks.has_role(_ADMIN_ROLE)
    async def setbalance(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        amount: int,
    ) -> None:
        if amount < 0:
            await interaction.response.send_message(
                "Amount must be 0 or greater.", ephemeral=True
            )
            return

        await models.get_or_create_user(user.id)
        new_bal = await models.set_user_balance(user.id, amount)
        log.info("Mayor %s set %s's balance to $%d", interaction.user, user, new_bal)
        await interaction.response.send_message(
            f"Set {user.mention}'s balance to **${new_bal:,}**.", ephemeral=True
        )

    @setbalance.error
    async def setbalance_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingRole):
            await _deny(interaction, _ADMIN_MSG)
        else:
            log.exception("Error in /setbalance command", exc_info=error)

    # ── /resetbalances (Mayor only) ────────────────────────────────────

    @app_commands.command(
        name="resetbalances",
        description="[Mayor] Reset all balances to starting amount and clear all bets",
    )
    @app_commands.describe(
        amount="Balance to set for all users (default: starting balance)",
        confirm="Type 'yes' to confirm the reset",
    )
    @app_commands.checks.has_role(_ADMIN_ROLE)
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
            "Mayor %s reset all balances to $%d (%d users affected)",
            interaction.user, reset_amount, count,
        )

        await interaction.followup.send(
            f"Reset **{count}** user(s) to **${reset_amount:.2f}** and cleared all bets.",
            ephemeral=True,
        )

    @resetbalances.error
    async def resetbalances_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingRole):
            await _deny(interaction, _ADMIN_MSG)
        else:
            log.exception("Error in /resetbalances command", exc_info=error)

    # ── /devalue (Mayor only) ──────────────────────────────────────────

    @app_commands.command(
        name="devalue",
        description="[Mayor] Devalue everyone's currency by a percentage",
    )
    @app_commands.describe(
        percent="Percentage to cut (e.g. 50 = everyone loses half their balance)",
        confirm="Type 'yes' to confirm",
    )
    @app_commands.checks.has_role(_ADMIN_ROLE)
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
            "Mayor %s devalued currency by %d%% (%d users, $%d removed)",
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
        if isinstance(error, app_commands.MissingRole):
            await _deny(interaction, _ADMIN_MSG)
        else:
            log.exception("Error in /devalue command", exc_info=error)


async def _deny(interaction: discord.Interaction, msg: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Wallet(bot))
