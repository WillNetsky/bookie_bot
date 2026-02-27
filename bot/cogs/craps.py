import logging
import random

import discord
from discord import app_commands
from discord.ext import commands

from bot.services import wallet_service, leaderboard_notifier

log = logging.getLogger(__name__)

DICE_FACES = {1: "⚀", 2: "⚁", 3: "⚂", 4: "⚃", 5: "⚄", 6: "⚅"}

_NATURAL_FLAVOR = [
    "Natural. Pay the man.",
    "Seven out of the gate. Shooter's hot.",
    "First roll, first blood.",
    "Yo-leven. Collect your bread.",
    "Natural. Easy money.",
]
_CRAPS_FLAVOR = [
    "Craps. Step back.",
    "Snake eyes. That's it.",
    "Boxcars. House wins.",
    "Ace-deuce. Shooter's done.",
    "Craps. Next shooter.",
]
_POINT_FLAVOR = [
    "Point's on {n}. Roll again.",
    "{n} is the mark. Don't seven out.",
    "Shooter's going for {n}.",
    "Mark it {n}.",
]
_POINT_HIT_FLAVOR = [
    "Hit that {n}. Pay up.",
    "Shooter made it. Collect.",
    "That's the number. Money moves.",
    "{n} again. Get paid.",
]
_SEVEN_OUT_FLAVOR = [
    "Seven. That's it.",
    "Sevened out. Pass the bones.",
    "Seven out. Next shooter.",
    "Seven. Step aside.",
]
_ONGOING_FLAVOR = [
    "Keep rolling.",
    "Not yet.",
    "Again.",
    "Still shooting.",
]


def _roll() -> tuple[int, int]:
    return random.randint(1, 6), random.randint(1, 6)


def _log_entry(d1: int, d2: int, total: int, label: str = "") -> str:
    suffix = f"  — {label}" if label else ""
    return f"{DICE_FACES[d1]} {DICE_FACES[d2]}  **{total}**{suffix}"


def _pick(choices: list[str], **kwargs: object) -> str:
    return random.choice(choices).format(**kwargs)


# Tracks consecutive 7-rolls per shooter across games (resets on any non-7 outcome)
_seven_streak: dict[int, int] = {}


def _update_seven_streak(shooter_id: int, rolled_seven: bool) -> int:
    if rolled_seven:
        _seven_streak[shooter_id] = _seven_streak.get(shooter_id, 0) + 1
    else:
        _seven_streak[shooter_id] = 0
    return _seven_streak[shooter_id]


# ── Modals ────────────────────────────────────────────────────────────

class _FadeModal(discord.ui.Modal, title="Fade the Shooter"):
    amount_input = discord.ui.TextInput(
        label="How much to fade?",
        placeholder="100",
        min_length=1,
        max_length=10,
    )

    def __init__(self, view: "_StreetCrapsView") -> None:
        super().__init__()
        self._view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            amount = int(self.amount_input.value)
        except ValueError:
            await interaction.response.send_message("Enter a valid number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be positive.", ephemeral=True)
            return

        uid = interaction.user.id
        view = self._view

        if uid in view.backs:
            await interaction.response.send_message(
                "You're already backing the shooter.", ephemeral=True
            )
            return

        # Refund previous fade if updating
        if uid in view.fades:
            await wallet_service.deposit(uid, view.fades[uid])

        new_bal = await wallet_service.withdraw(uid, amount)
        if new_bal is None:
            bal = await wallet_service.get_balance(uid)
            await interaction.response.send_message(
                f"Not enough. Balance: **${bal:,}**", ephemeral=True
            )
            return

        first_fade = uid not in view.fades
        view.fades[uid] = amount
        view.fade_names[uid] = interaction.user.display_name

        await interaction.response.send_message(
            f"You're fading **${amount:,}**.", ephemeral=True
        )
        if view.message:
            await view.message.edit(embed=view._build_embed("betting"), view=view)
            if first_fade and len(view.fades) == 1:
                try:
                    await view.message.add_reaction("🔴")
                except discord.HTTPException:
                    pass


class _BackModal(discord.ui.Modal, title="Back the Shooter"):
    amount_input = discord.ui.TextInput(
        label="How much to back?",
        placeholder="100",
        min_length=1,
        max_length=10,
    )

    def __init__(self, view: "_StreetCrapsView") -> None:
        super().__init__()
        self._view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            amount = int(self.amount_input.value)
        except ValueError:
            await interaction.response.send_message("Enter a valid number.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be positive.", ephemeral=True)
            return

        uid = interaction.user.id
        view = self._view

        if uid in view.fades:
            await interaction.response.send_message(
                "You're already fading the shooter.", ephemeral=True
            )
            return

        if uid in view.backs:
            await wallet_service.deposit(uid, view.backs[uid])

        new_bal = await wallet_service.withdraw(uid, amount)
        if new_bal is None:
            bal = await wallet_service.get_balance(uid)
            await interaction.response.send_message(
                f"Not enough. Balance: **${bal:,}**", ephemeral=True
            )
            return

        first_back = uid not in view.backs
        view.backs[uid] = amount
        view.back_names[uid] = interaction.user.display_name

        await interaction.response.send_message(
            f"You're backing the shooter for **${amount:,}**.", ephemeral=True
        )
        if view.message:
            await view.message.edit(embed=view._build_embed("betting"), view=view)
            if first_back and len(view.backs) == 1:
                try:
                    await view.message.add_reaction("🟢")
                except discord.HTTPException:
                    pass


# ── View ─────────────────────────────────────────────────────────────

class _StreetCrapsView(discord.ui.View):
    def __init__(self, shooter: discord.User | discord.Member, wager: int) -> None:
        super().__init__(timeout=300)
        self.shooter = shooter
        self.wager = wager
        self.fades: dict[int, int] = {}
        self.backs: dict[int, int] = {}
        self.fade_names: dict[int, str] = {}
        self.back_names: dict[int, str] = {}
        self.roll_log: list[str] = []
        self.point: int | None = None
        self.phase = "betting"
        self.message: discord.Message | None = None
        self.final_balances: dict[int, int] = {}

    # ── embed builder ─────────────────────────────────────────────────

    def _build_embed(self, state: str) -> discord.Embed:
        colors = {
            "betting":   discord.Color.yellow(),
            "point":     discord.Color.blurple(),
            "ongoing":   discord.Color.blurple(),
            "natural":   discord.Color.green(),
            "craps":     discord.Color.red(),
            "point_hit": discord.Color.green(),
            "seven_out": discord.Color.dark_red(),
        }
        titles = {
            "betting":   "Looking for action...",
            "point":     f"Point: {self.point}",
            "ongoing":   f"Point: {self.point}",
            "natural":   "Natural!",
            "craps":     "Craps!",
            "point_hit": "Point Made!",
            "seven_out": "Seven Out!",
        }
        embed = discord.Embed(
            title=f"🎲 Street Craps — {titles[state]}",
            color=colors[state],
        )
        embed.set_author(
            name=self.shooter.display_name,
            icon_url=self.shooter.display_avatar.url,
        )

        if state == "betting":
            fading_str = (
                "\n".join(
                    f"{self.fade_names[uid]}  ${self.fades[uid]:,}"
                    for uid in self.fades
                ) or "open"
            )
            backing_str = (
                "\n".join(
                    f"{self.back_names[uid]}  ${self.backs[uid]:,}"
                    for uid in self.backs
                ) or "open"
            )
            embed.description = (
                f"**{self.shooter.display_name}** is shooting for **${self.wager:,}**"
            )
            embed.add_field(name="Fading", value=fading_str, inline=True)
            embed.add_field(name="Backing", value=backing_str, inline=True)
            embed.set_footer(text="Fade to bet against the shooter. Back to bet with them.")

        else:
            # Show roll log
            if self.roll_log:
                embed.add_field(name="Rolls", value="\n".join(self.roll_log), inline=False)

            # Wager summary
            total_fade = sum(self.fades.values())
            total_back = sum(self.backs.values())
            embed.add_field(name="Shooter", value=f"${self.wager:,}", inline=True)
            if total_fade:
                embed.add_field(name="Fading", value=f"${total_fade:,}", inline=True)
            if total_back:
                embed.add_field(name="Backing", value=f"${total_back:,}", inline=True)

            # Payout summary on resolution
            if state in ("natural", "craps", "point_hit", "seven_out"):
                shooter_won = state in ("natural", "point_hit")
                lines = []
                sign = "+" if shooter_won else "-"
                bal_str = f"  →  ${self.final_balances[self.shooter.id]:,}" if self.shooter.id in self.final_balances else ""
                lines.append(f"{self.shooter.display_name}  {sign}${self.wager:,}{bal_str}")
                for uid in self.fades:
                    sign = "+" if not shooter_won else "-"
                    bal_str = f"  →  ${self.final_balances[uid]:,}" if uid in self.final_balances else ""
                    lines.append(f"{self.fade_names[uid]}  {sign}${self.fades[uid]:,}{bal_str}")
                for uid in self.backs:
                    sign = "+" if shooter_won else "-"
                    bal_str = f"  →  ${self.final_balances[uid]:,}" if uid in self.final_balances else ""
                    lines.append(f"{self.back_names[uid]}  {sign}${self.backs[uid]:,}{bal_str}")
                embed.add_field(name="Payouts", value="\n".join(lines), inline=False)

            # Footer flavor
            n = self.point
            footer = {
                "point":     _pick(_POINT_FLAVOR, n=n),
                "ongoing":   _pick(_ONGOING_FLAVOR),
                "natural":   _pick(_NATURAL_FLAVOR),
                "craps":     _pick(_CRAPS_FLAVOR),
                "point_hit": _pick(_POINT_HIT_FLAVOR, n=n),
                "seven_out": _pick(_SEVEN_OUT_FLAVOR),
            }.get(state, "")
            if footer:
                embed.set_footer(text=footer)

        return embed

    # ── buttons ───────────────────────────────────────────────────────

    @discord.ui.button(label="Fade", style=discord.ButtonStyle.danger)
    async def fade_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if self.phase != "betting":
            await interaction.response.send_message("Bets are locked.", ephemeral=True)
            return
        if interaction.user.id == self.shooter.id:
            await interaction.response.send_message("Can't fade yourself.", ephemeral=True)
            return
        await interaction.response.send_modal(_FadeModal(self))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.success)
    async def back_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if self.phase != "betting":
            await interaction.response.send_message("Bets are locked.", ephemeral=True)
            return
        if interaction.user.id == self.shooter.id:
            await interaction.response.send_message("Can't back yourself.", ephemeral=True)
            return
        await interaction.response.send_modal(_BackModal(self))

    @discord.ui.button(label="🎲 Roll", style=discord.ButtonStyle.primary)
    async def roll_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.shooter.id:
            await interaction.response.send_message("Not your dice.", ephemeral=True)
            return
        if self.phase == "betting":
            await self._do_comeout(interaction)
        elif self.phase == "rolling":
            await self._do_point_roll(interaction)

    # ── game logic ────────────────────────────────────────────────────

    async def _do_comeout(self, interaction: discord.Interaction) -> None:
        self.phase = "rolling"
        self.fade_btn.disabled = True
        self.back_btn.disabled = True
        self.roll_btn.label = "🎲 Roll Again"

        d1, d2 = _roll()
        total = d1 + d2

        if total in (7, 11):
            self.roll_log.append(_log_entry(d1, d2, total, "natural"))
            streak = _update_seven_streak(self.shooter.id, total == 7)
            await self._resolve(interaction, shooter_won=True, state="natural", seven_streak=streak)
        elif total in (2, 3, 12):
            self.roll_log.append(_log_entry(d1, d2, total, "craps"))
            _update_seven_streak(self.shooter.id, False)
            await self._resolve(interaction, shooter_won=False, state="craps")
        else:
            self.point = total
            self.roll_log.append(_log_entry(d1, d2, total, "point"))
            await interaction.response.edit_message(
                embed=self._build_embed("point"), view=self
            )

    async def _do_point_roll(self, interaction: discord.Interaction) -> None:
        d1, d2 = _roll()
        total = d1 + d2

        if total == self.point:
            self.roll_log.append(_log_entry(d1, d2, total, "hit!"))
            _update_seven_streak(self.shooter.id, False)
            await self._resolve(interaction, shooter_won=True, state="point_hit")
        elif total == 7:
            self.roll_log.append(_log_entry(d1, d2, total, "seven out"))
            streak = _update_seven_streak(self.shooter.id, True)
            await self._resolve(interaction, shooter_won=False, state="seven_out", seven_streak=streak)
        else:
            self.roll_log.append(_log_entry(d1, d2, total))
            await interaction.response.edit_message(
                embed=self._build_embed("ongoing"), view=self
            )

    async def _resolve(
        self,
        interaction: discord.Interaction,
        *,
        shooter_won: bool,
        state: str,
        seven_streak: int = 0,
    ) -> None:
        self.phase = "done"
        self.clear_items()
        self.stop()

        if shooter_won:
            await leaderboard_notifier.deposit_and_notify(self.shooter.id, self.wager * 2, "craps")
        for uid, amt in self.fades.items():
            if not shooter_won:
                await leaderboard_notifier.deposit_and_notify(uid, amt * 2, "craps")
        for uid, amt in self.backs.items():
            if shooter_won:
                await leaderboard_notifier.deposit_and_notify(uid, amt * 2, "craps")

        all_uids = {self.shooter.id} | self.fades.keys() | self.backs.keys()
        for uid in all_uids:
            self.final_balances[uid] = await wallet_service.get_balance(uid)

        await interaction.response.edit_message(
            embed=self._build_embed(state), view=self
        )

        if seven_streak >= 3:
            try:
                await interaction.channel.send("oh damn pullin an RC!")
            except Exception:
                pass

    async def on_timeout(self) -> None:
        if self.phase != "done":
            await wallet_service.deposit(self.shooter.id, self.wager)
            for uid, amt in self.fades.items():
                await wallet_service.deposit(uid, amt)
            for uid, amt in self.backs.items():
                await wallet_service.deposit(uid, amt)
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ── Cog ──────────────────────────────────────────────────────────────

class Craps(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(
        name="craps",
        description="Start a street craps game — others can fade or back you",
    )
    @app_commands.describe(amount="Amount to shoot for")
    async def craps(self, interaction: discord.Interaction, amount: int) -> None:
        if amount <= 0:
            await interaction.response.send_message(
                "Wager must be at least $1.", ephemeral=True
            )
            return

        try:
            await interaction.response.defer()
        except discord.NotFound:
            return  # Interaction expired before we could respond

        new_bal = await wallet_service.withdraw(interaction.user.id, amount)
        if new_bal is None:
            bal = await wallet_service.get_balance(interaction.user.id)
            await interaction.followup.send(
                f"Not enough. Balance: **${bal:,}**", ephemeral=True
            )
            return

        view = _StreetCrapsView(shooter=interaction.user, wager=amount)
        msg = await interaction.followup.send(embed=view._build_embed("betting"), view=view)
        view.message = interaction.channel.get_partial_message(msg.id)
        try:
            await view.message.add_reaction("🎲")
        except discord.HTTPException:
            pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Craps(bot))
