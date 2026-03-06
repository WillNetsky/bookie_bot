import asyncio
import logging
import random
from dataclasses import dataclass

import discord
from discord import app_commands
from discord.ext import commands

from bot.services import wallet_service, leaderboard_notifier

log = logging.getLogger(__name__)

# European single-zero wheel order (authentic sequence)
WHEEL = [
    0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36,
    11, 30, 8, 23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9,
    22, 18, 29, 7, 28, 12, 35, 3, 26,
]
RED = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
MAX_BETTORS = 10

# Spin animation: seconds between each wheel-strip update.
# Fast at first (ball flying), then dramatic slow-down as it settles.
SPIN_DELAYS = [
    # Fast spin — each value is a guaranteed frame duration (edit time + sleep)
    0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5,
    # Deceleration
    0.65, 0.85, 1.1, 1.4,
]

BET_LABELS = {
    "red": "Red", "black": "Black",
    "odd": "Odd", "even": "Even",
    "low": "1–18", "high": "19–36",
    "dozen1": "1st 12", "dozen2": "2nd 12", "dozen3": "3rd 12",
}
BET_PAYOUTS = {  # net multiplier (win receives stake × (mult + 1))
    "red": 1, "black": 1, "odd": 1, "even": 1,
    "low": 1, "high": 1,
    "dozen1": 2, "dozen2": 2, "dozen3": 2,
}


def _color(n: int) -> str:
    if n == 0:
        return "🟢"
    return "🔴" if n in RED else "⚫"


def _wheel_strip(center_idx: int) -> str:
    """
    7-number monospace window.  Each slot is 3 chars right-aligned + 1-char
    separator = 4 chars per slot.  Center slot occupies chars 12-14; the ▲
    and colour letter both land on col 14 (units digit / sole digit).

    Example output (centre = 32):
        3  26   0  32  15  19   4
                      ▲
          R   B   G   R   R   R   B
    """
    nums, clrs = [], []
    for offset in range(-3, 4):
        idx = (center_idx + offset) % len(WHEEL)
        n = WHEEL[idx]
        nums.append(f"{n:3d}")
        c = "G" if n == 0 else ("R" if n in RED else "B")
        clrs.append(f"{c:>3}")

    num_row = " ".join(nums)
    clr_row = " ".join(clrs)
    pointer = " " * 14 + "▲"
    return f"```\n{num_row}\n{pointer}\n{clr_row}\n```"


def _bet_wins(bet_type: str, number: int | None, result: int) -> bool:
    match bet_type:
        case "red":    return result in RED
        case "black":  return result != 0 and result not in RED
        case "odd":    return result != 0 and result % 2 == 1
        case "even":   return result != 0 and result % 2 == 0
        case "low":    return 1 <= result <= 18
        case "high":   return 19 <= result <= 36
        case "dozen1": return 1 <= result <= 12
        case "dozen2": return 13 <= result <= 24
        case "dozen3": return 25 <= result <= 36
        case "number": return result == number
    return False


@dataclass
class _Bettor:
    user_id: int
    name: str
    amount: int
    bet_type: str
    number: int | None = None
    result: str | None = None
    final_balance: int | None = None

    @property
    def label(self) -> str:
        return f"#{self.number}" if self.bet_type == "number" else BET_LABELS[self.bet_type]

    @property
    def win_payout(self) -> int:
        mult = 35 if self.bet_type == "number" else BET_PAYOUTS[self.bet_type]
        return self.amount * (mult + 1)


# ── Number modal ───────────────────────────────────────────────────────────────


class _NumberModal(discord.ui.Modal, title="Straight Number Bet"):
    number_input = discord.ui.TextInput(
        label="Number (0–36)",
        placeholder="Enter a number between 0 and 36",
        min_length=1,
        max_length=2,
    )

    def __init__(self, view: "_RouletteView") -> None:
        super().__init__()
        self._rview = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            n = int(self.number_input.value.strip())
            if not 0 <= n <= 36:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                "Please enter a number between 0 and 36.", ephemeral=True
            )
            return
        await self._rview._join(interaction, "number", number=n)


# ── View ───────────────────────────────────────────────────────────────────────


class _RouletteView(discord.ui.View):
    def __init__(self, host: discord.User | discord.Member, bet: int) -> None:
        super().__init__(timeout=300)
        self.host = host
        self.bet = bet
        self.bettors: list[_Bettor] = []
        self.phase = "betting"  # betting | spinning | done
        self.spin_idx: int = 0
        self.result: int | None = None
        self.message: discord.PartialMessage | None = None
        self._update_buttons()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _update_buttons(self) -> None:
        betting = self.phase == "betting"
        done = self.phase == "done"
        for btn in (
            self.red_btn, self.black_btn, self.odd_btn, self.even_btn,
            self.low_btn, self.high_btn, self.dozen1_btn, self.dozen2_btn,
            self.dozen3_btn, self.number_btn,
        ):
            btn.disabled = not betting
        self.spin_btn.disabled = not betting
        self.play_again_btn.disabled = not done

    def _build_embed(self) -> discord.Embed:
        colors = {
            "betting":  discord.Color.gold(),
            "spinning": discord.Color.blurple(),
            "done":     discord.Color.dark_grey(),
        }
        embed = discord.Embed(
            title="🎡 Roulette",
            color=colors.get(self.phase, discord.Color.blurple()),
        )
        embed.set_author(name=self.host.display_name, icon_url=self.host.display_avatar.url)

        if self.phase == "betting":
            embed.description = f"Bet: **${self.bet:,}** — Place your bets, then the host spins."
            bets_val = (
                "\n".join(f"{b.name} — {b.label}" for b in self.bettors)
                if self.bettors else "None yet — pick a side!"
            )
            embed.add_field(name="Bets", value=bets_val, inline=False)
            embed.set_footer(
                text=f"${self.bet:,} per player · Up to {MAX_BETTORS} players · Host spins"
            )

        elif self.phase == "spinning":
            embed.add_field(name="\u200b", value=_wheel_strip(self.spin_idx), inline=False)
            bets_val = "\n".join(f"{b.name} — {b.label}" for b in self.bettors)
            embed.add_field(name="Bets", value=bets_val, inline=False)

        elif self.phase == "done":
            n = self.result
            color_str = "Green" if n == 0 else ("Red" if n in RED else "Black")
            embed.add_field(
                name="Result",
                value=f"{_color(n)} **{n}** — {color_str}",
                inline=False,
            )
            if self.bettors:
                lines = []
                for b in self.bettors:
                    icon = "✅" if b.result == "win" else "❌"
                    bal_str = f" → **${b.final_balance:,}**" if b.final_balance is not None else ""
                    lines.append(f"{icon} {b.name} ({b.label}){bal_str}")
                embed.add_field(name="Payouts", value="\n".join(lines), inline=False)

        return embed

    async def _join(
        self, interaction: discord.Interaction, bet_type: str, number: int | None = None
    ) -> None:
        if self.phase != "betting":
            await interaction.response.send_message("The wheel is already spinning.", ephemeral=True)
            return

        uid = interaction.user.id
        existing = next((b for b in self.bettors if b.user_id == uid), None)
        if existing:
            # Switch bet type — same amount, no wallet movement
            existing.bet_type = bet_type
            existing.number = number
            await interaction.response.edit_message(embed=self._build_embed(), view=self)
            return

        if len(self.bettors) >= MAX_BETTORS:
            await interaction.response.send_message("Table is full.", ephemeral=True)
            return

        new_bal = await wallet_service.withdraw(uid, self.bet)
        if new_bal is None:
            bal = await wallet_service.get_balance(uid)
            await interaction.response.send_message(
                f"Not enough to bet. Balance: **${bal:,}** (need **${self.bet:,}**)", ephemeral=True
            )
            return

        self.bettors.append(
            _Bettor(user_id=uid, name=interaction.user.display_name,
                    amount=self.bet, bet_type=bet_type, number=number)
        )
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    # ── buttons — row 0: colors + spin ────────────────────────────────────────

    @discord.ui.button(label="Red", style=discord.ButtonStyle.danger, row=0)
    async def red_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "red")

    @discord.ui.button(label="Black", style=discord.ButtonStyle.secondary, row=0)
    async def black_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "black")

    @discord.ui.button(label="Odd", style=discord.ButtonStyle.secondary, row=0)
    async def odd_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "odd")

    @discord.ui.button(label="Even", style=discord.ButtonStyle.secondary, row=0)
    async def even_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "even")

    @discord.ui.button(label="Spin ▶", style=discord.ButtonStyle.primary, row=0)
    async def spin_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.host.id:
            await interaction.response.send_message("Only the host can spin.", ephemeral=True)
            return
        if not self.bettors:
            await interaction.response.send_message("No bets placed yet.", ephemeral=True)
            return
        await self._spin(interaction)

    # ── buttons — row 1: ranges + dozens ──────────────────────────────────────

    @discord.ui.button(label="1-18", style=discord.ButtonStyle.secondary, row=1)
    async def low_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "low")

    @discord.ui.button(label="19-36", style=discord.ButtonStyle.secondary, row=1)
    async def high_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "high")

    @discord.ui.button(label="1st 12", style=discord.ButtonStyle.secondary, row=1)
    async def dozen1_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "dozen1")

    @discord.ui.button(label="2nd 12", style=discord.ButtonStyle.secondary, row=1)
    async def dozen2_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "dozen2")

    @discord.ui.button(label="3rd 12", style=discord.ButtonStyle.secondary, row=1)
    async def dozen3_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._join(interaction, "dozen3")

    # ── buttons — row 2: number + play again ──────────────────────────────────

    @discord.ui.button(label="Pick Number (35:1)", style=discord.ButtonStyle.secondary, row=2)
    async def number_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.phase != "betting":
            await interaction.response.send_message("The wheel is already spinning.", ephemeral=True)
            return
        await interaction.response.send_modal(_NumberModal(self))

    @discord.ui.button(label="Play Again", style=discord.ButtonStyle.secondary, row=2, disabled=True)
    async def play_again_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.host.id:
            await interaction.response.send_message("Only the host can restart.", ephemeral=True)
            return
        self.bettors = []
        self.result = None
        self.phase = "betting"
        self._update_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    # ── game logic ────────────────────────────────────────────────────────────

    async def _spin(self, interaction: discord.Interaction) -> None:
        result_idx = random.randrange(len(WHEEL))
        # Walk backward from result so the animation lands exactly on it
        start_idx = (result_idx - len(SPIN_DELAYS)) % len(WHEEL)

        self.phase = "spinning"
        self._update_buttons()
        await interaction.response.defer()

        for frame, delay in enumerate(SPIN_DELAYS):
            self.spin_idx = (start_idx + frame) % len(WHEEL)
            t0 = asyncio.get_event_loop().time()
            if self.message:
                await self.message.edit(embed=self._build_embed(), view=self)
            elapsed = asyncio.get_event_loop().time() - t0
            remaining = delay - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)

        # Final frame: land on result
        self.spin_idx = result_idx
        self.result = WHEEL[result_idx]
        self.phase = "done"
        self._update_buttons()
        await self._resolve()

    async def _resolve(self) -> None:
        for b in self.bettors:
            if _bet_wins(b.bet_type, b.number, self.result):
                b.result = "win"
                await leaderboard_notifier.deposit_and_notify(b.user_id, b.win_payout, "roulette")
            else:
                b.result = "lose"
            b.final_balance = await wallet_service.get_balance(b.user_id)

        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)

    async def on_timeout(self) -> None:
        if self.phase == "betting":
            for b in self.bettors:
                await wallet_service.deposit(b.user_id, b.amount)
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ── Cog ───────────────────────────────────────────────────────────────────────


class Roulette(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(name="roulette", description="Spin the roulette wheel")
    @app_commands.describe(bet="Amount to wager")
    async def roulette(self, interaction: discord.Interaction, bet: int) -> None:
        if bet <= 0:
            await interaction.response.send_message("Bet must be positive.", ephemeral=True)
            return

        view = _RouletteView(host=interaction.user, bet=bet)
        await interaction.response.send_message(embed=view._build_embed(), view=view)
        original = await interaction.original_response()
        view.message = interaction.channel.get_partial_message(original.id)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Roulette(bot))
