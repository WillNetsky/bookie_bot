import asyncio
import logging
import random
from dataclasses import dataclass

import discord
from discord import app_commands
from discord.ext import commands

from bot.services import wallet_service, leaderboard_notifier

log = logging.getLogger(__name__)

# ── Symbols ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Symbol:
    emoji: str
    weight: int
    payout: int  # total returned = payout × line_bet on 3-of-a-kind


# Approx 85% RTP per payline
PENNY_SYMBOLS: tuple[_Symbol, ...] = (
    _Symbol("🍒", 6,   7),
    _Symbol("🍋", 4,  12),
    _Symbol("🍊", 3,  22),
    _Symbol("🔔", 2,  55),
    _Symbol("💎", 1, 175),
    _Symbol("🌟", 1, 700),
)

# Paylines: list of (reel, row) — reel 0=left, row 0=top
PAYLINES: list[list[tuple[int, int]]] = [
    [(0, 1), (1, 1), (2, 1)],  # 1 — middle
    [(0, 0), (1, 0), (2, 0)],  # 2 — top
    [(0, 2), (1, 2), (2, 2)],  # 3 — bottom
    [(0, 0), (1, 1), (2, 2)],  # 4 — diagonal ↘
    [(0, 2), (1, 1), (2, 0)],  # 5 — diagonal ↗
]
PAYLINE_LABELS = ["Mid", "Top", "Bot", "↘", "↗"]

# ── Machines ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Machine:
    id: str
    label: str
    emoji: str
    bet_per_line: int
    min_balance: int
    symbols: tuple[_Symbol, ...]


MACHINES: list[_Machine] = [
    _Machine("penny",      "Penny Slots",  "🎰",  1,      0, PENNY_SYMBOLS),
    _Machine("dollar",     "Dollar Slots", "💵",  5,  1_000, PENNY_SYMBOLS),
    _Machine("highroller", "High Roller",  "👑", 25, 10_000, PENNY_SYMBOLS),
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def _draw_reel(symbols: tuple[_Symbol, ...]) -> list[str]:
    weights = [s.weight for s in symbols]
    return [s.emoji for s in random.choices(symbols, weights=weights, k=3)]


def _blur_reel(symbols: tuple[_Symbol, ...]) -> list[str]:
    return [random.choice(symbols).emoji for _ in range(3)]


def _evaluate(
    reels: list[list[str]],
    payout_map: dict[str, int],
    active_lines: int,
) -> list[tuple[int, str, int]]:
    wins = []
    for i, pl in enumerate(PAYLINES[:active_lines]):
        syms = [reels[r][row] for r, row in pl]
        if syms[0] == syms[1] == syms[2] and syms[0] in payout_map:
            wins.append((i + 1, syms[0], payout_map[syms[0]]))
    return wins


def _grid_str(reels: list[list[str]], stopped: int, blur: list[list[str]]) -> str:
    rows = []
    for row in range(3):
        cols = []
        for reel in range(3):
            cols.append(reels[reel][row] if reel < stopped else blur[reel][row])
        rows.append("  ".join(cols))
    return "\n".join(rows)


def _pay_table_str(symbols: tuple[_Symbol, ...]) -> str:
    entries = [f"{s.emoji}×3 → **{s.payout}×**" for s in symbols]
    rows = ["  ·  ".join(entries[i:i + 2]) for i in range(0, len(entries), 2)]
    return "\n".join(rows)


# ── Slot View ─────────────────────────────────────────────────────────────────


class _SlotsView(discord.ui.View):
    def __init__(
        self,
        user: discord.User | discord.Member,
        machine: _Machine,
        balance: int,
    ) -> None:
        super().__init__(timeout=300)
        self.user = user
        self.machine = machine
        self.balance = balance
        self.active_lines = 1
        self.phase = "idle"  # idle | spinning | done
        self._payout_map = {s.emoji: s.payout for s in machine.symbols}
        # Reels: reels[reel][row]
        self.reels: list[list[str]] = [_blur_reel(machine.symbols) for _ in range(3)]
        self.blur: list[list[str]] = [_blur_reel(machine.symbols) for _ in range(3)]
        self.stopped = 3  # all "stopped" in idle (shows neutral symbols)
        self.wins: list[tuple[int, str, int]] = []
        self.total_won = 0
        self.spin_cost = 0
        self.message: discord.PartialMessage | None = None
        self._update_buttons()

    @property
    def cost(self) -> int:
        return self.active_lines * self.machine.bet_per_line

    def _update_buttons(self) -> None:
        spinning = self.phase == "spinning"
        for i, btn in enumerate([self.l1, self.l2, self.l3, self.l4, self.l5], 1):
            btn.disabled = spinning
            btn.style = (
                discord.ButtonStyle.success
                if i == self.active_lines
                else discord.ButtonStyle.secondary
            )
        self.spin_btn.disabled = spinning

    def _build_embed(self) -> discord.Embed:
        m = self.machine
        color = discord.Color.blurple() if self.phase == "spinning" else discord.Color.gold()
        embed = discord.Embed(title=f"{m.emoji} {m.label}", color=color)

        embed.add_field(
            name="\u200b",
            value=_grid_str(self.reels, self.stopped, self.blur),
            inline=False,
        )
        embed.add_field(
            name="Lines",
            value=f"**{self.active_lines}** line{'s' if self.active_lines > 1 else ''} · **${self.cost:,}** per spin",
            inline=True,
        )
        embed.add_field(name="Balance", value=f"**${self.balance:,}**", inline=True)
        embed.add_field(name="Pay Table", value=_pay_table_str(m.symbols), inline=False)

        if self.phase == "done":
            if self.wins:
                lines = []
                for line_num, emoji, mult in self.wins:
                    amt = mult * m.bet_per_line
                    lines.append(
                        f"Line {line_num} ({PAYLINE_LABELS[line_num - 1]}): "
                        f"{emoji}×3 → **{mult}×** = **${amt:,}**"
                    )
                lines.append(f"\nBet **${self.spin_cost:,}** · Won **${self.total_won:,}**")
                embed.add_field(name="🏆 Winner!", value="\n".join(lines), inline=False)
            else:
                embed.add_field(
                    name="Result",
                    value=f"No win · Bet **${self.spin_cost:,}**",
                    inline=False,
                )

        return embed

    async def _edit(self, delay: float) -> None:
        """Edit message and sleep for the remainder of `delay` seconds."""
        if not self.message:
            return
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        await self.message.edit(embed=self._build_embed(), view=self)
        remaining = delay - (loop.time() - t0)
        if remaining > 0:
            await asyncio.sleep(remaining)

    async def _set_lines(self, interaction: discord.Interaction, n: int) -> None:
        self.active_lines = n
        self._update_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    # ── line buttons (row 0) ──────────────────────────────────────────────────

    @discord.ui.button(label="1 Line",  style=discord.ButtonStyle.success,   row=0)
    async def l1(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._set_lines(interaction, 1)

    @discord.ui.button(label="2 Lines", style=discord.ButtonStyle.secondary, row=0)
    async def l2(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._set_lines(interaction, 2)

    @discord.ui.button(label="3 Lines", style=discord.ButtonStyle.secondary, row=0)
    async def l3(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._set_lines(interaction, 3)

    @discord.ui.button(label="4 Lines", style=discord.ButtonStyle.secondary, row=0)
    async def l4(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._set_lines(interaction, 4)

    @discord.ui.button(label="5 Lines", style=discord.ButtonStyle.secondary, row=0)
    async def l5(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._set_lines(interaction, 5)

    # ── spin button (row 1) ───────────────────────────────────────────────────

    @discord.ui.button(label="Spin ▶", style=discord.ButtonStyle.primary, row=1)
    async def spin_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("This machine is taken.", ephemeral=True)
            return

        new_bal = await wallet_service.withdraw(self.user.id, self.cost)
        if new_bal is None:
            bal = await wallet_service.get_balance(self.user.id)
            await interaction.response.send_message(
                f"Not enough to spin. Balance: **${bal:,}** (need **${self.cost:,}**)",
                ephemeral=True,
            )
            return

        self.balance = new_bal
        self.spin_cost = self.cost
        self.phase = "spinning"
        self.wins = []
        self.total_won = 0
        self._update_buttons()
        await interaction.response.defer()
        await self._animate()

    # ── animation ─────────────────────────────────────────────────────────────

    async def _animate(self) -> None:
        final = [_draw_reel(self.machine.symbols) for _ in range(3)]
        self.stopped = 0

        # All reels spinning
        for _ in range(3):
            self.blur = [_blur_reel(self.machine.symbols) for _ in range(3)]
            await self._edit(0.5)

        # Reel 1 locks
        self.reels[0] = final[0]
        self.stopped = 1
        await self._edit(0.5)

        # Reels 2+3 keep spinning
        for _ in range(2):
            self.blur[1] = _blur_reel(self.machine.symbols)
            self.blur[2] = _blur_reel(self.machine.symbols)
            await self._edit(0.5)

        # Reel 2 locks
        self.reels[1] = final[1]
        self.stopped = 2
        await self._edit(0.5)

        # Reel 3 keeps spinning
        for _ in range(2):
            self.blur[2] = _blur_reel(self.machine.symbols)
            await self._edit(0.5)

        # Reel 3 locks — resolve
        self.reels[2] = final[2]
        self.stopped = 3
        self.wins = _evaluate(final, self._payout_map, self.active_lines)
        self.total_won = sum(mult * self.machine.bet_per_line for _, _, mult in self.wins)

        if self.total_won > 0:
            await leaderboard_notifier.deposit_and_notify(
                self.user.id, self.total_won, "slots"
            )

        try:
            await wallet_service.record_game(
                self.user.id, "slots", self.spin_cost, self.total_won, won=self.total_won > 0
            )
        except Exception:
            pass

        self.balance = await wallet_service.get_balance(self.user.id)
        self.phase = "done"
        self._update_buttons()

        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ── Lobby View ────────────────────────────────────────────────────────────────


class _LobbyView(discord.ui.View):
    def __init__(self, user: discord.User | discord.Member, balance: int) -> None:
        super().__init__(timeout=120)
        self.user = user
        self.balance = balance
        self.message: discord.PartialMessage | None = None

        for machine in MACHINES:
            accessible = balance >= machine.min_balance
            label = f"{machine.emoji} {machine.label}"
            if not accessible:
                label += f"  (${machine.min_balance:,} min)"
            btn = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.success if accessible else discord.ButtonStyle.secondary,
                disabled=not accessible,
                custom_id=machine.id,
                row=0,
            )
            btn.callback = self._make_callback(machine)
            self.add_item(btn)

    def _make_callback(self, machine: _Machine):
        async def callback(interaction: discord.Interaction) -> None:
            if interaction.user.id != self.user.id:
                await interaction.response.send_message("Not your session.", ephemeral=True)
                return
            view = _SlotsView(self.user, machine, self.balance)
            view.message = self.message
            await interaction.response.edit_message(embed=view._build_embed(), view=view)
        return callback

    def _build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🎰 Slots",
            description="Pick a machine.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Balance", value=f"**${self.balance:,}**", inline=False)
        for m in MACHINES:
            accessible = self.balance >= m.min_balance
            status = "Available" if accessible else f"Requires ${m.min_balance:,}"
            embed.add_field(
                name=f"{m.emoji} {m.label}",
                value=f"${m.bet_per_line:,}/line · {status}",
                inline=True,
            )
        return embed

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ── Cog ───────────────────────────────────────────────────────────────────────


class Slots(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(name="slots", description="Play the slot machines")
    async def slots(self, interaction: discord.Interaction) -> None:
        balance = await wallet_service.get_balance(interaction.user.id)
        view = _LobbyView(user=interaction.user, balance=balance)
        await interaction.response.send_message(embed=view._build_embed(), view=view)
        original = await interaction.original_response()
        view.message = interaction.channel.get_partial_message(original.id)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Slots(bot))
