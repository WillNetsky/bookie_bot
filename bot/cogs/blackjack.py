import asyncio
import logging
import random
from dataclasses import dataclass, field

import discord
from discord import app_commands
from discord.ext import commands

from bot.services import wallet_service, leaderboard_notifier

log = logging.getLogger(__name__)

RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
SUITS = ["♠", "♥", "♦", "♣"]
MAX_PLAYERS = 6


def _draw() -> str:
    return random.choice(RANKS) + random.choice(SUITS)


def _value(hand: list[str]) -> int:
    total = 0
    aces = 0
    for card in hand:
        rank = card[:-1]  # strip suit (last char)
        if rank in ("J", "Q", "K"):
            total += 10
        elif rank == "A":
            aces += 1
            total += 11
        else:
            total += int(rank)
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total


def _is_bj(hand: list[str]) -> bool:
    return len(hand) == 2 and _value(hand) == 21


def _fmt(hand: list[str], hide_hole: bool = False) -> str:
    if hide_hole and len(hand) >= 2:
        return f"{hand[0]}  ??"
    return "  ".join(hand)


@dataclass
class _Player:
    user_id: int
    name: str
    bet: int
    hand: list[str] = field(default_factory=list)
    stood: bool = False
    doubled: bool = False
    result: str | None = None  # "blackjack" | "win" | "push" | "lose"
    final_balance: int | None = None

    @property
    def val(self) -> int:
        return _value(self.hand)

    @property
    def busted(self) -> bool:
        return self.val > 21

    @property
    def blackjack(self) -> bool:
        return _is_bj(self.hand)

    @property
    def done(self) -> bool:
        return self.stood or self.busted or self.blackjack or self.result is not None


# ── View ──────────────────────────────────────────────────────────────────────


class _BlackjackView(discord.ui.View):
    def __init__(self, host: discord.User | discord.Member, bet: int) -> None:
        super().__init__(timeout=300)
        self.host = host
        self.bet = bet
        self.players: list[_Player] = []
        self.dealer_hand: list[str] = []
        self.phase = "joining"  # joining | playing | dealer | done
        self.current_idx: int = 0
        self.message: discord.Message | None = None
        self._update_buttons()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _update_buttons(self) -> None:
        joining = self.phase == "joining"
        playing = self.phase == "playing"
        done = self.phase == "done"
        self.join_btn.disabled = not joining
        self.deal_btn.disabled = not joining
        self.hit_btn.disabled = not playing
        self.stand_btn.disabled = not playing
        self.double_btn.disabled = not playing
        self.restart_btn.disabled = not done

    def _build_embed(self) -> discord.Embed:
        colors = {
            "joining": discord.Color.yellow(),
            "playing": discord.Color.blurple(),
            "dealer": discord.Color.orange(),
            "done": discord.Color.dark_grey(),
        }
        embed = discord.Embed(
            title="🃏 Blackjack",
            color=colors.get(self.phase, discord.Color.blurple()),
        )
        embed.set_author(
            name=self.host.display_name,
            icon_url=self.host.display_avatar.url,
        )

        # Dealer row
        if self.dealer_hand:
            hide = self.phase == "playing"
            val_str = f" ({_value(self.dealer_hand)})" if not hide else ""
            embed.add_field(
                name=f"Dealer{val_str}",
                value=_fmt(self.dealer_hand, hide_hole=hide),
                inline=False,
            )
        else:
            embed.add_field(name="Dealer", value="Waiting...", inline=False)

        # Player fields
        if not self.players:
            embed.add_field(name="Players", value="None yet — press Join!", inline=False)
        else:
            for i, p in enumerate(self.players):
                label = p.name
                if self.phase == "playing" and i == self.current_idx and not p.done:
                    label += " ←"

                if p.result == "blackjack":
                    status = "🃏 Blackjack!"
                elif p.result == "win":
                    status = "✅ Win"
                elif p.result == "push":
                    status = "🔁 Push"
                elif p.result == "lose":
                    status = "❌ Lose"
                elif p.busted:
                    status = "💀 Bust"
                elif p.blackjack and self.phase != "joining":
                    status = "🃏 Blackjack!"
                elif p.stood:
                    status = "Stand"
                else:
                    status = ""

                lines = []
                if p.hand:
                    lines.append(f"{_fmt(p.hand)} ({p.val})")
                else:
                    lines.append("—")
                lines.append(f"Bet: **${p.bet:,}**")
                if status:
                    lines.append(status)
                if self.phase == "done" and p.final_balance is not None:
                    lines.append(f"Balance: **${p.final_balance:,}**")

                embed.add_field(name=label, value="\n".join(lines), inline=True)

        if self.phase == "joining":
            embed.set_footer(text=f"Bet: ${self.bet:,} · Up to {MAX_PLAYERS} players · Host presses Deal to start")

        return embed

    # ── buttons ───────────────────────────────────────────────────────────────

    @discord.ui.button(label="Join", style=discord.ButtonStyle.secondary)
    async def join_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        uid = interaction.user.id

        if self.phase != "joining":
            await interaction.response.send_message("Game has already started.", ephemeral=True)
            return
        if any(p.user_id == uid for p in self.players):
            await interaction.response.send_message("You're already at the table.", ephemeral=True)
            return
        if len(self.players) >= MAX_PLAYERS:
            await interaction.response.send_message("Table is full.", ephemeral=True)
            return

        new_bal = await wallet_service.withdraw(uid, self.bet)
        if new_bal is None:
            bal = await wallet_service.get_balance(uid)
            await interaction.response.send_message(
                f"Not enough to join. Balance: **${bal:,}** (need **${self.bet:,}**)", ephemeral=True
            )
            return

        self.players.append(_Player(user_id=uid, name=interaction.user.display_name, bet=self.bet))
        await interaction.response.send_message(f"Joined! **${self.bet:,}** deducted.", ephemeral=True)
        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Deal", style=discord.ButtonStyle.primary)
    async def deal_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.host.id:
            await interaction.response.send_message("Only the host can deal.", ephemeral=True)
            return
        if not self.players:
            await interaction.response.send_message("Need at least one player.", ephemeral=True)
            return
        await self._deal(interaction)

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.success)
    async def hit_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if self.phase != "playing":
            await interaction.response.send_message("Not your turn.", ephemeral=True)
            return
        p = self.players[self.current_idx]
        if interaction.user.id != p.user_id:
            await interaction.response.send_message("Not your turn.", ephemeral=True)
            return

        p.hand.append(_draw())
        # Disable double — can't double after hitting
        self.double_btn.disabled = True

        if p.busted:
            await interaction.response.edit_message(embed=self._build_embed(), view=self)
            await self._advance()
        else:
            await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.danger)
    async def stand_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if self.phase != "playing":
            await interaction.response.send_message("Not your turn.", ephemeral=True)
            return
        p = self.players[self.current_idx]
        if interaction.user.id != p.user_id:
            await interaction.response.send_message("Not your turn.", ephemeral=True)
            return

        p.stood = True
        await interaction.response.edit_message(embed=self._build_embed(), view=self)
        await self._advance()

    @discord.ui.button(label="Restart", style=discord.ButtonStyle.secondary, disabled=True)
    async def restart_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.host.id:
            await interaction.response.send_message("Only the host can restart.", ephemeral=True)
            return

        self.players = []
        self.dealer_hand = []
        self.phase = "joining"
        self.current_idx = 0
        self._update_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Double", style=discord.ButtonStyle.secondary)
    async def double_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if self.phase != "playing":
            await interaction.response.send_message("Not your turn.", ephemeral=True)
            return
        p = self.players[self.current_idx]
        if interaction.user.id != p.user_id:
            await interaction.response.send_message("Not your turn.", ephemeral=True)
            return
        if len(p.hand) != 2:
            await interaction.response.send_message(
                "Can only double on first two cards.", ephemeral=True
            )
            return

        new_bal = await wallet_service.withdraw(p.user_id, p.bet)
        if new_bal is None:
            bal = await wallet_service.get_balance(p.user_id)
            await interaction.response.send_message(
                f"Not enough to double. Balance: **${bal:,}**", ephemeral=True
            )
            return

        p.bet *= 2
        p.doubled = True
        p.hand.append(_draw())
        p.stood = True  # must stand after doubling

        await interaction.response.edit_message(embed=self._build_embed(), view=self)
        await self._advance()

    # ── game logic ────────────────────────────────────────────────────────────

    async def _deal(self, interaction: discord.Interaction) -> None:
        self.phase = "playing"
        for p in self.players:
            p.hand = [_draw(), _draw()]
        self.dealer_hand = [_draw(), _draw()]

        # Find first non-done player (skip instant blackjacks)
        self.current_idx = 0
        while self.current_idx < len(self.players) and self.players[self.current_idx].done:
            self.current_idx += 1

        self._update_buttons()

        if self.current_idx >= len(self.players):
            # All players have blackjack — go straight to dealer
            await interaction.response.edit_message(embed=self._build_embed(), view=self)
            await self._dealer_play()
        else:
            # Double starts enabled (first player always has 2 cards)
            self.double_btn.disabled = False
            await interaction.response.edit_message(embed=self._build_embed(), view=self)

    async def _advance(self) -> None:
        self.current_idx += 1
        while self.current_idx < len(self.players) and self.players[self.current_idx].done:
            self.current_idx += 1

        if self.current_idx >= len(self.players):
            await self._dealer_play()
        else:
            p = self.players[self.current_idx]
            self.double_btn.disabled = len(p.hand) != 2
            if self.message:
                await self.message.edit(embed=self._build_embed(), view=self)

    async def _dealer_play(self) -> None:
        self.phase = "dealer"
        self._update_buttons()
        # Reveal hole card
        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)

        while _value(self.dealer_hand) < 17:
            await asyncio.sleep(1.5)
            self.dealer_hand.append(_draw())
            if self.message:
                await self.message.edit(embed=self._build_embed(), view=self)

        await self._resolve()

    async def _resolve(self) -> None:
        self.phase = "done"
        self._update_buttons()

        dealer_val = _value(self.dealer_hand)
        dealer_bj = _is_bj(self.dealer_hand)

        for p in self.players:
            if p.busted:
                p.result = "lose"
            elif p.blackjack and not dealer_bj:
                p.result = "blackjack"
                await leaderboard_notifier.deposit_and_notify(p.user_id, p.bet + int(p.bet * 1.5), "blackjack")
            elif p.blackjack and dealer_bj:
                p.result = "push"
                await wallet_service.deposit(p.user_id, p.bet)
            elif dealer_bj:
                p.result = "lose"
            elif dealer_val > 21 or p.val > dealer_val:
                p.result = "win"
                await leaderboard_notifier.deposit_and_notify(p.user_id, p.bet * 2, "blackjack")
            elif p.val == dealer_val:
                p.result = "push"
                await wallet_service.deposit(p.user_id, p.bet)
            else:
                p.result = "lose"
            p.final_balance = await wallet_service.get_balance(p.user_id)

        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)

    async def on_timeout(self) -> None:
        if self.phase == "joining":
            for p in self.players:
                await wallet_service.deposit(p.user_id, p.bet)
        elif self.phase == "playing":
            for p in self.players:
                if p.result is None and not p.busted:
                    await wallet_service.deposit(p.user_id, p.bet)
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ── Cog ───────────────────────────────────────────────────────────────────────


class Blackjack(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(
        name="blackjack",
        description="Open a blackjack table — others can join before the deal",
    )
    @app_commands.describe(bet="Amount everyone at the table bets")
    async def blackjack(self, interaction: discord.Interaction, bet: int) -> None:
        if bet <= 0:
            await interaction.response.send_message("Bet must be positive.", ephemeral=True)
            return

        # Charge the host immediately
        new_bal = await wallet_service.withdraw(interaction.user.id, bet)
        if new_bal is None:
            bal = await wallet_service.get_balance(interaction.user.id)
            await interaction.response.send_message(
                f"Not enough to start. Balance: **${bal:,}** (need **${bet:,}**)", ephemeral=True
            )
            return

        view = _BlackjackView(host=interaction.user, bet=bet)
        view.players.append(_Player(user_id=interaction.user.id, name=interaction.user.display_name, bet=bet))
        await interaction.response.send_message(embed=view._build_embed(), view=view)
        view.message = await interaction.original_response()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Blackjack(bot))
