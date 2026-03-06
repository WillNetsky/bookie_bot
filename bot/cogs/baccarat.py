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
MAX_BETTORS = 8
CHOICE_LABEL = {"player": "Player", "banker": "Banker", "tie": "Tie"}


def _draw() -> str:
    return random.choice(RANKS) + random.choice(SUITS)


def _card_val(card: str) -> int:
    rank = card[:-1]
    if rank in ("10", "J", "Q", "K"):
        return 0
    if rank == "A":
        return 1
    return int(rank)


def _hand_val(hand: list[str]) -> int:
    return sum(_card_val(c) for c in hand) % 10


def _fmt_hand(hand: list[str]) -> str:
    return "  ".join(hand)


def _banker_draws(banker_hand: list[str], player_third: str | None) -> bool:
    """Return True if the Banker should draw a third card."""
    bval = _hand_val(banker_hand)
    if bval >= 7:
        return False
    if bval <= 2:
        return True
    # When Player stood (no third card), Banker draws on 0-5
    if player_third is None:
        return bval <= 5
    ptv = _card_val(player_third)
    if bval == 3:
        return ptv != 8
    if bval == 4:
        return ptv in (2, 3, 4, 5, 6, 7)
    if bval == 5:
        return ptv in (4, 5, 6, 7)
    if bval == 6:
        return ptv in (6, 7)
    return False  # bval == 7: stand


@dataclass
class _Bettor:
    user_id: int
    name: str
    amount: int
    choice: str  # "player" | "banker" | "tie"
    result: str | None = None  # "win" | "lose" | "push"
    final_balance: int | None = None


# ── View ──────────────────────────────────────────────────────────────────────


class _BaccaratView(discord.ui.View):
    def __init__(self, host: discord.User | discord.Member, bet: int) -> None:
        super().__init__(timeout=300)
        self.host = host
        self.bet = bet
        self.bettors: list[_Bettor] = []
        self.player_hand: list[str] = []
        self.banker_hand: list[str] = []
        self.phase = "betting"  # betting | dealing | done
        self.message: discord.PartialMessage | None = None
        self._update_buttons()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _update_buttons(self) -> None:
        betting = self.phase == "betting"
        done = self.phase == "done"
        self.bet_player_btn.disabled = not betting
        self.bet_banker_btn.disabled = not betting
        self.bet_tie_btn.disabled = not betting
        self.deal_btn.disabled = not betting
        self.play_again_btn.disabled = not done

    def _build_embed(self) -> discord.Embed:
        colors = {
            "betting": discord.Color.gold(),
            "dealing": discord.Color.blurple(),
            "done": discord.Color.dark_grey(),
        }
        embed = discord.Embed(title="🎴 Baccarat", color=colors.get(self.phase, discord.Color.blurple()))
        embed.set_author(name=self.host.display_name, icon_url=self.host.display_avatar.url)

        if self.phase == "betting":
            embed.description = f"Bet: **${self.bet:,}** — Choose your side, then the host deals."
            for choice in ("player", "banker", "tie"):
                names = [b.name for b in self.bettors if b.choice == choice]
                if names:
                    embed.add_field(
                        name=CHOICE_LABEL[choice],
                        value="\n".join(names),
                        inline=True,
                    )
            if not self.bettors:
                embed.add_field(name="Players", value="None yet — pick a side!", inline=False)
            embed.set_footer(text=f"Bet: ${self.bet:,} · Up to {MAX_BETTORS} players · Host presses Deal to start")
        else:
            pval = _hand_val(self.player_hand)
            bval = _hand_val(self.banker_hand)
            embed.add_field(
                name=f"Player ({pval})",
                value=_fmt_hand(self.player_hand),
                inline=True,
            )
            embed.add_field(
                name=f"Banker ({bval})" if self.banker_hand else "Banker",
                value=_fmt_hand(self.banker_hand) if self.banker_hand else "...",
                inline=True,
            )
            if self.phase == "done":
                if pval > bval:
                    outcome = "Player wins!"
                elif bval > pval:
                    outcome = "Banker wins!"
                else:
                    outcome = "Tie!"
                embed.add_field(name="Result", value=outcome, inline=False)

                if self.bettors:
                    lines = []
                    for b in self.bettors:
                        icon = "✅" if b.result == "win" else ("🔁" if b.result == "push" else "❌")
                        bal_str = f" → **${b.final_balance:,}**" if b.final_balance is not None else ""
                        lines.append(f"{icon} {b.name} ({CHOICE_LABEL[b.choice]}){bal_str}")
                    embed.add_field(name="Payouts", value="\n".join(lines), inline=False)

        return embed

    async def _safe_edit(self, interaction: discord.Interaction) -> None:
        try:
            await interaction.response.edit_message(embed=self._build_embed(), view=self)
        except discord.NotFound:
            if self.message:
                try:
                    await self.message.edit(embed=self._build_embed(), view=self)
                except discord.HTTPException:
                    pass

    async def _join(self, interaction: discord.Interaction, choice: str) -> None:
        if self.phase != "betting":
            await interaction.response.send_message("Game is already in progress.", ephemeral=True)
            return

        uid = interaction.user.id
        existing = next((b for b in self.bettors if b.user_id == uid), None)
        if existing:
            if existing.choice == choice:
                await interaction.response.send_message(
                    f"You're already betting on {CHOICE_LABEL[choice]}.", ephemeral=True
                )
                return
            # Switch side — bet amount already paid, no money movement needed
            existing.choice = choice
            await interaction.response.edit_message(embed=self._build_embed(), view=self)
            return

        if len(self.bettors) >= MAX_BETTORS:
            await interaction.response.send_message("Table is full.", ephemeral=True)
            return

        new_bal = await wallet_service.withdraw(uid, self.bet)
        if new_bal is None:
            bal = await wallet_service.get_balance(uid)
            await interaction.response.send_message(
                f"Not enough to join. Balance: **${bal:,}** (need **${self.bet:,}**)", ephemeral=True
            )
            return

        self.bettors.append(
            _Bettor(user_id=uid, name=interaction.user.display_name, amount=self.bet, choice=choice)
        )
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    # ── buttons ───────────────────────────────────────────────────────────────

    @discord.ui.button(label="Bet Player", style=discord.ButtonStyle.success)
    async def bet_player_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._join(interaction, "player")

    @discord.ui.button(label="Bet Banker", style=discord.ButtonStyle.danger)
    async def bet_banker_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._join(interaction, "banker")

    @discord.ui.button(label="Bet Tie", style=discord.ButtonStyle.secondary)
    async def bet_tie_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._join(interaction, "tie")

    @discord.ui.button(label="Deal", style=discord.ButtonStyle.primary)
    async def deal_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.host.id:
            await interaction.response.send_message("Only the host can deal.", ephemeral=True)
            return
        if not self.bettors:
            await interaction.response.send_message("No players at the table.", ephemeral=True)
            return
        await self._deal(interaction)

    @discord.ui.button(label="Play Again", style=discord.ButtonStyle.secondary, disabled=True)
    async def play_again_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.host.id:
            await interaction.response.send_message("Only the host can restart.", ephemeral=True)
            return
        self.bettors = []
        self.player_hand = []
        self.banker_hand = []
        self.phase = "betting"
        self._update_buttons()
        await self._safe_edit(interaction)

    # ── game logic ────────────────────────────────────────────────────────────

    async def _deal(self, interaction: discord.Interaction) -> None:
        self.phase = "dealing"
        self._update_buttons()
        await interaction.response.defer()

        # Step 1: Player's 2 cards
        self.player_hand = [_draw(), _draw()]
        self.banker_hand = []
        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)
        await asyncio.sleep(1.5)

        # Step 2: Banker's 2 cards
        self.banker_hand = [_draw(), _draw()]
        if self.message:
            await self.message.edit(embed=self._build_embed(), view=self)

        pval = _hand_val(self.player_hand)
        bval = _hand_val(self.banker_hand)
        natural = pval >= 8 or bval >= 8

        player_third: str | None = None
        if not natural:
            # Step 3: Player's 3rd card
            if pval <= 5:
                await asyncio.sleep(1.5)
                player_third = _draw()
                self.player_hand.append(player_third)
                if self.message:
                    await self.message.edit(embed=self._build_embed(), view=self)

            # Step 4: Banker's 3rd card
            if _banker_draws(self.banker_hand, player_third):
                await asyncio.sleep(1.5)
                self.banker_hand.append(_draw())
                if self.message:
                    await self.message.edit(embed=self._build_embed(), view=self)

        self.phase = "done"
        self._update_buttons()
        await self._resolve()

    async def _resolve(self) -> None:
        pval = _hand_val(self.player_hand)
        bval = _hand_val(self.banker_hand)

        if pval > bval:
            winner = "player"
        elif bval > pval:
            winner = "banker"
        else:
            winner = "tie"

        for b in self.bettors:
            if winner == "tie":
                if b.choice == "tie":
                    b.result = "win"
                    await leaderboard_notifier.deposit_and_notify(b.user_id, b.amount * 9, "baccarat")
                else:
                    # Player/Banker bets push on tie
                    b.result = "push"
                    await wallet_service.deposit(b.user_id, b.amount)
            elif b.choice == winner:
                b.result = "win"
                if winner == "banker":
                    # 5% commission on banker wins
                    payout = b.amount + int(b.amount * 0.95)
                else:
                    payout = b.amount * 2
                await leaderboard_notifier.deposit_and_notify(b.user_id, payout, "baccarat")
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


class Baccarat(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(
        name="baccarat",
        description="Open a baccarat table — bet on Player, Banker, or Tie",
    )
    @app_commands.describe(bet="Amount to wager")
    async def baccarat(self, interaction: discord.Interaction, bet: int) -> None:
        if bet <= 0:
            await interaction.response.send_message("Bet must be positive.", ephemeral=True)
            return

        view = _BaccaratView(host=interaction.user, bet=bet)
        await interaction.response.send_message(embed=view._build_embed(), view=view)
        original = await interaction.original_response()
        view.message = interaction.channel.get_partial_message(original.id)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Baccarat(bot))
