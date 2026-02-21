import logging
import random

import discord
from discord import app_commands
from discord.ext import commands

from bot.services import wallet_service

log = logging.getLogger(__name__)

DICE_FACES = {1: "⚀", 2: "⚁", 3: "⚂", 4: "⚃", 5: "⚄", 6: "⚅"}

_NATURAL_FLAVOR = [
    "A natural! The dice gods favor you today.",
    "Seven! Eleven! The crowd erupts!",
    "The Light shines upon your roll!",
    "Roll for initiative — critical success!",
    "Lok'tar! Fortune smiles on the bold.",
]

_CRAPS_FLAVOR = [
    "Craps! The house always wins, friend.",
    "Snake eyes. Fortune is a fickle mistress.",
    "Twelve. Boxcars. The Darkmoon goblin grins.",
    "Ace-deuce. Walk away and regroup.",
    "The dice have spoken. Poorly.",
]

_POINT_FLAVOR = [
    "Your mark is **{n}**. Roll it again to collect.",
    "Point established: **{n}**. Don't seven out!",
    "The dice remember **{n}**. Prove your worth.",
    "**{n}** is the number. The crowd holds its breath.",
]

_POINT_HIT_FLAVOR = [
    "Point made! Coins rain from the heavens!",
    "You hit your point! Loot secured.",
    "The dice remember, and so does your wallet.",
    "Well rolled! The goblin curses under his breath.",
    "Point made. The Darkmoon Faire pays its debts.",
]

_SEVEN_OUT_FLAVOR = [
    "Seven out! The house collects your gold.",
    "A seven... and your coins vanish. Classic.",
    "Seven out. Fortune favors the bold — next time.",
    "The Darkmoon goblin tips his hat. Your gold is his now.",
    "Seven. Just like that. Rough.",
]

_ONGOING_FLAVOR = [
    "Not yet. Roll again.",
    "Keep going. The point awaits.",
    "Still in it. Don't choke.",
    "Dice are warming up. Roll again.",
]


def _roll() -> tuple[int, int]:
    return random.randint(1, 6), random.randint(1, 6)


def _dice_str(d1: int, d2: int) -> str:
    return f"{DICE_FACES[d1]}  {DICE_FACES[d2]}"


def _build_embed(
    *,
    user: discord.User | discord.Member,
    d1: int,
    d2: int,
    total: int,
    wager: int,
    state: str,
    point: int | None = None,
    new_balance: int | None = None,
) -> discord.Embed:
    flavor = {
        "natural":   random.choice(_NATURAL_FLAVOR),
        "craps":     random.choice(_CRAPS_FLAVOR),
        "point":     random.choice(_POINT_FLAVOR).format(n=point),
        "ongoing":   random.choice(_ONGOING_FLAVOR),
        "point_hit": random.choice(_POINT_HIT_FLAVOR),
        "seven_out": random.choice(_SEVEN_OUT_FLAVOR),
    }[state]

    color = {
        "natural":   discord.Color.gold(),
        "craps":     discord.Color.red(),
        "point":     discord.Color.blurple(),
        "ongoing":   discord.Color.blurple(),
        "point_hit": discord.Color.green(),
        "seven_out": discord.Color.dark_red(),
    }[state]

    title = {
        "natural":   "Natural!",
        "craps":     "Craps!",
        "point":     f"Point: {point}",
        "ongoing":   f"Point: {point} — Roll Again",
        "point_hit": "Point Made!",
        "seven_out": "Seven Out!",
    }[state]

    embed = discord.Embed(title=f"🎲 Darkmoon Craps — {title}", color=color)
    embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
    embed.add_field(name="Roll", value=f"{_dice_str(d1, d2)}\n**{total}**", inline=True)
    embed.add_field(name="Wager", value=f"${wager:,}", inline=True)

    if state in ("natural", "point_hit"):
        embed.add_field(name="Result", value=f"+${wager:,}", inline=True)
    elif state in ("craps", "seven_out"):
        embed.add_field(name="Result", value=f"-${wager:,}", inline=True)
    elif point is not None:
        embed.add_field(name="Point", value=str(point), inline=True)

    if new_balance is not None:
        embed.add_field(name="Balance", value=f"${new_balance:,}", inline=False)

    embed.set_footer(text=flavor)
    return embed


class _CrapsView(discord.ui.View):
    def __init__(self, user_id: int, wager: int, point: int) -> None:
        super().__init__(timeout=120)
        self.user_id = user_id
        self.wager = wager
        self.point = point
        self.roll_num = 1
        self.message: discord.Message | None = None

    @discord.ui.button(label="Roll Again", style=discord.ButtonStyle.primary, emoji="🎲")
    async def roll_again(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "These aren't your dice!", ephemeral=True
            )
            return

        self.roll_num += 1
        d1, d2 = _roll()
        total = d1 + d2

        if total == self.point:
            new_bal = await wallet_service.deposit(self.user_id, self.wager * 2)
            embed = _build_embed(
                user=interaction.user, d1=d1, d2=d2, total=total,
                wager=self.wager, state="point_hit", point=self.point,
                new_balance=new_bal,
            )
            self.clear_items()
            self.stop()
            await interaction.response.edit_message(embed=embed, view=self)

        elif total == 7:
            bal = await wallet_service.get_balance(self.user_id)
            embed = _build_embed(
                user=interaction.user, d1=d1, d2=d2, total=total,
                wager=self.wager, state="seven_out", point=self.point,
                new_balance=bal,
            )
            self.clear_items()
            self.stop()
            await interaction.response.edit_message(embed=embed, view=self)

        else:
            embed = _build_embed(
                user=interaction.user, d1=d1, d2=d2, total=total,
                wager=self.wager, state="ongoing", point=self.point,
            )
            await interaction.response.edit_message(embed=embed, view=self)

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[union-attr]
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


class Craps(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(
        name="craps",
        description="Roll the bones at the Darkmoon Faire craps table",
    )
    @app_commands.describe(amount="Amount of gold to wager")
    async def craps(self, interaction: discord.Interaction, amount: int) -> None:
        if amount <= 0:
            await interaction.response.send_message(
                "Wager must be at least $1.", ephemeral=True
            )
            return

        new_bal = await wallet_service.withdraw(interaction.user.id, amount)
        if new_bal is None:
            bal = await wallet_service.get_balance(interaction.user.id)
            await interaction.response.send_message(
                f"Not enough gold! Balance: **${bal:,}**", ephemeral=True
            )
            return

        d1, d2 = _roll()
        total = d1 + d2

        if total in (7, 11):
            new_bal = await wallet_service.deposit(interaction.user.id, amount * 2)
            embed = _build_embed(
                user=interaction.user, d1=d1, d2=d2, total=total,
                wager=amount, state="natural", new_balance=new_bal,
            )
            await interaction.response.send_message(embed=embed)

        elif total in (2, 3, 12):
            embed = _build_embed(
                user=interaction.user, d1=d1, d2=d2, total=total,
                wager=amount, state="craps", new_balance=new_bal,
            )
            await interaction.response.send_message(embed=embed)

        else:
            view = _CrapsView(user_id=interaction.user.id, wager=amount, point=total)
            embed = _build_embed(
                user=interaction.user, d1=d1, d2=d2, total=total,
                wager=amount, state="point", point=total,
            )
            await interaction.response.send_message(embed=embed, view=view)
            view.message = await interaction.original_response()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Craps(bot))
