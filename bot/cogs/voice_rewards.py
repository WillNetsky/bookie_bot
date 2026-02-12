import time

import discord
from discord.ext import commands

from bot.config import COINS_PER_MINUTE
from bot.services import wallet_service


class VoiceRewards(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._join_times: dict[int, float] = {}

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if member.bot:
            return

        # User joined a voice channel
        if before.channel is None and after.channel is not None:
            self._join_times[member.id] = time.time()

        # User left a voice channel
        elif before.channel is not None and after.channel is None:
            join_time = self._join_times.pop(member.id, None)
            if join_time is None:
                return
            elapsed_minutes = int((time.time() - join_time) / 60)
            if elapsed_minutes > 0:
                await wallet_service.add_voice_reward(
                    member.id, elapsed_minutes, COINS_PER_MINUTE
                )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(VoiceRewards(bot))
