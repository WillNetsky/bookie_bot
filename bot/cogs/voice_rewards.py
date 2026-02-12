import logging
import time

import discord
from discord.ext import commands, tasks

from bot.config import VOICE_PAYOUT_INTERVAL, VOICE_PAYOUT_AMOUNT
from bot.services import wallet_service
from bot.db import models

log = logging.getLogger(__name__)


class VoiceRewards(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        # Tracks {user_id: last_payout_timestamp}
        self._last_payout: dict[int, float] = {}

    async def cog_load(self) -> None:
        self.payout_loop.start()

    async def cog_unload(self) -> None:
        self.payout_loop.cancel()

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if member.bot:
            return

        # User joined a voice channel — start tracking
        if before.channel is None and after.channel is not None:
            self._last_payout[member.id] = time.time()

        # User left all voice channels — pay remaining time and stop tracking
        elif before.channel is not None and after.channel is None:
            last = self._last_payout.pop(member.id, None)
            if last is None:
                return
            elapsed_minutes = int((time.time() - last) / 60)
            intervals = elapsed_minutes // VOICE_PAYOUT_INTERVAL
            if intervals > 0:
                reward = intervals * VOICE_PAYOUT_AMOUNT
                await models.get_or_create_user(member.id)
                await wallet_service.add_voice_reward(
                    member.id, intervals * VOICE_PAYOUT_INTERVAL, reward
                )

    @tasks.loop(minutes=VOICE_PAYOUT_INTERVAL)
    async def payout_loop(self) -> None:
        """Pay everyone currently in voice every VOICE_PAYOUT_INTERVAL minutes."""
        now = time.time()
        threshold = VOICE_PAYOUT_INTERVAL * 60

        for guild in self.bot.guilds:
            for vc in guild.voice_channels:
                for member in vc.members:
                    if member.bot:
                        continue

                    last = self._last_payout.get(member.id)

                    # If we don't have them tracked (bot restarted), start now
                    if last is None:
                        self._last_payout[member.id] = now
                        continue

                    elapsed = now - last
                    if elapsed >= threshold:
                        intervals = int(elapsed // threshold)
                        reward = intervals * VOICE_PAYOUT_AMOUNT
                        minutes = intervals * VOICE_PAYOUT_INTERVAL

                        await models.get_or_create_user(member.id)
                        new_balance = await wallet_service.add_voice_reward(
                            member.id, minutes, reward
                        )
                        self._last_payout[member.id] = now

                        log.info(
                            "Paid %d coins to %s (%d min in voice, balance: %d)",
                            reward, member.display_name, minutes, new_balance,
                        )

    @payout_loop.before_loop
    async def before_payout_loop(self) -> None:
        await self.bot.wait_until_ready()

        # Seed tracking for anyone already in voice (handles bot restarts)
        now = time.time()
        for guild in self.bot.guilds:
            for vc in guild.voice_channels:
                for member in vc.members:
                    if not member.bot:
                        self._last_payout[member.id] = now


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(VoiceRewards(bot))
