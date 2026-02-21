import logging

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot import config
from bot.db import models

log = logging.getLogger(__name__)

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_STREAMS_URL = "https://api.twitch.tv/helix/streams"


class TwitchCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.live_now: set[str] = set()
        self._token: str | None = None
        self._session: aiohttp.ClientSession | None = None

    async def cog_load(self) -> None:
        if not config.TWITCH_CLIENT_ID or not config.TWITCH_CLIENT_SECRET:
            log.warning("Twitch credentials not configured — stream notifications disabled.")
            return
        self._session = aiohttp.ClientSession()
        await self._refresh_token()
        self.poll_streams.start()

    async def cog_unload(self) -> None:
        self.poll_streams.cancel()
        if self._session:
            await self._session.close()
            self._session = None

    async def _refresh_token(self) -> None:
        assert self._session is not None
        async with self._session.post(
            TWITCH_TOKEN_URL,
            params={
                "client_id": config.TWITCH_CLIENT_ID,
                "client_secret": config.TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        ) as resp:
            data = await resp.json()
            self._token = data.get("access_token")
            log.info("Twitch token refreshed.")

    def _headers(self) -> dict:
        return {
            "Client-ID": config.TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {self._token}",
        }

    @tasks.loop(minutes=2)
    async def poll_streams(self) -> None:
        try:
            await self._check_streams()
        except Exception:
            log.exception("Error polling Twitch streams")

    @poll_streams.before_loop
    async def before_poll(self) -> None:
        await self.bot.wait_until_ready()

    async def _check_streams(self) -> None:
        if self._session is None:
            return

        all_watches = await models.get_all_twitch_watches()
        if not all_watches:
            return

        stream_names = list({row["stream_name"].lower() for row in all_watches})

        # Batch-query Twitch (up to 100 per request)
        live_info: dict[str, dict] = {}
        for i in range(0, len(stream_names), 100):
            batch = stream_names[i : i + 100]
            params = [("user_login", name) for name in batch]
            async with self._session.get(
                TWITCH_STREAMS_URL,
                headers=self._headers(),
                params=params,
            ) as resp:
                if resp.status == 401:
                    await self._refresh_token()
                    return
                data = await resp.json()
                for stream in data.get("data", []):
                    live_info[stream["user_login"].lower()] = stream

        live_logins = set(live_info.keys())
        newly_live = live_logins - self.live_now
        went_offline = self.live_now - live_logins

        self.live_now = live_logins

        # Post go-live notifications
        for stream_name in newly_live:
            info = live_info[stream_name]
            for watch in all_watches:
                if watch["stream_name"].lower() != stream_name:
                    continue
                channel = self.bot.get_channel(watch["channel_id"])
                if not isinstance(channel, discord.abc.Messageable):
                    continue
                embed = self._build_live_embed(info)
                try:
                    await channel.send(embed=embed)
                except Exception:
                    log.exception(
                        "Failed to send stream notification for %s in channel %s",
                        stream_name,
                        watch["channel_id"],
                    )

        if went_offline:
            log.debug("Streams went offline: %s", went_offline)

    def _build_live_embed(self, info: dict) -> discord.Embed:
        name = info.get("user_name", info.get("user_login", "Unknown"))
        game = info.get("game_name", "Unknown")
        title = info.get("title", "")
        viewers = info.get("viewer_count", 0)

        embed = discord.Embed(
            title=f"🟣 {name} is live on Twitch!",
            url=f"https://twitch.tv/{info.get('user_login', '')}",
            color=discord.Color.purple(),
        )
        embed.add_field(name="Playing", value=game, inline=True)
        embed.add_field(name="Viewers", value=f"{viewers:,}", inline=True)
        if title:
            embed.description = title
        return embed

    # ── Admin commands ────────────────────────────────────────────────────────

    @app_commands.command(
        name="addstream",
        description="Watch a Twitch stream and notify in a channel when it goes live",
    )
    @app_commands.describe(
        stream_name="Twitch username to watch",
        channel="Channel to post go-live notifications in",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def addstream(
        self,
        interaction: discord.Interaction,
        stream_name: str,
        channel: discord.TextChannel,
    ) -> None:
        await models.add_twitch_watch(stream_name, interaction.guild_id, channel.id)
        await interaction.response.send_message(
            f"Now watching **{stream_name}** — notifications will post in {channel.mention}.",
            ephemeral=True,
        )

    @app_commands.command(
        name="removestream",
        description="Stop watching a Twitch stream",
    )
    @app_commands.describe(stream_name="Twitch username to stop watching")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def removestream(
        self, interaction: discord.Interaction, stream_name: str
    ) -> None:
        removed = await models.remove_twitch_watch(stream_name, interaction.guild_id)
        if removed:
            await interaction.response.send_message(
                f"Stopped watching **{stream_name}**.", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"**{stream_name}** wasn't being watched.", ephemeral=True
            )

    @app_commands.command(
        name="streams",
        description="List Twitch streams being watched in this server",
    )
    async def streams(self, interaction: discord.Interaction) -> None:
        watches = await models.get_twitch_watches(interaction.guild_id)
        if not watches:
            await interaction.response.send_message(
                "No streams being watched. Use `/addstream` to add one.", ephemeral=True
            )
            return

        lines = []
        for w in watches:
            channel = self.bot.get_channel(w["channel_id"])
            ch_str = channel.mention if channel else f"<#{w['channel_id']}>"
            live_tag = " 🔴 LIVE" if w["stream_name"].lower() in self.live_now else ""
            lines.append(f"**{w['stream_name']}** → {ch_str}{live_tag}")

        embed = discord.Embed(
            title="Twitch Watch List",
            description="\n".join(lines),
            color=discord.Color.purple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @addstream.error
    @removestream.error
    async def _admin_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message(
                "You need **Manage Server** permission to use this command.", ephemeral=True
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(TwitchCog(bot))
