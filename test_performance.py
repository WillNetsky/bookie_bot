import asyncio
import logging
import time
import json
from bot.services.kalshi_api import kalshi_api, SPORTS

logging.basicConfig(level=logging.INFO)

async def measure_performance():
    print("--- Performance Test ---")
    
    start = time.perf_counter()
    await kalshi_api.refresh_sports()
    end = time.perf_counter()
    print(f"refresh_sports(): {end - start:.2f}s ({len(SPORTS)} sports)")
    
    start = time.perf_counter()
    discovery = await kalshi_api.discover_available(force=True)
    end = time.perf_counter()
    print(f"discover_available(force=True): {end - start:.2f}s")
    
    start = time.perf_counter()
    discovery_cached = await kalshi_api.discover_available(force=False)
    end = time.perf_counter()
    print(f"discover_available(cached): {end - start:.2f}s")
    
    start = time.perf_counter()
    all_games = await kalshi_api.get_all_games()
    end = time.perf_counter()
    print(f"get_all_games(): {end - start:.2f}s ({len(all_games)} games)")

if __name__ == "__main__":
    asyncio.run(measure_performance())
