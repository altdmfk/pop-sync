import asyncio
import time
from redis.asyncio import Redis
import pytest

pytestmark = pytest.mark.e2e

SLIDING_RL_LUA = """
local prev = tonumber(redis.call('GET', KEYS[1])) or 0
local curr = redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[1])
local weight = tonumber(ARGV[2])
local estimated = math.floor(prev * weight / 1000) + curr
return estimated
"""

async def main():
    r = Redis.from_url("redis://127.0.0.1:6379", decode_responses=True)
    rl_script = r.register_script(SLIDING_RL_LUA)
    
    await r.delete("rl:test_lua:100", "rl:test_lua:101")
    
    for _ in range(50):
        await r.incr("rl:test_lua:100")
    
    weight = 500
    ttl = 20
    
    estimated1 = await rl_script(
        keys=["rl:test_lua:100", "rl:test_lua:101"],
        args=[str(ttl), str(weight)]
    )
    print(f"1st req estimated: {estimated1} (expected 26)")
    
    estimated2 = await rl_script(
        keys=["rl:test_lua:100", "rl:test_lua:101"],
        args=[str(ttl), str(weight)]
    )
    print(f"2nd req estimated: {estimated2} (expected 27)")
    
    curr_ttl = await r.ttl("rl:test_lua:101")
    print(f"TTL of current window: {curr_ttl} (expected > 0)")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
