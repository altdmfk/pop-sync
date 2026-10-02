import asyncio
import time
import httpx
import uuid

GATEWAY_URL = "http://localhost:8001/auth"
RL_LIMIT = 50
RL_WINDOW_SEC = 10

async def send_requests(client, client_id, count):
    headers = {
        "Authorization": "Bearer super-token",
        "X-SPIFFE-ID": client_id,
        "X-Nonce": ""  # will be filled per request
    }
    
    async def make_request(idx):
        req_headers = headers.copy()
        req_headers["X-Nonce"] = str(uuid.uuid4())
        resp = await client.get(GATEWAY_URL, headers=req_headers)
        return resp.status_code

    tasks = [make_request(i) for i in range(count)]
    results = await asyncio.gather(*tasks)
    
    passed = results.count(200)
    ratelimited = results.count(429)
    other = len(results) - passed - ratelimited
    return passed, ratelimited, other

async def measure_burst():
    # Wait until we are 1 second before the next window boundary
    # Window boundary is when time.time() % 10 == 0
    now = time.time()
    next_boundary = ((int(now) // RL_WINDOW_SEC) + 1) * RL_WINDOW_SEC
    
    # We want to start sending requests 0.5s before boundary
    start_time_before = next_boundary - 0.5
    sleep_time = start_time_before - now
    if sleep_time < 0:
        # Too close to boundary, wait for the next one
        next_boundary += RL_WINDOW_SEC
        start_time_before = next_boundary - 0.5
        sleep_time = start_time_before - time.time()
        
    print(f"Waiting {sleep_time:.2f}s until 0.5s before window boundary...")
    await asyncio.sleep(sleep_time)
    
    client_id = f"spiffe://burst/{uuid.uuid4()}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        print(f"Sending {RL_LIMIT} requests right BEFORE boundary...")
        pass1, rl1, o1 = await send_requests(client, client_id, RL_LIMIT)
        print(f"Before boundary results - Passed: {pass1}, 429: {rl1}, Other: {o1}")
        
        # Wait until 0.5s AFTER boundary
        now = time.time()
        start_time_after = next_boundary + 0.5
        sleep_time2 = start_time_after - now
        if sleep_time2 > 0:
            print(f"Waiting {sleep_time2:.2f}s until 0.5s after window boundary...")
            await asyncio.sleep(sleep_time2)
            
        print(f"Sending {RL_LIMIT} requests right AFTER boundary...")
        pass2, rl2, o2 = await send_requests(client, client_id, RL_LIMIT)
        print(f"After boundary results - Passed: {pass2}, 429: {rl2}, Other: {o2}")
        
        total_passed = pass1 + pass2
        print(f"Total Passed across boundary: {total_passed} (expected: ~{RL_LIMIT} for sliding, ~{2 * RL_LIMIT} for fixed)")

if __name__ == "__main__":
    asyncio.run(measure_burst())
