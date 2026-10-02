import asyncio
import httpx
import uuid
import time

GATEWAYS = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
CONTROL_PLANE = "http://localhost:8080"

async def test_rl_fallback():
    print("=== Rate Limit Fallback Test ===")
    
    # Note: This test runs with Redis UP in a normal environment, giving 200: 50, 429: 40.
    # If you intentionally stop Redis (e.g. docker stop aegis-sync-main-redis-1), see the expectation note below.
    
    spiffe_id = f"spiffe://example.org/attacker-{uuid.uuid4()}"
    print(f"Target SPIFFE ID: {spiffe_id}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Rate limit check happens BEFORE nonce check.
        # So even if nonce check fails with 503 (Fail closed), rate limit will return 429 if limit exceeded.
        
        tasks = []
        for i in range(90):
            node = GATEWAYS[i % 3]
            headers = {
                "Authorization": "Bearer test_token",
                "X-SPIFFE-ID": spiffe_id,
                "X-Nonce": str(uuid.uuid4())
            }
            tasks.append(client.get(f"{node}/auth", headers=headers))
            
        print("Sending 90 requests distributed across 3 nodes...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        status_codes = {}
        for r in results:
            if isinstance(r, Exception):
                status_codes["Exception"] = status_codes.get("Exception", 0) + 1
            else:
                status_codes[r.status_code] = status_codes.get(r.status_code, 0) + 1
                
        print(f"Result Status Codes: {status_codes}")
        # Expected if Redis is UP: 200: 50, 429: 40 (Global limit of 50).
        # Expected ONLY if Redis is intentionally DOWN:
        # Node fallback limit is 15. 3 nodes * 15 = 45 requests pass RL.
        # Passing RL means they go to nonce check, which returns 503.
        # The remaining 45 requests return 429.

if __name__ == '__main__':
    asyncio.run(test_rl_fallback())
