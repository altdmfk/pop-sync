import asyncio
import httpx
import uuid
import time
import requests

GATEWAYS = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
CONTROL_PLANE = "http://localhost:8080"

async def test_rl_fallback():
    print("=== Rate Limit Fallback Test ===")
    # 1. Redis Down (Implicitly down in local standalone run)
    print("Assuming Redis is down/unreachable...")
    # requests.post(f"{CONTROL_PLANE}/chaos/redis/down")
    # time.sleep(2)
    
    # 2. Generate random spiffe_id
    spiffe_id = f"spiffe://example.org/attacker-{uuid.uuid4()}"
    print(f"Target SPIFFE ID: {spiffe_id}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # We need to bypass nonce check fail-closed to test rate limit.
        # But wait, rate limit check happens BEFORE nonce check!
        # So even if nonce check fails with 503, rate limit will return 429 if limit exceeded.
        
        tasks = []
        # Send 30 requests to each node (total 90)
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
        # Expected:
        # Node fallback limit is 15. 3 nodes * 15 = 45 requests should pass RL.
        # Passing RL means they go to nonce check, which will return 503 (since Redis is down).
        # The remaining 90 - 45 = 45 requests should return 429 (Rate limited).
        
    print("Restoring Redis...")
    # requests.post(f"{CONTROL_PLANE}/chaos/redis/up")

if __name__ == '__main__':
    asyncio.run(test_rl_fallback())
