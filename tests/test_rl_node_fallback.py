import asyncio
import httpx
import uuid
import time
import subprocess
import os
import pytest

pytestmark = pytest.mark.e2e

async def _test_node(client, node_url, node_name, spiffe_id):
    tasks = []
    for _ in range(30):
        headers = {
            "Authorization": "Bearer test_token",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": str(uuid.uuid4())
        }
        tasks.append(client.get(f"{node_url}/auth", headers=headers))
        
    results = await asyncio.gather(*tasks, return_exceptions=True)
    status_codes = {200: 0, 429: 0, 503: 0, "Exception": 0}
    
    for r in results:
        if isinstance(r, Exception):
            status_codes["Exception"] += 1
        else:
            status_codes[r.status_code] = status_codes.get(r.status_code, 0) + 1
            
    print(f"{node_name}: 200 {status_codes.get(200, 0)} / 429 {status_codes.get(429, 0)} / 503 {status_codes.get(503, 0)} / Exception {status_codes.get('Exception', 0)}")
    return status_codes

async def main():
    print("=== Rate Limit Fallback Node-by-Node Test ===")
    
    env = os.environ.copy()
    env["REDIS_URL"] = "redis://127.0.0.1:9999"
    
    p_a = subprocess.Popen(["python", "gateway.py", "--node", "Node-A", "--port", "8001"], env=env)
    p_b = subprocess.Popen(["python", "gateway.py", "--node", "Node-B", "--port", "8002"], env=env)
    p_c = subprocess.Popen(["python", "gateway.py", "--node", "Node-C", "--port", "8003"], env=env)
    
    print("Waiting for gateways to start (8 seconds)...")
    await asyncio.sleep(8)
    
    spiffe_id = f"spiffe://example.org/node_test_{uuid.uuid4()}"
    print(f"Target SPIFFE ID: {spiffe_id}\n")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        await _test_node(client, "http://127.0.0.1:8001", "Node-A", spiffe_id)
        await _test_node(client, "http://127.0.0.1:8002", "Node-B", spiffe_id)
        await _test_node(client, "http://127.0.0.1:8003", "Node-C", spiffe_id)
        
    p_a.terminate()
    p_b.terminate()
    p_c.terminate()

@pytest.mark.e2e
async def test_rl_node_fallback():
    await main()

if __name__ == '__main__':
    asyncio.run(main())

