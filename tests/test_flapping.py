import httpx
import asyncio
import time
import uuid
import os
import pytest

pytestmark = pytest.mark.e2e

ADMIN_SECRET_TOKEN = os.getenv("ADMIN_SECRET_TOKEN", "super-secret-admin")

async def fire(client, i):
    try:
        resp = await client.get("http://localhost:8001/auth", headers={
            "Authorization": "Bearer test",
            "X-Nonce": str(uuid.uuid4()),
            "X-SPIFFE-ID": f"spiffe://example.org/test_{i}"
        }, timeout=5.0)
        return resp.status_code
    except Exception as e:
        return str(e)

async def check_cb_state(client):
    try:
        resp = await client.get("http://localhost:8001/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        return resp.json()["stats"]["circuit_state"]
    except Exception:
        return "UNKNOWN"

async def main():
    async with httpx.AsyncClient() as client:
        print("1. Bringing DB down...")
        await client.post("http://localhost:8080/chaos/db/down", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        
        for i in range(5):
            await fire(client, i)
        print(f"CB state: {await check_cb_state(client)}")
        
        print("2. Firing 200 concurrent requests while DB is down...")
        tasks = [asyncio.create_task(fire(client, i)) for i in range(200)]
        
        await asyncio.sleep(0.2)
        
        print("3. Instantly bringing DB up (which resets CB to CLOSED)...")
        await client.post("http://localhost:8080/chaos/db/up", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        print(f"CB state right after up: {await check_cb_state(client)}")
        
        print("4. Monitoring CB state for 3 seconds...")
        for _ in range(6):
            await asyncio.sleep(0.5)
            print(f"CB state: {await check_cb_state(client)}")
            
        await asyncio.gather(*tasks)

@pytest.mark.e2e
async def test_flapping():
    await main()

if __name__ == "__main__":
    asyncio.run(main())

