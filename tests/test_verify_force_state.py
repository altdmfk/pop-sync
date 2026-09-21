import httpx
import time
import asyncio
import uuid
import os

ADMIN_SECRET_TOKEN = os.getenv("ADMIN_SECRET_TOKEN", "super-secret-admin")

async def main():
    async with httpx.AsyncClient() as client:
        print("--- 1. 토큰 없이 새 엔드포인트 호출 (401 확인) ---")
        resp = await client.post("http://localhost:8001/chaos/cb/force/OPEN")
        print(f"Status: {resp.status_code}, Body: {resp.text}")

        print("\n--- 2. chaos/db/down 호출 후 3개 노드 즉시 OPEN 확인 ---")
        # Get baseline requests to prove no dummy nonces/requests were sent
        r_before = await client.get("http://localhost:8001/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        reqs_before = r_before.json()['stats']['requests']

        await client.post("http://localhost:8080/chaos/db/down", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        
        nodes = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
        for node in nodes:
            r = await client.get(f"{node}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
            print(f"{node} CB State: {r.json()['stats']['circuit_state']}")

        print("\n--- 4. 이 과정에서 dummy auth 요청(nonce 생성)이 없었는지 확인 ---")
        r_after = await client.get("http://localhost:8001/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        reqs_after = r_after.json()['stats']['requests']
        print(f"Node-A Requests Before: {reqs_before}, After: {reqs_after}. (Should be equal)")

        print("\n--- 3. force_state(CLOSED) 직후 실제 실패 요청이 디바운스에 막히지 않는지 확인 ---")
        await client.post("http://localhost:8001/chaos/cb/force/CLOSED", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        print("Forced Node-A to CLOSED. Firing 3 requests...")
        for i in range(3):
            await client.get("http://localhost:8001/auth", headers={
                "Authorization": "Bearer test",
                "X-Nonce": str(uuid.uuid4()),
                "X-SPIFFE-ID": f"spiffe://example.org/test_{i}"
            })
        r = await client.get("http://localhost:8001/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        print(f"Node-A CB State after 3 failures (should be OPEN): {r.json()['stats']['circuit_state']}")
        
        print("\n--- 5. Node-B 격리 상태에서 chaos/db/down 호출 동작 확인 ---")
        # Bring everything up and heal
        await client.post("http://localhost:8080/chaos/db/up", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        await client.post("http://localhost:8080/chaos/partition/Node-B/heal", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        await asyncio.sleep(0.5)
        
        print("Isolating Node-B...")
        await client.post("http://localhost:8080/chaos/partition/Node-B/isolate", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        
        print("Calling chaos/db/down (Node-B will 503)...")
        resp = await client.post("http://localhost:8080/chaos/db/down", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        print(f"chaos/db/down API Status: {resp.status_code} (Should be 200, ignoring 503)")
        
        for node in nodes:
            r = await client.get(f"{node}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
            print(f"{node} CB State: {r.json()['stats']['circuit_state']} (Partitioned: {r.json().get('is_partitioned')})")

        # Cleanup
        await client.post("http://localhost:8080/chaos/partition/Node-B/heal", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        await client.post("http://localhost:8080/chaos/db/up", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})

if __name__ == "__main__":
    asyncio.run(main())
