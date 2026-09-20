import asyncio
import httpx
import uuid
import time
import subprocess

CONTROL_PLANE = "http://localhost:8080"
GATEWAY_A = "http://localhost:8001"

async def test_1_ddos_spam():
    print("=== TEST 1: DDoS Spam ===")
    async with httpx.AsyncClient(timeout=30.0) as client:
        print("Sending 20 concurrent /chaos/ddos requests to Control Plane...")
        tasks = [client.post(f"{CONTROL_PLANE}/chaos/ddos") for _ in range(20)]
        start = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        duration = time.time() - start
        
        exceptions = [r for r in results if isinstance(r, Exception)]
        timeouts = [r for r in exceptions if isinstance(r, httpx.TimeoutException)]
        success = [r for r in results if not isinstance(r, Exception) and r.status_code == 200]
        
        print(f"Duration: {duration:.2f}s")
        print(f"Total Requests: 20")
        print(f"Success: {len(success)}")
        print(f"Exceptions: {len(exceptions)}")
        print(f"Timeouts: {len(timeouts)}")
        
        print("\nChecking Gateway-A Docker Stats:")
        stats = subprocess.check_output(["docker", "stats", "task_b-gateway-a-1", "--no-stream"]).decode("utf-8")
        print(stats)

async def test_2_rl_leak():
    print("\n=== TEST 2: Rate Limiter Memory Leak ===")
    async with httpx.AsyncClient(timeout=30.0) as client:
        await client.post(f"{CONTROL_PLANE}/chaos/redis/down")
        print("Sending 1000 requests with random SPIFFE IDs to Gateway A...")
        tasks = []
        for _ in range(1000):
            headers = {"Authorization": "Bearer test_token", "X-SPIFFE-ID": f"spiffe://example.org/attacker-{uuid.uuid4()}"}
            tasks.append(client.get(f"{GATEWAY_A}/auth", headers=headers))
            
        await asyncio.gather(*tasks, return_exceptions=True)
        print("Check local_rl_size in metrics manually.")
        await client.post(f"{CONTROL_PLANE}/chaos/redis/up")

async def test_3_cb_stress():
    print("\n=== TEST 3: DB Down Concurrent Requests (CB Stress) ===")
    async with httpx.AsyncClient(timeout=30.0) as client:
        await client.post(f"{CONTROL_PLANE}/chaos/db/up")
        await asyncio.sleep(1)
        await client.post(f"{CONTROL_PLANE}/chaos/db/down")
        
        print("Sending 500 concurrent requests to Gateway A immediately after DB down...")
        headers = {"Authorization": "Bearer test_token", "X-SPIFFE-ID": "spiffe://example.org/test3"}
        
        tasks = []
        for _ in range(500):
            tasks.append(client.get(f"{GATEWAY_A}/auth", headers=headers))
            
        start = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        duration = time.time() - start
        
        status_codes = {}
        for r in results:
            if isinstance(r, Exception):
                status_codes["Exception"] = status_codes.get("Exception", 0) + 1
            else:
                status_codes[r.status_code] = status_codes.get(r.status_code, 0) + 1
                
        print(f"Duration for 500 reqs: {duration:.2f}s")
        print(f"Result Status Codes: {status_codes}")
        
async def test_4_flapping():
    print("\n=== TEST 4: Flapping (Rapid DB Up + Burst) ===")
    async with httpx.AsyncClient(timeout=30.0) as client:
        await client.post(f"{CONTROL_PLANE}/chaos/db/down")
        await asyncio.sleep(1) 
        
        print("Sending DB UP and immediately blasting 200 requests...")
        await client.post(f"{CONTROL_PLANE}/chaos/db/up")
        
        headers = {"Authorization": "Bearer test_token", "X-SPIFFE-ID": "spiffe://example.org/test4"}
        tasks = []
        for _ in range(200):
            tasks.append(client.get(f"{GATEWAY_A}/auth", headers=headers))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        status_codes = {}
        for r in results:
            if isinstance(r, Exception):
                status_codes["Exception"] = status_codes.get("Exception", 0) + 1
            else:
                status_codes[r.status_code] = status_codes.get(r.status_code, 0) + 1
                
        print(f"Result Status Codes: {status_codes}")

async def main():
    try: await test_1_ddos_spam()
    except Exception as e: print(f"Failed Test 1: {e}")
    try: await test_2_rl_leak()
    except Exception as e: print(f"Failed Test 2: {e}")
    try: await test_3_cb_stress()
    except Exception as e: print(f"Failed Test 3: {e}")
    try: await test_4_flapping()
    except Exception as e: print(f"Failed Test 4: {e}")

if __name__ == '__main__':
    asyncio.run(main())
