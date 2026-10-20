import asyncio
import httpx
import time
import uuid

NODE_A = "http://localhost:8001"
ADMIN_TOKEN = "super-secret-admin"

async def measure():
    for iteration in range(3):
        print(f"\n--- Iteration {iteration + 1} ---")
        spiffe_id = f"spiffe://example.org/effect-test-{uuid.uuid4()}"
        nonce_base = str(uuid.uuid4())

        async with httpx.AsyncClient(timeout=30.0) as client:
            before = (await client.get(f"{NODE_A}/metrics", headers={"X-Admin-Token": ADMIN_TOKEN})).json()["stats"]
            
            print(f"Firing 100 concurrent requests for {spiffe_id}...")
            tasks = []
            for i in range(100):
                headers = {
                    "Authorization": "Bearer test",
                    "X-SPIFFE-ID": spiffe_id,
                    "X-Nonce": f"{nonce_base}-{i}"
                }
                tasks.append(client.get(f"{NODE_A}/auth", headers=headers))
                
            start_time = time.time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            total_duration = time.time() - start_time
            
            after = (await client.get(f"{NODE_A}/metrics", headers={"X-Admin-Token": ADMIN_TOKEN})).json()["stats"]
            
            requests_inc = after["requests"] - before["requests"]
            l1_hits_inc = after["l1_hits"] - before["l1_hits"]
            l2_hits_inc = after["l2_hits"] - before["l2_hits"]
            db_fallbacks_inc = after["db_fallbacks"] - before["db_fallbacks"]
            db_queries_inc = after["db_queries_executed"] - before["db_queries_executed"]
            coalesced_inc = after["singleflight_coalesced"] - before["singleflight_coalesced"]
            circuit_blocked_inc = after["circuit_blocked"] - before["circuit_blocked"]
            l1_double_inc = after["l1_double_check_hits"] - before["l1_double_check_hits"]
            
            latencies = []
            exceptions = 0
            for r in results:
                if isinstance(r, Exception):
                    exceptions += 1
                else:
                    latencies.append(r.elapsed.total_seconds() * 1000)
                
            if latencies:
                latencies.sort()
                p50 = latencies[int(len(latencies) * 0.50)]
                p95 = latencies[int(len(latencies) * 0.95)]
                max_lat = latencies[-1]
            else:
                p50 = p95 = max_lat = 0.0
            
            print(f"Total Duration: {total_duration*1000:.1f}ms (Exceptions: {exceptions})")
            print(f"Latencies: p50={p50:.1f}ms, p95={p95:.1f}ms, max={max_lat:.1f}ms")
            
            print(f"Increments:")
            print(f"  requests: {requests_inc}")
            print(f"  l1_hits: {l1_hits_inc}")
            print(f"  l2_hits: {l2_hits_inc}")
            print(f"  db_fallbacks: {db_fallbacks_inc}")
            print(f"  db_queries_executed: {db_queries_inc}")
            print(f"  singleflight_coalesced: {coalesced_inc}")
            print(f"  circuit_blocked: {circuit_blocked_inc}")
            print(f"  l1_double_check_hits: {l1_double_inc}")
            
            lhs = db_fallbacks_inc
            rhs = db_queries_inc + coalesced_inc + l1_double_inc + circuit_blocked_inc
            invariant_holds = (lhs == rhs)
            
            print(f"Invariant check: db_fallbacks({lhs}) == rhs({rhs}) -> {'True' if invariant_holds else 'False'}")
            
            assert invariant_holds, "Invariant failed!"
            
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(measure())
