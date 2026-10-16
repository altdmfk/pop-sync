import httpx
import uuid
import time
import os
import pytest

pytestmark = pytest.mark.e2e

BASE_URL = "http://localhost:8080"
NODE_A = "http://localhost:8001"
NODE_B = "http://localhost:8002"
NODE_C = "http://localhost:8003"

ADMIN_SECRET_TOKEN = os.getenv("ADMIN_SECRET_TOKEN", "super-secret-admin")

def print_header(title):
    print(f"\n{'='*50}\n{title}\n{'='*50}")

@pytest.mark.e2e
def test_nonce_atomicity():
    print_header("Scenario 1: Nonce Atomicity")
    resp = httpx.post(f"{BASE_URL}/chaos/nonce", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
    data = resp.json()
    print("Concurrent requests sent.")
    node_a = data["results"]["Node-A"]
    node_b = data["results"]["Node-B"]
    print(f"Node-A: {node_a}")
    print(f"Node-B: {node_b}")
    
    assert "200" in node_a or "200" in node_b
    assert "401" in node_a or "401" in node_b
    print("Nonce Atomicity verified.")

@pytest.mark.e2e
def test_rate_limiting():
    print_header("Scenario 2: O(1) Rate Limiting")
    resp = httpx.post(f"{BASE_URL}/chaos/ddos", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
    data = resp.json()
    print(f"Responses: {data['responses']}")
    assert "429" in data["responses"], "Expected 429 Too Many Requests"
    print("Rate Limiting verified.")

@pytest.mark.e2e
def test_circuit_breaker():
    print_header("Scenario 3: Circuit Breaker & Fallback Degradation")
    print("Bringing DB down...")
    httpx.post(f"{BASE_URL}/chaos/db/down", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
    time.sleep(1)
    
    print("Sending requests to trigger breaker...")
    for _ in range(5):
        resp = httpx.get(f"{NODE_A}/auth", headers={
            "Authorization": "Bearer test",
            "X-Nonce": str(uuid.uuid4()),
            "X-SPIFFE-ID": f"spiffe://example.org/cb_{uuid.uuid4()}"
        })
    
    metrics = httpx.get(f"{NODE_A}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}).json()
    state = metrics["stats"]["circuit_state"]
    print(f"Circuit state is: {state}")
    assert state == "OPEN", "Circuit should be OPEN after 3 failures"
    
    print("Restoring DB...")
    httpx.post(f"{BASE_URL}/chaos/db/up", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
    
    # Wait for reset timeout (10s)
    print("Waiting 10s for HALF_OPEN transition...")
    time.sleep(11)
    
    resp = httpx.get(f"{NODE_A}/auth", headers={
        "Authorization": "Bearer test",
        "X-Nonce": str(uuid.uuid4()),
        "X-SPIFFE-ID": f"spiffe://example.org/cb_{uuid.uuid4()}"
    })
    
    metrics = httpx.get(f"{NODE_A}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}).json()
    state = metrics["stats"]["circuit_state"]
    print(f"Circuit state is: {state}")
    assert state == "CLOSED", "Circuit should recover to CLOSED"
    print("Circuit Breaker verified.")

import random

@pytest.mark.e2e
def test_partition_catchup():
    print_header("Scenario 4: Hard Partition & Stream Catch-up (Robustness Test)")
    
    # Run 5 times with random timings to prove robustness
    for iteration in range(5):
        print(f"\n--- Iteration {iteration + 1} ---")
        
        # Populate L1 cache on Node-B
        for i in range(5):
            httpx.get(f"{NODE_B}/auth", headers={
                "Authorization": "Bearer test",
                "X-Nonce": str(uuid.uuid4()),
                "X-SPIFFE-ID": f"spiffe://example.org/catchup_test_{iteration}_{i}"
            })
        
        size_before = httpx.get(f"{NODE_B}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}).json()["stats"]["l1_cache_size"]
        
        # Random sleep BEFORE isolate to simulate arbitrary timing within the Redis block window
        time.sleep(random.uniform(0.0, 0.5))
        
        print("Isolating Node-B...")
        httpx.post(f"{BASE_URL}/chaos/partition/Node-B/isolate", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        
        # Random sleep AFTER isolate, can be 0, to simulate immediate publishing
        time.sleep(random.uniform(0.0, 0.2))
        
        print("Publishing 4 invalidation events...")
        for _ in range(4):
            httpx.post(f"{BASE_URL}/chaos/invalidate", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        
        size_during = httpx.get(f"{NODE_B}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}).json()["stats"]["l1_cache_size"]
        assert size_during == size_before, "Cache should be stale (events not processed during partition)"
        
        print("Healing Node-B...")
        httpx.post(f"{BASE_URL}/chaos/partition/Node-B/heal", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        time.sleep(1) # Give it time to catch up upon healing
        
        metrics = httpx.get(f"{NODE_B}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}).json()
        catchup = metrics["stats"].get("last_catchup", {})
        print(f"Catch-up stats: {catchup}")
        
        assert catchup.get("pending_count", 0) >= 4, f"Failed at iteration {iteration+1}: Should have caught up at least 4 pending events"
        
    print("Partition Catch-up verified across random timings.")

def run_all():
    try:
        test_nonce_atomicity()
        test_rate_limiting()
        test_partition_catchup()
        test_circuit_breaker()
        print_header("All Test Scenarios Passed!")
    except Exception as e:
        print(f"\nTEST FAILED: {e}")

if __name__ == "__main__":
    run_all()
