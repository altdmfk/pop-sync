import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock
import redis

from gateway import app, l1_cache, stats, stats_lock, cb, in_flight, release_cluster_lock, db_loader_task
import gateway

@pytest.mark.asyncio
async def test_cluster_singleflight_follower_coalesces_from_l2(mock_common_dependencies):
    """
    Test cluster follower behavior:
    When another node is already the cluster leader (SET NX fails), the follower
    polls L2 with backoff, retrieves the result once populated, and resolves without
    querying the Policy DB.
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    # Lock acquisition fails: another node holds the cluster lock
    m_redis.set = AsyncMock(return_value=False)
    m_redis.exists = AsyncMock(return_value=True)

    # First L2 check returns None, then second returns the cached policy
    call_count = 0
    async def mock_redis_get(key):
        nonlocal call_count
        if key == "policy:test_cluster_comp_key":
            call_count += 1
            if call_count >= 2:
                return b"admin"
        return None

    m_redis.get = AsyncMock(side_effect=mock_redis_get)

    with patch("gateway.generate_composite_key", return_value="test_cluster_comp_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {
                "Authorization": "Bearer token",
                "X-SPIFFE-ID": "spiffe://cluster-follower",
                "X-Nonce": "nonce-cf-1"
            }
            resp = await client.get("/auth", headers=headers)

            assert resp.status_code == 200
            assert resp.json()["policy"] == "admin"
            assert resp.json()["source"] == "L2"

            # Node B did NOT execute a DB query!
            assert stats["db_queries_executed"] == 0
            assert stats["singleflight_coalesced"] == 1
            assert stats["singleflight_leader_queries"] == 0
            assert stats["db_fallbacks"] == 1
            assert m_db.call_count == 0

            # Invariant check
            assert stats["db_fallbacks"] == (
                stats["db_queries_executed"] +
                stats["singleflight_coalesced"] +
                stats["l1_double_check_hits"] +
                stats["circuit_blocked"]
            )

@pytest.mark.asyncio
async def test_cluster_singleflight_multiple_local_requests_with_cluster_follower(mock_common_dependencies):
    """
    Test synergy with local singleflight:
    10 concurrent requests arrive at Node B.
    Request 1 becomes Node B's local leader, but is a Cluster Follower in Redis.
    Requests 2..10 coalesce locally on Node B's in_flight future.
    When the cluster leader's result appears in L2, all 10 requests resolve.
    Node B executes 0 DB queries; 10 requests coalesce.
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    # Cluster lock acquisition fails
    m_redis.set = AsyncMock(return_value=False)
    m_redis.exists = AsyncMock(return_value=True)

    call_count = 0
    async def mock_redis_get(key):
        nonlocal call_count
        if "policy:" in key:
            call_count += 1
            # First 12 calls (10 from auth() + 2 from db_loader_task) miss L2
            if call_count >= 13:
                return b"admin"
        return None

    m_redis.get = AsyncMock(side_effect=mock_redis_get)

    with patch("gateway.generate_composite_key", return_value="test_cluster_burst_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            tasks = [
                client.get(
                    "/auth",
                    headers={
                        "Authorization": "Bearer token",
                        "X-SPIFFE-ID": "spiffe://cluster-burst",
                        "X-Nonce": f"nonce-burst-{i}"
                    }
                )
                for i in range(10)
            ]
            results = await asyncio.gather(*tasks)

            for r in results:
                assert r.status_code == 200
                assert r.json()["policy"] == "admin"

            assert stats["db_queries_executed"] == 0
            assert stats["singleflight_coalesced"] == 10
            assert stats["singleflight_leader_queries"] == 0
            assert stats["db_fallbacks"] == 10
            assert m_db.call_count == 0

            assert stats["db_fallbacks"] == (
                stats["db_queries_executed"] +
                stats["singleflight_coalesced"] +
                stats["l1_double_check_hits"] +
                stats["circuit_blocked"]
            )

@pytest.mark.asyncio
async def test_cluster_singleflight_leader_crashes_follower_takes_over(mock_common_dependencies):
    """
    Test crash recovery:
    1. Node B initially fails to acquire lock (another leader was elected).
    2. Leader crashes without writing to L2.
    3. The lock key in Redis expires (EXISTS returns 0).
    4. Node B detects lock expiration, takes over the lock (SET NX returns True),
       promotes itself to leader, and queries the DB.
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    m_db.return_value = {"status": "allow", "role": "admin"}

    # First SET NX returns False (follower), second SET NX returns True (takeover)
    set_call_count = 0
    async def mock_redis_set(*args, **kwargs):
        nonlocal set_call_count
        set_call_count += 1
        if set_call_count == 1:
            return False
        return True

    m_redis.set = AsyncMock(side_effect=mock_redis_set)
    # Lock existed, then expired (returns 0)
    m_redis.exists = AsyncMock(return_value=0)
    m_redis.get = AsyncMock(return_value=None)

    with patch("gateway.generate_composite_key", return_value="test_takeover_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token",
                    "X-SPIFFE-ID": "spiffe://takeover-test",
                    "X-Nonce": "nonce-takeover-1"
                }
            )

            assert resp.status_code == 200
            assert resp.json()["policy"] == "admin"
            assert resp.json()["source"] == "DB"

            # Takeover succeeded and executed DB query
            assert stats["db_queries_executed"] == 1
            assert stats["singleflight_leader_queries"] == 1
            assert stats["singleflight_coalesced"] == 0
            assert m_db.call_count == 1

            assert stats["db_fallbacks"] == (
                stats["db_queries_executed"] +
                stats["singleflight_coalesced"] +
                stats["l1_double_check_hits"] +
                stats["circuit_blocked"]
            )

@pytest.mark.asyncio
async def test_cluster_singleflight_redis_down_fallback(mock_common_dependencies):
    """
    Test resilience during Redis outage:
    If Redis fails during cluster lock acquisition, the node catches the error,
    gracefully degrades to process-local leader, and queries the Policy DB directly.
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    m_db.return_value = {"status": "allow", "role": "admin"}

    m_redis.get = AsyncMock(return_value=None)
    m_redis.set = AsyncMock(side_effect=redis.ConnectionError("Redis connection lost"))

    with patch("gateway.generate_composite_key", return_value="test_redis_err_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token",
                    "X-SPIFFE-ID": "spiffe://redis-error",
                    "X-Nonce": "nonce-redis-err"
                }
            )

            assert resp.status_code == 200
            assert resp.json()["policy"] == "admin"
            assert resp.json()["source"] == "DB"
            assert stats["db_queries_executed"] == 1

@pytest.mark.asyncio
async def test_cluster_singleflight_toggle_disabled(mock_common_dependencies, monkeypatch):
    """
    Test CLUSTER_SINGLEFLIGHT_ENABLED=False toggle:
    When disabled, node bypasses cluster locking and runs purely as process-local singleflight.
    """
    monkeypatch.setattr(gateway, "CLUSTER_SINGLEFLIGHT_ENABLED", False)
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    m_db.return_value = {"status": "allow", "role": "admin"}

    with patch("gateway.generate_composite_key", return_value="test_toggle_off_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token",
                    "X-SPIFFE-ID": "spiffe://toggle-off",
                    "X-Nonce": "nonce-toggle-off"
                }
            )

            assert resp.status_code == 200
            assert resp.json()["policy"] == "admin"
            assert stats["db_queries_executed"] == 1
            # Cluster lock was never attempted
            assert not any("lock:sf:" in str(call) for call in m_redis.set.call_args_list)

@pytest.mark.asyncio
async def test_cluster_singleflight_two_simulated_nodes(mock_common_dependencies):
    """
    Simulate Node-A (Leader) and Node-B (Follower) executing concurrently with a shared mock Redis:
    - Shared key store for policy and locks.
    - Node-A acquires lock and runs delayed DB query (50ms).
    - Node-B arrives concurrently, fails lock, polls L2, and completes via L2.
    - Across both nodes, exactly 1 DB query is executed.
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    # Shared in-memory Redis mock store
    shared_redis_store = {}
    shared_lock = asyncio.Lock()

    async def shared_redis_get(key):
        async with shared_lock:
            val = shared_redis_store.get(key)
            if val is not None and isinstance(val, str):
                return val.encode()
            return val

    async def shared_redis_set(key, val, nx=False, ex=None, px=None, **kwargs):
        async with shared_lock:
            if nx and key in shared_redis_store:
                return False
            shared_redis_store[key] = val
            return True

    async def shared_redis_exists(key):
        async with shared_lock:
            return 1 if key in shared_redis_store else 0

    m_redis.get = AsyncMock(side_effect=shared_redis_get)
    m_redis.set = AsyncMock(side_effect=shared_redis_set)
    m_redis.exists = AsyncMock(side_effect=shared_redis_exists)

    db_call_count = 0
    async def delayed_db_call(*args, **kwargs):
        nonlocal db_call_count
        db_call_count += 1
        await asyncio.sleep(0.06)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call

    with patch("gateway.generate_composite_key", return_value="shared_comp_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client_a, \
                   AsyncClient(transport=transport, base_url="http://test") as client_b:

            headers_a = {"Authorization": "Bearer token", "X-SPIFFE-ID": "spiffe://sim-a", "X-Nonce": "nonce-sim-a"}
            headers_b = {"Authorization": "Bearer token", "X-SPIFFE-ID": "spiffe://sim-b", "X-Nonce": "nonce-sim-b"}

            # Fire requests from Node A and Node B almost simultaneously
            task_a = asyncio.create_task(client_a.get("/auth", headers=headers_a))
            await asyncio.sleep(0.005)
            # Clear Node B's local in_flight and L1 cache to simulate a distinct process
            in_flight.clear()
            l1_cache.clear()
            task_b = asyncio.create_task(client_b.get("/auth", headers=headers_b))

            resp_a, resp_b = await asyncio.gather(task_a, task_b)

            assert resp_a.status_code == 200
            assert resp_b.status_code == 200
            assert resp_a.json()["policy"] == "admin"
            assert resp_b.json()["policy"] == "admin"

            # Crucial assertion: Policy DB was called EXACTLY ONCE across both simulated nodes!
            assert db_call_count == 1


@pytest.mark.asyncio
async def test_cluster_lock_release_cancellation_preserves_probe_and_inflight_cleanup(mock_common_dependencies):
    """
    Verify that if asyncio.CancelledError is raised during release_cluster_lock
    inside the db_loader_task finally: block, the inner cleanup block unconditionally
    executes: cb._half_open_probe_in_flight is reset to False and the comp_key is
    removed from in_flight (zero deadlock, zero stale future leak).
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    comp_key = "test_cancel_cleanup_key"
    fut = asyncio.get_running_loop().create_future()
    in_flight[comp_key] = fut

    # Force circuit breaker into HALF_OPEN with an active probe slot
    cb.force_state("HALF_OPEN")
    cb._half_open_probe_in_flight = True
    assert cb._half_open_probe_in_flight is True

    # Configure as cluster leader
    m_redis.set = AsyncMock(return_value=True)
    m_redis.get = AsyncMock(return_value=None)

    async def cancel_on_db_call(*args, **kwargs):
        raise asyncio.CancelledError("Simulated client/DB cancellation")

    async def cancel_on_release(*args, **kwargs):
        raise asyncio.CancelledError("Simulated cancellation during lock release")

    with patch("gateway.release_cluster_lock", side_effect=cancel_on_release), \
         patch("gateway.db_async_call", side_effect=cancel_on_db_call):
        task = asyncio.create_task(db_loader_task(comp_key, "spiffe://test", "res", fut, use_singleflight=True))
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Assert that cb._half_open_probe_in_flight was unconditionally reset to False
    assert cb._half_open_probe_in_flight is False
    # Assert comp_key was unconditionally removed from in_flight
    assert comp_key not in in_flight


@pytest.mark.asyncio
async def test_release_cluster_lock_fencing_token_mismatch_does_not_delete():
    """
    Test fencing token safety in release_cluster_lock for both Lua eval and fallback paths:
    - When Redis holds "token-leader-B", releasing with "token-stale-leader-A" does NOT delete lock_key.
    - When Redis holds "token-leader-B", releasing with "token-leader-B" DOES delete lock_key.
    """
    lock_key = "lock:sf:test_fencing_key"

    # 1. Path A: Lua eval path
    class EvalClient:
        def __init__(self, stored_token):
            self.store = {lock_key: stored_token}

        async def eval(self, script, numkeys, key, token):
            if "GET" in script and "DEL" in script:
                if self.store.get(key) == token:
                    self.store.pop(key, None)
                    return 1
                return 0
            return 0

    client_eval = EvalClient("token-leader-B")
    await release_cluster_lock(client_eval, lock_key, "token-stale-leader-A")
    assert lock_key in client_eval.store, "Lua eval deleted lock despite fencing token mismatch!"
    await release_cluster_lock(client_eval, lock_key, "token-leader-B")
    assert lock_key not in client_eval.store, "Lua eval failed to delete lock with matching fencing token!"

    # 2. Path B: Non-eval fallback path (minimal get/delete mock)
    class FallbackClient:
        def __init__(self, stored_token):
            self.store = {lock_key: stored_token}

        async def get(self, key):
            return self.store.get(key)

        async def delete(self, key):
            return self.store.pop(key, None)

    client_fb = FallbackClient("token-leader-B")
    await release_cluster_lock(client_fb, lock_key, "token-stale-leader-A")
    assert lock_key in client_fb.store, "Fallback deleted lock despite fencing token mismatch!"
    await release_cluster_lock(client_fb, lock_key, "token-leader-B")
    assert lock_key not in client_fb.store, "Fallback failed to delete lock with matching fencing token!"

    # 3. Path B2: Non-eval fallback path with bytes value
    client_fb_bytes = FallbackClient(b"token-leader-B")
    await release_cluster_lock(client_fb_bytes, lock_key, "token-stale-leader-A")
    assert lock_key in client_fb_bytes.store, "Fallback deleted lock with bytes token mismatch!"
    await release_cluster_lock(client_fb_bytes, lock_key, "token-leader-B")
    assert lock_key not in client_fb_bytes.store, "Fallback failed to delete lock with matching bytes token!"


@pytest.mark.asyncio
async def test_cluster_singleflight_subsecond_ttl_precision(mock_common_dependencies, monkeypatch):
    """
    Verify sub-second precision for CLUSTER_SF_LOCK_TTL_SEC:
    When configured with 0.5s, Redis set is called with px=500 and nx=True,
    and the request succeeds cleanly without error.
    """
    monkeypatch.setattr(gateway, "CLUSTER_SF_LOCK_TTL_SEC", 0.5)
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    m_db.return_value = {"status": "allow", "role": "admin"}
    m_redis.get = AsyncMock(return_value=None)
    m_redis.set = AsyncMock(return_value=True)

    with patch("gateway.generate_composite_key", return_value="test_subsecond_ttl_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token",
                    "X-SPIFFE-ID": "spiffe://subsecond-ttl",
                    "X-Nonce": "nonce-subsecond-1"
                }
            )

            assert resp.status_code == 200
            assert resp.json()["policy"] == "admin"
            assert resp.json()["source"] == "DB"

    # Find the cluster single-flight lock acquisition call
    sf_lock_call = None
    for call in m_redis.set.call_args_list:
        args, kwargs = call
        if len(args) > 0 and str(args[0]).startswith("lock:sf:"):
            sf_lock_call = call
            break

    assert sf_lock_call is not None
    args, kwargs = sf_lock_call
    assert kwargs.get("nx") is True
    assert kwargs.get("px") == 500
    assert "ex" not in kwargs


@pytest.mark.asyncio
async def test_cluster_singleflight_follower_timeout_executes_db_fallback(mock_common_dependencies, monkeypatch):
    """
    Test Should fix-4:
    Pre-populate lock:sf:{key} so current node enters cluster follower L2 polling loop.
    With L2 unpopulated, after CLUSTER_SF_TIMEOUT_SEC expires, follower falls back
    to querying Policy DB directly, returns a valid DB response, and satisfies
    both Invariant 1 and Invariant 2.
    """
    monkeypatch.setattr(gateway, "CLUSTER_SF_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(gateway, "CLUSTER_SF_POLL_INTERVAL_SEC", 0.01)

    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    # Lock held by another node, L2 unpopulated
    m_redis.set = AsyncMock(return_value=False)
    m_redis.exists = AsyncMock(return_value=1)
    m_redis.get = AsyncMock(return_value=None)
    m_db.return_value = {"status": "allow", "role": "admin"}

    with patch("gateway.generate_composite_key", return_value="test_timeout_fallback_key"):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {
                "Authorization": "Bearer token",
                "X-SPIFFE-ID": "spiffe://follower-timeout",
                "X-Nonce": "nonce-timeout-1"
            }
            resp = await client.get("/auth", headers=headers)

            assert resp.status_code == 200
            assert resp.json()["policy"] == "admin"
            assert resp.json()["source"] == "DB"

            # DB was queried after follower timeout
            assert stats["db_queries_executed"] == 1
            assert stats["singleflight_coalesced"] == 0
            assert stats["db_fallbacks"] == 1
            assert m_db.call_count == 1

            # Invariant 1: Request Classification Invariant
            assert stats["requests"] == (
                stats["rate_limited"]
                + stats["nonce_replays"]
                + stats["pop_rejected"]
                + stats["l1_hits"]
                + stats["l2_hits"]
                + stats["db_fallbacks"]
                + stats["redis_outage_blocked"]
            )

            # Invariant 2: Strong DB Fallback Invariant
            assert stats["db_fallbacks"] == (
                stats["db_queries_executed"]
                + stats["singleflight_coalesced"]
                + stats["l1_double_check_hits"]
                + (stats["circuit_blocked"] - stats["pubkey_circuit_blocked"])
            )
