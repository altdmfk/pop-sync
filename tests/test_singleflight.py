import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
import uuid
from unittest.mock import patch, AsyncMock

from gateway import app, l1_cache, stats, stats_lock, cb, in_flight

@pytest.mark.asyncio
async def test_singleflight_off_100_requests(mock_common_dependencies, monkeypatch):
    import gateway
    monkeypatch.setattr(gateway, "SINGLEFLIGHT_ENABLED", False)
    
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.1)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test_off",
            "X-Nonce": "nonce-123"
        }
        
        tasks = [client.get("/auth", headers=headers) for _ in range(100)]
        results = await asyncio.gather(*tasks)
        
        for r in results:
            assert r.status_code == 200
            assert r.json()["policy"] == "admin"
            
        assert stats["db_queries_executed"] == 100
        assert stats["singleflight_coalesced"] == 0

@pytest.mark.asyncio
async def test_singleflight_100_requests(mock_common_dependencies):
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.1)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test1",
            "X-Nonce": "nonce-123"
        }
        
        tasks = [client.get("/auth", headers=headers) for _ in range(100)]
        results = await asyncio.gather(*tasks)
        
        for r in results:
            assert r.status_code == 200
            assert r.json()["policy"] == "admin"
            
        assert stats["db_queries_executed"] == 1
        assert stats["singleflight_leader_queries"] == 1
        assert stats["singleflight_coalesced"] == 99

@pytest.mark.asyncio
async def test_leader_cancellation(mock_common_dependencies):
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.2)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test2",
            "X-Nonce": "nonce-123"
        }
        
        leader_task = asyncio.create_task(client.get("/auth", headers=headers))
        await asyncio.sleep(0.05)
        
        follower_task = asyncio.create_task(client.get("/auth", headers=headers))
        await asyncio.sleep(0.05)
        
        leader_task.cancel()
        
        follower_resp = await follower_task
        assert follower_resp.status_code == 200
        assert follower_resp.json()["policy"] == "admin"
        
        assert stats["db_queries_executed"] == 1
        assert stats["singleflight_coalesced"] == 1

@pytest.mark.asyncio
async def test_loader_failure(mock_common_dependencies):
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    class TestException(Exception): pass
    
    async def failing_db_call(*args, **kwargs):
        await asyncio.sleep(0.1)
        raise TestException("DB Failed")
    m_db.side_effect = failing_db_call
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test3",
            "X-Nonce": "nonce-123"
        }
        
        tasks = [client.get("/auth", headers=headers) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        for r in results:
            assert r.status_code == 200
            assert r.json()["status"] == "degraded_allow"
            
        assert stats["db_queries_executed"] == 1
        assert stats["singleflight_coalesced"] == 9
        assert cb.failures == 1

@pytest.mark.asyncio
async def test_double_check_l1(mock_common_dependencies):
    """
    [L1 더블체크 테스트]
    Leader가 Task로 생성되어 실행되기 직전에 다른 비동기 흐름에 의해 L1 캐시가 채워졌다고 가정합니다.
    (락을 사용하지 않는 순수 비동기 구조이므로, l1_cache.get을 패치하여 첫 번째 조회(/auth 진입 시)는 미스,
    두 번째 조회(db_loader_task 내부)는 히트하도록 시뮬레이션합니다.)
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    m_db.return_value = {"status": "allow", "role": "admin"}
    
    from gateway import l1_cache
    
    with patch.object(l1_cache, "get", side_effect=[None, "DENY"]):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {
                "Authorization": "Bearer token",
                "X-SPIFFE-ID": "spiffe://test4",
                "X-Nonce": "nonce-123"
            }
            
            resp = await client.get("/auth", headers=headers)
            
            assert resp.status_code == 200
            assert resp.json()["policy"] == "DENY"
            assert resp.json()["source"] == "L1"
            assert stats["db_queries_executed"] == 0
            assert stats["l1_double_check_hits"] == 1

@pytest.mark.asyncio
async def test_stats_invariants(mock_common_dependencies):
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.05)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": "Bearer token", "X-Nonce": "nonce-1"}
        tasks = [client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv1"}) for _ in range(5)]
        
        await asyncio.gather(*tasks)
        resp_l1 = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv1", "X-Nonce": "nonce-2"})
        
        m_redis.get.side_effect = lambda k: b"DENY" if k == "policy:test_l2" else None
        with patch("gateway.generate_composite_key", return_value="test_l2"):
            resp_l2 = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv2", "X-Nonce": "nonce-3"})
            
        assert resp_l1.status_code == 200
        assert resp_l2.status_code == 200

        from gateway import l1_cache
        m_redis.get.side_effect = lambda k: l1_cache.set("test_l1_double", "DENY") or None
        with patch("gateway.generate_composite_key", return_value="test_l1_double"):
            resp_l1_dc = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv_dc", "X-Nonce": "nonce-4"})
            assert resp_l1_dc.status_code == 200

        from gateway import cb
        cb.force_state("OPEN")
        m_redis.get.side_effect = AsyncMock(return_value=None)
        with patch("gateway.generate_composite_key", return_value="test_cb"):
            resp_cb = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv_cb", "X-Nonce": "nonce-5"})
            assert resp_cb.status_code == 200
            assert resp_cb.json()["status"] == "degraded_allow"

        # Invariant 1: requests == sum of classification counters (Assuming no cancellations)
        total_classified = (
            stats["rate_limited"] + 
            stats["nonce_replays"] + 
            stats["pop_rejected"] + 
            stats["l1_hits"] + 
            stats["l2_hits"] + 
            stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )
        assert stats["requests"] == total_classified

        # Invariant 2: db_queries_executed <= db_fallbacks
        assert stats["db_queries_executed"] <= stats["db_fallbacks"]

        # Invariant 3: coalesced + db_queries_executed <= requests
        assert stats["singleflight_coalesced"] + stats["db_queries_executed"] <= stats["requests"]
        
        # Strong Invariant (Including CB Open and L1 Double Check)
        assert stats["db_fallbacks"] == (
            stats["db_queries_executed"] + 
            stats["singleflight_coalesced"] + 
            stats["l1_double_check_hits"] + 
            stats["circuit_blocked"]
        )

@pytest.mark.asyncio
async def test_unretrieved_future_exception(mock_common_dependencies):
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    class TestException(Exception): pass
    async def failing_db_call(*args, **kwargs):
        await asyncio.sleep(0.1)
        raise TestException("DB Failed")
    m_db.side_effect = failing_db_call

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://unretrieved",
            "X-Nonce": "nonce-unretrieved"
        }
        
        leader_task = asyncio.create_task(client.get("/auth", headers=headers))
        await asyncio.sleep(0.02) # Let it start
        leader_task.cancel() # Cancel leader
        
        try:
            await leader_task
        except asyncio.CancelledError:
            pass
            
        await asyncio.sleep(0.15)


def test_circuit_breaker_debounce_and_half_open_boundary(monkeypatch):
    """
    Boundary test for Circuit Breaker:
    - 2.0s debounce boundary (CLOSED -> OPEN): failures within 2.0s are ignored.
    - At/after 2.0s, failures increment count towards threshold and transition to OPEN.
    - After reset_timeout expires, can_execute() transitions OPEN -> HALF_OPEN.
    - In HALF_OPEN, a successful request transitions back to CLOSED.
    """
    import time
    from gateway import CircuitBreaker
    
    current_time = 1000.0
    monkeypatch.setattr(time, "time", lambda: current_time)
    
    cb_test = CircuitBreaker(max_failures=3, reset_timeout=10.0, debounce_sec=2.0)
    cb_test.reset()
    assert cb_test.current_state == "CLOSED"
    
    current_time = 1001.0
    cb_test.record_failure()
    assert cb_test.failures == 0, "Failure within debounce window must be debounced"
    assert cb_test.current_state == "CLOSED"
    
    current_time = 1001.99
    cb_test.record_failure()
    assert cb_test.failures == 0
    
    current_time = 1002.0
    cb_test.record_failure()
    assert cb_test.failures == 1
    assert cb_test.current_state == "CLOSED"
    
    current_time = 1002.5
    cb_test.record_failure()
    assert cb_test.failures == 2
    
    current_time = 1003.0
    cb_test.record_failure()
    assert cb_test.failures == 3
    assert cb_test.current_state == "OPEN"
    
    can_exec, state = cb_test.can_execute()
    assert can_exec is False
    assert state == "OPEN"
    
    current_time = 1012.9
    can_exec, state = cb_test.can_execute()
    assert can_exec is False
    assert state == "OPEN"
    
    current_time = 1013.1
    can_exec, state = cb_test.can_execute()
    assert can_exec is True
    assert state == "HALF_OPEN"
    assert cb_test.current_state == "HALF_OPEN"

    current_time = 1013.5
    cb_test.record_failure()
    assert cb_test.current_state == "OPEN"
    assert cb_test.last_failure_time == 1013.5
    assert cb_test.last_transition_time == 1013.5

    current_time = 1023.6
    can_exec, state = cb_test.can_execute()
    assert can_exec is True
    assert state == "HALF_OPEN"
    
    cb_test.record_success()
    assert cb_test.current_state == "CLOSED"
    assert cb_test.failures == 0

    current_time = 1024.5
    cb_test.record_failure()
    assert cb_test.failures == 0
    assert cb_test.current_state == "CLOSED"


@pytest.mark.asyncio
async def test_singleflight_exception_propagation_and_map_cleanup(mock_common_dependencies):
    """
    Boundary test:
    1. Single-Flight exception in db loader propagates to leader and followers.
    2. in_flight map is cleaned up (popped) in finally block.
    3. Subsequent request creates a fresh loader and executes without being stuck.
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    call_count = 0
    async def failing_db_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        if call_count == 1:
            raise RuntimeError("DB Temporary Outage")
        return {"status": "allow", "role": "admin"}
        
    m_db.side_effect = failing_db_call
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test_cleanup",
        }
        
        tasks = [client.get("/auth", headers={**headers, "X-Nonce": f"nonce-err-{i}"}) for i in range(4)]
        results = await asyncio.gather(*tasks)
        
        for r in results:
            assert r.status_code == 200
            assert r.json()["status"] == "degraded_allow"
            
        assert len(in_flight) == 0, f"in_flight map must be empty, but had: {in_flight}"
        
        cb.reset()
        resp2 = await client.get("/auth", headers={**headers, "X-Nonce": "nonce-succ-1"})
        assert resp2.status_code == 200
        assert resp2.json()["policy"] == "admin"
        assert len(in_flight) == 0


@pytest.mark.asyncio
async def test_singleflight_db_overload_exception_propagation(mock_common_dependencies):
    """
    Boundary test:
    Verify DBOverloadError propagates as 503 to followers and cleans up in_flight.
    """
    from gateway import DBOverloadError
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    async def overload_db_call(*args, **kwargs):
        await asyncio.sleep(0.05)
        raise DBOverloadError("DB concurrency limit reached (Fast-Fail)")
        
    m_db.side_effect = overload_db_call
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test_overload",
        }
        
        tasks = [client.get("/auth", headers={**headers, "X-Nonce": f"nonce-ov-{i}"}) for i in range(3)]
        results = await asyncio.gather(*tasks)
        
        for r in results:
            assert r.status_code == 503
            assert r.json()["message"] == "Service Unavailable - DB Overload"
            
        assert len(in_flight) == 0


@pytest.mark.asyncio
async def test_invariant_equality_under_redis_503(mock_common_dependencies):
    """
    Boundary test:
    Verify that simulated Redis outage during Nonce check in /auth
    increments both 'requests' and 'redis_outage_blocked', maintaining
    100% Invariant equality:
    requests == (rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks + redis_outage_blocked)
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies
    
    import redis.exceptions
    with patch("gateway.consume_nonce", new_callable=AsyncMock) as m_nonce:
        m_nonce.side_effect = redis.exceptions.ConnectionError("Redis connection refused")
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {
                "Authorization": "Bearer token",
                "X-SPIFFE-ID": "spiffe://test_redis_outage",
            }
            
            for i in range(5):
                resp = await client.get("/auth", headers={**headers, "X-Nonce": f"nonce-outage-{i}"})
                assert resp.status_code == 503
                assert "Nonce Check Failed" in resp.json()["message"]
                
            assert stats["requests"] == 5
            assert stats["redis_outage_blocked"] == 5
            
            classified_sum = (
                stats["rate_limited"] +
                stats["nonce_replays"] +
                stats["pop_rejected"] +
                stats["l1_hits"] +
                stats["l2_hits"] +
                stats["db_fallbacks"] +
                stats["redis_outage_blocked"]
            )
            assert stats["requests"] == classified_sum


@pytest.mark.asyncio
async def test_invariant_equality_under_rate_limit_503(mock_common_dependencies):
    """
    Verify that when check_rate_limit raises an exception in /auth,
    it returns 503 and increments both 'requests' and 'redis_outage_blocked',
    maintaining 100% Invariant equality:
    requests == (rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks + redis_outage_blocked)
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    with patch("gateway.check_rate_limit", new_callable=AsyncMock) as m_rl:
        m_rl.side_effect = RuntimeError("Rate limit unexpected failure")

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {
                "Authorization": "Bearer token",
                "X-SPIFFE-ID": "spiffe://test_rl_outage",
            }

            for i in range(5):
                resp = await client.get("/auth", headers={**headers, "X-Nonce": f"nonce-rl-outage-{i}"})
                assert resp.status_code == 503
                assert "Rate Limit Failed" in resp.json()["message"]

            assert stats["requests"] == 5
            assert stats["redis_outage_blocked"] == 5

            classified_sum = (
                stats["rate_limited"] +
                stats["nonce_replays"] +
                stats["pop_rejected"] +
                stats["l1_hits"] +
                stats["l2_hits"] +
                stats["db_fallbacks"] +
                stats["redis_outage_blocked"]
            )
            assert stats["requests"] == classified_sum


@pytest.mark.asyncio
async def test_half_open_allows_strictly_single_probe_under_concurrency(mock_common_dependencies):
    """
    Verify that when the circuit breaker reset timeout expires, exactly ONE trial probe
    is permitted through HALF_OPEN gating under high concurrency (10 concurrent distinct-key requests).
    The single trial hits the DB (db_queries_executed == 1), the other 9 are circuit-blocked
    (circuit_blocked == 9), and upon successful probe completion the circuit transitions to CLOSED.
    Mathematical invariants MUST hold 100%.
    """
    import time
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    # Delay DB call slightly so all 10 concurrent requests arrive while the probe is in flight
    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.08)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call

    # Force circuit to OPEN with reset timeout already expired
    cb.force_state("OPEN")
    cb.last_failure_time = time.time() - cb.reset_timeout - 5.0
    assert cb.current_state == "OPEN"
    assert cb.is_probe_active is False

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 10 concurrent cache-miss requests with DISTINCT keys to avoid SingleFlight coalescing
        tasks = [
            client.get(
                "/auth",
                headers={
                    "Authorization": f"Bearer token-{i}",
                    "X-SPIFFE-ID": f"spiffe://probe-node-{i}",
                    "X-Nonce": f"nonce-probe-{i}",
                }
            )
            for i in range(10)
        ]
        results = await asyncio.gather(*tasks)

        successful_probes = [r for r in results if r.json().get("source") == "DB"]
        blocked_requests = [r for r in results if r.json().get("source") == "Fallback_Degradation"]

        assert len(successful_probes) == 1, f"Expected exactly 1 DB probe, got {len(successful_probes)}"
        assert len(blocked_requests) == 9, f"Expected 9 circuit-blocked requests, got {len(blocked_requests)}"

        assert stats["db_queries_executed"] == 1
        assert stats["circuit_blocked"] == 9
        assert stats["singleflight_coalesced"] == 0
        assert stats["db_fallbacks"] == 10
        assert stats["requests"] == 10

        assert stats["requests"] == (
            stats["rate_limited"] +
            stats["nonce_replays"] +
            stats["pop_rejected"] +
            stats["l1_hits"] +
            stats["l2_hits"] +
            stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )

        assert stats["db_fallbacks"] == (
            stats["db_queries_executed"] +
            stats["singleflight_coalesced"] +
            stats["l1_double_check_hits"] +
            stats["circuit_blocked"]
        )

        assert cb.current_state == "CLOSED"
        assert cb.is_probe_active is False

        m_db.side_effect = None
        m_db.return_value = {"status": "allow", "role": "admin"}
        resp_after = await client.get(
            "/auth",
            headers={
                "Authorization": "Bearer token-after",
                "X-SPIFFE-ID": "spiffe://probe-node-after",
                "X-Nonce": "nonce-after-probe",
            }
        )
        assert resp_after.status_code == 200
        assert resp_after.json()["source"] == "DB"
        assert stats["db_queries_executed"] == 2


@pytest.mark.asyncio
async def test_half_open_probe_slot_released_on_cancellation_or_overload(mock_common_dependencies):
    """
    Verify anti-deadlock safety in HALF_OPEN state:
    1. If the single probe encounters DBOverloadError (fast-fail without recording failure),
       the probe slot MUST be released and a subsequent request can retry the probe.
    2. If the single probe encounters coroutine cancellation (asyncio.CancelledError),
       the probe slot MUST be released and a subsequent request can retry without being permanently stuck.
    """
    from gateway import DBOverloadError, background_tasks
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        cb.force_state("HALF_OPEN")
        assert cb.current_state == "HALF_OPEN"
        assert cb.is_probe_active is False

        m_db.side_effect = DBOverloadError("DB concurrency limit reached (Fast-Fail)")

        resp_overload = await client.get(
            "/auth",
            headers={
                "Authorization": "Bearer token-ov",
                "X-SPIFFE-ID": "spiffe://test-overload",
                "X-Nonce": "nonce-ov-1",
            }
        )
        assert resp_overload.status_code == 503
        assert "DB Overload" in resp_overload.json()["message"]

        assert cb.current_state == "HALF_OPEN"
        assert cb.is_probe_active is False

        m_db.side_effect = None
        m_db.return_value = {"status": "allow", "role": "admin"}

        resp_retry1 = await client.get(
            "/auth",
            headers={
                "Authorization": "Bearer token-retry1",
                "X-SPIFFE-ID": "spiffe://test-retry1",
                "X-Nonce": "nonce-retry-1",
            }
        )
        assert resp_retry1.status_code == 200
        assert resp_retry1.json()["policy"] == "admin"
        assert cb.current_state == "CLOSED"
        assert cb.is_probe_active is False

        cb.force_state("HALF_OPEN")
        assert cb.current_state == "HALF_OPEN"
        assert cb.is_probe_active is False

        async def cancelling_db_call(*args, **kwargs):
            raise asyncio.CancelledError()
        m_db.side_effect = cancelling_db_call

        try:
            await client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token-cancel",
                    "X-SPIFFE-ID": "spiffe://test-cancel",
                    "X-Nonce": "nonce-cancel-1",
                }
            )
        except (asyncio.CancelledError, Exception):
            pass

        await asyncio.sleep(0.05)

        assert cb.current_state == "HALF_OPEN"
        assert cb.is_probe_active is False

        m_db.side_effect = None
        m_db.return_value = {"status": "allow", "role": "admin"}

        resp_retry2 = await client.get(
            "/auth",
            headers={
                "Authorization": "Bearer token-retry2",
                "X-SPIFFE-ID": "spiffe://test-retry2",
                "X-Nonce": "nonce-retry-2",
            }
        )
        assert resp_retry2.status_code == 200
        assert resp_retry2.json()["policy"] == "admin"
        assert cb.current_state == "CLOSED"
        assert cb.is_probe_active is False

        cb.force_state("HALF_OPEN")
        assert cb.current_state == "HALF_OPEN"
        assert cb.is_probe_active is False

        async def slow_db_call(*args, **kwargs):
            await asyncio.sleep(2.0)
            return {"status": "allow", "role": "admin"}
        m_db.side_effect = slow_db_call

        req_task = asyncio.create_task(client.get(
            "/auth",
            headers={
                "Authorization": "Bearer token-slow",
                "X-SPIFFE-ID": "spiffe://test-slow",
                "X-Nonce": "nonce-slow-1",
            }
        ))
        await asyncio.sleep(0.05)
        assert cb.is_probe_active is True

        for bg_task in list(background_tasks):
            bg_task.cancel()
        req_task.cancel()

        try:
            await req_task
        except (asyncio.CancelledError, Exception):
            pass

        await asyncio.sleep(0.05)
        assert cb.current_state == "HALF_OPEN"
        assert cb.is_probe_active is False

        m_db.side_effect = None
        m_db.return_value = {"status": "allow", "role": "admin"}
        resp_retry3 = await client.get(
            "/auth",
            headers={
                "Authorization": "Bearer token-retry3",
                "X-SPIFFE-ID": "spiffe://test-retry3",
                "X-Nonce": "nonce-retry-3",
            }
        )
        assert resp_retry3.status_code == 200
        assert cb.current_state == "CLOSED"


def test_circuit_breaker_single_probe_direct_unit(monkeypatch):
    """
    Direct unit test for CircuitBreaker atomic single-probe gating:
    - Verifies is_probe_active property and lock safety
    - Verifies can_execute() returns (True, 'HALF_OPEN') exactly once and (False, 'HALF_OPEN') thereafter
    - Verifies release_half_open_probe() releases probe slot
    - Verifies record_failure() in HALF_OPEN immediately re-opens and resets probe slot
    - Verifies record_success() in HALF_OPEN closes circuit and resets probe slot
    - Verifies check_and_half_open() transitions OPEN -> HALF_OPEN with probe slot unacquired
    - Verifies force_state / force_open / force_close reset probe slot
    """
    import time
    from gateway import CircuitBreaker

    current_time = 2000.0
    monkeypatch.setattr(time, "time", lambda: current_time)

    test_cb = CircuitBreaker(max_failures=3, reset_timeout=10.0, debounce_sec=2.0)
    assert test_cb.current_state == "CLOSED"
    assert test_cb.is_probe_active is False

    test_cb.force_open()
    assert test_cb.current_state == "OPEN"
    assert test_cb.is_probe_active is False

    can_exec, state = test_cb.can_execute()
    assert can_exec is False
    assert state == "OPEN"
    assert test_cb.is_probe_active is False

    current_time = 2011.0
    can_exec, state = test_cb.can_execute()
    assert can_exec is True
    assert state == "HALF_OPEN"
    assert test_cb.is_probe_active is True

    can_exec2, state2 = test_cb.can_execute()
    assert can_exec2 is False
    assert state2 == "HALF_OPEN"
    assert test_cb.is_probe_active is True

    test_cb.release_half_open_probe()
    assert test_cb.is_probe_active is False
    assert test_cb.current_state == "HALF_OPEN"

    can_exec3, state3 = test_cb.can_execute()
    assert can_exec3 is True
    assert state3 == "HALF_OPEN"
    assert test_cb.is_probe_active is True

    test_cb.record_failure()
    assert test_cb.current_state == "OPEN"
    assert test_cb.is_probe_active is False

    current_time = 2022.0
    assert test_cb.check_and_half_open() == "HALF_OPEN"
    assert test_cb.is_probe_active is False

    can_exec4, state4 = test_cb.can_execute()
    assert can_exec4 is True
    assert state4 == "HALF_OPEN"
    assert test_cb.is_probe_active is True

    test_cb.record_success()
    assert test_cb.current_state == "CLOSED"
    assert test_cb.is_probe_active is False

    test_cb.force_state("HALF_OPEN")
    assert test_cb.is_probe_active is False
    test_cb.can_execute()
    assert test_cb.is_probe_active is True
    test_cb.force_close()
    assert test_cb.current_state == "CLOSED"
    assert test_cb.is_probe_active is False


@pytest.mark.asyncio
async def test_singleflight_leader_follower_efficiency_observability(mock_common_dependencies):
    """
    Observability verification for technical presentation:
    1. Verify singleflight_leader_queries correctly tracks leader task initiations.
    2. Verify leader vs. follower breakdown:
       - 50 concurrent requests for Key A coalesce into 1 leader + 49 followers.
       - 50 concurrent requests for Key B coalesce into 1 leader + 49 followers.
       - Total: 2 leader queries, 98 coalesced followers, 100 db_fallbacks, 2 db_queries_executed.
    3. Mathematical Strong Invariant strictly holds:
       db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + circuit_blocked
    """
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.08)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        tasks_a = [
            client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token",
                    "X-SPIFFE-ID": "spiffe://obs-key-a",
                    "X-Nonce": f"nonce-a-{i}",
                }
            )
            for i in range(50)
        ]
        tasks_b = [
            client.get(
                "/auth",
                headers={
                    "Authorization": "Bearer token",
                    "X-SPIFFE-ID": "spiffe://obs-key-b",
                    "X-Nonce": f"nonce-b-{i}",
                }
            )
            for i in range(50)
        ]

        results = await asyncio.gather(*tasks_a, *tasks_b)

        for r in results:
            assert r.status_code == 200
            assert r.json()["policy"] == "admin"

        assert stats["db_queries_executed"] == 2
        assert stats["singleflight_leader_queries"] == 2
        assert stats["singleflight_coalesced"] == 98
        assert stats["db_fallbacks"] == 100
        assert stats["requests"] == 100

        assert stats["requests"] == (
            stats["rate_limited"] +
            stats["nonce_replays"] +
            stats["pop_rejected"] +
            stats["l1_hits"] +
            stats["l2_hits"] +
            stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )

        assert stats["db_fallbacks"] == (
            stats["db_queries_executed"] +
            stats["singleflight_coalesced"] +
            stats["l1_double_check_hits"] +
            stats["circuit_blocked"]
        )


@pytest.mark.asyncio
async def test_follower_cancellation_zero_drift_invariant_1(mock_common_dependencies):
    _m_rl, _m_nonce, m_redis, m_db = mock_common_dependencies

    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.15)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {
            "Authorization": "Bearer token",
            "X-SPIFFE-ID": "spiffe://test-follower-cancel",
            "X-Nonce": "nonce-leader",
        }

        leader_task = asyncio.create_task(client.get("/auth", headers=headers))
        await asyncio.sleep(0.03)

        follower_task = asyncio.create_task(
            client.get("/auth", headers={**headers, "X-Nonce": "nonce-follower"})
        )
        await asyncio.sleep(0.02)

        follower_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await follower_task

        leader_resp = await leader_task
        assert leader_resp.status_code == 200

        assert stats["requests"] == 2
        assert stats["db_fallbacks"] == 2
        assert stats["db_queries_executed"] == 1
        assert stats["singleflight_coalesced"] == 1

        classified_sum = (
            stats["rate_limited"] +
            stats["nonce_replays"] +
            stats["pop_rejected"] +
            stats["l1_hits"] +
            stats["l2_hits"] +
            stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )
        assert stats["requests"] == classified_sum

        resolved_sum = (
            stats["db_queries_executed"] +
            stats["singleflight_coalesced"] +
            stats["l1_double_check_hits"] +
            stats["circuit_blocked"]
        )
        assert stats["db_fallbacks"] == resolved_sum






