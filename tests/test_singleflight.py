import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
import uuid
from unittest.mock import patch, AsyncMock

from gateway import app, l1_cache, stats, stats_lock, cb, in_flight

@pytest.fixture(autouse=True)
def reset_globals():
    import gateway
    gateway.DB_MODE = "async"
    l1_cache.cache.clear()
    in_flight.clear()
    cb.reset()
    cb.last_transition_time = 0
    with stats_lock:
        for k in stats:
            if isinstance(stats[k], int):
                stats[k] = 0
            elif isinstance(stats[k], float):
                stats[k] = 0.0
        stats["circuit_state"] = "CLOSED"
        stats["circuit_blocked"] = 0
        stats["l1_double_check_hits"] = 0

@pytest.fixture
def mock_dependencies():
    with patch("gateway.check_rate_limit", new_callable=AsyncMock) as m_rl, \
         patch("gateway.consume_nonce", new_callable=AsyncMock) as m_nonce, \
         patch("gateway.redis_client") as m_redis, \
         patch("gateway.db_async_call", new_callable=AsyncMock) as m_db:
         
        m_rl.return_value = True
        m_nonce.return_value = True
        m_redis.get = AsyncMock(return_value=None)
        m_redis.set = AsyncMock(return_value=True)
        
        m_db.return_value = {"status": "allow", "role": "admin"}
        
        yield m_redis, m_db

@pytest.mark.asyncio
async def test_singleflight_off_100_requests(mock_dependencies, monkeypatch):
    import gateway
    monkeypatch.setattr(gateway, "SINGLEFLIGHT_ENABLED", False)
    
    m_redis, m_db = mock_dependencies
    
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
            
        # Circuit breaker may open if failures occurred, but this is a success test.
        # But wait, MAX_DB_WORKERS is 5! db_semaphore limit!
        # db_queries_executed might not be exactly 100 if DBOverloadError is raised!
        # But in test_globals fixture, DB_MODE="async", which bypasses semaphore!
        # [주석] mock 환경이라 세마포어/서킷 영향이 없어서 100이 나오며, 실제 DB 환경에서는 달라질 수 있음.
        assert stats["db_queries_executed"] == 100
        assert stats["singleflight_coalesced"] == 0

@pytest.mark.asyncio
async def test_singleflight_100_requests(mock_dependencies):
    m_redis, m_db = mock_dependencies
    
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
        assert stats["singleflight_coalesced"] == 99

@pytest.mark.asyncio
async def test_leader_cancellation(mock_dependencies):
    m_redis, m_db = mock_dependencies
    
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
async def test_loader_failure(mock_dependencies):
    m_redis, m_db = mock_dependencies
    
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
async def test_double_check_l1(mock_dependencies):
    """
    [L1 더블체크 테스트]
    Leader가 Task로 생성되어 실행되기 직전에 다른 비동기 흐름에 의해 L1 캐시가 채워졌다고 가정합니다.
    (락을 사용하지 않는 순수 비동기 구조이므로, l1_cache.get을 패치하여 첫 번째 조회(/auth 진입 시)는 미스,
    두 번째 조회(db_loader_task 내부)는 히트하도록 시뮬레이션합니다.)
    """
    m_redis, m_db = mock_dependencies
    m_db.return_value = {"status": "allow", "role": "admin"}
    
    from gateway import l1_cache
    
    # 첫 번째 L1 확인은 None(캐시 미스), 두 번째(더블체크)는 "DENY"를 반환하도록 조작
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
async def test_stats_invariants(mock_dependencies):
    """
    db_fallbacks 정의: "DB 경로로 진입한 요청 수, 서킷 차단과 L1 더블체크 히트 포함"
    불변식 검증 테스트
    """
    m_redis, m_db = mock_dependencies
    
    async def delayed_db_call(*args, **kwargs):
        await asyncio.sleep(0.05)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. DB Fallback (1 leader + 4 followers) = 5 requests
        headers = {"Authorization": "Bearer token", "X-Nonce": "nonce-1"}
        tasks = [client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv1"}) for _ in range(5)]
        
        # 2. L1 Hit (1 request, since previous populated it)
        await asyncio.gather(*tasks) # Wait for cache to populate
        resp_l1 = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv1", "X-Nonce": "nonce-2"})
        
        # 3. L2 Hit (1 request, different sid, mock L2 to return hit)
        m_redis.get.side_effect = lambda k: b"DENY" if k == "policy:test_l2" else None
        with patch("gateway.generate_composite_key", return_value="test_l2"):
            resp_l2 = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv2", "X-Nonce": "nonce-3"})
            
        assert resp_l1.status_code == 200
        assert resp_l2.status_code == 200

        # 4. L1 Double Check Hit (1 request)
        from gateway import l1_cache
        m_redis.get.side_effect = lambda k: l1_cache.set("test_l1_double", "DENY") or None
        with patch("gateway.generate_composite_key", return_value="test_l1_double"):
            resp_l1_dc = await client.get("/auth", headers={**headers, "X-SPIFFE-ID": "spiffe://inv_dc", "X-Nonce": "nonce-4"})
            assert resp_l1_dc.status_code == 200

        # 5. Circuit Breaker OPEN (1 request)
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
            stats["l1_hits"] + 
            stats["l2_hits"] + 
            stats["db_fallbacks"]
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
async def test_unretrieved_future_exception(mock_dependencies):
    m_redis, m_db = mock_dependencies
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
            
        # Wait for background db_loader_task to finish and set exception
        await asyncio.sleep(0.15)
        # Without fix, garbage collection will complain. We can't easily assert on gc warnings directly in pytest without specific fixtures, but running pytest -W error will fail if there is a warning.
