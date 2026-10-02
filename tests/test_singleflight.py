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
    m_redis, m_db = mock_dependencies
    
    async def delayed_db_call(*args, **kwargs):
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db_call
    
    from gateway import generate_composite_key, l1_cache
    comp_key = generate_composite_key("spiffe://test4", "token", "resource", "read")
    
    async def delayed_redis_get(*args, **kwargs):
        l1_cache.set(comp_key, "DENY")
        return None
    m_redis.get.side_effect = delayed_redis_get
    
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
