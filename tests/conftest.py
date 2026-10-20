import os
import sys
import pytest
from unittest.mock import patch, AsyncMock

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

os.environ["ADMIN_SECRET_TOKEN"] = "super-secret-admin"

@pytest.fixture(autouse=True)
def clean_gateway_state():
    """Autouse fixture ensuring clean gateway state across all test modules."""
    import gateway
    gateway.POP_VERIFY_ENABLED = False
    gateway.SINGLEFLIGHT_ENABLED = True
    gateway.DB_MODE = "async"
    gateway.l1_cache.clear()
    gateway.in_flight.clear()
    gateway.local_rate_limits.clear()
    gateway.cb.reset()
    gateway.cb.force_state("CLOSED")
    with gateway.stats_lock:
        for k in list(gateway.stats.keys()):
            if isinstance(gateway.stats[k], int):
                gateway.stats[k] = 0
            elif isinstance(gateway.stats[k], float):
                gateway.stats[k] = 0.0
        gateway.stats["circuit_state"] = "CLOSED"
        gateway.stats["circuit_blocked"] = 0
        gateway.stats["pubkey_circuit_blocked"] = 0
        gateway.stats["l1_double_check_hits"] = 0
        gateway.stats["redis_outage_blocked"] = 0
        gateway.stats["half_open_probe_active"] = False
        gateway.stats["singleflight_leader_queries"] = 0
        gateway.stats["last_catchup"] = {
            "pending_count": 0,
            "pel_drained": 0,
            "autoclaimed": 0,
            "new_drained": 0,
            "duration_ms": 0,
            "timestamp": 0.0,
        }

    yield

    gateway.POP_VERIFY_ENABLED = False
    gateway.SINGLEFLIGHT_ENABLED = True
    gateway.l1_cache.clear()
    gateway.in_flight.clear()
    gateway.local_rate_limits.clear()
    gateway.cb.reset()
    gateway.cb.force_state("CLOSED")
    with gateway.stats_lock:
        for k in list(gateway.stats.keys()):
            if isinstance(gateway.stats[k], int):
                gateway.stats[k] = 0
            elif isinstance(gateway.stats[k], float):
                gateway.stats[k] = 0.0
        gateway.stats["circuit_state"] = "CLOSED"
        gateway.stats["circuit_blocked"] = 0
        gateway.stats["pubkey_circuit_blocked"] = 0
        gateway.stats["l1_double_check_hits"] = 0
        gateway.stats["redis_outage_blocked"] = 0
        gateway.stats["half_open_probe_active"] = False
        gateway.stats["singleflight_leader_queries"] = 0
        gateway.stats["last_catchup"] = {
            "pending_count": 0,
            "pel_drained": 0,
            "autoclaimed": 0,
            "new_drained": 0,
            "duration_ms": 0,
            "timestamp": 0.0,
        }

@pytest.fixture
def mock_common_dependencies():
    """Consolidated mock fixture for gateway dependencies."""
    class MockResponse:
        def __init__(self, data, status=200):
            self._data = data
            self.status_code = status
        def json(self):
            return self._data
        def raise_for_status(self):
            if self.status_code >= 400:
                import httpx
                req = httpx.Request("GET", "mock")
                raise httpx.HTTPStatusError("mock", request=req, response=self)

    async def mock_get(url, params=None, **kwargs):
        import gateway
        if "/pubkey" in url:
            if params and params.get("sid", "").endswith("/nokey"):
                return MockResponse({}, status=404)
            return MockResponse({"public_key": getattr(gateway, "TEST_PUBLIC_KEY", None)})
        elif "/policy" in url:
            return MockResponse({"status": "allow", "role": "admin"})
        return MockResponse({})

    with patch("gateway.check_rate_limit", new_callable=AsyncMock) as m_rl, \
         patch("gateway.consume_nonce", new_callable=AsyncMock) as m_nonce, \
         patch("gateway.redis_client") as m_redis, \
         patch("gateway.db_async_call", new_callable=AsyncMock) as m_db, \
         patch("gateway.async_db_client", new=m_db):
         
        m_rl.return_value = True
        m_nonce.return_value = True
        m_redis.get = AsyncMock(return_value=None)
        m_redis.set = AsyncMock(return_value=True)
        m_redis.setex = AsyncMock(return_value=True)
        m_db.return_value = {"status": "allow", "role": "admin"}
        m_db.get = AsyncMock(side_effect=mock_get)
        
        yield m_rl, m_nonce, m_redis, m_db

