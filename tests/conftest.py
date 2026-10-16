import os
import sys
import pytest
from unittest.mock import patch, AsyncMock

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Set environment variables for tests
os.environ["ADMIN_SECRET_TOKEN"] = "super-secret-admin"

@pytest.fixture(autouse=True)
def clean_gateway_state():
    """Autouse fixture ensuring clean gateway state across all test modules."""
    import gateway
    gateway.POP_VERIFY_ENABLED = False
    gateway.SINGLEFLIGHT_ENABLED = True
    gateway.DB_MODE = "async"
    gateway.l1_cache.cache.clear()
    gateway.in_flight.clear()
    gateway.local_rate_limits.clear()
    gateway.cb.reset()
    gateway.cb.last_transition_time = 0
    gateway.cb.last_failure_time = 0
    with gateway.stats_lock:
        for k in gateway.stats:
            if isinstance(gateway.stats[k], int):
                gateway.stats[k] = 0
            elif isinstance(gateway.stats[k], float):
                gateway.stats[k] = 0.0
        gateway.stats["circuit_state"] = "CLOSED"
        gateway.stats["circuit_blocked"] = 0
        gateway.stats["l1_double_check_hits"] = 0
        gateway.stats["redis_outage_blocked"] = 0

    yield

    gateway.POP_VERIFY_ENABLED = False
    gateway.SINGLEFLIGHT_ENABLED = True
    gateway.l1_cache.cache.clear()
    gateway.in_flight.clear()
    gateway.local_rate_limits.clear()
    gateway.cb.reset()

@pytest.fixture
def mock_common_dependencies():
    """Consolidated mock fixture for gateway dependencies."""
    with patch("gateway.check_rate_limit", new_callable=AsyncMock) as m_rl, \
         patch("gateway.consume_nonce", new_callable=AsyncMock) as m_nonce, \
         patch("gateway.redis_client") as m_redis, \
         patch("gateway.db_async_call", new_callable=AsyncMock) as m_db:
         
        m_rl.return_value = True
        m_nonce.return_value = True
        m_redis.get = AsyncMock(return_value=None)
        m_redis.set = AsyncMock(return_value=True)
        m_redis.setex = AsyncMock(return_value=True)
        m_db.return_value = {"status": "allow", "role": "admin"}
        
        yield m_rl, m_nonce, m_redis, m_db
