import os
import sys
import time
import pytest
from unittest.mock import patch, AsyncMock

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

os.environ["ADMIN_SECRET_TOKEN"] = "super-secret-admin"

@pytest.fixture(autouse=True)
def clean_gateway_state():
    """Autouse fixture ensuring clean gateway state across all test modules."""
    import gateway
    gateway.UPSTREAM_URL = ""
    gateway.USE_REDIS_SENTINEL = False
    gateway.is_partitioned = False
    gateway.POP_VERIFY_ENABLED = False
    gateway.SINGLEFLIGHT_ENABLED = True
    gateway.CLUSTER_SINGLEFLIGHT_ENABLED = True
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

    gateway.UPSTREAM_URL = ""
    gateway.USE_REDIS_SENTINEL = False
    gateway.is_partitioned = False
    gateway.POP_VERIFY_ENABLED = False
    gateway.SINGLEFLIGHT_ENABLED = True
    gateway.CLUSTER_SINGLEFLIGHT_ENABLED = True
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
        m_redis.ping = AsyncMock(return_value=True)
        m_redis.get = AsyncMock(return_value=None)
        m_redis.set = AsyncMock(return_value=True)
        m_redis.setex = AsyncMock(return_value=True)
        m_redis.time = AsyncMock(side_effect=lambda: (int(time.time()), int((time.time() % 1) * 1_000_000)))
        m_db.return_value = {"status": "allow", "role": "admin"}
        m_db.get = AsyncMock(side_effect=mock_get)
        
        yield m_rl, m_nonce, m_redis, m_db


@pytest.fixture
async def e2e_cluster():
    """
    Cluster fixture for E2E integration tests.
    - Checks live Redis availability; if unreachable, skips the test cleanly.
    - Connects to an existing live cluster (e.g. running via Docker Compose)
      or launches missing components (Policy DB, Gateways A/B/C, Control Plane) as subprocesses.
    - Awaits healthiness across all endpoints.
    - Yields cluster configuration mapping.
    - Gracefully terminates any spawned subprocesses on teardown.
    """
    import asyncio
    import subprocess
    import httpx

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    db_url = os.getenv("DB_URL", "http://localhost:8000")
    cp_url = os.getenv("CONTROL_PLANE_URL", "http://localhost:8080")
    node_urls = {
        "Node-A": os.getenv("GATEWAY_A", "http://localhost:8001"),
        "Node-B": os.getenv("GATEWAY_B", "http://localhost:8002"),
        "Node-C": os.getenv("GATEWAY_C", "http://localhost:8003"),
    }
    admin_token = os.getenv("ADMIN_SECRET_TOKEN", "super-secret-admin")

    import redis.asyncio as aioredis
    try:
        r = aioredis.from_url(redis_url, socket_timeout=1.0)
        await r.ping()
        await r.aclose()
    except Exception as e:
        pytest.skip(f"Live Redis not available at {redis_url}: {e}")

    procs = []
    env = os.environ.copy()
    env["ADMIN_SECRET_TOKEN"] = admin_token
    env["REDIS_URL"] = redis_url
    env["DB_URL"] = db_url
    env["CLUSTER_SINGLEFLIGHT_ENABLED"] = "true"
    env["SINGLEFLIGHT_ENABLED"] = "true"

    async with httpx.AsyncClient(timeout=3.0) as client:
        # Check Policy DB
        try:
            await client.get(f"{db_url}/policy/probe?sid=test&res=test")
        except Exception:
            p_db = subprocess.Popen([sys.executable, "policy_db.py"], env=env)
            procs.append(p_db)

        # Check Gateways
        for name, url in node_urls.items():
            port = url.split(":")[-1]
            try:
                await client.get(f"{url}/health")
            except Exception:
                p_node = subprocess.Popen([sys.executable, "gateway.py", "--node", name, "--port", str(port)], env=env)
                procs.append(p_node)

        # Check Control Plane
        try:
            await client.get(f"{cp_url}/health")
        except Exception:
            p_cp = subprocess.Popen([sys.executable, "control_plane.py"], env=env)
            procs.append(p_cp)

    if procs:
        ready = False
        start_wait = time.time()
        async with httpx.AsyncClient(timeout=1.0) as client:
            all_urls = list(node_urls.values()) + [cp_url]
            while time.time() - start_wait < 15.0:
                try:
                    resps = await asyncio.gather(*[client.get(f"{u}/health") for u in all_urls], return_exceptions=True)
                    if all(getattr(r, "status_code", None) == 200 for r in resps):
                        ready = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)

        if not ready:
            for p in procs:
                p.terminate()
            pytest.fail("Cluster services failed to reach healthy status within 15 seconds")

    try:
        yield {
            "redis_url": redis_url,
            "db_url": db_url,
            "control_plane": cp_url,
            "gateways": node_urls,
            "admin_token": admin_token,
        }
    finally:
        for p in procs:
            try:
                p.terminate()
                p.wait(timeout=2.0)
            except Exception:
                pass


