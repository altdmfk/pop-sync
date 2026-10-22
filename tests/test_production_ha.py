import asyncio
import os
import pytest
from unittest.mock import AsyncMock, MagicMock
import httpx

import gateway
from gateway import parse_sentinel_hosts


@pytest.mark.asyncio
async def test_pdp_mode_when_upstream_url_empty(mock_common_dependencies):
    """
    Case 1: When UPSTREAM_URL is empty, /auth operates in pure PDP mode,
    returning standard 200 OK authorization decision JSON.
    """
    gateway.UPSTREAM_URL = ""
    headers = {
        "Authorization": "Bearer tok",
        "X-Nonce": "nonce-pdp-1",
        "X-SPIFFE-ID": "spiffe://test/pdp-user",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp = await client.get("/auth", headers=headers)

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "allow"
    assert data["source"] in ("L1", "L2", "DB")
    assert "policy" in data


@pytest.mark.asyncio
async def test_pep_mode_passthrough_on_allow(mock_common_dependencies, monkeypatch):
    """
    Case 2: When UPSTREAM_URL is set and policy is 'allow', /auth forwards
    the request to upstream with stripped hop-by-hop/sensitive headers,
    injected identity context headers, and returns upstream status code and body.
    """
    gateway.UPSTREAM_URL = "http://mock-upstream:8080"
    gateway.UPSTREAM_TIMEOUT_SEC = 2.5

    mock_upstream_client = AsyncMock(spec=httpx.AsyncClient)
    mock_upstream_response = httpx.Response(
        status_code=201,
        content=b'{"created": true, "resource_id": 999}',
        headers={
            "content-type": "application/json",
            "x-upstream-response-id": "resp-xyz",
            "connection": "close",
            "transfer-encoding": "chunked",
        },
    )
    mock_upstream_client.request = AsyncMock(return_value=mock_upstream_response)
    monkeypatch.setattr(gateway, "get_async_http_client", lambda: mock_upstream_client)

    headers = {
        "Authorization": "Bearer tok",
        "X-Nonce": "nonce-pep-2",
        "X-SPIFFE-ID": "spiffe://test/pep-allow-user",
        "X-Client-Custom": "custom-val-123",
        "X-Admin-Token": "secret-admin-token",  # Sensitive header -> stripped
        "Host": "gateway.internal",  # Hop-by-hop -> stripped
        "Connection": "keep-alive",  # Hop-by-hop -> stripped
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp = await client.post("/auth?action=provision&tier=premium", headers=headers, content=b'{"name": "test"}')

    assert resp.status_code == 201
    assert resp.json() == {"created": True, "resource_id": 999}
    assert resp.headers.get("x-upstream-response-id") == "resp-xyz"
    assert "connection" not in resp.headers or resp.headers["connection"] != "close"

    assert mock_upstream_client.request.await_count == 1
    call_args = mock_upstream_client.request.await_args
    assert call_args.kwargs["method"] == "POST"
    assert call_args.kwargs["url"] == "http://mock-upstream:8080/auth?action=provision&tier=premium"
    assert call_args.kwargs["content"] == b'{"name": "test"}'
    assert call_args.kwargs["timeout"] == 2.5

    sent_headers = httpx.Headers(call_args.kwargs["headers"])
    assert sent_headers["X-Authenticated-SPIFFE-ID"] == "spiffe://test/pep-allow-user"
    assert sent_headers["X-Policy-Decision"] == "allow"
    assert sent_headers["X-Auth-Source"] in ("L1", "L2", "DB")
    assert sent_headers["X-Client-Custom"] == "custom-val-123"
    assert "x-admin-token" not in sent_headers
    assert "host" not in sent_headers
    assert "connection" not in sent_headers


@pytest.mark.asyncio
async def test_pep_mode_blocks_forwarding_on_deny(mock_common_dependencies, monkeypatch):
    """
    Case 3: When UPSTREAM_URL is set and policy decision is DENY,
    upstream forwarding is completely blocked (0 HTTP calls) and 403 Forbidden is returned.
    """
    gateway.UPSTREAM_URL = "http://mock-upstream:8080"
    m_rl, m_nonce, m_redis, m_db = mock_common_dependencies
    m_db.return_value = {"status": "deny"}

    mock_upstream_client = AsyncMock(spec=httpx.AsyncClient)
    monkeypatch.setattr(gateway, "get_async_http_client", lambda: mock_upstream_client)

    headers = {
        "Authorization": "Bearer tok",
        "X-Nonce": "nonce-pep-3",
        "X-SPIFFE-ID": "spiffe://test/pep-deny-user",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp = await client.get("/auth", headers=headers)

    assert resp.status_code == 403
    data = resp.json()
    assert data["status"] == "deny"
    assert data["policy"] == "DENY"
    assert data["message"] == "Access Denied by Policy"
    assert mock_upstream_client.request.await_count == 0


@pytest.mark.asyncio
async def test_pep_mode_upstream_exceptions_preserve_circuit_and_invariants(mock_common_dependencies, monkeypatch):
    """
    Case 4: When Upstream raises TimeoutException (504) or ConnectError (502),
    Policy DB Circuit Breaker remains CLOSED, and Invariant 1 and 2 hold 100%.
    """
    gateway.UPSTREAM_URL = "http://mock-upstream:8080"
    mock_upstream_client = AsyncMock(spec=httpx.AsyncClient)
    monkeypatch.setattr(gateway, "get_async_http_client", lambda: mock_upstream_client)

    # 4a. TimeoutException -> 504
    mock_upstream_client.request = AsyncMock(
        side_effect=httpx.TimeoutException("Upstream timed out", request=httpx.Request("GET", "mock"))
    )
    headers_timeout = {
        "Authorization": "Bearer tok",
        "X-Nonce": "nonce-err-timeout",
        "X-SPIFFE-ID": "spiffe://test/err-timeout",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp_to = await client.get("/auth", headers=headers_timeout)

    assert resp_to.status_code == 504
    assert resp_to.json() == {"status": "error", "message": "Upstream Timeout"}
    assert gateway.cb.current_state == "CLOSED"

    # 4b. ConnectError -> 502
    mock_upstream_client.request = AsyncMock(
        side_effect=httpx.ConnectError("Connection refused", request=httpx.Request("GET", "mock"))
    )
    headers_connect = {
        "Authorization": "Bearer tok",
        "X-Nonce": "nonce-err-connect",
        "X-SPIFFE-ID": "spiffe://test/err-connect",
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp_conn = await client.get("/auth", headers=headers_connect)

    assert resp_conn.status_code == 502
    assert resp_conn.json() == {"status": "error", "message": "Bad Gateway - Upstream Unreachable"}
    assert gateway.cb.current_state == "CLOSED"

    # Assert Invariants 1 & 2
    with gateway.stats_lock:
        s = dict(gateway.stats)

    inv1_lhs = s["requests"]
    inv1_rhs = (
        s["rate_limited"] +
        s["nonce_replays"] +
        s["pop_rejected"] +
        s["l1_hits"] +
        s["l2_hits"] +
        s["db_fallbacks"] +
        s["redis_outage_blocked"]
    )
    assert inv1_lhs == inv1_rhs, f"Invariant 1 mismatch: {inv1_lhs} != {inv1_rhs}"

    policy_circuit_blocked = s["circuit_blocked"] - s["pubkey_circuit_blocked"]
    inv2_lhs = s["db_fallbacks"]
    inv2_rhs = (
        s["db_queries_executed"] +
        s["singleflight_coalesced"] +
        s["l1_double_check_hits"] +
        policy_circuit_blocked
    )
    assert inv2_lhs == inv2_rhs, f"Invariant 2 mismatch: {inv2_lhs} != {inv2_rhs}"


@pytest.mark.asyncio
async def test_deep_healthcheck_healthy(mock_common_dependencies):
    """
    Case 5: GET /health returns 200 OK and connected status when Redis ping succeeds.
    """
    m_rl, m_nonce, m_redis, m_db = mock_common_dependencies
    m_redis.ping = AsyncMock(return_value=True)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["node"] == gateway.NODE_NAME
    assert data["redis"] == "connected"
    assert data["circuit_state"] == "CLOSED"


@pytest.mark.asyncio
async def test_deep_healthcheck_redis_unhealthy_and_timeout(mock_common_dependencies):
    """
    Case 6: GET /health returns 503 Service Unavailable when Redis ping throws
    an exception or exceeds timeout.
    """
    m_rl, m_nonce, m_redis, m_db = mock_common_dependencies

    # 6a. Redis exception -> 503
    m_redis.ping = AsyncMock(side_effect=Exception("Redis node failure"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp_err = await client.get("/health")

    assert resp_err.status_code == 503
    data_err = resp_err.json()
    assert data_err["status"] == "unhealthy"
    assert data_err["redis"] == "disconnected"
    assert data_err["detail"] == "Redis Unhealthy"
    assert data_err["circuit_state"] == "CLOSED"

    # 6b. Redis timeout -> 503
    async def slow_ping():
        await asyncio.sleep(0.5)
        return True

    m_redis.ping = AsyncMock(side_effect=slow_ping)
    gateway.HEALTHCHECK_PING_TIMEOUT_SEC = 0.05
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp_to = await client.get("/health")

    assert resp_to.status_code == 503
    data_to = resp_to.json()
    assert data_to["status"] == "unhealthy"
    assert data_to["redis"] == "disconnected"
    assert data_to["detail"] == "Redis Unhealthy"


@pytest.mark.asyncio
async def test_deep_healthcheck_node_isolated(mock_common_dependencies):
    """
    Case 7: GET /health returns 503 Service Unavailable when node is partitioned (isolated).
    """
    gateway.is_partitioned = True

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "unhealthy"
    assert data["node"] == gateway.NODE_NAME
    assert data["redis"] == "isolated"
    assert data["detail"] == "Node Isolated"
    assert data["circuit_state"] == "CLOSED"


@pytest.mark.asyncio
async def test_redis_sentinel_ha_support(monkeypatch):
    """
    Case 8: Validates parse_sentinel_hosts parser, Sentinel master_for dynamic connection,
    and Lua script registration when USE_REDIS_SENTINEL=True.
    """
    # 8a. Parser validation
    hosts_str = "sentinel-1:26379, sentinel-2:26380, sentinel-3:26381"
    parsed = parse_sentinel_hosts(hosts_str)
    assert parsed == [("sentinel-1", 26379), ("sentinel-2", 26380), ("sentinel-3", 26381)]

    single_host = parse_sentinel_hosts("localhost:26379")
    assert single_host == [("localhost", 26379)]

    # Parser error cases
    with pytest.raises(ValueError, match="missing port"):
        parse_sentinel_hosts("sentinel-missing-port")

    with pytest.raises(ValueError):
        parse_sentinel_hosts("sentinel:notanumber")

    with pytest.raises(ValueError):
        parse_sentinel_hosts("")

    with pytest.raises(ValueError):
        parse_sentinel_hosts("sentinel:0")

    # 8b. Sentinel client connection & script registration
    gateway.USE_REDIS_SENTINEL = True
    gateway.REDIS_SENTINEL_HOSTS = "s1:26379, s2:26379"
    gateway.REDIS_SENTINEL_MASTER_NAME = "mymaster"
    gateway.REDIS_SENTINEL_SOCKET_TIMEOUT = 0.5

    mock_master = MagicMock()
    mock_master.register_script = MagicMock(side_effect=lambda lua: f"script_registered_{hash(lua)}")
    mock_sentinel_instance = MagicMock()
    mock_sentinel_instance.master_for = MagicMock(return_value=mock_master)
    mock_sentinel_cls = MagicMock(return_value=mock_sentinel_instance)

    monkeypatch.setattr(gateway, "Sentinel", mock_sentinel_cls)
    monkeypatch.setattr(gateway, "redis_client", None)
    monkeypatch.setattr(gateway, "nonce_script", None)
    monkeypatch.setattr(gateway, "rl_script", None)

    client = gateway.get_redis_client()
    assert client is mock_master
    mock_sentinel_cls.assert_called_once_with(
        [("s1", 26379), ("s2", 26379)],
        socket_timeout=0.5,
        decode_responses=True,
    )
    mock_sentinel_instance.master_for.assert_called_once_with(
        "mymaster",
        socket_timeout=0.5,
        decode_responses=True,
    )
    assert mock_master.register_script.call_count == 2
    assert gateway.nonce_script is not None
    assert gateway.rl_script is not None

    # 8c. /chaos/redis/up recovery route also creates connection via Sentinel when enabled
    admin_token = os.environ.get("ADMIN_SECRET_TOKEN", "super-secret-admin")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as http_client:
        resp = await http_client.post("/chaos/redis/up", headers={"X-Admin-Token": admin_token})

    assert resp.status_code == 200
    assert resp.json() == {"status": "up"}
    assert gateway.redis_client is mock_master
