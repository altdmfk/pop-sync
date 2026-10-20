import pytest
import asyncio
import json
from unittest.mock import patch, AsyncMock, MagicMock
import control_plane

@pytest.mark.asyncio
async def test_control_plane_single_poller_fan_out():
    """
    Verify that Control Plane:
    1. Runs a single background poller loop instead of polling per SSE subscriber.
    2. Fans out metrics to all active subscribers via asyncio.Queue.
    3. Cleans up subscriber queue on disconnect in finally block.
    """
    control_plane.subscribers.clear()
    if control_plane.metrics_poller_task and not control_plane.metrics_poller_task.done():
        control_plane.metrics_poller_task.cancel()
    
    poll_count = 0
    async def mock_fetch():
        nonlocal poll_count
        poll_count += 1
        return {"Node-A": {"stats": {"requests": poll_count}}}
        
    with patch("control_plane.fetch_metrics", side_effect=mock_fetch):
        q1 = asyncio.Queue(maxsize=10)
        q2 = asyncio.Queue(maxsize=10)
        control_plane.subscribers.add(q1)
        control_plane.subscribers.add(q2)
        
        poller = asyncio.create_task(control_plane.broadcast_metrics_loop())
        try:
            msg1 = await asyncio.wait_for(q1.get(), timeout=2.0)
            msg2 = await asyncio.wait_for(q2.get(), timeout=2.0)
            
            assert json.loads(msg1) == {"Node-A": {"stats": {"requests": 1}}}
            assert json.loads(msg2) == {"Node-A": {"stats": {"requests": 1}}}
            assert poll_count == 1, "Single poller should fetch metrics once for all subscribers"
            
            control_plane.subscribers.discard(q1)
            assert len(control_plane.subscribers) == 1
            assert q1 not in control_plane.subscribers
            assert q2 in control_plane.subscribers
            
        finally:
            poller.cancel()
            try:
                await poller
            except asyncio.CancelledError:
                pass
            control_plane.subscribers.clear()

@pytest.mark.asyncio
async def test_control_plane_metrics_stream_endpoint():
    """
    Verify /metrics/stream registers subscriber queue and unregisters on client disconnect.
    """
    control_plane.subscribers.clear()
    if control_plane.metrics_poller_task and not control_plane.metrics_poller_task.done():
        control_plane.metrics_poller_task.cancel()

    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = {"Node-A": {"status": "ok"}}
        control_plane.latest_metrics_snapshot = {"Node-A": {"status": "ok"}}
        
        req = MagicMock()
        disconnected = False
        async def mock_is_disconnected():
            return disconnected
        req.is_disconnected = mock_is_disconnected
        
        resp = await control_plane.metrics_stream(req)
        assert len(control_plane.subscribers) == 1
        
        gen = resp.body_iterator
        first_event = await gen.__anext__()
        assert "Node-A" in str(first_event)
        
        disconnected = True
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()
            
        assert len(control_plane.subscribers) == 0

@pytest.mark.asyncio
async def test_fetch_metrics_concurrent_and_error_handling():
    """
    Verify that fetch_metrics:
    1. Polls gateways concurrently using asyncio.gather.
    2. Calls resp.raise_for_status() and returns offline error shape on non-200 or exception.
    3. Allows single-poller fan-out loop to survive per-node failures.
    """
    in_flight = 0
    max_in_flight = 0

    async def mock_get(url, headers=None):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1

        if "8001" in url:
            resp = MagicMock()
            resp.status_code = 200
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {"node": "Node-A", "status": "ok", "stats": {"requests": 10}}
            return resp
        elif "8002" in url:
            resp = MagicMock()
            resp.status_code = 503
            def raise_err():
                raise control_plane.httpx.HTTPStatusError("503 error", request=MagicMock(), response=resp)
            resp.raise_for_status = MagicMock(side_effect=raise_err)
            resp.json.return_value = {"status": "error", "message": "Node Isolated"}
            return resp
        else:
            raise control_plane.httpx.ConnectError("Connection refused")

    mock_client = MagicMock()
    mock_client.get = AsyncMock(side_effect=mock_get)

    with patch("control_plane.get_async_http_client", return_value=mock_client):
        results = await control_plane.fetch_metrics()

    assert max_in_flight > 1, "fetch_metrics must poll gateways concurrently with asyncio.gather"
    assert results["Node-A"] == {"node": "Node-A", "status": "ok", "stats": {"requests": 10}}
    assert results["Node-B"] == {"node": "Node-B", "status": "offline", "stats": {}}
    assert results["Node-C"] == {"node": "Node-C", "status": "offline", "stats": {}}

@pytest.mark.asyncio
async def test_chaos_auth_check():
    """
    Verify /chaos/auth_check validates X-Admin-Token with hmac.compare_digest:
    - Returns 401 on missing or mismatched token.
    - Returns 200 on matching token.
    """
    from httpx import AsyncClient, ASGITransport
    transport = ASGITransport(app=control_plane.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_missing = await client.get("/chaos/auth_check")
        assert resp_missing.status_code == 401

        resp_wrong = await client.get("/chaos/auth_check", headers={"X-Admin-Token": "wrong-token"})
        assert resp_wrong.status_code == 401

        resp_ok = await client.get("/chaos/auth_check", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
        assert resp_ok.status_code == 200
        assert resp_ok.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_policy_db_chaos_auth():
    """
    Verify policy_db /chaos/db/{state} validates X-Admin-Token with hmac.compare_digest:
    - Returns 401 on missing or mismatched token.
    - Returns 200 and updates state on matching token.
    """
    import policy_db
    from httpx import AsyncClient, ASGITransport
    transport = ASGITransport(app=policy_db.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_missing = await client.post("/chaos/db/down")
        assert resp_missing.status_code == 401

        resp_wrong = await client.post("/chaos/db/down", headers={"X-Admin-Token": "wrong-token"})
        assert resp_wrong.status_code == 401

        resp_down = await client.post("/chaos/db/down", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
        assert resp_down.status_code == 200
        assert resp_down.json() == {"is_down": True}

        resp_up = await client.post("/chaos/db/up", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
        assert resp_up.status_code == 200
        assert resp_up.json() == {"is_down": False}

@pytest.mark.asyncio
async def test_broadcast_metrics_loop_drops_failed_subscriber(caplog):
    """
    Verify that broadcast_metrics_loop logs a warning and removes a subscriber
    whose queue raises an exception during put_nowait, while still delivering to healthy subscribers.
    """
    control_plane.subscribers.clear()
    if control_plane.metrics_poller_task and not control_plane.metrics_poller_task.done():
        control_plane.metrics_poller_task.cancel()

    bad_q = MagicMock()
    bad_q.full.return_value = False
    bad_q.put_nowait.side_effect = RuntimeError("Broken subscriber queue")

    good_q = asyncio.Queue(maxsize=10)

    control_plane.subscribers.add(bad_q)
    control_plane.subscribers.add(good_q)

    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = {"Node-A": {"status": "ok", "stats": {}}}
        poller = asyncio.create_task(control_plane.broadcast_metrics_loop())
        try:
            msg = await asyncio.wait_for(good_q.get(), timeout=2.0)
            assert json.loads(msg) == {"Node-A": {"status": "ok", "stats": {}}}
            assert bad_q not in control_plane.subscribers
            assert good_q in control_plane.subscribers
            assert any("Broken subscriber queue" in rec.message for rec in caplog.records)
        finally:
            poller.cancel()
            try:
                await poller
            except asyncio.CancelledError:
                pass
            control_plane.subscribers.clear()


@pytest.mark.asyncio
async def test_chaos_audit_auth_protection():
    """
    Verify GET /chaos/audit is protected by admin_auth_middleware:
    - Returns 401 when X-Admin-Token is missing or incorrect.
    - Proceeds when X-Admin-Token is valid.
    """
    from httpx import AsyncClient, ASGITransport
    transport = ASGITransport(app=control_plane.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_missing = await client.get("/chaos/audit")
        assert resp_missing.status_code == 401

        resp_wrong = await client.get("/chaos/audit", headers={"X-Admin-Token": "bad-token"})
        assert resp_wrong.status_code == 401

        with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
            m_fetch.return_value = {
                "Node-A": {
                    "node": "Node-A",
                    "status": "online",
                    "stats": {
                        "requests": 0, "rate_limited": 0, "nonce_replays": 0, "pop_rejected": 0,
                        "l1_hits": 0, "l2_hits": 0, "db_fallbacks": 0, "redis_outage_blocked": 0,
                        "db_queries_executed": 0, "singleflight_coalesced": 0,
                        "l1_double_check_hits": 0, "circuit_blocked": 0, "circuit_state": "CLOSED"
                    }
                }
            }
            resp_ok = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp_ok.status_code == 200
            assert resp_ok.json()["overall_invariant_pass"] is True


@pytest.mark.asyncio
async def test_chaos_audit_healthy_and_degraded_nodes():
    """
    Verify GET /chaos/audit accurately evaluates both core Invariants, DB reduction ratio,
    cache hit ratio, and Single-Flight coalescing ratio across healthy, degraded, and offline nodes.
    """
    from httpx import AsyncClient, ASGITransport

    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "circuit_state": "CLOSED",
            "stats": {
                "requests": 100,
                "rate_limited": 10,
                "nonce_replays": 5,
                "pop_rejected": 5,
                "l1_hits": 20,
                "l2_hits": 10,
                "db_fallbacks": 50,
                "redis_outage_blocked": 0,
                "db_queries_executed": 5,
                "singleflight_coalesced": 40,
                "l1_double_check_hits": 3,
                "circuit_blocked": 2,
                "circuit_state": "CLOSED",
            }
        },
        "Node-B": {
            "node": "Node-B",
            "status": "online",
            "circuit_state": "OPEN",
            "stats": {
                "requests": 60,
                "rate_limited": 15,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 5,
                "l2_hits": 0,
                "db_fallbacks": 20,
                "redis_outage_blocked": 20,
                "db_queries_executed": 0,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 20,
                "circuit_state": "OPEN",
            }
        },
        "Node-C": {
            "node": "Node-C",
            "status": "offline",
            "stats": {}
        }
    }

    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics

        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            assert data["overall_invariant_pass"] is True
            assert data["online_nodes_count"] == 2
            assert data["total_nodes_count"] == 3

            node_a = data["nodes"]["Node-A"]
            assert node_a["status"] == "online"
            assert node_a["circuit_state"] == "CLOSED"
            assert node_a["invariant_pass"] is True
            assert node_a["invariant_1"]["pass"] is True
            assert node_a["invariant_1"]["requests"] == 100
            assert node_a["invariant_1"]["classified_sum"] == 100
            assert node_a["invariant_2"]["pass"] is True
            assert node_a["invariant_2"]["db_fallbacks"] == 50
            assert node_a["invariant_2"]["resolved_sum"] == 50
            assert node_a["cache_hit_ratio_pct"] == 30.0
            assert node_a["singleflight_coalescing_ratio_pct"] == 80.0
            assert node_a["db_load_reduction_percentage"] == 90.0

            node_b = data["nodes"]["Node-B"]
            assert node_b["status"] == "online"
            assert node_b["circuit_state"] == "OPEN"
            assert node_b["invariant_pass"] is True
            assert node_b["invariant_1"]["pass"] is True
            assert node_b["invariant_1"]["requests"] == 60
            assert node_b["invariant_1"]["classified_sum"] == 60
            assert node_b["invariant_2"]["pass"] is True
            assert node_b["invariant_2"]["db_fallbacks"] == 20
            assert node_b["invariant_2"]["resolved_sum"] == 20
            assert node_b["db_load_reduction_percentage"] == 100.0

            node_c = data["nodes"]["Node-C"]
            assert node_c["status"] == "offline"
            assert node_c["invariant_pass"] is None


@pytest.mark.asyncio
async def test_chaos_audit_invariant_mismatch_failure():
    """
    Verify GET /chaos/audit flags overall_invariant_pass as False if an online node violates
    Invariant 1 or Invariant 2.
    """
    from httpx import AsyncClient, ASGITransport

    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "stats": {
                "requests": 100,
                "rate_limited": 10,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 20,
                "l2_hits": 10,
                "db_fallbacks": 50,
                "redis_outage_blocked": 0,
                "db_queries_executed": 50,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "circuit_state": "CLOSED"
            }
        }
    }

    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics

        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            assert data["overall_invariant_pass"] is False
            node_a = data["nodes"]["Node-A"]
            assert node_a["invariant_pass"] is False
            assert node_a["invariant_1"]["pass"] is False
            assert node_a["invariant_1"]["requests"] == 100
            assert node_a["invariant_1"]["classified_sum"] == 90
            assert node_a["invariant_2"]["pass"] is True


@pytest.mark.asyncio
async def test_chaos_audit_invariant_2_holds_during_pubkey_circuit_blocked():
    from httpx import AsyncClient, ASGITransport

    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "circuit_state": "OPEN",
            "stats": {
                "requests": 5,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 5,
                "l1_hits": 0,
                "l2_hits": 0,
                "db_fallbacks": 0,
                "redis_outage_blocked": 0,
                "db_queries_executed": 0,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 5,
                "pubkey_circuit_blocked": 5,
                "circuit_state": "OPEN",
            }
        }
    }

    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics

        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            assert data["overall_invariant_pass"] is True
            node_a = data["nodes"]["Node-A"]
            assert node_a["invariant_pass"] is True
            assert node_a["invariant_1"]["pass"] is True
            assert node_a["invariant_1"]["requests"] == 5
            assert node_a["invariant_1"]["classified_sum"] == 5
            assert node_a["invariant_2"]["pass"] is True
            assert node_a["invariant_2"]["db_fallbacks"] == 0
            assert node_a["invariant_2"]["resolved_sum"] == 0
            assert node_a["invariant_2"]["details"]["circuit_blocked"] == 5
            assert node_a["invariant_2"]["details"]["pubkey_circuit_blocked"] == 5





