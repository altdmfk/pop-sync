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
        
        # Simulate client disconnect
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




