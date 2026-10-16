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
