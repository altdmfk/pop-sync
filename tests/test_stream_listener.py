import asyncio
from unittest.mock import AsyncMock, patch
import pytest

import gateway


@pytest.mark.asyncio
async def test_pel_recovery_drains_all_batches_and_switches_to_live(monkeypatch):
    """
    Verify that during PEL recovery (id "0"):
    1. >100 pending messages (e.g. 150) are fully drained across multiple batches
       until an empty batch is returned, before switching to ">".
    2. All pending message IDs are XACKed and evicted from L1.
    3. Live ">" reading still works after PEL drain.
    """
    monkeypatch.setattr(gateway, "PARTITION_SLEEP_SEC", 0.01)

    pel_messages = [
        (f"1000-{i}".encode(), {b"key": f"pel_key_{i}".encode()})
        for i in range(150)
    ]
    live_messages = [
        (b"2000-0", {b"key": b"live_key_0"})
    ]

    for i in range(150):
        gateway.l1_cache.set(f"pel_key_{i}", "allow")
    gateway.l1_cache.set("live_key_0", "allow")

    acked_ids = []
    read_sequence = []
    pel_cursor = 0
    live_delivered = False

    mock_stream_client = AsyncMock()
    mock_redis_client = AsyncMock()
    mock_stream_client.xgroup_create = AsyncMock(return_value=True)
    mock_redis_client.delete = AsyncMock(return_value=1)

    async def mock_xack(stream, group, msg_id):
        acked_ids.append(msg_id)
        return 1

    mock_stream_client.xack = AsyncMock(side_effect=mock_xack)

    async def mock_xreadgroup(group, consumer, streams, count=None, block=None):
        nonlocal pel_cursor, live_delivered
        stream_id = streams[gateway.STREAM_NAME]
        read_sequence.append((stream_id, count, block))

        if stream_id == "0":
            if pel_cursor == 0 and gateway.is_partitioned:
                return []
            batch = pel_messages[pel_cursor : pel_cursor + count]
            pel_cursor += len(batch)
            if not batch:
                return [[gateway.STREAM_NAME.encode(), []]]
            return [[gateway.STREAM_NAME.encode(), batch]]

        if stream_id == ">":
            # In catchup phase (block is None), no new un-delivered messages yet
            if block is None:
                return [[gateway.STREAM_NAME.encode(), []]]
            # In live steady-state (block is set), deliver live message then stop listener
            if not live_delivered:
                live_delivered = True
                return [[gateway.STREAM_NAME.encode(), live_messages]]
            raise asyncio.CancelledError()

    mock_stream_client.xreadgroup = AsyncMock(side_effect=mock_xreadgroup)

    monkeypatch.setattr(gateway, "stream_redis_client", mock_stream_client)
    monkeypatch.setattr(gateway, "redis_client", mock_redis_client)

    # Start partitioned so stream_listener enters PEL recovery upon healing
    gateway.is_partitioned = True
    listener_task = asyncio.create_task(gateway.stream_listener())
    await asyncio.sleep(0.03)
    gateway.is_partitioned = False

    await asyncio.wait_for(listener_task, timeout=2.0)

    # First ">" read must only happen AFTER "0" was read 3 times (100, 50, 0 empty batch)
    first_gt_idx = next(i for i, (sid, _, _) in enumerate(read_sequence) if sid == ">")
    zero_reads_before_gt = [entry for entry in read_sequence[:first_gt_idx] if entry[0] == "0"]
    assert len(zero_reads_before_gt) == 3, (
        f"Expected 3 XREADGROUP '0' calls (100, 50, empty) before switching to '>', got {read_sequence[:first_gt_idx + 1]}"
    )

    expected_pel_ids = [f"1000-{i}".encode() for i in range(150)]
    assert acked_ids == expected_pel_ids + [b"2000-0"]
    assert gateway.stats["last_catchup"]["pending_count"] == 150
    for i in range(150):
        assert gateway.l1_cache.get(f"pel_key_{i}") is None
    assert gateway.l1_cache.get("live_key_0") is None


@pytest.mark.asyncio
async def test_stream_listener_deduplicated_message_helper():
    """
    Verify that the repeated message-processing loop in stream_listener is
    deduplicated into a single local helper inside gateway.py with unchanged behavior.
    """
    import inspect

    listener_src = inspect.getsource(gateway.stream_listener)
    assert listener_src.count("for msg_id, msg_data in messages:") == 0, (
        "Repeated 'for msg_id, msg_data in messages:' loops in stream_listener should be extracted into a helper"
    )
    assert hasattr(gateway, "_process_stream_messages")

    gateway.l1_cache.set("k1", "allow")
    mock_stream_client = AsyncMock()
    mock_redis_client = AsyncMock()

    messages = [
        (b"1-0", {b"key": b"k1"}),
        (b"1-1", {b"malformed": b"no_key"}),
    ]
    await gateway._process_stream_messages(messages, mock_stream_client, mock_redis_client)

    assert gateway.l1_cache.get("k1") is None
    mock_redis_client.delete.assert_awaited_once_with("policy:k1")
    assert mock_stream_client.xack.await_count == 2


@pytest.mark.asyncio
async def test_invalidate_cache_xadd_maxlen_approximate():
    """
    Verify invalidate_cache calls xadd with maxlen=10000 and approximate=True.
    """
    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock(return_value=b"1-0")

    with patch("gateway.get_redis_client", return_value=mock_redis):
        res = await gateway.invalidate_cache("test_key_123")

    assert res == {"status": "published"}
    mock_redis.xadd.assert_awaited_once_with(
        gateway.STREAM_NAME,
        {"key": "test_key_123"},
        maxlen=10000,
        approximate=True,
    )


@pytest.mark.asyncio
async def test_lifespan_awaits_background_tasks_before_closing_clients():
    """
    Verify (B-04):
    1. Module-level background_tasks holds strong refs and discards on completion.
    2. Lifespan teardown cancels stream/cleanup tasks and awaits both those tasks and
       background_tasks BEFORE closing HTTP and Redis clients.
    3. Executor shutdown uses wait=False, cancel_futures=True.
    """
    from unittest.mock import MagicMock

    assert hasattr(gateway, "background_tasks"), "gateway must define module-level background_tasks set"
    assert isinstance(gateway.background_tasks, set)

    events = []

    async def fake_stream_listener():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.02)
            events.append("stream_listener_done")
            raise

    async def fake_cleanup_local_rl():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.02)
            events.append("cleanup_local_rl_done")
            raise

    mock_redis = AsyncMock()
    mock_redis.register_script = MagicMock(return_value=AsyncMock())
    async def redis_close():
        events.append("redis_close")
    mock_redis.close = AsyncMock(side_effect=redis_close)

    mock_stream_redis = AsyncMock()
    async def stream_redis_close():
        events.append("stream_redis_close")
    mock_stream_redis.close = AsyncMock(side_effect=stream_redis_close)

    mock_sync_http = MagicMock()
    mock_sync_http.is_closed = False
    def sync_http_close():
        events.append("sync_http_close")
    mock_sync_http.close = MagicMock(side_effect=sync_http_close)

    mock_async_http = AsyncMock()
    mock_async_http.is_closed = False
    async def async_http_close():
        events.append("async_http_close")
    mock_async_http.aclose = AsyncMock(side_effect=async_http_close)

    mock_db_exec = MagicMock()
    mock_pop_exec = MagicMock()

    async def slow_probe():
        await asyncio.sleep(0.03)
        events.append("bg_probe_done")

    orig_state = (
        gateway.redis_client,
        gateway.stream_redis_client,
        gateway.sync_http_client,
        gateway.async_db_client,
        gateway.db_executor,
        gateway.pop_executor,
        gateway.db_semaphore,
        gateway.nonce_script,
        gateway.rl_script,
    )
    try:
        with patch("gateway.stream_listener", side_effect=fake_stream_listener), \
             patch("gateway.cleanup_local_rl", side_effect=fake_cleanup_local_rl), \
             patch("gateway.Redis.from_url", side_effect=[mock_redis, mock_stream_redis]), \
             patch("gateway.httpx.Client", return_value=mock_sync_http), \
             patch("gateway.httpx.AsyncClient", return_value=mock_async_http), \
             patch("gateway.ThreadPoolExecutor", side_effect=[mock_db_exec, mock_pop_exec]), \
             patch("gateway.probe_db_health", side_effect=slow_probe):

            async with gateway.lifespan(gateway.app):
                await asyncio.sleep(0)
                await gateway.set_partition("heal")
                assert len(gateway.background_tasks) >= 1
    finally:
        (
            gateway.redis_client,
            gateway.stream_redis_client,
            gateway.sync_http_client,
            gateway.async_db_client,
            gateway.db_executor,
            gateway.pop_executor,
            gateway.db_semaphore,
            gateway.nonce_script,
            gateway.rl_script,
        ) = orig_state

    assert len(gateway.background_tasks) == 0

    task_events = {"stream_listener_done", "cleanup_local_rl_done", "bg_probe_done"}
    close_events = {"sync_http_close", "async_http_close", "redis_close", "stream_redis_close"}

    assert task_events.issubset(set(events)), f"Missing task completion events in {events}"
    assert close_events.issubset(set(events)), f"Missing client close events in {events}"

    last_task_idx = max(events.index(e) for e in task_events)
    first_close_idx = min(events.index(e) for e in close_events)
    assert last_task_idx < first_close_idx, (
        f"Expected all background tasks to finish before any client closes, got order: {events}"
    )

    mock_db_exec.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    mock_pop_exec.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
