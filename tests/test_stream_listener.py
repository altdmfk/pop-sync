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


@pytest.mark.asyncio
async def test_stream_listener_xautoclaim_and_enriched_catchup_telemetry(monkeypatch):
    """
    Verify XAUTOCLAIM Step B during partition catchup and enriched last_catchup telemetry:
    1. Step A drains own PEL ("0") -> pel_drained.
    2. Step B claims idle pending messages from other/stale consumers via xautoclaim -> autoclaimed.
    3. Step C drains newly accumulated messages (">") -> new_drained.
    4. All messages are invalidated from L1 and L2, and XACKed.
    5. stats["last_catchup"] contains all 6 fields:
       pending_count, pel_drained, autoclaimed, new_drained, duration_ms, timestamp.
    """
    monkeypatch.setattr(gateway, "PARTITION_SLEEP_SEC", 0.01)
    monkeypatch.setattr(gateway, "STREAM_CLAIM_MIN_IDLE_MS", 30000)

    # Populate L1 cache for keys to be invalidated
    gateway.l1_cache.set("pel_k0", "allow")
    gateway.l1_cache.set("pel_k1", "allow")
    gateway.l1_cache.set("claim_k0", "allow")
    gateway.l1_cache.set("claim_k1", "allow")
    gateway.l1_cache.set("new_k0", "allow")

    acked_ids = []
    pel_calls = 0
    new_calls = 0
    claim_calls = 0

    mock_stream_client = AsyncMock()
    mock_redis_client = AsyncMock()
    mock_stream_client.xgroup_create = AsyncMock(return_value=True)
    mock_redis_client.delete = AsyncMock(return_value=1)

    async def mock_xack(stream, group, msg_id):
        acked_ids.append(msg_id)
        return 1

    mock_stream_client.xack = AsyncMock(side_effect=mock_xack)

    async def mock_xreadgroup(group, consumer, streams, count=None, block=None):
        nonlocal pel_calls, new_calls
        stream_id = streams[gateway.STREAM_NAME]

        if stream_id == "0":
            pel_calls += 1
            if pel_calls == 1:
                return [[gateway.STREAM_NAME.encode(), [
                    (b"pel-0", {b"key": b"pel_k0"}),
                    (b"pel-1", {b"key": b"pel_k1"}),
                ]]]
            return [[gateway.STREAM_NAME.encode(), []]]

        if stream_id == ">":
            if block is None:
                new_calls += 1
                if new_calls == 1:
                    return [[gateway.STREAM_NAME.encode(), [
                        (b"new-0", {b"key": b"new_k0"}),
                    ]]]
                return [[gateway.STREAM_NAME.encode(), []]]
            # Live phase: stop listener
            raise asyncio.CancelledError()

    mock_stream_client.xreadgroup = AsyncMock(side_effect=mock_xreadgroup)

    async def mock_xautoclaim(name, group, consumer, min_idle_time, start_id="0-0", count=None):
        nonlocal claim_calls
        claim_calls += 1
        assert min_idle_time == 30000
        assert group == gateway.STREAM_GROUP
        if claim_calls == 1:
            assert start_id == "0-0"
            return ("100-1", [(b"claim-0", {b"key": b"claim_k0"})])
        elif claim_calls == 2:
            assert start_id == "100-1"
            return ("0-0", [(b"claim-1", {b"key": b"claim_k1"})])
        return ("0-0", [])

    mock_stream_client.xautoclaim = AsyncMock(side_effect=mock_xautoclaim)

    monkeypatch.setattr(gateway, "stream_redis_client", mock_stream_client)
    monkeypatch.setattr(gateway, "redis_client", mock_redis_client)

    gateway.is_partitioned = True
    task = asyncio.create_task(gateway.stream_listener())
    await asyncio.sleep(0.03)
    gateway.is_partitioned = False

    await asyncio.wait_for(task, timeout=2.0)

    catchup = gateway.stats["last_catchup"]
    assert catchup["pending_count"] == 5
    assert catchup["pel_drained"] == 2
    assert catchup["autoclaimed"] == 2
    assert catchup["new_drained"] == 1
    assert catchup["duration_ms"] >= 0
    assert catchup["timestamp"] > 0

    assert acked_ids == [b"pel-0", b"pel-1", b"claim-0", b"claim-1", b"new-0"]

    assert gateway.l1_cache.get("pel_k0") is None
    assert gateway.l1_cache.get("pel_k1") is None
    assert gateway.l1_cache.get("claim_k0") is None
    assert gateway.l1_cache.get("claim_k1") is None
    assert gateway.l1_cache.get("new_k0") is None

    deleted_keys = [call.args[0] for call in mock_redis_client.delete.await_args_list]
    for expected_key in ["policy:pel_k0", "policy:pel_k1", "policy:claim_k0", "policy:claim_k1", "policy:new_k0"]:
        assert expected_key in deleted_keys


@pytest.mark.asyncio
async def test_stream_listener_xautoclaim_graceful_fallback_without_xautoclaim(monkeypatch):
    """
    Verify graceful degradation if the active Redis client or mock does NOT support xautoclaim
    (AttributeError or ResponseError):
    - Stream listener completes catchup without crashing.
    - stats['last_catchup']['autoclaimed'] is 0.
    - pel_drained and new_drained are correctly tracked.
    """
    monkeypatch.setattr(gateway, "PARTITION_SLEEP_SEC", 0.01)

    mock_stream_client = AsyncMock()
    mock_redis_client = AsyncMock()
    mock_stream_client.xgroup_create = AsyncMock(return_value=True)
    mock_stream_client.xack = AsyncMock(return_value=1)
    mock_redis_client.delete = AsyncMock(return_value=1)

    # Simulate older Redis client raising ResponseError or client lacking xautoclaim
    mock_stream_client.xautoclaim.side_effect = AttributeError("'AsyncMock' object has no attribute 'xautoclaim'")

    pel_done = False
    new_done = False
    async def mock_xreadgroup(group, consumer, streams, count=None, block=None):
        nonlocal pel_done, new_done
        stream_id = streams[gateway.STREAM_NAME]
        if stream_id == "0":
            if not pel_done:
                pel_done = True
                return [[gateway.STREAM_NAME.encode(), [(b"pel-x", {b"key": b"key_x"})]]]
            return [[gateway.STREAM_NAME.encode(), []]]
        if stream_id == ">":
            if block is None:
                if not new_done:
                    new_done = True
                    return [[gateway.STREAM_NAME.encode(), [(b"new-y", {b"key": b"key_y"})]]]
                return [[gateway.STREAM_NAME.encode(), []]]
            raise asyncio.CancelledError()

    mock_stream_client.xreadgroup = AsyncMock(side_effect=mock_xreadgroup)

    monkeypatch.setattr(gateway, "stream_redis_client", mock_stream_client)
    monkeypatch.setattr(gateway, "redis_client", mock_redis_client)

    gateway.is_partitioned = True
    task = asyncio.create_task(gateway.stream_listener())
    await asyncio.sleep(0.03)
    gateway.is_partitioned = False

    await asyncio.wait_for(task, timeout=2.0)

    catchup = gateway.stats["last_catchup"]
    assert catchup["autoclaimed"] == 0
    assert catchup["pel_drained"] == 1
    assert catchup["new_drained"] == 1
    assert catchup["pending_count"] == 2


@pytest.mark.asyncio
async def test_stream_listener_undecodable_stream_key_acked_and_logged():
    mock_stream_client = AsyncMock()
    mock_redis_client = AsyncMock()

    acked_ids = []
    async def mock_xack(stream, group, msg_id):
        acked_ids.append(msg_id)
        return 1

    mock_stream_client.xack = AsyncMock(side_effect=mock_xack)
    mock_redis_client.delete = AsyncMock(return_value=1)

    gateway.l1_cache.set("valid_key", "allow")
    assert gateway.l1_cache.get("valid_key") == "allow"

    messages = [
        (b"msg-corrupted", {b"key": b"\xff\xfe\xfa"}),
        (b"msg-empty", {}),
        (b"msg-valid", {b"key": b"valid_key"}),
    ]

    await gateway._process_stream_messages(messages, mock_stream_client, mock_redis_client)

    assert acked_ids == [b"msg-corrupted", b"msg-empty", b"msg-valid"]
    assert gateway.l1_cache.get("valid_key") is None
    mock_redis_client.delete.assert_awaited_once_with("policy:valid_key")

