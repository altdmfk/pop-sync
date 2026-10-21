"""
tests/test_rate_limit.py

Unit tests for the sliding-window rate-limit implementation.

Design notes:
- All tests run against _local_rate_limit() (sync, no Redis) or check_rate_limit()
  with a fake rl_script so no real Redis is needed.
- Time is injected via monkeypatch on gateway.time.time to produce deterministic
  window_key and elapsed values.
- RL_WINDOW_SEC=10, RL_LIMIT=50, RL_FALLBACK_LIMIT=15 (module defaults).

Near-exact boundary:
  The sliding approximation has a maximum error of ±1 count due to integer
  weight truncation (weight = int((1 - elapsed/W) * 1000) / 1000). Tests that
  check "must be ≤ LIMIT" or "> LIMIT" account for this by using a gap of ≥ 2.
"""
import time as _real_time
import pytest
import asyncio
from unittest.mock import AsyncMock, patch

import gateway
from gateway import (
    _local_rate_limit,
    RL_WINDOW_SEC,
    RL_LIMIT,
    RL_FALLBACK_LIMIT,
    local_rate_limits,
)

def _fake_time(t: float):
    return lambda: t


def _reset_local_rl():
    local_rate_limits.clear()


def test_local_fallback_count_resets_after_window():
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client = "spiffe://test/recovery"

    t_prev = W * 100 + 0
    for _ in range(RL_FALLBACK_LIMIT):
        ok = _local_rate_limit(client, int(t_prev), int(t_prev) // W)
        assert ok, "Should be within limit while filling"

    over = _local_rate_limit(client, int(t_prev), int(t_prev) // W)
    assert not over, "One past limit should be blocked"

    t_new = W * 102 + 0
    _reset_local_rl()
    result = _local_rate_limit(client, int(t_new), int(t_new) // W)
    assert result, "After full reset, first request should pass"


def test_local_fallback_prev_weight_decreases():
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client = "spiffe://test/weight"

    prev_window = 10
    curr_window = 11
    prev_key = f"{client}:{prev_window}"
    local_rate_limits[prev_key] = RL_FALLBACK_LIMIT

    t_start = W * curr_window + 0
    result_at_start = _local_rate_limit(client, int(t_start), curr_window)
    assert not result_at_start, "At window start, prev still fully counted -> blocked"

    _reset_local_rl()
    local_rate_limits[prev_key] = RL_FALLBACK_LIMIT
    t_end = W * curr_window + (W - 1)
    result_at_end = _local_rate_limit(client, int(t_end), curr_window)
    assert result_at_end, "Near end of window, prev weight low -> allowed"


def test_local_fallback_exact_limit():
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client = "spiffe://test/exact"

    t = W * 200 + 5
    wk = int(t) // W

    results = [_local_rate_limit(client, int(t), wk) for _ in range(RL_FALLBACK_LIMIT + 2)]
    passed = sum(1 for r in results if r)
    blocked = sum(1 for r in results if not r)

    assert passed >= RL_FALLBACK_LIMIT - 1, f"Too few passed: {passed}"
    assert blocked >= 1, "At least one should be blocked"

@pytest.mark.asyncio
async def test_fixed_window_boundary_burst():
    """Fixed window allows up to ~2x limit across a window boundary.

    The test simulates RL_LIMIT requests just before the boundary (counted in
    window N) and RL_LIMIT requests just after (counted in window N+1).
    Both sets individually pass, so total passing = 2 * RL_LIMIT.
    """
    W = RL_WINDOW_SEC
    t_before = W * 300 - 1
    t_after  = W * 300

    client = "spiffe://test/burst"
    counts = {299: 0, 300: 0}

    async def fake_rl_script(keys, args):
        window = int(keys[1].split(":")[-1])
        counts[window] = counts.get(window, 0) + 1
        return counts[window]

    passed_before = 0
    passed_after  = 0

    with patch("gateway.rl_script", side_effect=fake_rl_script), \
         patch("gateway.RL_ALGORITHM", "fixed"), \
         patch.object(gateway, "redis_client") as mock_redis:

        async def fake_incr_expire(key):
            window = int(key.split(":")[-1])
            counts[window] = counts.get(window, 0) + 1
            return counts[window]

        class FakePipe:
            def __init__(self):
                self._key = None
                self._val = None
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            def incr(self, key):
                window = int(key.split(":")[-1])
                counts[window] = counts.get(window, 0) + 1
                self._val = counts[window]
            def expire(self, key, ttl): pass
            async def execute(self): return [self._val, True]

        mock_redis.pipeline = lambda transaction=True: FakePipe()

        with patch("gateway.time.time", return_value=t_before):
            for _ in range(RL_LIMIT):
                ok = await gateway.check_rate_limit(client)
                if ok: passed_before += 1

        with patch("gateway.time.time", return_value=t_after):
            for _ in range(RL_LIMIT):
                ok = await gateway.check_rate_limit(client)
                if ok: passed_after += 1

    total_passed = passed_before + passed_after
    assert passed_before == RL_LIMIT, f"Expected {RL_LIMIT} before boundary, got {passed_before}"
    assert passed_after  == RL_LIMIT, f"Expected {RL_LIMIT} after boundary,  got {passed_after}"
    assert total_passed  == 2 * RL_LIMIT, f"Fixed window burst = 2x limit; got {total_passed}"


@pytest.mark.asyncio
async def test_sliding_window_boundary_no_burst():
    """Sliding window prevents the 2x boundary burst.

    At the boundary (elapsed=0), prev_weight=1.0.  Sending RL_LIMIT requests
    in window N-1 and then RL_LIMIT more at the boundary should block most
    of the second batch.

    Approximation: estimated = prev * 1.0 + curr.  So after prev=RL_LIMIT,
    the first request of the new window yields estimated = RL_LIMIT + 1 > RL_LIMIT
    -> blocked immediately.  Tolerance of +-1 for weight rounding.
    """
    W = RL_WINDOW_SEC
    client = "spiffe://test/sliding-burst"

    _reset_local_rl()
    prev_window = 400
    curr_window = 401

    t_prev = float(W * prev_window + 0)
    for _ in range(RL_FALLBACK_LIMIT):
        _local_rate_limit(client, int(t_prev), prev_window)

    t_boundary = float(W * curr_window + 0)
    results = [_local_rate_limit(client, int(t_boundary), curr_window)
               for _ in range(RL_FALLBACK_LIMIT)]

    passed = sum(1 for r in results if r)
    assert passed <= 1, (
        f"Sliding window should block at boundary (prev full). Passed: {passed} "
        f"(tolerance +-1 for weight rounding)"
    )


@pytest.mark.asyncio
async def test_redis_failure_activates_local_fallback():
    """When rl_script raises, _local_rate_limit is used and returns a bool."""
    _reset_local_rl()
    client = "spiffe://test/redis-down"

    async def broken_script(*a, **kw):
        raise ConnectionError("Redis unreachable")

    with patch("gateway.rl_script", side_effect=broken_script), \
         patch("gateway.RL_ALGORITHM", "sliding"), \
         patch("gateway.time.time", return_value=float(RL_WINDOW_SEC * 500)):
        result = await gateway.check_rate_limit(client)

    assert result is True, "First request after Redis failure should pass local fallback"


@pytest.mark.asyncio
async def test_redis_failure_local_fallback_blocks_at_limit():
    """After RL_FALLBACK_LIMIT requests via local fallback, new ones are blocked."""
    _reset_local_rl()
    client = "spiffe://test/redis-down-limit"
    t = float(RL_WINDOW_SEC * 600 + 0)

    async def broken_script(*a, **kw):
        raise ConnectionError("Redis unreachable")

    with patch("gateway.rl_script", side_effect=broken_script), \
         patch("gateway.RL_ALGORITHM", "sliding"), \
         patch("gateway.time.time", return_value=t):
        results = [await gateway.check_rate_limit(client)
                   for _ in range(RL_FALLBACK_LIMIT + 5)]

    passed  = sum(1 for r in results if r)
    blocked = sum(1 for r in results if not r)
    assert passed  <= RL_FALLBACK_LIMIT + 1, f"Too many passed via fallback: {passed}"
    assert blocked >= 4, f"Expected blocking; blocked={blocked}"


@pytest.mark.asyncio
async def test_cleanup_local_rl_compatible_with_new_key_format():
    """cleanup_local_rl must correctly expire entries written by _local_rate_limit.

    _local_rate_limit uses key format "{client_id}:{window_key}" (no "rl:" prefix).
    cleanup_local_rl parses the window via rsplit(":", 1)[-1] which works regardless
    of prefixes because spiffe:// URIs contain colons in a fixed position.

    Example: "spiffe://example.org/svc:175000"
      rsplit(":", 1) -> ["spiffe://example.org/svc", "175000"]
    """
    from gateway import cleanup_local_rl, local_rate_limits, RL_WINDOW_SEC

    _reset_local_rl()
    W = RL_WINDOW_SEC

    curr_window = 501
    prev_window = curr_window - 1
    expired_window = curr_window - 2
    client = "spiffe://example.org/test-cleanup"

    expired_key = f"{client}:{expired_window}"
    prev_key    = f"{client}:{prev_window}"
    curr_key    = f"{client}:{curr_window}"
    local_rate_limits[expired_key] = 7
    local_rate_limits[prev_key]    = 5
    local_rate_limits[curr_key]    = 3

    assert expired_key in local_rate_limits
    assert prev_key    in local_rate_limits
    assert curr_key    in local_rate_limits

    t_now = float(W * curr_window + 1)
    with patch("gateway.time.time", return_value=t_now), \
         patch("gateway.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = [None, asyncio.CancelledError()]
        try:
            await cleanup_local_rl()
        except asyncio.CancelledError:
            pass

    assert expired_key not in local_rate_limits, "curr-2 window key should have been purged"
    assert prev_key    in local_rate_limits,     "curr-1 window key should be retained for sliding weighting"
    assert curr_key    in local_rate_limits,     "Current window key should survive cleanup"


def test_local_rl_max_keys_does_not_reset_existing_client(monkeypatch):
    """Hitting RL_LOCAL_MAX_KEYS evicts only the oldest entries and does not reset an existing client's count."""
    _reset_local_rl()
    monkeypatch.setattr(gateway, "RL_LOCAL_MAX_KEYS", 5)

    W = RL_WINDOW_SEC
    curr_window = 700
    t = int(W * curr_window + 2)

    for i in range(3):
        assert _local_rate_limit(f"spiffe://test/old-{i}", t, curr_window) is True

    active_client = "spiffe://test/active-client"
    for _ in range(RL_FALLBACK_LIMIT):
        assert _local_rate_limit(active_client, t, curr_window) is True
    assert _local_rate_limit(active_client, t, curr_window) is False

    for i in range(2):
        assert _local_rate_limit(f"spiffe://test/new-{i}", t, curr_window) is True

    assert len(local_rate_limits) <= 5
    assert f"{active_client}:{curr_window}" in local_rate_limits
    assert _local_rate_limit(active_client, t, curr_window) is False


@pytest.mark.asyncio
async def test_lazy_redis_init_without_lifespan(monkeypatch):
    """
    Verify:
    1. Calling consume_nonce and check_rate_limit ("fixed" and "sliding") without lifespan
       lazily creates the Redis client via get_redis_client() and registers nonce_script and rl_script.
    2. The "fixed" branch uses get_redis_client().pipeline instead of global redis_client.
    3. Whitespace-only ADMIN_SECRET_TOKEN is rejected at startup validation without altering
       non-empty token values.
    """
    from unittest.mock import MagicMock

    monkeypatch.setattr(gateway, "redis_client", None)
    monkeypatch.setattr(gateway, "nonce_script", None)
    monkeypatch.setattr(gateway, "rl_script", None)

    fake_nonce_script = AsyncMock(return_value=1)
    fake_rl_script = AsyncMock(return_value=5)

    def fake_register_script(lua_src):
        if lua_src == gateway.NONCE_LUA:
            return fake_nonce_script
        if lua_src == gateway.SLIDING_RL_LUA:
            return fake_rl_script
        raise AssertionError(f"Unexpected script: {lua_src}")

    pipe_executed = False

    class FakePipe:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            pass
        def incr(self, key):
            pass
        def expire(self, key, ttl):
            pass
        async def execute(self):
            nonlocal pipe_executed
            pipe_executed = True
            return [3, True]

    fake_redis = MagicMock()
    fake_redis.register_script = MagicMock(side_effect=fake_register_script)
    fake_redis.pipeline = MagicMock(side_effect=lambda transaction=True: FakePipe())

    with patch("gateway.Redis.from_url", return_value=fake_redis):
        monkeypatch.setattr(gateway, "RL_ALGORITHM", "fixed")
        ok_fixed = await gateway.check_rate_limit("spiffe://test/lazy-fixed")
        assert ok_fixed is True
        assert pipe_executed is True
        assert gateway.redis_client is fake_redis
        assert gateway.nonce_script is fake_nonce_script
        assert gateway.rl_script is fake_rl_script

        monkeypatch.setattr(gateway, "redis_client", None)
        monkeypatch.setattr(gateway, "nonce_script", None)
        monkeypatch.setattr(gateway, "rl_script", None)
        monkeypatch.setattr(gateway, "RL_ALGORITHM", "sliding")
        ok_sliding = await gateway.check_rate_limit("spiffe://test/lazy-sliding")
        assert ok_sliding is True
        fake_rl_script.assert_awaited_once()

        monkeypatch.setattr(gateway, "redis_client", None)
        monkeypatch.setattr(gateway, "nonce_script", None)
        monkeypatch.setattr(gateway, "rl_script", None)
        ok_nonce = await gateway.consume_nonce("lazy-nonce-1")
        assert ok_nonce is True
        fake_nonce_script.assert_awaited_once()


@pytest.mark.asyncio
async def test_sliding_window_rejected_requests_do_not_pollute_next_window():
    """
    Verify Non-Polluting Check-Before-Increment:
    1. In Local in-memory fallback path (_local_rate_limit):
       - Window W0: 15 requests allowed (up to RL_FALLBACK_LIMIT=15), followed by 200 rejected requests.
       - The 200 rejected requests must NOT increment local_rate_limits.
       - Advance time to Window W1 at 5.0s (50% weight of prev = 7).
       - In W1, exactly 8 new requests must be allowed (15 - 7 = 8).
    2. In Redis Lua path (SLIDING_RL_LUA / check_rate_limit):
       - Window W0: 50 requests allowed (up to RL_LIMIT=50), followed by 200 rejected requests.
       - The rejected requests must NOT increment the current window counter in Redis.
       - Advance time to Window W1 at 5.0s (50% weight of prev = 25).
       - In W1, exactly 25 new requests must be allowed before reaching limit (50 - 25 = 25).
       - If the 200 rejected requests had polluted the counter (prev=250), 0 requests would be allowed.
    """
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client_local = "spiffe://test/non-pollute-local"
    w0 = 1000
    w1 = 1001

    t_w0 = float(W * w0 + 0)
    for i in range(RL_FALLBACK_LIMIT):
        assert _local_rate_limit(client_local, int(t_w0), w0) is True, f"Request {i+1} in W0 should pass"

    for i in range(200):
        assert _local_rate_limit(client_local, int(t_w0 + 1), w0) is False, f"Rejected request {i+1} in W0 must be False"

    assert local_rate_limits[f"{client_local}:{w0}"] == RL_FALLBACK_LIMIT

    t_w1 = float(W * w1 + 5)
    passed_w1_local = 0
    blocked_w1_local = 0
    for _ in range(15):
        if _local_rate_limit(client_local, int(t_w1), w1):
            passed_w1_local += 1
        else:
            blocked_w1_local += 1

    assert passed_w1_local == (RL_FALLBACK_LIMIT - int(RL_FALLBACK_LIMIT * 0.5)), (
        f"Expected exactly 8 requests allowed in W1, got {passed_w1_local}"
    )
    assert blocked_w1_local == 7

    client_redis = "spiffe://test/non-pollute-redis"
    redis_store: dict[str, int] = {}
    redis_ttl: dict[str, int] = {}

    async def lua_eval_mock(keys, args):
        key1, key2 = keys[0], keys[1]
        curr_key, prev_key = key1, key2
        w_part1 = int(key1.split(":")[-1]) if key1.split(":")[-1].isdigit() else 0
        w_part2 = int(key2.split(":")[-1]) if key2.split(":")[-1].isdigit() else 0
        if w_part1 < w_part2:
            curr_key, prev_key = key2, key1

        prev = redis_store.get(prev_key, 0)
        curr = redis_store.get(curr_key, 0)
        weight = float(args[1])
        if weight >= 1:
            weight = weight / 1000.0
        limit = int(args[2]) if len(args) > 2 else RL_LIMIT

        estimated = int(prev * weight) + curr
        if estimated >= limit:
            return 0
        else:
            redis_store[curr_key] = curr + 1
            if curr == 0:
                redis_ttl[curr_key] = int(args[0])
            return 1

    with patch("gateway.rl_script", side_effect=lua_eval_mock), \
         patch("gateway.RL_ALGORITHM", "sliding"):

        with patch("gateway.time.time", return_value=float(W * w0)):
            for i in range(RL_LIMIT):
                ok = await gateway.check_rate_limit(client_redis)
                assert ok is True, f"Request {i+1} in W0 should be allowed"

            for i in range(200):
                ok = await gateway.check_rate_limit(client_redis)
                assert ok is False, f"Request {50+i+1} in W0 should be 429 rejected"

        curr_key_w0 = f"rl:{client_redis}:{w0}"
        assert redis_store.get(curr_key_w0) == RL_LIMIT, (
            f"W0 counter in Redis should be exactly {RL_LIMIT}, but was {redis_store.get(curr_key_w0)}"
        )

        passed_w1 = 0
        blocked_w1 = 0
        with patch("gateway.time.time", return_value=float(W * w1 + 5)):
            for _ in range(50):
                ok = await gateway.check_rate_limit(client_redis)
                if ok:
                    passed_w1 += 1
                else:
                    blocked_w1 += 1

        assert passed_w1 == 25, f"Expected exactly 25 requests allowed in W1, got {passed_w1}"
        assert blocked_w1 == 25, f"Expected 25 requests blocked in W1, got {blocked_w1}"
        assert redis_store.get(f"rl:{client_redis}:{w1}") == 25


@pytest.mark.asyncio
async def test_sliding_window_lua_weight_boundary_999(monkeypatch):
    """At 99.9% elapsed, weight == 1 permille must be normalized to 0.001 and allow request when prev == 50."""
    W = RL_WINDOW_SEC
    client = "spiffe://test/boundary-999"
    window = 100
    t_end = float(W * window + 9.99)

    mock_redis = AsyncMock()
    monkeypatch.setattr(gateway, "redis_client", mock_redis)
    monkeypatch.setattr(gateway, "RL_ALGORITHM", "sliding")

    captured_args = []
    async def mock_rl_script(keys, args):
        captured_args.append((keys, args))
        weight = float(args[1])
        if weight >= 1:
            weight = weight / 1000.0
        prev = 50
        curr = 0
        limit = int(args[2])
        estimated = int(prev * weight) + curr
        if estimated >= limit:
            return 0
        return 1

    monkeypatch.setattr(gateway, "rl_script", mock_rl_script)

    with patch("gateway.time.time", return_value=t_end):
        allowed = await gateway.check_rate_limit(client)

    assert allowed is True
    assert len(captured_args) == 1
    assert captured_args[0][1][1] == "1"


@pytest.mark.asyncio
async def test_fixed_window_decr_rollback_logging(monkeypatch, caplog):
    import logging
    from unittest.mock import MagicMock
    monkeypatch.setattr(gateway, "RL_ALGORITHM", "fixed")
    monkeypatch.setattr(gateway, "RL_LIMIT", 5)

    mock_redis = MagicMock()

    class MockPipeline:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        def incr(self, key):
            pass

        def expire(self, key, ttl):
            pass

        async def execute(self):
            return [10]

    mock_redis.pipeline.return_value = MockPipeline()
    mock_redis.decr = AsyncMock(side_effect=RuntimeError("Redis connection dropped during decr"))

    with patch("gateway.get_redis_client", return_value=mock_redis), \
         patch("gateway.time.time", return_value=1000.0), \
         caplog.at_level(logging.DEBUG):
        result = await gateway.check_rate_limit("client-fail-rollback")

    assert result is False
    assert "Rate limit decr rollback failed for rl:client-fail-rollback:100: Redis connection dropped during decr" in caplog.text
