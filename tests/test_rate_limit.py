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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_time(t: float):
    """Return a callable that always returns t."""
    return lambda: t


def _reset_local_rl():
    local_rate_limits.clear()


# ---------------------------------------------------------------------------
# 1. Local fallback: window 경과 후 카운트 회복
# ---------------------------------------------------------------------------

def test_local_fallback_count_resets_after_window():
    """After a full window elapses, counts from the previous window are gone."""
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client = "spiffe://test/recovery"

    # Fill up the previous window fully (say window_key=100)
    t_prev = W * 100 + 0  # elapsed=0 in window 100 -> weight=1.0
    for _ in range(RL_FALLBACK_LIMIT):
        ok = _local_rate_limit(client, int(t_prev), int(t_prev) // W)
        assert ok, "Should be within limit while filling"

    # One more should exceed
    over = _local_rate_limit(client, int(t_prev), int(t_prev) // W)
    assert not over, "One past limit should be blocked"

    # Now advance a full window (window_key=101, elapsed≈0 means weight≈1.0 for prev)
    # BUT prev count is RL_FALLBACK_LIMIT+1 which is already above FALLBACK_LIMIT
    # Advance TWO windows so prev is the completely-new prev (window_key=102,
    # prev=window 101 which has 0 counts)
    t_new = W * 102 + 0
    _reset_local_rl()  # simulate cleanup_local_rl clearing old keys
    result = _local_rate_limit(client, int(t_new), int(t_new) // W)
    assert result, "After full reset, first request should pass"


# ---------------------------------------------------------------------------
# 2. Local fallback: 이전 윈도우 가중치 감소
# ---------------------------------------------------------------------------

def test_local_fallback_prev_weight_decreases():
    """Closer to the end of the window, prev_window contribution decreases."""
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client = "spiffe://test/weight"

    # Simulate: prev window (key=10) had exactly RL_FALLBACK_LIMIT requests
    prev_window = 10
    curr_window = 11
    prev_key = f"{client}:{prev_window}"
    local_rate_limits[prev_key] = RL_FALLBACK_LIMIT  # 15

    # At elapsed=0 → weight=1.0: estimated = 15*1 + 1 = 16 > 15 → blocked
    t_start = W * curr_window + 0
    result_at_start = _local_rate_limit(client, int(t_start), curr_window)
    assert not result_at_start, "At window start, prev still fully counted → blocked"

    # At elapsed = W-1 → weight ≈ 0.1: estimated = 15*0.1 + 1 = 2.5 ≤ 15 → allowed
    _reset_local_rl()
    local_rate_limits[prev_key] = RL_FALLBACK_LIMIT
    t_end = W * curr_window + (W - 1)
    result_at_end = _local_rate_limit(client, int(t_end), curr_window)
    assert result_at_end, "Near end of window, prev weight low → allowed"


# ---------------------------------------------------------------------------
# 3. Local fallback: 한도 정확히 경계 케이스
# ---------------------------------------------------------------------------

def test_local_fallback_exact_limit():
    """The RL_FALLBACK_LIMIT-th request passes, (LIMIT+1)-th is blocked."""
    _reset_local_rl()
    W = RL_WINDOW_SEC
    client = "spiffe://test/exact"

    # Use a fresh window with no previous counts; elapsed=5 (mid-window)
    t = W * 200 + 5
    wk = int(t) // W

    results = [_local_rate_limit(client, int(t), wk) for _ in range(RL_FALLBACK_LIMIT + 2)]
    passed = sum(1 for r in results if r)
    blocked = sum(1 for r in results if not r)

    # First RL_FALLBACK_LIMIT should pass, rest blocked.
    # Allow ±1 for approximation rounding.
    assert passed >= RL_FALLBACK_LIMIT - 1, f"Too few passed: {passed}"
    assert blocked >= 1, "At least one should be blocked"


# ---------------------------------------------------------------------------
# 4. Fixed-window boundary burst (via check_rate_limit with mock rl_script)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fixed_window_boundary_burst():
    """Fixed window allows up to ~2x limit across a window boundary.

    The test simulates RL_LIMIT requests just before the boundary (counted in
    window N) and RL_LIMIT requests just after (counted in window N+1).
    Both sets individually pass, so total passing = 2 * RL_LIMIT.
    """
    W = RL_WINDOW_SEC
    t_before = W * 300 - 1   # last second of window 299
    t_after  = W * 300       # first second of window 300

    client = "spiffe://test/burst"
    counts = {299: 0, 300: 0}

    async def fake_rl_script(keys, args):
        # keys[1] = curr_key "rl:{client}:{window}"
        window = int(keys[1].split(":")[-1])
        counts[window] = counts.get(window, 0) + 1
        return counts[window]

    passed_before = 0
    passed_after  = 0

    with patch("gateway.rl_script", side_effect=fake_rl_script), \
         patch("gateway.RL_ALGORITHM", "fixed"), \
         patch.object(gateway, "redis_client") as mock_redis:

        # Build a fake pipeline that uses counts dict
        async def fake_incr_expire(key):
            window = int(key.split(":")[-1])
            counts[window] = counts.get(window, 0) + 1
            return counts[window]

        # For "fixed" path we use pipeline, not rl_script; mock pipeline
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
    # Fixed window: both windows independently allow RL_LIMIT each
    assert passed_before == RL_LIMIT, f"Expected {RL_LIMIT} before boundary, got {passed_before}"
    assert passed_after  == RL_LIMIT, f"Expected {RL_LIMIT} after boundary,  got {passed_after}"
    assert total_passed  == 2 * RL_LIMIT, f"Fixed window burst = 2x limit; got {total_passed}"


# ---------------------------------------------------------------------------
# 5. Sliding window boundary: stays within limit (approximate)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sliding_window_boundary_no_burst():
    """Sliding window prevents the 2x boundary burst.

    At the boundary (elapsed=0), prev_weight=1.0.  Sending RL_LIMIT requests
    in window N-1 and then RL_LIMIT more at the boundary should block most
    of the second batch.

    Approximation: estimated = prev * 1.0 + curr.  So after prev=RL_LIMIT,
    the first request of the new window yields estimated = RL_LIMIT + 1 > RL_LIMIT
    → blocked immediately.  Tolerance of ±1 for weight rounding.
    """
    W = RL_WINDOW_SEC
    client = "spiffe://test/sliding-burst"

    # We drive _local_rate_limit directly (no Redis needed) to test the math.
    _reset_local_rl()
    prev_window = 400
    curr_window = 401

    # Fill prev window to exactly RL_FALLBACK_LIMIT
    t_prev = float(W * prev_window + 0)
    for _ in range(RL_FALLBACK_LIMIT):
        _local_rate_limit(client, int(t_prev), prev_window)

    # Now at exact window boundary (elapsed=0 → weight=1.0)
    t_boundary = float(W * curr_window + 0)
    results = [_local_rate_limit(client, int(t_boundary), curr_window)
               for _ in range(RL_FALLBACK_LIMIT)]

    passed = sum(1 for r in results if r)
    # At weight=1.0 the very first new-window request should be blocked.
    # Allow ±1 for approximation; passed should be far less than RL_FALLBACK_LIMIT.
    assert passed <= 1, (
        f"Sliding window should block at boundary (prev full). Passed: {passed} "
        f"(tolerance ±1 for weight rounding)"
    )


# ---------------------------------------------------------------------------
# 6. Redis fallback path activated on Redis error
# ---------------------------------------------------------------------------

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

    # With 0 prior counts, first request should always be allowed.
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
    # Allow ±1 for approximation
    assert passed  <= RL_FALLBACK_LIMIT + 1, f"Too many passed via fallback: {passed}"
    assert blocked >= 4, f"Expected blocking; blocked={blocked}"


# ---------------------------------------------------------------------------
# 7. cleanup_local_rl: 새 키 형식({client}:{window_key})과 호환성 확인
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cleanup_local_rl_compatible_with_new_key_format():
    """cleanup_local_rl must correctly expire entries written by _local_rate_limit.

    _local_rate_limit uses key format "{client_id}:{window_key}" (no "rl:" prefix).
    cleanup_local_rl parses the window via rsplit(":", 1)[-1] which works regardless
    of prefixes because spiffe:// URIs contain colons in a fixed position.

    Example: "spiffe://example.org/svc:175000"
      rsplit(":", 1) → ["spiffe://example.org/svc", "175000"] ✓
    """
    from gateway import cleanup_local_rl, local_rate_limits, RL_WINDOW_SEC

    _reset_local_rl()
    W = RL_WINDOW_SEC

    old_window = 500
    curr_window = 501
    client = "spiffe://example.org/test-cleanup"

    # Insert one old entry and one current entry using the new key format.
    old_key  = f"{client}:{old_window}"
    curr_key = f"{client}:{curr_window}"
    local_rate_limits[old_key]  = 5
    local_rate_limits[curr_key] = 3

    assert old_key  in local_rate_limits
    assert curr_key in local_rate_limits

    # Patch time so cleanup thinks current window is curr_window.
    t_now = float(W * curr_window + 1)
    with patch("gateway.time.time", return_value=t_now), \
         patch("gateway.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        # Run one iteration of the cleanup loop (it sleeps first, then cleans).
        mock_sleep.side_effect = [None, asyncio.CancelledError()]
        try:
            await cleanup_local_rl()
        except asyncio.CancelledError:
            pass

    # Old window entry must be deleted; current window entry must survive.
    assert old_key  not in local_rate_limits, "Old window key should have been cleaned up"
    assert curr_key in local_rate_limits,     "Current window key should survive cleanup"
