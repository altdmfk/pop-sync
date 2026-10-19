import argparse
import asyncio
import base64
import binascii
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import hashlib
import hmac
import logging
import os
import threading
import time
from typing import Any, Optional, Tuple

from cryptography.exceptions import InvalidSignature, UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx
from redis.asyncio import Redis
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration helpers
def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))

def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))

# Redis timeouts
REDIS_TIMEOUT_SEC            = _float("REDIS_TIMEOUT_SEC", 0.2)
STREAM_SOCKET_TIMEOUT        = _float("STREAM_SOCKET_TIMEOUT", 5.0)
REDIS_DOWN_SOCKET_TIMEOUT    = _float("REDIS_DOWN_SOCKET_TIMEOUT", 0.1)

# Stream synchronization
STREAM_NAME                  = os.getenv("STREAM_NAME", "invalidation_stream")
STREAM_CATCHUP_COUNT         = _int("STREAM_CATCHUP_COUNT", 100)
STREAM_NORMAL_COUNT          = _int("STREAM_NORMAL_COUNT", 10)
STREAM_BLOCK_MS              = _int("STREAM_BLOCK_MS", 500)
PARTITION_SLEEP_SEC          = _float("PARTITION_SLEEP_SEC", 0.5)

# Rate limiting
RL_WINDOW_SEC                = _int("RL_WINDOW_SEC", 10)
RL_LIMIT                     = _int("RL_LIMIT", 50)
RL_FALLBACK_LIMIT            = _int("RL_FALLBACK_LIMIT", 15)
RL_LOCAL_MAX_KEYS            = _int("RL_LOCAL_MAX_KEYS", 10000)
# RL_ALGORITHM: "sliding" (weighted-counter approximation) or "fixed" (pipeline INCR)
RL_ALGORITHM                 = os.getenv("RL_ALGORITHM", "sliding").lower()

# Proof of Possession (Scope B)
POP_VERIFY_ENABLED           = os.getenv("POP_VERIFY_ENABLED", "false").lower() == "true"
POP_EXECUTOR_WORKERS         = _int("POP_EXECUTOR_WORKERS", 4)

# Replay window and cache TTLs
NONCE_TTL_SEC                = _int("NONCE_TTL_SEC", 60)
L2_CACHE_TTL_SEC             = _int("L2_CACHE_TTL_SEC", 60)

# L1 cache configuration
L1_MAX_SIZE                  = _int("L1_MAX_SIZE", 1000)
L1_TTL_SEC                   = _int("L1_TTL_SEC", 60)

# Rate limiting modifiers
RL_EXPIRE_MULTIPLIER         = _int("RL_EXPIRE_MULTIPLIER", 2)

# Circuit breaker thresholds
CB_MAX_FAILURES              = _int("CB_MAX_FAILURES", 3)
CB_RESET_TIMEOUT_SEC         = _int("CB_RESET_TIMEOUT_SEC", 10)
CB_DEBOUNCE_SEC              = _float("CB_DEBOUNCE_SEC", 2.0)
CB_FORCE_STATE_MARGIN_SEC    = _float("CB_FORCE_STATE_MARGIN_SEC", 1.0)
VALID_CB_STATES              = frozenset(["OPEN", "CLOSED", "HALF_OPEN"])

# DB timeouts and connection pool limits
DB_TIMEOUT_SEC               = _float("DB_TIMEOUT_SEC", 1.0)
DB_SEMAPHORE_ACQUIRE_TIMEOUT = _float("DB_SEMAPHORE_ACQUIRE_TIMEOUT", 0.1)
MAX_DB_WORKERS               = _int("MAX_DB_WORKERS", 5)

node_name = os.getenv("NODE_NAME", "Node-X")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
db_url    = os.getenv("DB_URL",    "http://localhost:8000")
DB_MODE   = os.getenv("DB_MODE",   "sync")
SINGLEFLIGHT_ENABLED = os.getenv("SINGLEFLIGHT_ENABLED", "true").lower() == "true"

STREAM_GROUP = f"gateway_group_{node_name}"

db_executor: Optional[ThreadPoolExecutor] = None
db_semaphore: Optional[asyncio.Semaphore] = None
pop_executor: Optional[ThreadPoolExecutor] = None

class DBOverloadError(Exception):
    pass

redis_client: Optional[Redis] = None
stream_redis_client: Optional[Redis] = None
nonce_script = None
rl_script    = None
sync_http_client: Optional[httpx.Client] = None
async_db_client: Optional[httpx.AsyncClient] = None

def get_async_db_client() -> httpx.AsyncClient:
    global async_db_client
    if async_db_client is None or (isinstance(async_db_client, httpx.AsyncClient) and async_db_client.is_closed):
        async_db_client = httpx.AsyncClient(
            timeout=DB_TIMEOUT_SEC,
            limits=httpx.Limits(max_connections=MAX_DB_WORKERS, max_keepalive_connections=MAX_DB_WORKERS),
        )
    return async_db_client

def get_sync_http_client() -> httpx.Client:
    global sync_http_client
    if sync_http_client is None or (isinstance(sync_http_client, httpx.Client) and sync_http_client.is_closed):
        sync_http_client = httpx.Client(
            timeout=DB_TIMEOUT_SEC,
            limits=httpx.Limits(max_connections=MAX_DB_WORKERS, max_keepalive_connections=MAX_DB_WORKERS),
        )
    return sync_http_client

def get_redis_client() -> Redis:
    global redis_client, nonce_script, rl_script
    if redis_client is None:
        redis_client = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
        nonce_script = redis_client.register_script(NONCE_LUA)
        rl_script    = redis_client.register_script(SLIDING_RL_LUA)
    return redis_client

def get_pop_executor() -> ThreadPoolExecutor:
    global pop_executor
    if pop_executor is None:
        pop_executor = ThreadPoolExecutor(max_workers=POP_EXECUTOR_WORKERS)
    return pop_executor

def get_db_executor() -> ThreadPoolExecutor:
    global db_executor
    if db_executor is None:
        db_executor = ThreadPoolExecutor(max_workers=MAX_DB_WORKERS)
    return db_executor

def get_db_semaphore() -> asyncio.Semaphore:
    global db_semaphore
    if db_semaphore is None:
        db_semaphore = asyncio.Semaphore(MAX_DB_WORKERS)
    return db_semaphore

class L1Cache:
    """Thread-safe LRU cache with lazy TTL expiration.
    
    Lazy expiration avoids background cleanup threads and GIL contention:
    expired entries are pruned upon access or when capacity limits force eviction.
    """
    def __init__(self, max_size: int = L1_MAX_SIZE, ttl: int = L1_TTL_SEC):
        self.cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self.cache:
                val, expires_at = self.cache[key]
                if time.time() < expires_at:
                    self.cache.move_to_end(key)
                    return val
                del self.cache[key]
            return None

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        effective_ttl = ttl if ttl is not None else self.ttl
        with self._lock:
            if key in self.cache:
                del self.cache[key]
            elif self.max_size > 0 and len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
            if self.max_size > 0:
                self.cache[key] = (value, time.time() + effective_ttl)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self.cache.clear()

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def __len__(self) -> int:
        with self._lock:
            return len(self.cache)

l1_cache = L1Cache(max_size=L1_MAX_SIZE, ttl=L1_TTL_SEC)
is_partitioned = False
partition_lock = asyncio.Lock()
redis_swap_lock = asyncio.Lock()

class CircuitBreaker:
    """Thread-safe three-state Circuit Breaker (CLOSED -> OPEN -> HALF_OPEN -> CLOSED).
    
    Protects downstream Policy DB from cascading failures under sustained outage.
    Incorporates a 2.0s debounce window to prevent flapping caused by delayed in-flight
    timeouts arriving after state transitions.
    """
    def __init__(
        self,
        max_failures: int = CB_MAX_FAILURES,
        reset_timeout: float = CB_RESET_TIMEOUT_SEC,
        debounce_sec: float = CB_DEBOUNCE_SEC,
    ):
        self.state: str = "CLOSED"
        self.failures: int = 0
        self.max_failures: int = max_failures
        self.reset_timeout: float = reset_timeout
        self.debounce_sec: float = debounce_sec
        self.last_failure_time: float = 0.0
        self.last_transition_time: float = time.time()
        self._lock = threading.Lock()

    @property
    def current_state(self) -> str:
        with self._lock:
            return self.state

    def check_and_half_open(self) -> str:
        with self._lock:
            if self.state == "OPEN" and time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "HALF_OPEN"
                self.last_transition_time = time.time()
            return self.state

    def reset(self) -> str:
        with self._lock:
            self.failures = 0
            if self.state != "CLOSED":
                self.state = "CLOSED"
                self.last_transition_time = time.time()
            self.last_failure_time = 0.0
            return self.state

    def record_failure(self) -> Tuple[None, str]:
        with self._lock:
            if self.state == "HALF_OPEN":
                self.failures += 1
                self.state = "OPEN"
                self.last_transition_time = time.time()
                self.last_failure_time = time.time()
                return (None, self.state)

            if time.time() - self.last_transition_time < self.debounce_sec:
                return (None, self.state)

            self.failures += 1
            if self.failures >= self.max_failures:
                if self.state != "OPEN":
                    self.state = "OPEN"
                    self.last_transition_time = time.time()
                    self.last_failure_time = time.time()
            return (None, self.state)

    def record_success(self) -> Tuple[None, str]:
        with self._lock:
            self.failures = 0
            if self.state != "CLOSED":
                self.state = "CLOSED"
                self.last_transition_time = time.time()
            return (None, self.state)

    def can_execute(self) -> Tuple[bool, str]:
        with self._lock:
            if self.state == "CLOSED":
                return (True, self.state)
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "HALF_OPEN"
                    self.last_transition_time = time.time()
                    return (True, self.state)
                return (False, self.state)
            if self.state == "HALF_OPEN":
                return (True, self.state)
            return (False, self.state)

    def force_state(self, state: str) -> str:
        if state not in VALID_CB_STATES:
            raise ValueError(f"Invalid state: {state}")
        with self._lock:
            self.state = state
            self.last_transition_time = time.time() - self.debounce_sec - CB_FORCE_STATE_MARGIN_SEC
            if state == "OPEN":
                self.failures = self.max_failures
                self.last_failure_time = time.time()
            elif state == "HALF_OPEN":
                self.failures = self.max_failures
                self.last_failure_time = time.time() - self.reset_timeout - CB_FORCE_STATE_MARGIN_SEC
            elif state == "CLOSED":
                self.failures = 0
                self.last_failure_time = 0.0
            return self.state

cb = CircuitBreaker()

stats_lock = threading.Lock()
stats = {
    "l1_hits": 0,
    "l2_hits": 0,
    "db_fallbacks": 0,
    "requests": 0,
    "rate_limited": 0,
    "nonce_replays": 0,
    "pop_rejected": 0,
    "pop_verified": 0,
    "latency_sum": 0.0,
    "circuit_state": "CLOSED",
    "sync_lag": 0,
    "db_queries_executed": 0,
    "singleflight_coalesced": 0,
    "circuit_blocked": 0,
    "l1_double_check_hits": 0,
    "redis_outage_blocked": 0
}

NONCE_LUA = """
local key = KEYS[1]
local value = ARGV[1]
local ttl = ARGV[2]
if redis.call("EXISTS", key) == 1 then
    return 0
else
    redis.call("SET", key, value, "EX", ttl)
    return 1
end
"""

# Lua sliding-window rate-limit script.
# KEYS[1] = prev window key  "rl:{client_id}:{window-1}"
# KEYS[2] = curr window key  "rl:{client_id}:{window}"
# ARGV[1] = TTL seconds (integer)
# ARGV[2] = weight*1000 (integer) = floor((1 - elapsed/W) * 1000)
# Returns estimated count (integer). Caller compares to RL_LIMIT.
# prev_key TTL is NOT reset here: it was set when that window was created
# (by a prior INCR call) and will expire naturally. Resetting it here would
# extend its lifetime by up to W seconds, causing stale counts.
SLIDING_RL_LUA = """
local prev = tonumber(redis.call('GET', KEYS[1])) or 0
local curr = redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[1])
local weight = tonumber(ARGV[2])
local estimated = math.floor(prev * weight / 1000) + curr
return estimated
"""

local_rate_limits = OrderedDict()
local_rl_lock = asyncio.Lock()
in_flight: dict[str, asyncio.Future] = {}
background_tasks: set[asyncio.Task] = set()

async def cleanup_local_rl():
    while True:
        await asyncio.sleep(RL_WINDOW_SEC)
        now_window = int(time.time()) // RL_WINDOW_SEC
        async with local_rl_lock:
            to_delete = []
            for k in local_rate_limits.keys():
                try:
                    w = int(k.rsplit(":", 1)[-1])
                    if w < now_window - 1:
                        to_delete.append(k)
                except ValueError:
                    continue
            for k in to_delete:
                del local_rate_limits[k]

async def check_rate_limit(client_id: str) -> bool:
    """Return True if request is within rate limit, False if it should be 429-ed.

    Redis path (normal): uses RL_ALGORITHM ("sliding" or "fixed").
    Local fallback path (Redis down): always uses sliding approximation with
    RL_FALLBACK_LIMIT as the threshold.  Errors from this path are re-raised so
    the caller can decide (currently: 503 for Redis failure, 429 for RL exceeded).
    """
    now = int(time.time())
    window_key = now // RL_WINDOW_SEC

    try:
        if RL_ALGORITHM == "sliding":
            if rl_script is None:
                get_redis_client()
            elapsed    = now % RL_WINDOW_SEC
            weight     = int((1 - elapsed / RL_WINDOW_SEC) * 1000)  # 0..1000
            prev_key   = f"rl:{client_id}:{window_key - 1}"
            curr_key   = f"rl:{client_id}:{window_key}"
            estimated  = await rl_script(
                keys=[prev_key, curr_key],
                args=[str(RL_WINDOW_SEC * RL_EXPIRE_MULTIPLIER), str(weight)],
            )
            return int(estimated) <= RL_LIMIT
        else:  # "fixed"
            key = f"rl:{client_id}:{window_key}"
            async with get_redis_client().pipeline(transaction=True) as pipe:
                pipe.incr(key)
                pipe.expire(key, RL_WINDOW_SEC * RL_EXPIRE_MULTIPLIER)
                results = await pipe.execute()
            return results[0] <= RL_LIMIT

    except Exception:
        # Redis unavailable — fall back to per-node in-memory sliding counter.
        # Note: subsequent nonce check (consume_nonce) also needs Redis and will
        # return 503 for RL-passing requests. Fallback still provides meaningful
        # 429 responses to clients that exceed RL_FALLBACK_LIMIT.
        return _local_rate_limit(client_id, now, window_key)


def _local_rate_limit(client_id: str, now: int, window_key: int) -> bool:
    """In-memory sliding-window fallback used when Redis is unavailable.

    Uses two integer buckets (prev/curr) per client.
    Threshold: RL_FALLBACK_LIMIT (default 15, i.e. per-node).

    Thread-safety: this is an await-free synchronous function called only from
    check_rate_limit(), which runs exclusively on the asyncio event-loop thread.
    dict reads and writes are therefore non-concurrent and require no additional lock.
    """
    # Key format: "{client_id}:{window_key}" (no "rl:" prefix to avoid confusion).
    prev_key  = f"{client_id}:{window_key - 1}"
    curr_key  = f"{client_id}:{window_key}"

    prev_count = local_rate_limits.get(prev_key, 0)
    if prev_key in local_rate_limits:
        local_rate_limits.move_to_end(prev_key)
    curr_count = local_rate_limits.get(curr_key, 0) + 1
    if curr_key in local_rate_limits:
        local_rate_limits.move_to_end(curr_key)
    else:
        # OOM guard: evict oldest entries when too many keys accumulated.
        while len(local_rate_limits) >= RL_LOCAL_MAX_KEYS:
            local_rate_limits.popitem(last=False)
    local_rate_limits[curr_key] = curr_count

    elapsed   = now % RL_WINDOW_SEC
    weight    = 1.0 - (elapsed / RL_WINDOW_SEC)
    estimated = prev_count * weight + curr_count
    return estimated <= RL_FALLBACK_LIMIT

async def consume_nonce(nonce: str, ttl: int = NONCE_TTL_SEC, act: str = "read") -> bool:
    if nonce_script is None:
        get_redis_client()
    key = f"nonce:{nonce}"
    result = await nonce_script(keys=[key], args=["1", str(ttl)])
    return result == 1

def generate_composite_key(sid: str, uid: str, res: str, act: str) -> str:
    sid = sid.strip()
    uid = uid.strip()
    res = res.strip()
    act = act.strip().upper()
    payload = f"{len(sid)}:{sid}{len(uid)}:{uid}{len(res)}:{res}{len(act)}:{act}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

def db_sync_call(key: str, sid: str, res: str):
    client = get_sync_http_client()
    resp = client.get(f"{db_url}/policy/{key}", params={"sid": sid, "res": res}, timeout=DB_TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json()

async def db_async_call(key: str, sid: str, res: str):
    client = get_async_db_client()
    resp = await client.get(f"{db_url}/policy/{key}", params={"sid": sid, "res": res}, timeout=DB_TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json()

async def db_loader_task(comp_key: str, sid: str, res: str, fut: asyncio.Future, use_singleflight: bool = True):
    try:
        if use_singleflight:
            cached_policy = l1_cache.get(comp_key)
            if cached_policy is not None:
                with stats_lock:
                    stats["l1_double_check_hits"] += 1
                if not fut.done():
                    fut.set_result({
                        "status": "deny" if cached_policy == "DENY" else "allow",
                        "source": "L1",
                        "policy": cached_policy,
                    })
                return

        with stats_lock:
            stats["db_queries_executed"] += 1

        if DB_MODE == "async":
            db_resp = await db_async_call(comp_key, sid, res)
        else:
            sem = get_db_semaphore()
            executor = get_db_executor()
            try:
                await asyncio.wait_for(sem.acquire(), timeout=DB_SEMAPHORE_ACQUIRE_TIMEOUT)
            except asyncio.TimeoutError:
                raise DBOverloadError("DB concurrency limit reached (Fast-Fail)")
            try:
                loop = asyncio.get_running_loop()
                db_resp = await loop.run_in_executor(executor, db_sync_call, comp_key, sid, res)
            finally:
                sem.release()

        cb.record_success()

        db_status = db_resp.get("status")
        policy = "DENY" if db_status == "deny" else db_resp.get("role")

        l1_cache.set(comp_key, policy)
        try:
            client = get_redis_client()
            await client.set(f"policy:{comp_key}", policy, ex=L2_CACHE_TTL_SEC)
        except Exception as e:
            logging.warning(f"Failed to populate L2 policy cache: {e}")

        if not fut.done():
            fut.set_result({
                "status": "deny" if db_status == "deny" else "allow",
                "source": "DB",
                "policy": policy,
            })
    except BaseException as e:
        if not fut.done():
            if isinstance(e, asyncio.CancelledError):
                fut.cancel()
            else:
                fut.set_exception(e)
                # Suppress unretrieved future exception warnings when leader task was cancelled
                try:
                    fut.exception()
                except BaseException:
                    pass
        if not isinstance(e, (DBOverloadError, asyncio.CancelledError)):
            cb.record_failure()
        if isinstance(e, asyncio.CancelledError):
            raise
    finally:
        if in_flight.get(comp_key) is fut:
            in_flight.pop(comp_key, None)

async def _process_stream_messages(messages, active_stream_client, active_redis_client):
    for msg_id, msg_data in messages:
        key_bytes = msg_data.get(b"key")
        if not key_bytes:
            logging.warning(f"Malformed message skipped: {msg_data}")
            await active_stream_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
            continue
        key = key_bytes.decode()
        l1_cache.delete(key)
        try:
            await active_redis_client.delete(f"policy:{key}")
        except Exception as e:
            logging.warning(f"Failed to delete L2 cache: {e}")
        await active_stream_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)

async def stream_listener():
    try:
        client = stream_redis_client or get_redis_client()
        await client.xgroup_create(STREAM_NAME, STREAM_GROUP, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            logging.error(f"Failed to create stream group: {e}")
            raise

    was_partitioned = False
    while True:
        try:
            async with partition_lock:
                current_partitioned = is_partitioned
            if current_partitioned:
                was_partitioned = True
                await asyncio.sleep(PARTITION_SLEEP_SEC)
                continue

            active_stream_client = stream_redis_client or get_redis_client()
            active_redis_client = redis_client or get_redis_client()

            if was_partitioned:
                # Partition resolution phase: drain unacknowledged entries from the Pending Entries List (PEL)
                # using ID '0' before processing new stream messages with '>', ensuring strict invalidation ordering.
                start_time = time.time()
                total_pending = 0

                while True:
                    pending = await active_stream_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: "0"}, count=STREAM_CATCHUP_COUNT)
                    if not pending or not pending[0][1]:
                        break
                    messages = pending[0][1]
                    total_pending += len(messages)
                    await _process_stream_messages(messages, active_stream_client, active_redis_client)

                # Catch up on messages accumulated during partition that were never delivered to this consumer group
                while True:
                    new_msgs = await active_stream_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: ">"}, count=STREAM_CATCHUP_COUNT)
                    if not new_msgs or not new_msgs[0][1]:
                        break
                    messages = new_msgs[0][1]
                    total_pending += len(messages)
                    await _process_stream_messages(messages, active_stream_client, active_redis_client)

                catchup_duration = int((time.time() - start_time) * 1000)
                with stats_lock:
                    stats["last_catchup"] = {
                        "pending_count": total_pending,
                        "duration_ms": catchup_duration,
                        "timestamp": time.time()
                    }
                was_partitioned = False
                continue

            # Steady-state consumer loop: prioritize outstanding PEL entries before blocking on new events
            pending = await active_stream_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: "0"}, count=STREAM_NORMAL_COUNT)
            if pending and pending[0][1]:
                messages = pending[0][1]
            else:
                messages_data = await active_stream_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: ">"}, count=STREAM_NORMAL_COUNT, block=STREAM_BLOCK_MS)
                if not messages_data:
                    continue

                async with partition_lock:
                    current_partitioned = is_partitioned
                if current_partitioned:
                    continue

                messages = messages_data[0][1]

            await _process_stream_messages(messages, active_stream_client, active_redis_client)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Stream listener error: {e}")
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, stream_redis_client, sync_http_client, async_db_client
    global db_semaphore, db_executor, nonce_script, rl_script, pop_executor

    db_semaphore = asyncio.Semaphore(MAX_DB_WORKERS)
    db_executor = ThreadPoolExecutor(max_workers=MAX_DB_WORKERS)
    pop_executor = ThreadPoolExecutor(max_workers=POP_EXECUTOR_WORKERS)
    redis_client = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
    nonce_script = redis_client.register_script(NONCE_LUA)
    rl_script    = redis_client.register_script(SLIDING_RL_LUA)
    stream_redis_client = Redis.from_url(redis_url, socket_timeout=STREAM_SOCKET_TIMEOUT)
    sync_http_client = httpx.Client(
        timeout=DB_TIMEOUT_SEC,
        limits=httpx.Limits(max_connections=MAX_DB_WORKERS, max_keepalive_connections=MAX_DB_WORKERS),
    )
    async_db_client = httpx.AsyncClient(
        timeout=DB_TIMEOUT_SEC,
        limits=httpx.Limits(max_connections=MAX_DB_WORKERS, max_keepalive_connections=MAX_DB_WORKERS),
    )
    task = asyncio.create_task(stream_listener())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    task_cleanup = asyncio.create_task(cleanup_local_rl())
    background_tasks.add(task_cleanup)
    task_cleanup.add_done_callback(background_tasks.discard)
    tasks = [task, task_cleanup]
    yield
    task.cancel()
    task_cleanup.cancel()
    await asyncio.gather(*tasks, *background_tasks, return_exceptions=True)
    if sync_http_client is not None and not sync_http_client.is_closed:
        sync_http_client.close()
    if async_db_client is not None and not async_db_client.is_closed:
        await async_db_client.aclose()
    if db_executor is not None:
        db_executor.shutdown(wait=False, cancel_futures=True)
    if pop_executor is not None:
        pop_executor.shutdown(wait=False, cancel_futures=True)
    if redis_client is not None:
        await redis_client.close()
    if stream_redis_client is not None:
        await stream_redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.exception_handler(HTTPException)
async def unified_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": exc.detail}
    )

ADMIN_SECRET_TOKEN = os.environ.get("ADMIN_SECRET_TOKEN")
if not ADMIN_SECRET_TOKEN or not ADMIN_SECRET_TOKEN.strip():
    raise RuntimeError("ADMIN_SECRET_TOKEN environment variable is required")

@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path == "/health":
        return await call_next(request)
    if path == "/metrics" or path.startswith("/chaos/"):
        admin_token = request.headers.get("X-Admin-Token")
        # Constant-time comparison to prevent timing attacks against administrative secret
        if not admin_token or not hmac.compare_digest(admin_token, ADMIN_SECRET_TOKEN):
            return JSONResponse(status_code=401, content={"status": "error", "message": "Unauthorized Admin Access"})
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return JSONResponse(status_code=401, content={"status": "error", "message": "Missing Authorization Header"})

    return await call_next(request)

@app.middleware("http")
async def measure_latency(request: Request, call_next):
    async with partition_lock:
        current_partitioned = is_partitioned
    if current_partitioned and not (request.url.path == "/health" or request.url.path.startswith("/chaos/partition") or request.url.path.startswith("/metrics") or request.url.path.startswith("/chaos/redis") or request.url.path.startswith("/chaos/cb/force/") or request.url.path.startswith("/chaos/invalidate/")):
        return JSONResponse(status_code=503, content={"status": "error", "message": "Node Isolated"})

    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    if request.url.path == "/auth":
        with stats_lock:
            stats["requests"] += 1
            stats["latency_sum"] += duration
    return response

def generate_canonical_payload(method: str, path: str, spiffe_id: str, nonce: str) -> bytes:
    # Scope B: Simplified request digest ECDSA signature
    # Omitting Host, Query, Timestamp, Body (since /auth is GET and uses single-use nonce)
    canonical_str = f"{method.upper()}\n{path}\n{spiffe_id}\n{nonce}"
    return canonical_str.encode("utf-8")

def verify_pop_signature(public_key_pem: str, signature: bytes, canonical_payload: bytes) -> bool:
    try:
        public_key = load_pem_public_key(public_key_pem.encode("utf-8"))
        if not isinstance(public_key, ec.EllipticCurvePublicKey):
            return False

        if len(signature) == 64:
            r = int.from_bytes(signature[:32], byteorder="big")
            s = int.from_bytes(signature[32:], byteorder="big")
            signature = encode_dss_signature(r, s)

        public_key.verify(
            signature,
            canonical_payload,
            ec.ECDSA(hashes.SHA256())
        )
        return True
    except (ValueError, TypeError, UnsupportedAlgorithm, InvalidSignature):
        return False
    except Exception as e:
        logging.warning(f"Signature verify error: {e}")
        return False

@app.get("/auth")
async def auth(
    request: Request,
    authorization: str = Header(None),
    x_nonce: str = Header(None),
    x_spiffe_id: str = Header(None),
    x_res: str = Header("resource"),
    x_act: str = Header("read"),
    x_pop_signature: str = Header(None)
):
    if not authorization or not x_nonce or not x_spiffe_id:
        raise HTTPException(status_code=401, detail="Missing Headers")

    try:
        allowed = await check_rate_limit(x_spiffe_id)
        if not allowed:
            with stats_lock:
                stats["rate_limited"] += 1
            return JSONResponse(status_code=429, headers={"Retry-After": str(RL_WINDOW_SEC)}, content={"status": "error", "message": "Too Many Requests"})
    except Exception as e:
        logging.warning(f"Rate limit check failed (Redis down?): {e}")
        with stats_lock:
            stats["redis_outage_blocked"] += 1
        raise HTTPException(status_code=503, detail="Service Unavailable - Rate Limit Failed")

    # Nonce verification runs BEFORE PoP ECDSA signature verification to burn attacker nonces atomically in Redis,
    # preventing signature verification CPU exhaustion (DoS) with replayed/forged nonces.
    try:
        nonce_ok = await consume_nonce(x_nonce, act=x_act)
    except Exception as e:
        logging.warning(f"Nonce check failed (Redis down?): {e}")
        with stats_lock:
            stats["redis_outage_blocked"] += 1
        raise HTTPException(status_code=503, detail="Service Unavailable - Nonce Check Failed")

    if not nonce_ok:
        with stats_lock:
            stats["nonce_replays"] += 1
        raise HTTPException(status_code=401, detail="Replay Detected")

    if POP_VERIFY_ENABLED:
        if not x_pop_signature:
            with stats_lock:
                stats["pop_rejected"] += 1
            raise HTTPException(status_code=400, detail="Missing PoP Signature")

        pubkey_key = f"pubkey:{x_spiffe_id}"
        pubkey_pem = l1_cache.get(pubkey_key)

        if not pubkey_pem:
            try:
                r_client = get_redis_client()
                l2_val = await r_client.get(f"policy:{pubkey_key}")
                if l2_val:
                    pubkey_pem = l2_val.decode()
                    l1_cache.set(pubkey_key, pubkey_pem)
            except Exception as e:
                logging.debug(f"L2 pubkey cache read bypassed: {e}")

        if not pubkey_pem:
            can_exec, current_state = cb.can_execute()
            if not can_exec:
                with stats_lock:
                    stats["circuit_state"] = current_state
                    stats["circuit_blocked"] += 1
                    stats["pop_rejected"] += 1
                raise HTTPException(status_code=503, detail="Service Unavailable - Key Fetch Failed")
            try:
                if DB_MODE == "async":
                    client = get_async_db_client()
                    resp = await client.get(f"{db_url}/pubkey", params={"sid": x_spiffe_id}, timeout=DB_TIMEOUT_SEC)
                    resp.raise_for_status()
                    pubkey_pem = resp.json().get("public_key")
                else:
                    sem = get_db_semaphore()
                    executor = get_db_executor()
                    client = get_sync_http_client()
                    try:
                        await asyncio.wait_for(sem.acquire(), timeout=DB_SEMAPHORE_ACQUIRE_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise DBOverloadError("DB concurrency limit reached (Fast-Fail)")
                    try:
                        resp = await asyncio.get_running_loop().run_in_executor(
                            executor,
                            lambda: client.get(f"{db_url}/pubkey", params={"sid": x_spiffe_id}, timeout=DB_TIMEOUT_SEC)
                        )
                        resp.raise_for_status()
                        pubkey_pem = resp.json().get("public_key")
                    finally:
                        sem.release()
                cb.record_success()
                if pubkey_pem:
                    l1_cache.set(pubkey_key, pubkey_pem)
                    try:
                        r_client = get_redis_client()
                        await r_client.setex(f"policy:{pubkey_key}", L2_CACHE_TTL_SEC, pubkey_pem)
                    except Exception as e:
                        logging.warning(f"Failed to populate L2 pubkey cache: {e}")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    with stats_lock:
                        stats["pop_rejected"] += 1
                    raise HTTPException(status_code=401, detail="Unknown Key ID")
                cb.record_failure()
                with stats_lock:
                    stats["circuit_state"] = cb.current_state
                    stats["pop_rejected"] += 1
                raise HTTPException(status_code=503, detail="Service Unavailable - Key Fetch Failed")
            except Exception as e:
                if not isinstance(e, DBOverloadError):
                    cb.record_failure()
                logging.warning(f"Failed to fetch pubkey: {e}")
                with stats_lock:
                    stats["circuit_state"] = cb.current_state
                    stats["pop_rejected"] += 1
                raise HTTPException(status_code=503, detail="Service Unavailable - Key Fetch Failed")

        if not pubkey_pem:
            with stats_lock:
                stats["pop_rejected"] += 1
            raise HTTPException(status_code=401, detail="Unknown Key ID")

        try:
            padding_needed = (4 - len(x_pop_signature) % 4) % 4
            signature_bytes = base64.urlsafe_b64decode(x_pop_signature + "=" * padding_needed)
        except (binascii.Error, ValueError):
            with stats_lock:
                stats["pop_rejected"] += 1
            raise HTTPException(status_code=401, detail="Invalid PoP Signature Encoding")

        canonical_payload = generate_canonical_payload(request.method, request.url.path, x_spiffe_id, x_nonce)

        is_valid = await asyncio.get_running_loop().run_in_executor(
            get_pop_executor(),
            verify_pop_signature,
            pubkey_pem,
            signature_bytes,
            canonical_payload
        )

        if not is_valid:
            with stats_lock:
                stats["pop_rejected"] += 1
            raise HTTPException(status_code=401, detail="Invalid PoP Signature")

        with stats_lock:
            stats["pop_verified"] += 1

    token = authorization.replace("Aegis ", "").replace("Bearer ", "")
    comp_key = generate_composite_key(x_spiffe_id, token, x_res, x_act)

    cached_policy = l1_cache.get(comp_key)
    if cached_policy is not None:
        with stats_lock:
            stats["l1_hits"] += 1
        ret_status = "deny" if cached_policy == "DENY" else "allow"
        return {"status": ret_status, "source": "L1", "policy": cached_policy}

    try:
        r_client = get_redis_client()
        l2_val = await r_client.get(f"policy:{comp_key}")
        if l2_val:
            with stats_lock:
                stats["l2_hits"] += 1
            val = l2_val.decode()
            l1_cache.set(comp_key, val)
            ret_status = "deny" if val == "DENY" else "allow"
            return {"status": ret_status, "source": "L2", "policy": val}
    except Exception as e:
        logging.warning(f"L2 cache fetch failed (Redis down?): {e}")

    if SINGLEFLIGHT_ENABLED:
        fut = in_flight.get(comp_key)
        if fut:
            with stats_lock:
                stats["singleflight_coalesced"] += 1
                stats["db_fallbacks"] += 1
            try:
                return await asyncio.shield(fut)
            except DBOverloadError:
                return JSONResponse(status_code=503, content={"status": "error", "message": "Service Unavailable - DB Overload"})
            except Exception:
                with stats_lock:
                    stats["circuit_state"] = cb.current_state
                return JSONResponse(status_code=200, content={"status": "degraded_allow", "source": "Fallback_Degradation_PostFail"})

    can_exec, current_state = cb.can_execute()
    if not can_exec:
        with stats_lock:
            stats["circuit_state"] = current_state
            stats["db_fallbacks"] += 1
            stats["circuit_blocked"] += 1
        return JSONResponse(status_code=200, content={"status": "degraded_allow", "source": "Fallback_Degradation"})

    fut = asyncio.get_running_loop().create_future()
    if SINGLEFLIGHT_ENABLED:
        in_flight[comp_key] = fut

    with stats_lock:
        stats["db_fallbacks"] += 1

    task = asyncio.create_task(db_loader_task(comp_key, x_spiffe_id, x_res, fut, use_singleflight=SINGLEFLIGHT_ENABLED))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    try:
        return await asyncio.shield(fut)
    except DBOverloadError as e:
        logging.warning(str(e))
        return JSONResponse(status_code=503, content={"status": "error", "message": "Service Unavailable - DB Overload"})
    except Exception as e:
        logging.warning(f"DB fallback failed: {e}")
        with stats_lock:
            stats["circuit_state"] = cb.current_state
        return JSONResponse(status_code=200, content={"status": "degraded_allow", "source": "Fallback_Degradation_PostFail"})

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/metrics")
async def get_metrics():
    current_state = cb.current_state
    try:
        r_client = get_redis_client()
        redis_ok = await r_client.ping()
    except Exception:
        redis_ok = False

    with stats_lock:
        stats["circuit_state"] = current_state
        stats["l1_cache_size"] = len(l1_cache)
        stats["local_rl_size"] = len(local_rate_limits)
        snapshot = dict(stats)
    async with partition_lock:
        current_partitioned = is_partitioned
    return {
        "node": node_name,
        "db_mode": DB_MODE,
        "singleflight_enabled": SINGLEFLIGHT_ENABLED,
        "rl_algorithm": RL_ALGORITHM,
        "pop_verify_enabled": POP_VERIFY_ENABLED,
        "stats": snapshot,
        "is_partitioned": current_partitioned,
        "redis_ok": redis_ok
    }

probe_semaphore = asyncio.Semaphore(1)

async def probe_db_health():
    if probe_semaphore.locked():
        return
    async with probe_semaphore:
        try:
            if DB_MODE == "async":
                await db_async_call("probe", "probe", "probe")
            else:
                await asyncio.to_thread(db_sync_call, "probe", "probe", "probe")
            cb.record_success()
        except Exception as e:
            logging.warning(f"DB probe failed upon healing: {e}")
            cb.force_state("OPEN")

@app.post("/chaos/partition/{state}")
async def set_partition(state: str):
    if state not in ["isolate", "heal"]:
        raise HTTPException(status_code=400, detail=f"Invalid partition state: {state}")
    global is_partitioned
    async with partition_lock:
        is_partitioned = (state == "isolate")
        current_partitioned = is_partitioned
    if state == "heal":
        task = asyncio.create_task(probe_db_health())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    return {"status": current_partitioned}

@app.post("/chaos/redis/{state}")
async def set_redis(state: str):
    if state not in ["up", "down"]:
        raise HTTPException(status_code=400, detail=f"Invalid redis state: {state}")
    global redis_client, stream_redis_client, nonce_script, rl_script
    old_redis = redis_client
    old_stream = stream_redis_client

    if state == "down":
        new_redis = Redis.from_url("redis://127.0.0.1:9999", socket_timeout=REDIS_DOWN_SOCKET_TIMEOUT)
        new_stream = Redis.from_url("redis://127.0.0.1:9999", socket_timeout=REDIS_DOWN_SOCKET_TIMEOUT)
    else:
        new_redis = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
        new_stream = Redis.from_url(redis_url, socket_timeout=STREAM_SOCKET_TIMEOUT)

    new_nonce_script = new_redis.register_script(NONCE_LUA)
    new_rl_script    = new_redis.register_script(SLIDING_RL_LUA)

    async with redis_swap_lock:
        nonce_script = new_nonce_script
        rl_script    = new_rl_script
        redis_client = new_redis
        stream_redis_client = new_stream

    # Graceful drain: wait 2.0s before closing replaced Redis clients so in-flight commands finish without connection aborts.
    async def close_old_clients(r, s):
        await asyncio.sleep(2.0)
        try:
            if r is not None:
                await r.close()
            if s is not None:
                await s.close()
        except Exception as e:
            logging.warning(f"Failed to close old Redis clients: {e}")

    task = asyncio.create_task(close_old_clients(old_redis, old_stream))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return {"status": state}

@app.post("/chaos/invalidate/{key}")
async def invalidate_cache(key: str):
    try:
        client = get_redis_client()
        await client.xadd(STREAM_NAME, {"key": key}, maxlen=10000, approximate=True)
    except Exception as e:
        logging.warning(f"Failed to publish invalidation: {e}")
    return {"status": "published"}

@app.post("/chaos/cb/force/{state}")
async def force_cb_state(state: str, request: Request):
    if state not in VALID_CB_STATES:
        raise HTTPException(status_code=400, detail=f"Invalid circuit breaker state: {state}")
    cb.force_state(state)
    return {"status": state}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", default="Node-X")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db-mode", default="sync", choices=["sync", "async"])
    args, _ = parser.parse_known_args()
    os.environ["NODE_NAME"] = args.node
    os.environ["DB_MODE"] = args.db_mode
    uvicorn.run("gateway:app", host="0.0.0.0", port=args.port)

