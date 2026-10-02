import asyncio
import hashlib
import time
import os
import httpx
import logging
import threading
from collections import OrderedDict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
import uvicorn
import argparse
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration — override via env vars; defaults match original values.
# ---------------------------------------------------------------------------

def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))

def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))

# Redis
REDIS_TIMEOUT_SEC            = _float("REDIS_TIMEOUT_SEC", 0.2)     # aggressive; keeps /auth p99 tight
STREAM_SOCKET_TIMEOUT        = _float("STREAM_SOCKET_TIMEOUT", 5.0) # must be > STREAM_BLOCK_MS / 1000
REDIS_DOWN_SOCKET_TIMEOUT    = _float("REDIS_DOWN_SOCKET_TIMEOUT", 0.1) # fast-fail for simulated dead Redis

# Stream / partition
STREAM_NAME                  = os.getenv("STREAM_NAME", "invalidation_stream")
STREAM_CATCHUP_COUNT         = _int("STREAM_CATCHUP_COUNT", 100)
STREAM_NORMAL_COUNT          = _int("STREAM_NORMAL_COUNT", 10)
STREAM_BLOCK_MS              = _int("STREAM_BLOCK_MS", 500)          # < STREAM_SOCKET_TIMEOUT * 1000
PARTITION_SLEEP_SEC          = _float("PARTITION_SLEEP_SEC", 0.5)

# Rate limiting
RL_WINDOW_SEC                = _int("RL_WINDOW_SEC", 10)             # aligns with Retry-After header value
RL_LIMIT                     = _int("RL_LIMIT", 50)                  # per-node; global ≈ RL_LIMIT × N_nodes
RL_FALLBACK_LIMIT            = _int("RL_FALLBACK_LIMIT", 15)         # when Redis is down: 50 / 3 nodes ≈ 15
RL_LOCAL_MAX_KEYS            = _int("RL_LOCAL_MAX_KEYS", 10000)      # OOM guard for fallback dict

# Nonce / L2 cache
NONCE_TTL_SEC                = _int("NONCE_TTL_SEC", 60)             # replay window; must equal L2_CACHE_TTL_SEC
L2_CACHE_TTL_SEC             = _int("L2_CACHE_TTL_SEC", 60)          # evict together with nonce entries

# L1 cache
L1_MAX_SIZE                  = _int("L1_MAX_SIZE", 1000)
L1_TTL_SEC                   = _int("L1_TTL_SEC", 60)                # ≤ L2_CACHE_TTL_SEC to avoid stale L1 hits

# Rate limiting modifiers
RL_EXPIRE_MULTIPLIER         = _int("RL_EXPIRE_MULTIPLIER", 2)

# Circuit breaker
CB_MAX_FAILURES              = _int("CB_MAX_FAILURES", 3)
CB_RESET_TIMEOUT_SEC         = _int("CB_RESET_TIMEOUT_SEC", 10)
CB_DEBOUNCE_SEC              = _float("CB_DEBOUNCE_SEC", 2.0)
CB_FORCE_STATE_MARGIN_SEC    = _float("CB_FORCE_STATE_MARGIN_SEC", 1.0)
VALID_CB_STATES              = frozenset(["OPEN", "CLOSED", "HALF_OPEN"])

# DB / thread pool
DB_TIMEOUT_SEC               = _float("DB_TIMEOUT_SEC", 1.0)         # policy_db simulates 2s hang → gateway times out first
DB_SEMAPHORE_ACQUIRE_TIMEOUT = _float("DB_SEMAPHORE_ACQUIRE_TIMEOUT", 0.1)
MAX_DB_WORKERS               = _int("MAX_DB_WORKERS", 5)

# ---------------------------------------------------------------------------

node_name = os.getenv("NODE_NAME", "Node-X")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
db_url    = os.getenv("DB_URL",    "http://localhost:8000")
DB_MODE   = os.getenv("DB_MODE",   "sync")  # "sync" or "async"
SINGLEFLIGHT_ENABLED = os.getenv("SINGLEFLIGHT_ENABLED", "true").lower() == "true"

STREAM_GROUP = f"gateway_group_{node_name}"

db_executor = None
db_semaphore = None

class DBOverloadError(Exception): pass

redis_client = None
stream_redis_client = None
nonce_script = None
sync_http_client = None
async_db_client = None

class L1Cache:
    def __init__(self, max_size=1000, ttl=60):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key in self.cache:
                val, expires_at = self.cache[key]
                if time.time() < expires_at:
                    self.cache.move_to_end(key)
                    return val
                else:
                    del self.cache[key]
            return None

    def set(self, key, value):
        with self._lock:
            if key in self.cache:
                del self.cache[key]
            elif len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
            self.cache[key] = (value, time.time() + self.ttl)
            
    def delete(self, key):
        with self._lock:
            if key in self.cache:
                del self.cache[key]

    def __contains__(self, key):
        return self.get(key) is not None

    def __len__(self):
        with self._lock:
            return len(self.cache)

l1_cache = L1Cache(max_size=L1_MAX_SIZE, ttl=L1_TTL_SEC)
is_partitioned = False
partition_lock = asyncio.Lock()
redis_swap_lock = asyncio.Lock()

class CircuitBreaker:
    def __init__(self):
        self.state = "CLOSED"
        self.failures = 0
        self.max_failures = CB_MAX_FAILURES
        self.reset_timeout = CB_RESET_TIMEOUT_SEC
        self.last_failure_time = 0
        self.last_transition_time = time.time()
        self._lock = threading.Lock()

    def check_and_half_open(self):
        with self._lock:
            if self.state == "OPEN" and time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "HALF_OPEN"
                self.last_transition_time = time.time()
            return self.state

    def reset(self):
        with self._lock:
            self.failures = 0
            if self.state != "CLOSED":
                self.state = "CLOSED"
                self.last_transition_time = time.time()
            self.last_failure_time = 0
            return self.state

    def record_failure(self):
        with self._lock:
            if time.time() - self.last_transition_time < CB_DEBOUNCE_SEC:
                return (None, self.state)

            self.failures += 1
            if self.failures >= self.max_failures:
                if self.state != "OPEN":
                    self.state = "OPEN"
                    self.last_transition_time = time.time()
                    self.last_failure_time = time.time()
            return (None, self.state)
            
    def record_success(self):
        with self._lock:
            self.failures = 0
            if self.state != "CLOSED":
                self.state = "CLOSED"
                self.last_transition_time = time.time()
            return (None, self.state)

    def can_execute(self):
        with self._lock:
            if self.state == "CLOSED": return (True, self.state)
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "HALF_OPEN"
                    self.last_transition_time = time.time()
                    return (True, self.state)
                return (False, self.state)
            if self.state == "HALF_OPEN": return (True, self.state)
            return (False, self.state)

    def force_state(self, state: str):
        if state not in VALID_CB_STATES:
            raise ValueError(f"Invalid state: {state}")
        with self._lock:
            self.state = state
            self.last_transition_time = time.time() - CB_DEBOUNCE_SEC - CB_FORCE_STATE_MARGIN_SEC
            if state == "OPEN":
                self.failures = self.max_failures
                self.last_failure_time = time.time()
            elif state == "HALF_OPEN":
                self.failures = self.max_failures
                self.last_failure_time = time.time() - self.reset_timeout - CB_FORCE_STATE_MARGIN_SEC
            elif state == "CLOSED":
                self.failures = 0
                self.last_failure_time = 0
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
    "latency_sum": 0.0,
    "circuit_state": "CLOSED",
    "sync_lag": 0,
    "db_queries_executed": 0,
    "singleflight_coalesced": 0,
    "circuit_blocked": 0,
    "l1_double_check_hits": 0
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

local_rate_limits = {}
local_rl_lock = asyncio.Lock()
in_flight: dict[str, asyncio.Future] = {}

async def cleanup_local_rl():
    while True:
        await asyncio.sleep(RL_WINDOW_SEC)
        now_window = int(time.time()) // RL_WINDOW_SEC
        async with local_rl_lock:
            to_delete = []
            for k in local_rate_limits.keys():
                try:
                    w = int(k.rsplit(":", 1)[-1])
                    if w < now_window:
                        to_delete.append(k)
                except ValueError:
                    continue
            for k in to_delete:
                del local_rate_limits[k]

async def check_rate_limit(client_id: str) -> bool:
    now = int(time.time())
    window_key = now // RL_WINDOW_SEC
    key = f"rl:{client_id}:{window_key}"

    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, RL_WINDOW_SEC * RL_EXPIRE_MULTIPLIER)
        results = await pipe.execute()
    return results[0] <= RL_LIMIT

async def consume_nonce(nonce: str, ttl: int = NONCE_TTL_SEC, act: str = "read") -> bool:
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
    resp = sync_http_client.get(f"{db_url}/policy/{key}", params={"sid": sid, "res": res}, timeout=DB_TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json()

async def db_async_call(key: str, sid: str, res: str):
    resp = await async_db_client.get(f"{db_url}/policy/{key}", params={"sid": sid, "res": res}, timeout=DB_TIMEOUT_SEC)
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
                    fut.set_result({"status": "deny" if cached_policy == "DENY" else "allow", "source": "L1", "policy": cached_policy})
                return

        with stats_lock:
            stats["db_queries_executed"] += 1

        if DB_MODE == "async":
            db_resp = await db_async_call(comp_key, sid, res)
        else:
            try:
                await asyncio.wait_for(db_semaphore.acquire(), timeout=DB_SEMAPHORE_ACQUIRE_TIMEOUT)
            except asyncio.TimeoutError:
                raise DBOverloadError("DB concurrency limit reached (Fast-Fail)")
            try:
                loop = asyncio.get_running_loop()
                db_resp = await loop.run_in_executor(db_executor, db_sync_call, comp_key, sid, res)
            finally:
                db_semaphore.release()
                
        cb.record_success()
        
        db_status = db_resp.get("status")
        policy = "DENY" if db_status == "deny" else db_resp.get("role")
            
        l1_cache.set(comp_key, policy)
        try:
            await redis_client.set(f"policy:{comp_key}", policy, ex=L2_CACHE_TTL_SEC)
        except Exception as e:
            logging.warning(f"Failed to set L2 cache: {e}")
            
        if not fut.done():
            fut.set_result({"status": "deny" if db_status == "deny" else "allow", "source": "DB", "policy": policy})
    except Exception as e:
        if not fut.done():
            fut.set_exception(e)
        if not isinstance(e, DBOverloadError):
            cb.record_failure()
    finally:
        in_flight.pop(comp_key, None)

async def stream_listener():
    try:
        await stream_redis_client.xgroup_create(STREAM_NAME, STREAM_GROUP, id="0", mkstream=True)
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
                
            if was_partitioned:
                start_time = time.time()
                total_pending = 0
                
                # 1. Pending
                pending = await stream_redis_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: "0"}, count=STREAM_CATCHUP_COUNT)
                if pending and pending[0][1]:
                    messages = pending[0][1]
                    total_pending += len(messages)
                    for msg_id, msg_data in messages:
                        key_bytes = msg_data.get(b"key")
                        if not key_bytes:
                            logging.warning(f"Malformed message skipped: {msg_data}")
                            await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
                            continue
                        key = key_bytes.decode()
                        if key in l1_cache:
                            l1_cache.delete(key)
                        try:
                            await redis_client.delete(f"policy:{key}")
                        except Exception as e:
                            logging.warning(f"Failed to delete L2 cache: {e}")
                        await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
                
                # 2. Unread accumulated during partition
                while True:
                    new_msgs = await stream_redis_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: ">"}, count=STREAM_CATCHUP_COUNT)
                    if not new_msgs or not new_msgs[0][1]:
                        break
                    messages = new_msgs[0][1]
                    total_pending += len(messages)
                    for msg_id, msg_data in messages:
                        key_bytes = msg_data.get(b"key")
                        if not key_bytes:
                            logging.warning(f"Malformed message skipped: {msg_data}")
                            await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
                            continue
                        key = key_bytes.decode()
                        if key in l1_cache:
                            l1_cache.delete(key)
                        try:
                            await redis_client.delete(f"policy:{key}")
                        except Exception as e:
                            logging.warning(f"Failed to delete L2 cache: {e}")
                        await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
                
                catchup_duration = int((time.time() - start_time) * 1000)
                with stats_lock:
                    stats["last_catchup"] = {
                        "pending_count": total_pending,
                        "duration_ms": catchup_duration,
                        "timestamp": time.time()
                    }
                was_partitioned = False
                continue

            # NORMAL LOOP
            pending = await stream_redis_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: "0"}, count=STREAM_NORMAL_COUNT)
            if pending and pending[0][1]:
                messages = pending[0][1]
            else:
                messages_data = await stream_redis_client.xreadgroup(STREAM_GROUP, node_name, {STREAM_NAME: ">"}, count=STREAM_NORMAL_COUNT, block=STREAM_BLOCK_MS)
                if not messages_data:
                    continue
                
                async with partition_lock:
                    current_partitioned = is_partitioned
                if current_partitioned:
                    continue
                    
                messages = messages_data[0][1]

            for msg_id, msg_data in messages:
                key_bytes = msg_data.get(b"key")
                if not key_bytes:
                    logging.warning(f"Malformed message skipped: {msg_data}")
                    await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
                    continue
                key = key_bytes.decode()
                if key in l1_cache:
                    l1_cache.delete(key)
                try:
                    await redis_client.delete(f"policy:{key}")
                except Exception as e:
                    logging.warning(f"Failed to delete L2 cache: {e}")
                await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Stream listener error: {e}")
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, stream_redis_client, sync_http_client, async_db_client, db_semaphore, db_executor, nonce_script
    db_semaphore = asyncio.Semaphore(MAX_DB_WORKERS)
    db_executor = ThreadPoolExecutor(max_workers=MAX_DB_WORKERS)
    redis_client = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
    nonce_script = redis_client.register_script(NONCE_LUA)
    stream_redis_client = Redis.from_url(redis_url, socket_timeout=STREAM_SOCKET_TIMEOUT)
    sync_http_client = httpx.Client(timeout=DB_TIMEOUT_SEC)
    async_db_client = httpx.AsyncClient(timeout=DB_TIMEOUT_SEC, limits=httpx.Limits(max_connections=MAX_DB_WORKERS))
    task = asyncio.create_task(stream_listener())
    task_cleanup = asyncio.create_task(cleanup_local_rl())
    yield
    task.cancel()
    task_cleanup.cancel()
    sync_http_client.close()
    await async_db_client.aclose()
    if db_executor:
        db_executor.shutdown(wait=True)
    await redis_client.close()
    await stream_redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.exception_handler(HTTPException)
async def unified_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": exc.detail}
    )

ADMIN_SECRET_TOKEN = os.environ.get("ADMIN_SECRET_TOKEN")
if not ADMIN_SECRET_TOKEN:
    raise RuntimeError("ADMIN_SECRET_TOKEN environment variable is required")

@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path == "/health":
        return await call_next(request)
    if path == "/metrics" or path.startswith("/chaos/"):
        admin_token = request.headers.get("X-Admin-Token")
        if admin_token != ADMIN_SECRET_TOKEN:
            return JSONResponse(status_code=401, content={"status": "error", "message": "Unauthorized Admin Access"})
        return await call_next(request)
    
    # All other routes require Authorization header
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

@app.get("/auth")
async def auth(
    authorization: str = Header(None),
    x_nonce: str = Header(None),
    x_spiffe_id: str = Header(None),
    x_res: str = Header("resource"),
    x_act: str = Header("read")
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
        raise HTTPException(status_code=503, detail="Service Unavailable - Rate Limit Failed")
        
    try:
        nonce_ok = await consume_nonce(x_nonce, act=x_act)
        if not nonce_ok:
            with stats_lock:
                stats["nonce_replays"] += 1
            raise HTTPException(status_code=401, detail="Replay Detected")
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        logging.warning(f"Nonce check failed (Redis down?): {e}")
        raise HTTPException(status_code=503, detail="Service Unavailable - Nonce Check Failed")

    token = authorization.replace("Aegis ", "").replace("Bearer ", "")
    comp_key = generate_composite_key(x_spiffe_id, token, x_res, x_act)
    
    cached_policy = l1_cache.get(comp_key)
    if cached_policy is not None:
        with stats_lock:
            stats["l1_hits"] += 1
        ret_status = "deny" if cached_policy == "DENY" else "allow"
        return {"status": ret_status, "source": "L1", "policy": cached_policy}
        
    try:
        l2_val = await redis_client.get(f"policy:{comp_key}")
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
            except DBOverloadError as e:
                return JSONResponse(status_code=503, content={"status": "error", "message": "Service Unavailable - DB Overload"})
            except Exception as e:
                with stats_lock:
                    stats["circuit_state"] = cb.state
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
    try:
        return await asyncio.shield(fut)
    except DBOverloadError as e:
        logging.warning(str(e))
        return JSONResponse(status_code=503, content={"status": "error", "message": "Service Unavailable - DB Overload"})
    except Exception as e:
        logging.warning(f"DB fallback failed: {e}")
        with stats_lock:
            stats["circuit_state"] = cb.state
        return JSONResponse(status_code=200, content={"status": "degraded_allow", "source": "Fallback_Degradation_PostFail"})

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/metrics")
async def get_metrics():
    with cb._lock:
        current_state = cb.state
    try:
        redis_ok = await redis_client.ping()
    except Exception:
        redis_ok = False

    with stats_lock:
        stats["circuit_state"] = current_state
        stats["l1_cache_size"] = len(l1_cache)
        stats["local_rl_size"] = len(local_rate_limits)
        snapshot = dict(stats)
    async with partition_lock:
        current_partitioned = is_partitioned
    return {"node": node_name, "db_mode": DB_MODE, "singleflight_enabled": SINGLEFLIGHT_ENABLED, "stats": snapshot, "is_partitioned": current_partitioned, "redis_ok": redis_ok}

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
        asyncio.create_task(probe_db_health())
    return {"status": current_partitioned}

@app.post("/chaos/redis/{state}")
async def set_redis(state: str):
    if state not in ["up", "down"]:
        raise HTTPException(status_code=400, detail=f"Invalid redis state: {state}")
    global redis_client, stream_redis_client, nonce_script
    old_redis = redis_client
    old_stream = stream_redis_client
    
    if state == "down":
        new_redis = Redis.from_url("redis://127.0.0.1:9999", socket_timeout=REDIS_DOWN_SOCKET_TIMEOUT)
        new_stream = Redis.from_url("redis://127.0.0.1:9999", socket_timeout=REDIS_DOWN_SOCKET_TIMEOUT)
    else:
        new_redis = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
        new_stream = Redis.from_url(redis_url, socket_timeout=STREAM_SOCKET_TIMEOUT)
        
    new_nonce_script = new_redis.register_script(NONCE_LUA)
    
    # Atomic swap
    async with redis_swap_lock:
        nonce_script = new_nonce_script
        redis_client = new_redis
        stream_redis_client = new_stream
    
    # Safely close old clients
    async def close_old_clients(r, s):
        await asyncio.sleep(2.0)
        try:
            await r.close()
            await s.close()
        except Exception as e:
            logging.warning(f"Failed to close old Redis clients: {e}")
            
    asyncio.create_task(close_old_clients(old_redis, old_stream))
    return {"status": state}

@app.post("/chaos/invalidate/{key}")
async def invalidate_cache(key: str):
    try:
        await redis_client.xadd(STREAM_NAME, {"key": key})
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
