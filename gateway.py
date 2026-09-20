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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor

MAX_DB_WORKERS = 5
db_executor = None
db_semaphore = None

class DBOverloadError(Exception): pass


# Parse args before any initialization that uses node_name
parser = argparse.ArgumentParser()
parser.add_argument("--node", default="Node-X")
parser.add_argument("--port", type=int, default=8000)
args, _ = parser.parse_known_args()

node_name = args.node
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
db_url = os.getenv("DB_URL", "http://localhost:8000")

# Constants
STREAM_NAME = "invalidation_stream"
STREAM_GROUP = f"gateway_group_{node_name}"
STREAM_CATCHUP_COUNT = 100
STREAM_NORMAL_COUNT = 10
STREAM_BLOCK_MS = 500
PARTITION_SLEEP_SEC = 0.5
REDIS_TIMEOUT_SEC = 0.2

redis_client = None
stream_redis_client = None
sync_http_client = None
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

l1_cache = L1Cache()
is_partitioned = False

class CircuitBreaker:
    def __init__(self):
        self.state = "CLOSED"
        self.failures = 0
        self.max_failures = 3
        self.reset_timeout = 10
        self.last_failure_time = 0
        self._lock = threading.Lock()

    def check_and_half_open(self):
        with self._lock:
            if self.state == "OPEN" and time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "HALF_OPEN"

    def reset(self):
        with self._lock:
            self.failures = 0
            self.state = "CLOSED"
            self.last_failure_time = 0

    def record_failure(self):
        with self._lock:
            self.failures += 1
            if self.failures >= self.max_failures:
                if self.state != "OPEN":
                    self.state = "OPEN"
                    self.last_failure_time = time.time()
            
    def record_success(self):
        with self._lock:
            self.failures = 0
            self.state = "CLOSED"

    def can_execute(self):
        with self._lock:
            if self.state == "CLOSED": return True
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "HALF_OPEN"
                    return True
                return False
            if self.state == "HALF_OPEN": return True
            return False

cb = CircuitBreaker()

stats = {
    "l1_hits": 0,
    "l2_hits": 0,
    "db_fallbacks": 0,
    "requests": 0,
    "rate_limited": 0,
    "nonce_replays": 0,
    "latency_sum": 0.0,
    "circuit_state": "CLOSED",
    "sync_lag": 0
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

async def cleanup_local_rl():
    window = 10
    while True:
        await asyncio.sleep(window)
        now_window = int(time.time()) // window
        async with local_rl_lock:
            # Delete keys whose window_key is older than the current window
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
    window = 10
    limit = 50
    now = int(time.time())
    window_key = now // window
    key = f"rl:{client_id}:{window_key}"
    
    try:
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, window * 2)
            results = await pipe.execute()
        return results[0] <= limit
    except Exception as e:
        # Fallback to local memory rate limiting
        # Option B chosen: Apply a more conservative limit during Redis outage
        # since accurate global coordination is impossible. Prevents 3x amplification.
        fallback_limit = 15
        async with local_rl_lock:
            local_key = f"{client_id}:{window_key}"
            count = local_rate_limits.get(local_key, 0) + 1
            local_rate_limits[local_key] = count
            return count <= fallback_limit

async def consume_nonce(nonce: str) -> bool:
    script = redis_client.register_script(NONCE_LUA)
    key = f"nonce:{nonce}"
    result = await script(keys=[key], args=["1", 60])
    return result == 1

def generate_composite_key(sid: str, uid: str, res: str, act: str) -> str:
    sid = sid.strip()
    uid = uid.strip()
    res = res.strip()
    act = act.strip().upper()
    payload = f"{len(sid)}:{sid}{len(uid)}:{uid}{len(res)}:{res}{len(act)}:{act}"
    return hashlib.sha256(payload.encode()).hexdigest()

def db_sync_call(key: str, sid: str, res: str):
    # Intentional synchronous blocking call to simulate legacy JDBC / DB operations
    # that threaten the reactive event loop (Paper Aegis-Mesh Section 5.4).
    # This is offloaded via asyncio.to_thread to prevent blocking the main loop.
    try:
        resp = sync_http_client.get(f"{db_url}/policy/{key}", params={"sid": sid, "res": res}, timeout=1.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise e

async def stream_listener():
    try:
        await stream_redis_client.xgroup_create(STREAM_NAME, STREAM_GROUP, id="0", mkstream=True)
    except Exception:
        pass # Group might already exist

    was_partitioned = False
    while True:
        try:
            if is_partitioned:
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
                        await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
                
                catchup_duration = int((time.time() - start_time) * 1000)
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
                
                # [Fix] Post-read State Barrier: 
                # Prevent processing if partition signal arrived during the blocking read.
                # The message is now in the PEL (Pending Entries List) but not ACK'd,
                # so the catch-up logic will safely process it upon healing.
                if is_partitioned:
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
                await stream_redis_client.xack(STREAM_NAME, STREAM_GROUP, msg_id)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Stream listener error: {e}")
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, stream_redis_client, sync_http_client, db_semaphore, db_executor
    db_semaphore = asyncio.Semaphore(MAX_DB_WORKERS)
    db_executor = ThreadPoolExecutor(max_workers=MAX_DB_WORKERS)
    redis_client = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
    stream_redis_client = Redis.from_url(redis_url, socket_timeout=5.0)
    sync_http_client = httpx.Client(timeout=1.0)
    task = asyncio.create_task(stream_listener())
    task_cleanup = asyncio.create_task(cleanup_local_rl())
    yield
    task.cancel()
    task_cleanup.cancel()
    sync_http_client.close()
    if db_executor:
        db_executor.shutdown(wait=True)
    await redis_client.close()
    await stream_redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def auth_bypass_middleware(request: Request, call_next):
    path = request.url.path
    # Fast-Path Bypass
    if path == "/metrics" or path.startswith("/chaos/"):
        return await call_next(request)
    
    # All other routes require Authorization header
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return JSONResponse(status_code=401, content={"detail": "Missing Authorization Header"})
    
    return await call_next(request)
@app.middleware("http")
async def measure_latency(request: Request, call_next):
    if is_partitioned and not (request.url.path.startswith("/chaos/partition") or request.url.path.startswith("/metrics") or request.url.path.startswith("/chaos/redis")):
        return JSONResponse(status_code=503, content={"error": "Node Isolated"})
        
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    if request.url.path == "/auth":
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
            stats["rate_limited"] += 1
            return JSONResponse(status_code=429, headers={"Retry-After": "10"}, content={"error": "Too Many Requests"})
    except Exception as e:
        logging.warning(f"Rate limit check failed (Redis down?): {e}")
        
    try:
        nonce_ok = await consume_nonce(x_nonce)
        if not nonce_ok:
            stats["nonce_replays"] += 1
            raise HTTPException(status_code=401, detail="Replay Detected")
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        logging.warning(f"Nonce check failed (Redis down?): {e}")
        raise HTTPException(status_code=503, detail="Nonce Verification Unavailable")

    token = authorization.replace("Bearer ", "")
    comp_key = generate_composite_key(x_spiffe_id, token, x_res, x_act)
    
    cached_policy = l1_cache.get(comp_key)
    if cached_policy is not None:
        stats["l1_hits"] += 1
        return {"status": "allow", "source": "L1", "policy": cached_policy}
        
    try:
        l2_val = await redis_client.get(f"policy:{comp_key}")
        if l2_val:
            stats["l2_hits"] += 1
            val = l2_val.decode()
            l1_cache.set(comp_key, val)
            return {"status": "allow", "source": "L2", "policy": val}
    except Exception as e:
        logging.warning(f"L2 cache fetch failed (Redis down?): {e}")
        
    stats["db_fallbacks"] += 1
    
    if not cb.can_execute():
        stats["circuit_state"] = cb.state
        return JSONResponse(status_code=200, content={"status": "degraded_allow", "source": "Fallback_Degradation"})
        
    try:
        try:
            await asyncio.wait_for(db_semaphore.acquire(), timeout=0.1)
        except asyncio.TimeoutError:
            raise DBOverloadError("DB concurrency limit reached (Fast-Fail)")
        try:
            loop = asyncio.get_running_loop()
            db_resp = await loop.run_in_executor(db_executor, db_sync_call, comp_key, x_spiffe_id, x_res)
        finally:
            db_semaphore.release()
        cb.record_success()
        stats["circuit_state"] = cb.state
        
        policy = db_resp.get("role")
        l1_cache.set(comp_key, policy)
        try:
            await redis_client.set(f"policy:{comp_key}", policy, ex=60)
        except Exception as e:
            logging.warning(f"Failed to set L2 cache: {e}")
            
        return {"status": "allow", "source": "DB", "policy": policy}
    except DBOverloadError as e:
        logging.warning(str(e))
        return JSONResponse(status_code=503, content={"error": "Service Unavailable - DB Overload"})
    except Exception as e:
        logging.warning(f"DB fallback failed: {e}")
        cb.record_failure()
        stats["circuit_state"] = cb.state
        return JSONResponse(status_code=200, content={"status": "degraded_allow", "source": "Fallback_Degradation_PostFail"})

@app.get("/metrics")
async def get_metrics():
    cb.check_and_half_open()
    stats["circuit_state"] = cb.state
    stats["l1_cache_size"] = len(l1_cache)
    stats["local_rl_size"] = len(local_rate_limits)
    return {"node": node_name, "stats": stats, "is_partitioned": is_partitioned}

@app.post("/chaos/partition/{state}")
async def set_partition(state: str):
    global is_partitioned
    is_partitioned = (state == "isolate")
    return {"status": is_partitioned}

@app.post("/chaos/redis/{state}")
async def set_redis(state: str):
    global redis_client, stream_redis_client
    await redis_client.close()
    await stream_redis_client.close()
    if state == "down":
        redis_client = Redis.from_url("redis://127.0.0.1:9999", socket_timeout=0.1)
        stream_redis_client = Redis.from_url("redis://127.0.0.1:9999", socket_timeout=0.1)
    else:
        redis_client = Redis.from_url(redis_url, socket_timeout=REDIS_TIMEOUT_SEC)
        stream_redis_client = Redis.from_url(redis_url, socket_timeout=5.0)
    return {"status": state}

@app.post("/chaos/invalidate/{key}")
async def invalidate_cache(key: str):
    try:
        await redis_client.xadd(STREAM_NAME, {"key": key})
    except Exception as e:
        logging.warning(f"Failed to publish invalidation: {e}")
    return {"status": "published"}

@app.post("/chaos/cb/reset")
async def reset_cb():
    cb.reset()
    return {"status": "reset"}

if __name__ == "__main__":
    uvicorn.run("gateway:app", host="0.0.0.0", port=args.port)
