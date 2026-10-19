import asyncio
import hmac
import json
import logging
import os
import random
from typing import Optional
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
from sse_starlette.sse import EventSourceResponse

ADMIN_SECRET_TOKEN = os.environ.get("ADMIN_SECRET_TOKEN")
if not ADMIN_SECRET_TOKEN:
    raise RuntimeError("ADMIN_SECRET_TOKEN environment variable is required")

HTTP_TIMEOUT_SEC = 5.0
METRICS_STREAM_SLEEP_SEC = 1.0
DDOS_REQUEST_COUNT = 60
TEST_INVALIDATE_KEY = "a8f5f167f44f4964e6c998dee827110c"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

GATEWAYS = {
    "Node-A": os.getenv("GATEWAY_A", "http://localhost:8001"),
    "Node-B": os.getenv("GATEWAY_B", "http://localhost:8002"),
    "Node-C": os.getenv("GATEWAY_C", "http://localhost:8003"),
}
POLICY_DB = os.getenv("POLICY_DB", "http://policy_db:8000")

async_http_client: Optional[httpx.AsyncClient] = None
index_html_cache: Optional[str] = None
subscribers: set[asyncio.Queue] = set()
metrics_poller_task: Optional[asyncio.Task] = None
latest_metrics_snapshot: Optional[dict] = None

def get_async_http_client() -> httpx.AsyncClient:
    global async_http_client
    if async_http_client is None or (isinstance(async_http_client, httpx.AsyncClient) and async_http_client.is_closed):
        async_http_client = httpx.AsyncClient(
            timeout=HTTP_TIMEOUT_SEC,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
    return async_http_client

async def broadcast_metrics_loop():
    global latest_metrics_snapshot
    while True:
        try:
            metrics = await fetch_metrics()
            latest_metrics_snapshot = metrics
            payload = json.dumps(metrics)
            for q in list(subscribers):
                if q.full():
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                try:
                    q.put_nowait(payload)
                except Exception as e:
                    logging.warning(f"Failed to enqueue metrics for subscriber, dropping: {e}")
                    subscribers.discard(q)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Error in metrics poller loop: {e}")
        await asyncio.sleep(METRICS_STREAM_SLEEP_SEC)

def ensure_poller_running():
    global metrics_poller_task
    if metrics_poller_task is None or metrics_poller_task.done():
        metrics_poller_task = asyncio.create_task(broadcast_metrics_loop())

@asynccontextmanager
async def lifespan(app: FastAPI):
    global async_http_client, index_html_cache, metrics_poller_task
    async_http_client = httpx.AsyncClient(
        timeout=HTTP_TIMEOUT_SEC,
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )

    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "static", "index.html")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            index_html_cache = f.read()
    except Exception as e:
        logging.error(f"Failed to load index.html: {e}")

    ensure_poller_running()
    yield
    if metrics_poller_task and not metrics_poller_task.done():
        metrics_poller_task.cancel()
        try:
            await metrics_poller_task
        except asyncio.CancelledError:
            pass
    if async_http_client is not None and not async_http_client.is_closed:
        await async_http_client.aclose()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith("/chaos/") and path != "/chaos/auth_check":
        admin_token = request.headers.get("X-Admin-Token")
        # Constant-time comparison to prevent timing attacks
        if not admin_token or not hmac.compare_digest(admin_token, ADMIN_SECRET_TOKEN):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized Admin Access"})
    elif path == "/metrics/stream":
        token = request.query_params.get("token") or request.headers.get("X-Admin-Token")
        if not token or not hmac.compare_digest(token, ADMIN_SECRET_TOKEN):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized Admin Access"})
    return await call_next(request)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def get_index():
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    if index_html_cache is None:
        return JSONResponse(status_code=503, content={"detail": "Service Unavailable - index.html not loaded"})
    return HTMLResponse(index_html_cache)

@app.get("/chaos/auth_check")
async def auth_check(request: Request):
    admin_token = request.headers.get("X-Admin-Token")
    if not admin_token or not hmac.compare_digest(admin_token, ADMIN_SECRET_TOKEN):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized Admin Access"})
    return {"status": "ok"}

async def fetch_metrics():
    client = get_async_http_client()
    reqs = [
        client.get(
            f"{url}/metrics",
            headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}
        )
        for url in GATEWAYS.values()
    ]
    resps = await asyncio.gather(*reqs, return_exceptions=True)
    results = {}
    for name, resp in zip(GATEWAYS.keys(), resps):
        try:
            if isinstance(resp, Exception):
                raise resp
            resp.raise_for_status()
            if isinstance(getattr(resp, "status_code", None), int) and resp.status_code != 200:
                raise httpx.HTTPStatusError(
                    f"Non-200 status code: {resp.status_code}",
                    request=getattr(resp, "request", None),
                    response=resp,
                )
            results[name] = resp.json()
        except Exception as e:
            logging.warning(f"Failed to fetch metrics from {name}: {e}")
            results[name] = {"node": name, "status": "offline", "stats": {}}
    return results

@app.get("/metrics/stream")
async def metrics_stream(request: Request):
    ensure_poller_running()
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    if latest_metrics_snapshot is not None:
        q.put_nowait(json.dumps(latest_metrics_snapshot))
    subscribers.add(q)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(q.get(), timeout=METRICS_STREAM_SLEEP_SEC)
                    yield {"data": data}
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass
        finally:
            subscribers.discard(q)

    return EventSourceResponse(event_generator())

@app.post("/chaos/nonce")
async def chaos_nonce():
    nonce = str(uuid.uuid4())
    headers = {
        "Authorization": f"Aegis test_token_{nonce}",
        "X-Nonce": nonce,
        "X-SPIFFE-ID": "spiffe://example.org/workload",
        "X-Act": "write"
    }

    metrics = await fetch_metrics()
    available_nodes = [name for name, data in metrics.items() if data.get('status') != 'offline' and not data.get('is_partitioned')]

    if len(available_nodes) < 2:
        return {"nonce": nonce, "results": {}, "error": "Not enough available nodes (min 2) for parallel test."}

    target_nodes = available_nodes[:2]
    client = get_async_http_client()
    reqs = [
        client.get(f"{GATEWAYS[target_nodes[0]]}/auth", headers=headers),
        client.get(f"{GATEWAYS[target_nodes[1]]}/auth", headers=headers)
    ]
    resps = await asyncio.gather(*reqs, return_exceptions=True)
    out = []
    for r in resps:
        if isinstance(r, Exception):
            out.append("Error/Timeout")
        else:
            out.append(f"Status: {r.status_code} ({r.json()})")
    return {"nonce": nonce, "results": {target_nodes[0]: out[0], target_nodes[1]: out[1]}}

@app.post("/chaos/same_key_burst")
async def chaos_same_key_burst(node: str, count: int = 50):
    if node not in GATEWAYS:
        return JSONResponse(status_code=400, content={"detail": f"Unknown node {node}"})

    client = get_async_http_client()
    spiffe_id = f"spiffe://example.org/burst_{uuid.uuid4()}"
    headers = {
        "Authorization": "Aegis test_token",
        "X-SPIFFE-ID": spiffe_id
    }

    try:
        resp_before = await client.get(f"{GATEWAYS[node]}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        stats_before = resp_before.json().get("stats", {})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})

    reqs = []
    for _ in range(count):
        h = headers.copy()
        h["X-Nonce"] = str(uuid.uuid4())
        reqs.append(client.get(f"{GATEWAYS[node]}/auth", headers=h))

    await asyncio.gather(*reqs, return_exceptions=True)

    try:
        resp_after = await client.get(f"{GATEWAYS[node]}/metrics", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        stats_after = resp_after.json().get("stats", {})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})

    delta_db = stats_after.get("db_queries_executed", 0) - stats_before.get("db_queries_executed", 0)
    delta_coal = stats_after.get("singleflight_coalesced", 0) - stats_before.get("singleflight_coalesced", 0)
    delta_l1 = stats_after.get("l1_hits", 0) - stats_before.get("l1_hits", 0)

    return {
        "node": node,
        "count": count,
        "delta_db_queries_executed": delta_db,
        "delta_singleflight_coalesced": delta_coal,
        "delta_l1_hits": delta_l1
    }

@app.post("/chaos/ddos")
async def chaos_ddos():
    headers = {
        "Authorization": "Aegis test_token",
        "X-SPIFFE-ID": "spiffe://example.org/attacker"
    }
    metrics = await fetch_metrics()
    available_nodes = [name for name, data in metrics.items() if data.get('status') != 'offline' and not data.get('is_partitioned')]
    if not available_nodes:
        available_nodes = list(GATEWAYS.keys())

    client = get_async_http_client()
    reqs = []
    for _ in range(DDOS_REQUEST_COUNT):
        h = headers.copy()
        h["X-Nonce"] = str(uuid.uuid4())
        target_node = random.choice(available_nodes)
        reqs.append(client.get(f"{GATEWAYS[target_node]}/auth", headers=h))

    resps = await asyncio.gather(*reqs, return_exceptions=True)
    codes = {}
    for r in resps:
        if not isinstance(r, Exception):
            codes[str(r.status_code)] = codes.get(str(r.status_code), 0) + 1
        else:
            codes["Timeout"] = codes.get("Timeout", 0) + 1
    return {"status": "flooded", "responses": codes}

@app.post("/chaos/partition/{node}/{state}")
async def chaos_partition(node: str, state: str):
    if node not in GATEWAYS:
        return JSONResponse(status_code=400, content={"detail": f"Unknown node {node}"})
    client = get_async_http_client()
    try:
        resp = await client.post(f"{GATEWAYS[node]}/chaos/partition/{state}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        resp.raise_for_status()
        return {"node": node, "state": state}
    except Exception as e:
        logging.error(f"Partition {node} failed: {e}")
        return JSONResponse(status_code=500, content={"detail": str(e)})

@app.post("/chaos/redis/{state}")
async def chaos_redis(state: str):
    client = get_async_http_client()
    reqs = [
        client.post(f"{url}/chaos/redis/{state}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        for url in GATEWAYS.values()
    ]
    await asyncio.gather(*reqs, return_exceptions=True)
    return {"redis_cluster": state}

@app.post("/chaos/invalidate")
async def chaos_invalidate():
    test_key = TEST_INVALIDATE_KEY
    metrics = await fetch_metrics()
    available_nodes = [name for name, data in metrics.items() if data.get('status') != 'offline' and not data.get('is_partitioned')]

    if not available_nodes:
        return JSONResponse(status_code=500, content={"detail": "No available nodes"})

    target_node = random.choice(available_nodes)
    client = get_async_http_client()

    try:
        resp = await client.post(f"{GATEWAYS[target_node]}/chaos/invalidate/{test_key}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        resp.raise_for_status()
        return {"status": "invalidated", "node": target_node}
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})

@app.post("/chaos/db/{state}")
async def chaos_db(state: str):
    if state not in ["up", "down"]:
        return JSONResponse(status_code=400, content={"detail": f"Invalid db state: {state}"})
    client = get_async_http_client()
    try:
        resp = await client.post(
            f"{POLICY_DB}/chaos/db/{state}",
            headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}
        )
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to change DB state: {e}")
        return JSONResponse(status_code=500, content={"detail": f"Failed to change DB state: {e}"})

    results = {}
    target_state = "CLOSED" if state == "up" else "OPEN"
    tasks = [
        client.post(f"{url}/chaos/cb/force/{target_state}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        for url in GATEWAYS.values()
    ]
    resps = await asyncio.gather(*tasks, return_exceptions=True)
    for name, r in zip(GATEWAYS.keys(), resps):
        if isinstance(r, Exception) or (hasattr(r, "status_code") and r.status_code >= 400):
            logging.warning(f"Failed to force {target_state} on {name}: {r}")
            results[name] = "failed"
        else:
            results[name] = "success"

    return {"db_state": state, "results": results}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("control_plane:app", host='0.0.0.0', port=8080)

