import random
import json
import logging
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse
import httpx
import asyncio
import os
ADMIN_SECRET_TOKEN = os.environ.get("ADMIN_SECRET_TOKEN")
if not ADMIN_SECRET_TOKEN:
    raise RuntimeError("ADMIN_SECRET_TOKEN environment variable is required")
import uuid
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

GATEWAYS = {
    "Node-A": os.getenv("GATEWAY_A", "http://localhost:8001"),
    "Node-B": os.getenv("GATEWAY_B", "http://localhost:8002"),
    "Node-C": os.getenv("GATEWAY_C", "http://localhost:8003"),
}
POLICY_DB = os.getenv("POLICY_DB", "http://policy_db:8000")

async_http_client = None
index_html_cache = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global async_http_client, index_html_cache
    async_http_client = httpx.AsyncClient(timeout=5.0)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "static", "index.html")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            index_html_cache = f.read()
    except Exception as e:
        logging.error(f"Failed to load index.html: {e}")
        
    yield
    await async_http_client.aclose()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path == "/metrics/stream" or path.startswith("/chaos/"):
        # 브라우저 EventSource API는 커스텀 헤더 지원이 없어 부득이하게 쿼리파라미터로 토큰을 받음. 
        # (access log에 토큰이 평문으로 남을 수 있는 트레이드오프 존재)
        admin_token = request.headers.get("X-Admin-Token") or request.query_params.get("token")
        if admin_token != ADMIN_SECRET_TOKEN:
            return JSONResponse(status_code=401, content={"detail": "Unauthorized Admin Access"})
    return await call_next(request)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def get_index():
    if index_html_cache is None:
        return JSONResponse(status_code=503, content={"error": "Service Unavailable - index.html not loaded"})
    return HTMLResponse(index_html_cache)

@app.get("/chaos/auth_check")
async def auth_check():
    return {"status": "ok"}


async def fetch_metrics():
    results = {}
    for name, url in GATEWAYS.items():
        try:
            resp = await async_http_client.get(
                f"{url}/metrics",
                headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}
            )
            results[name] = resp.json()
        except Exception as e:
            logging.warning(f"Failed to fetch metrics from {name}: {e}")
            results[name] = {"node": name, "status": "offline", "stats": {}}
    return results

@app.get("/metrics/stream")
async def metrics_stream():
    async def event_generator():
        while True:
            metrics = await fetch_metrics()
            yield {"data": json.dumps(metrics)}
            await asyncio.sleep(1)
    return EventSourceResponse(event_generator())

@app.post("/chaos/nonce")
async def chaos_nonce():
    nonce = str(uuid.uuid4())
    headers = {
        "Authorization": f"Bearer test_token_{nonce}",
        "X-Nonce": nonce,
        "X-SPIFFE-ID": "spiffe://example.org/workload"
    }

    metrics = await fetch_metrics()
    available_nodes = [name for name, data in metrics.items() if data.get('status') != 'offline' and not data.get('is_partitioned')]
    
    if len(available_nodes) < 2:
        return {"nonce": nonce, "results": {}, "error": "Not enough available nodes (min 2) for parallel test."}

    target_nodes = available_nodes[:2]

    reqs = [
        async_http_client.get(f"{GATEWAYS[target_nodes[0]]}/auth", headers=headers),
        async_http_client.get(f"{GATEWAYS[target_nodes[1]]}/auth", headers=headers)
    ]
    resps = await asyncio.gather(*reqs, return_exceptions=True)
    out = []
    for r in resps:
        if isinstance(r, Exception): out.append("Error/Timeout")
        else: out.append(f"Status: {r.status_code} ({r.json()})")
    return {"nonce": nonce, "results": {target_nodes[0]: out[0], target_nodes[1]: out[1]}}

@app.post("/chaos/ddos")
async def chaos_ddos():
    headers = {
        "Authorization": "Bearer test_token",
        "X-SPIFFE-ID": "spiffe://example.org/attacker"
    }
    metrics = await fetch_metrics()
    available_nodes = [name for name, data in metrics.items() if data.get('status') != 'offline' and not data.get('is_partitioned')]
    if not available_nodes:
        available_nodes = list(GATEWAYS.keys())

    reqs = []
    for i in range(60):
        h = headers.copy()
        h["X-Nonce"] = str(uuid.uuid4())
        target_node = random.choice(available_nodes)
        reqs.append(async_http_client.get(f"{GATEWAYS[target_node]}/auth", headers=h))
    
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
        return JSONResponse(status_code=400, content={"error": f"Unknown node {node}"})
    try:
        resp = await async_http_client.post(f"{GATEWAYS[node]}/chaos/partition/{state}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        resp.raise_for_status()
        return {"node": node, "state": state}
    except Exception as e:
        logging.error(f"Partition {node} failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/chaos/redis/{state}")
async def chaos_redis(state: str):
    reqs = []
    for url in GATEWAYS.values():
        reqs.append(async_http_client.post(f"{url}/chaos/redis/{state}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}))
    await asyncio.gather(*reqs, return_exceptions=True)
    return {"redis_cluster": state}

@app.post("/chaos/invalidate")
async def chaos_invalidate():
    test_key = "a8f5f167f44f4964e6c998dee827110c"
    metrics = await fetch_metrics()
    available_nodes = [name for name, data in metrics.items() if data.get('status') != 'offline' and not data.get('is_partitioned')]
    
    if not available_nodes:
        return JSONResponse(status_code=500, content={"error": "No available nodes"})
        
    target_node = random.choice(available_nodes)
    
    try:
        resp = await async_http_client.post(f"{GATEWAYS[target_node]}/chaos/invalidate/{test_key}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        resp.raise_for_status()
        return {"status": "invalidated", "node": target_node}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/chaos/db/{state}")
async def chaos_db(state: str):
    if state not in ["up", "down"]:
        return JSONResponse(status_code=400, content={"error": f"Invalid db state: {state}"})
    try:
        resp = await async_http_client.post(f"{POLICY_DB}/chaos/db/{state}")
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to change DB state: {e}")
        return JSONResponse(status_code=500, content={"error": f"Failed to change DB state: {e}"})

    if state == "up":
        tasks = [async_http_client.post(f"{url}/chaos/cb/force/CLOSED", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}) for url in GATEWAYS.values()]
        resps = await asyncio.gather(*tasks, return_exceptions=True)
        for url, r in zip(GATEWAYS.values(), resps):
            if isinstance(r, Exception) or (hasattr(r, "status_code") and r.status_code >= 400):
                logging.warning(f"Failed to force CLOSED on {url}: {r}")
    elif state == "down":
        tasks = [async_http_client.post(f"{url}/chaos/cb/force/OPEN", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}) for url in GATEWAYS.values()]
        resps = await asyncio.gather(*tasks, return_exceptions=True)
        for url, r in zip(GATEWAYS.values(), resps):
            if isinstance(r, Exception) or (hasattr(r, "status_code") and r.status_code >= 400):
                logging.warning(f"Failed to force OPEN on {url}: {r}")
    return {"db_state": state}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("control_plane:app", host='0.0.0.0', port=8080)
