import json
import logging
from fastapi import FastAPI
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    global async_http_client
    async_http_client = httpx.AsyncClient(timeout=5.0)
    yield
    await async_http_client.aclose()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def get_index():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "static", "index.html")
    with open(file_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

import random

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
    try:
        resp = await async_http_client.post(f"{GATEWAYS['Node-A']}/chaos/invalidate/{test_key}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        resp.raise_for_status()
        return {"status": "invalidated"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/chaos/db/{state}")
async def chaos_db(state: str):
    await async_http_client.post(f"{POLICY_DB}/chaos/db/{state}")
    if state == "up":
        tasks = [async_http_client.post(f"{url}/chaos/cb/reset", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN}) for url in GATEWAYS.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
    elif state == "down":
        headers = {"Authorization": "Bearer test_token", "X-SPIFFE-ID": "spiffe://example.org/test"}
        tasks = []
        for _ in range(5):
            h = headers.copy()
            h["X-Nonce"] = str(uuid.uuid4())
            target_node = random.choice(list(GATEWAYS.keys()))
            tasks.append(async_http_client.get(f"{GATEWAYS[target_node]}/auth", headers=h))
        await asyncio.gather(*tasks, return_exceptions=True)
    return {"db_state": state}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("control_plane:app", host='0.0.0.0', port=8080)
