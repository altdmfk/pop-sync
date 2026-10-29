"""
High-Precision Dummy Backend Server for PoP-Sync and Envoy Benchmarks.

Provides a minimal, zero-noise FastAPI backend to isolate L7 proxy and ext_authz overhead.
Features:
- Pure Overhead Mode (`mode=overhead`): Immediate 200 OK response.
- Simulated I/O Mode (`mode=db_sim`): 20ms database I/O simulation with peak concurrency tracking.
- Internal Metrics & Reset Endpoints (`/internal/metrics`, `/internal/reset`).
"""

import asyncio
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI(title="Benchmark Mock Backend", version="1.0.0")

# Thread/Asyncio safe state metrics
_state_lock = asyncio.Lock()
_total_requests_reached: int = 0
_simulated_db_queries_executed: int = 0
_current_concurrency: int = 0
_peak_concurrency: int = 0

DEFAULT_BENCH_MODE: str = os.getenv("DEFAULT_BENCH_MODE", "overhead")
SIMULATED_DB_LATENCY_SEC: float = float(os.getenv("SIMULATED_DB_LATENCY_SEC", "0.02"))


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Lightweight health check endpoint for proxy and orchestrator liveness probes."""
    return {"status": "healthy"}


@app.get("/api/v1/posts/{post_id}")
async def get_post(
    post_id: str,
    request: Request,
    mode: Optional[str] = Query(None),
    x_bench_mode: Optional[str] = Header(None, alias="X-Bench-Mode"),
    x_auth_source: Optional[str] = Header(None, alias="X-Auth-Source"),
) -> Response:
    """Core business endpoint for measuring proxy and authorization overhead.

    Switches between:
    - overhead: Immediate 200 OK without simulated delay.
    - db_sim: Simulates 20ms database I/O while tracking concurrency.
    """
    global _total_requests_reached, _simulated_db_queries_executed
    global _current_concurrency, _peak_concurrency

    # Effective mode precedence: Header > Query Parameter > Environment Default
    effective_mode: str = x_bench_mode or mode or DEFAULT_BENCH_MODE

    if effective_mode == "db_sim":
        async with _state_lock:
            _total_requests_reached += 1
            _simulated_db_queries_executed += 1
            _current_concurrency += 1
            if _current_concurrency > _peak_concurrency:
                _peak_concurrency = _current_concurrency

        try:
            # Simulate real database query latency (20ms)
            await asyncio.sleep(SIMULATED_DB_LATENCY_SEC)
        finally:
            async with _state_lock:
                _current_concurrency -= 1

        return JSONResponse(
            status_code=200,
            content={
                "id": post_id,
                "status": "ok",
                "mode": "db_sim",
                "auth_source": x_auth_source or "none",
            },
        )

    # Pure Overhead Mode: zero-wait immediate return
    async with _state_lock:
        _total_requests_reached += 1

    return JSONResponse(
        status_code=200,
        content={
            "id": post_id,
            "status": "ok",
            "mode": "overhead",
            "auth_source": x_auth_source or "none",
        },
    )


@app.get("/internal/metrics")
async def get_internal_metrics(reset: bool = Query(False)) -> Dict[str, Any]:
    """Exposes internal backend execution metrics with optional atomic reset."""
    global _total_requests_reached, _simulated_db_queries_executed
    global _current_concurrency, _peak_concurrency

    async with _state_lock:
        data: Dict[str, Any] = {
            "total_requests_reached": _total_requests_reached,
            "simulated_db_queries_executed": _simulated_db_queries_executed,
            "current_concurrency": _current_concurrency,
            "peak_concurrency": _peak_concurrency,
        }
        if reset:
            _total_requests_reached = 0
            _simulated_db_queries_executed = 0
            _current_concurrency = 0
            _peak_concurrency = 0

    return data


@app.post("/internal/reset")
async def reset_metrics() -> Dict[str, Any]:
    """Explicitly resets all metrics counters between benchmark iterations."""
    global _total_requests_reached, _simulated_db_queries_executed
    global _current_concurrency, _peak_concurrency

    async with _state_lock:
        _total_requests_reached = 0
        _simulated_db_queries_executed = 0
        _current_concurrency = 0
        _peak_concurrency = 0

    return {"status": "reset", "metrics": {"total_requests_reached": 0, "peak_concurrency": 0}}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, access_log=False)
