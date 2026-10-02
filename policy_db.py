import asyncio
from fastapi import FastAPI, HTTPException

import os

CHAOS_DOWN_SLEEP_SEC = 2.0
SIMULATED_DB_LATENCY_SEC = float(os.getenv("SIMULATED_DB_LATENCY_SEC", "0.02"))

app = FastAPI()

# Chaos state
is_down = False
is_down_lock = asyncio.Lock()

@app.get("/policy/{key}")
async def get_policy(key: str, sid: str = "", res: str = ""):
    global is_down
    async with is_down_lock:
        current_down = is_down
        
    if current_down:
        await asyncio.sleep(CHAOS_DOWN_SLEEP_SEC)
        raise HTTPException(status_code=503, detail="DB Outage")
    
    await asyncio.sleep(SIMULATED_DB_LATENCY_SEC)
    
    if res == "restricted" or sid.endswith("/deny"):
        return {"status": "deny", "role": "none"}
        
    return {"status": "allow", "role": "admin"}

# Hardcoded test public key (P-256)
TEST_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEK/FZtF232TXMUVkVdwFY/kwB5ixL
NDFx4B1gvVh9I0AfzjCRSGCkMWJT2xWt6HJkqbUN7wNGdVbQIj0HQFQczA==
-----END PUBLIC KEY-----"""

@app.get("/pubkey")
async def get_pubkey(sid: str = ""):
    if sid.endswith("/nokey"):
        raise HTTPException(status_code=404, detail="Key not found")
    # Simulate DB latency
    await asyncio.sleep(SIMULATED_DB_LATENCY_SEC)
    return {"public_key": TEST_PUBLIC_KEY}

@app.post("/chaos/db/{state}")
async def set_db_state(state: str):
    global is_down
    async with is_down_lock:
        is_down = (state == "down")
        current_down = is_down
    return {"is_down": current_down}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
