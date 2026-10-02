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
