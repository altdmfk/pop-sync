import asyncio
from fastapi import FastAPI, HTTPException

app = FastAPI()

# Chaos state
is_down = False

@app.get("/policy/{key}")
async def get_policy(key: str, sid: str = "", res: str = ""):
    global is_down
    if is_down:
        # Simulate hang or failure
        await asyncio.sleep(2)
        raise HTTPException(status_code=503, detail="DB Outage")
    
    # Simulate DB latency
    await asyncio.sleep(0.02)
    
    if res == "restricted" or sid.endswith("/deny"):
        return {"status": "deny", "role": "none"}
        
    return {"status": "allow", "role": "admin"}

@app.post("/chaos/db/{state}")
async def set_db_state(state: str):
    global is_down
    is_down = (state == "down")
    return {"is_down": is_down}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
