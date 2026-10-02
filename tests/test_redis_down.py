import asyncio
import httpx
import uuid
import sys

GATEWAY_URL = "http://localhost:8001/auth"
client_id = f"spiffe://burst/{uuid.uuid4()}"

async def main():
    headers = {
        "Authorization": "Bearer super-token",
        "X-SPIFFE-ID": client_id,
    }
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        async def make_request(idx):
            req_headers = headers.copy()
            req_headers["X-Nonce"] = str(uuid.uuid4())
            try:
                resp = await client.get(GATEWAY_URL, headers=req_headers)
                return f"Request {idx}: Status {resp.status_code} - Body: {resp.text}"
            except Exception as e:
                return f"Request {idx} failed: {e}"

        tasks = [make_request(i+1) for i in range(20)]
        results = await asyncio.gather(*tasks)
        for res in results:
            print(res)

if __name__ == "__main__":
    asyncio.run(main())
