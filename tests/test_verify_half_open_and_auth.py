import httpx
import asyncio

async def test_auth_and_half_open():
    async with httpx.AsyncClient() as client:
        print("=== 1. 인증 흐름 검증 ===")
        # 1-1. 잘못된 토큰으로 호출 -> 401
        r_bad = await client.post("http://localhost:8080/chaos/ddos", headers={"X-Admin-Token": "wrong-token"})
        print(f"Wrong token test: status={r_bad.status_code}, response={r_bad.json()}")
        assert r_bad.status_code == 401

        # 1-2. 올바른 토큰으로 호출 -> 200
        r_good = await client.post("http://localhost:8080/chaos/ddos", headers={"X-Admin-Token": "super-secret-admin"})
        print(f"Correct token test: status={r_good.status_code}, flooded={r_good.json().get('status')}")
        assert r_good.status_code == 200

        # 1-3. stream 엔드포인트 토큰 검증
        r_stream_bad = await client.get("http://localhost:8080/metrics/stream?token=wrong-token")
        print(f"Stream wrong token test: status={r_stream_bad.status_code}")
        assert r_stream_bad.status_code == 401

        print("\n=== 2. HALF_OPEN 강제 주입 및 can_execute 검증 ===")
        # 2-1. HALF_OPEN 강제 주입
        r_force = await client.post("http://localhost:8001/chaos/cb/force/HALF_OPEN", headers={"X-Admin-Token": "super-secret-admin"})
        print(f"Force HALF_OPEN: status={r_force.status_code}, body={r_force.json()}")
        assert r_force.status_code == 200
        assert r_force.json().get("status") == "HALF_OPEN"

        # 2-2. /metrics로 상태 확인
        r_metrics = await client.get("http://localhost:8001/metrics", headers={"X-Admin-Token": "super-secret-admin"})
        state = r_metrics.json()["stats"]["circuit_state"]
        print(f"Node-A circuit state after force HALF_OPEN: {state}")
        assert state == "HALF_OPEN"

        # 2-3. 복구 상태에서 정상 성공 요청 1건 전송하여 CLOSED 전환 확인
        await client.post("http://localhost:8080/chaos/db/up", headers={"X-Admin-Token": "super-secret-admin"})
        r_probe = await client.get("http://localhost:8001/auth", headers={
            "Authorization": "Bearer test",
            "X-Nonce": "half-open-test-nonce-1",
            "X-SPIFFE-ID": "spiffe://example.org/half_open_tester"
        })
        print(f"Probe request in HALF_OPEN: status={r_probe.status_code}, response={r_probe.json()}")

        r_metrics_after = await client.get("http://localhost:8001/metrics", headers={"X-Admin-Token": "super-secret-admin"})
        state_after = r_metrics_after.json()["stats"]["circuit_state"]
        print(f"Node-A circuit state after successful probe: {state_after} (Expected: CLOSED)")
        assert state_after == "CLOSED"

        print("\nALL VERIFICATIONS PASSED!")

if __name__ == "__main__":
    asyncio.run(test_auth_and_half_open())
