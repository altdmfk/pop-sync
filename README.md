# PoP-Sync

네트워크 파티션, Redis 장애, 대규모 DDoS 상황에서 분산 게이트웨이의 복원력을 실시간으로 시연하는 카오스 엔지니어링 데모다.

---

## 주요 기능

### 서킷 브레이커
Policy DB 장애 감지 시 OPEN 상태로 전환하여 연쇄 장애를 차단한다. 2.0초 디바운스로 상태 진동을 제어하며, CLOSED -> OPEN -> HALF_OPEN -> CLOSED 사이클을 안정적으로 관리한다.

### L1/L2 캐시 및 단일 비행
정책 조회는 다단계로 동작한다. 노드 로컬의 L1(OrderedDict LRU, 지연 만료)을 확인하고, 없으면 중앙 Redis(L2)를 조회한다. 모두 캐시 미스일 때 Policy DB를 호출한다.
`SINGLEFLIGHT_ENABLED` 및 `CLUSTER_SINGLEFLIGHT_ENABLED`가 기본 활성화되어 있다. 노드 내부의 `in_flight` Future 병합뿐 아니라, Redis 분산 락(`SET NX lock:sf:{key}`)과 L2 폴링 메커니즘을 통해 클러스터 전역에서도 동일 키 동시 DB 호출을 1회로 병합하여 백엔드 DB 과부하를 방지한다 (효과 검증: docs/benchmarks/singleflight-effect.md).

### 스트림 기반 무효화 동기화
네트워크 단절 발생 시 지연되었던 Redis Stream 대기 메시지(PEL)를 누락분부터 연속 적용한다. 이를 통해 최종 일관성을 유지한다.

### O(1) 유량 제한 및 Nonce 리플레이 방어
`RL_ALGORITHM` 설정으로 유량 제한 방식을 결정한다(기본값 `sliding`). `sliding` 방식은 Lua 스크립트 기반 가중 합산 근사치로 동작하며, `fixed` 방식은 Redis Pipeline을 사용한다 (경계 버스트 측정 결과: docs/benchmarks/rate-limit-sliding.md). 유량 제한은 노드 단위가 아닌 클라이언트 ID(`SPIFFE ID`) 기준의 클러스터 전역 한도를 적용한다.
Lua 스크립트 기반 원자적 Nonce 소진으로 DDoS와 리플레이 공격을 방어한다.

### PoP 검증 (Scope B)
FIDO2 모델을 간소화한 ECDSA P-256 서명 검증을 지원한다(`POP_VERIFY_ENABLED` 기본값 `False`). Policy DB에서 공개키를 조회하여 노드 캐시(L1/L2)에 보관한다. 리플레이 방지를 위해 Nonce 검증 통과 후에 서명 검증을 수행한다. 통계 분류를 위해 서명 누락과 검증 실패 카운터를 별도로 집계한다.

---

## 신뢰 경계 및 계승

본 게이트웨이는 클라이언트 식별과 유량 제한에 `X-SPIFFE-ID` HTTP 헤더를 사용한다.

- **mTLS 위임**: mTLS 인증은 상위 L4/L7 리버스 프록시(Envoy 등)에서 처리한다고 가정한다. 프록시가 클라이언트 인증서를 검증하고 `X-SPIFFE-ID` 헤더를 주입하는 구조다.
- **데모 스코프**: 프록시 계층 구현과 mTLS 인증서 파싱은 게이트웨이 범위에 포함하지 않는다. 테스트 환경에는 단일 데모 키만 등록되어 있다. 프록시가 신뢰 경계를 보장한다는 전제 아래 분산 시스템 복원력에 집중한다.

### fido2-pop-gateway 및 pop-mesh 대비 변경 사항
| 계승 / 변경 / 미이식 | 원본 구현체 | 현재 코드베이스 (PoP-Sync) |
| --- | --- | --- |
| **언어/프레임워크** | Java / Reactor | Python / FastAPI |
| **인메모리 캐시** | Caffeine | OrderedDict LRU (지연 만료) |
| **무효화 전송 계층** | Redis Pub/Sub | Redis Streams (XREADGROUP 기반 PEL 복구 지원) |
| **유량 제한** | Sliding Window Log | 가중 합산 근사치 기반 슬라이딩 카운터 |
| **PoP 서명** | WebAuthn CBOR Envelope | ECDSA P-256 (Method, Path, Query, Body Digest, SPIFFE ID, Nonce 정규화 바인딩, Scope B) |

---

## 요청 처리 파이프라인 (`/auth` 단계별 동작 흐름)

클라이언트가 `GET /auth` 엔드포인트를 호출하면 게이트웨이는 아래 **6단계 파이프라인**을 순서대로 수행한다. 어느 단계에서든 요청이 종료되면 해당 분류 카운터가 1 증가한다. 전체 유입 요청 수(`requests`)와 세부 카운터 합계는 항상 일치한다.

```text
[클라이언트 GET /auth 요청]
   │
   ├─ 0. 미들웨어 격리 검사 ── (노드 파티션 상태) ──> 503 Node Isolated (진입 전 차단, requests 미집계)
   │
   ├─ 1. 필수 헤더 검증 ────── (Authorization / X-Nonce / X-SPIFFE-ID 누락) ──> 401 Missing Headers
   │
   ├─ 2. 유량 제어
   │     ├─ Redis 정상: 클러스터 전역 Sliding/Fixed (50 req / 10s) 초과 ──> 429 Too Many Requests (rate_limited +1)
   │     └─ Redis 장애: 노드 로컬 슬라이딩 윈도우 (15 req / 10s) 초과 ───> 429 Too Many Requests (rate_limited +1)
   │
   ├─ 3. 원자적 Nonce 소진 (※ 고비용 암호 연산 전에 실행하여 Nonce 즉시 소진)
   │     ├─ Redis 장애/타임아웃 (분산 락 불가 → 데이터 오염 방지) ─────────> 503 Fail-Closed (redis_outage_blocked +1)
   │     └─ 중복 Nonce (EXISTS == 1) ────────────────────────────────────> 401 Replay Detected (nonce_replays +1)
   │
   ├─ 4. PoP 서명 검증 (POP_VERIFY_ENABLED=true 시 활성화)
   │     ├─ X-PoP-Signature 헤더 누락 ───────────────────────────────────> 400 Missing PoP Signature (pop_rejected +1)
   │     ├─ X-Body-Digest 접두사 누락 / 바디 다이제스트 불일치 ──────────> 400 Bad Request (pop_rejected +1)
   │     ├─ 공개키 조회 (L1 → L2 → Policy DB /pubkey)
   │     │     ├─ 서킷 브레이커 OPEN 상태 ───────────────────────────────> 503 Key Fetch Failed [Fail-Closed] (circuit_blocked +1, pubkey_circuit_blocked +1, pop_rejected +1)
   │     │     ├─ 미등록 SPIFFE ID (DB 404) ─────────────────────────────> 401 Unknown Key ID (pop_rejected +1)
   │     │     └─ 공개키 획득 성공 (L1/L2 적재)
   │     ├─ 정규화 서명 검증 (Method + Path + Query + BodyDigest + SPIFFE + Nonce)
   │     │     └─ 서명 불일치 (InvalidSignature) ────────────────────────> 401 Invalid PoP Signature (pop_rejected +1)
   │     └─ 서명 검증 성공 ──────────────────────────────────────────────> pop_verified +1 후 다음 단계 진행
   │
   ├─ 5. 다단계 캐시 조회 (복합 키: SHA-256("len:sid" + "len:token" + "len:res" + "len:act"))
   │     ├─ L1 (로컬 OrderedDict LRU, TTL 60s) 히트 ─────────────────────> 200 OK [source: "L1"] (l1_hits +1)
   │     └─ L2 (중앙 Redis, TTL 60s) 히트 (L1 승격) ────────────────────> 200 OK [source: "L2"] (l2_hits +1)
   │
   └─ 6. Policy DB 폴백 및 단일 비행 (L1/L2 모두 캐시 미스 → db_fallbacks +1)
         ├─ 동일 키 In-Flight 존재 (Single-Flight Follower 합류) ──────────> Leader Future 완료 대기 후 반환 (singleflight_coalesced +1)
         ├─ 서킷 브레이커 OPEN 차단 ─────────────────────────────────────> 200 OK [status: "degraded_allow", source: "Fallback_Degradation"] (circuit_blocked +1)
         └─ DB Loader 실행 (Leader)
               ├─ 태스크 진입 직후 L1 더블체크 히트 ─────────────────────> 200 OK [source: "L1"] (l1_double_check_hits +1)
               ├─ 클러스터 전역 조율 (`CLUSTER_SINGLEFLIGHT_ENABLED=true`)
               │     ├─ L2 적재 선행 확인 히트 ─────────────────────────> 200 OK [source: "L2"] (singleflight_coalesced +1)
               │     ├─ Cluster Follower (Redis `SET NX` 락 획득 실패) ──> L2 폴링(25ms) 대기 후 적재 감지 즉시 반환 (singleflight_coalesced +1)
               │     │     └─ Cluster Leader 크래시/락 만료 시 ──────────> 후속 Follower가 락 인계 후 DB 조회
               │     └─ Cluster Leader (Redis `SET NX` 락 획득 성공) ────> Policy DB 실제 쿼리 수행
               ├─ DB 세마포어(MAX_DB_WORKERS=5) 0.1s 내 획득 실패 ───────> 503 DB Overload [Fast-Fail] (db_queries_executed +1)
               ├─ Policy DB 응답 성공 (L1/L2 적재, 락 안전 해제, cb.record_success) ───> 200 OK [status: "allow"|"deny", source: "DB"] (db_queries_executed +1)
               └─ Policy DB 타임아웃/503 (cb.record_failure, 락 안전 해제) ───> 200 OK [status: "degraded_allow", source: "Fallback_Degradation_PostFail"] (db_queries_executed +1)
```

---

## 카오스 엔지니어링 및 복원력 시나리오 상세

클러스터를 직접 구동하지 않더라도 각 시나리오의 주입 방법, 내부 메커니즘, HTTP/JSON 응답, 메트릭 변화를 파악할 수 있도록 9가지 시나리오를 상세히 기술한다.

### 전체 시나리오 요약 매트릭스

| # | 시나리오명 | 트리거 (API / 설정) | 핵심 방어 메커니즘 | 기대 HTTP 응답 및 상태 | 핵심 관측 메트릭 |
| :-: | :--- | :--- | :--- | :--- | :--- |
| **1** | **중복 요청 및 리플레이 방어** | `POST /chaos/nonce` | Redis Lua 원자적 Nonce 소진 (`NONCE_LUA`) | 1개 노드 `200 OK`, 다른 노드 `401 Replay Detected` (Redis 장애 시 양쪽 `503`) | `nonce_replays +1` (장애 시 `redis_outage_blocked +1`) |
| **2** | **대규모 DDoS 및 경계 버스트 방어** | `POST /chaos/ddos` (60건 동시 폭주) | Check-Before-Increment O(1) 가중 합산 슬라이딩 윈도우 (`50 req / 10s`) | 첫 실행 `200` 50건 / `429` 10건, 연속 실행 시 `429` 60건 차단 | `rate_limited` 증가, `Retry-After: 10` 헤더 |
| **3** | **동일 키 폭주 및 클러스터 전역 단일 비행 병합** | `POST /chaos/same_key_burst` (`count=50`) | Redis 분산 락(`SET NX`) 기반 Leader 선출 + Follower L2 폴링 및 In-Flight Future 병합 (`asyncio.shield`) | 전 노드 요청 모두 `200 OK`, **클러스터 전체에서 실제 DB 쿼리는 1회만 실행** (노드별 중복 쿼리 방지) | 클러스터 전체 `db_queries_executed = 1`, `singleflight_coalesced = N - 1` |
| **4** | **백엔드 DB 장애 격리 및 서킷 브레이커** | `POST /chaos/db/down` → `/chaos/db/up` | 3-State 서킷 브레이커, HALF_OPEN 단일 프로브 원자적 게이팅, 2.0s 디바운스 | 캐시 히트는 `200 OK(L1/L2)`, 캐시 미스는 `200 OK(degraded_allow)` 폴백 | `circuit_state: OPEN`, `circuit_blocked` 증가 |
| **5** | **중앙 Redis 장애 및 로컬 생존 모드** | `POST /chaos/redis/down` → `/chaos/redis/up` | 로컬 Rate Limit 폴백(`15 req / 10s`) + Nonce `503 Fail-Closed` | 노드당 한도(15건) 초과분은 `429`, 통과분(15건)은 Nonce 단계에서 `503` 차단 | `local_rl_size` 활성화, `redis_outage_blocked` 증가, Invariant `PASS` |
| **6** | **네트워크 파티션 및 PEL 긴급 복구** | `POST /chaos/partition/Node-B/isolate` → `heal` | 고립 노드 `503` 차단(Split-Brain 방어) + 복구 시 Stream PEL(`"0"`) 드레인 및 XAUTOCLAIM 유휴 회수 | 단절 중 `/auth`는 `503 Node Isolated`, 복구 즉시 누락 무효화 일괄 반영 | `is_partitioned: true→false`, `last_catchup` 상세 지표 기록 |
| **7** | **실시간 캐시 무효화 스트림 전파** | `POST /chaos/invalidate` | Redis Streams `XADD` (`maxlen~10000`) + 노드별 `XREADGROUP` | 전 노드의 L1 및 L2 캐시에서 대상 정책 키 즉시 삭제 및 `XACK` | 전 노드 `l1_cache_size` 감소 후 재조회 시 `source: "DB"` |
| **8** | **ECDSA P-256 PoP 서명 검증** | `POP_VERIFY_ENABLED=true` | Nonce 선행 소진 후 Query/Body Digest 바인딩 ECDSA 검증 및 `/pubkey` 서킷 연동 | 정상 `200`, 서명 누락/바디 다이제스트 오류 `400`, 위변조/미등록 키 `401`, DB 서킷 OPEN 시 `503` | `pop_verified` / `pop_rejected` 분리 집계 |
| **9** | **정책 거부(DENY) 인가 검증** | `X-Res: restricted` 또는 `.../deny` | DB `status: "deny"` 명시적 판별 및 `"DENY"` 캐싱 | `200 OK` (`{"status": "deny", "policy": "DENY"}`) 반환 및 L1/L2 캐싱 | 후속 요청 시 L1/L2에서 `"DENY"` 즉시 반환 |

---

### 시나리오 1: 중복 요청 및 리플레이 방어
- **검증 목적**: 동일한 일회용 토큰(`X-Nonce`)을 가진 쓰기(`X-Act: write`) 요청이 두 게이트웨이 노드에 동시에 유입될 때, 클러스터 전역에서 1회만 승인되고 나머지는 차단되는지 검증한다.
- **실행 흐름 (`POST /chaos/nonce` / `tests/test_scenarios.py::test_nonce_atomicity`)**:
  1. Control Plane이 새로운 UUID `nonce`를 생성하여 두 게이트웨이(예: `Node-A`, `Node-B`)에 동일한 헤더(`X-Nonce`, `X-SPIFFE-ID: spiffe://example.org/workload`, `X-Act: write`)로 동시에 `GET /auth` 요청을 발송한다.
  2. 두 노드는 Redis에 등록된 Lua 스크립트(`NONCE_LUA`)를 호출한다.
  3. Redis는 단일 스레드로 스크립트를 원자적으로 실행하므로, 먼저 도착한 요청만 `EXISTS nonce:{uuid} == 0` 조건을 통과하여 키를 생성(`TTL 60s`)하고 `1`을 반환받는다.
  4. 동시에 도착한 다른 노드의 요청은 `EXISTS == 1`에 걸려 `0`을 반환받는다.
- **실행하지 않고 확인하는 결과**:
  - **정상 상태 (Redis UP)**:
    ```json
    {
      "nonce": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
      "results": {
        "Node-A": "Status: 200 ({'status': 'allow', 'source': 'DB', 'policy': 'admin'})",
        "Node-B": "Status: 401 ({'status': 'error', 'message': 'Replay Detected'})"
      }
    }
    ```
    → 한 노드는 `200 OK`, 다른 노드는 `401 Replay Detected`를 반환한다.
  - **Redis 장애 상태 (Redis DOWN)**:
    - 분산 조율이 불가능한 상태에서 요청을 통과시키면 이중 처리나 데이터 오염이 발생한다. 따라서 양쪽 노드 모두 **`503 Service Unavailable - Nonce Check Failed` (Fail-Closed)**로 안전하게 차단한다.
  - **메트릭 변화**: 승인된 노드의 `db_fallbacks`(또는 `l1_hits`/`l2_hits`) `+1`, 차단된 노드의 `nonce_replays` `+1`.

---

### 시나리오 2: 대규모 트래픽 및 경계 버스트 방어 (O(1) Sliding-Window Rate Limiter)
- **검증 목적**: 공격자가 다수 노드로 트래픽을 분산해도 클러스터 전역 한도(`50 req / 10s`)로 차단(`429`)하고, 고정 윈도우의 윈도우 경계 버스트를 방어하는지 검증한다.
- **실행 흐름 (`POST /chaos/ddos` / `tests/test_scenarios.py::test_rate_limiting`)**:
  1. Control Plane이 공격자 ID(`spiffe://example.org/attacker`)로 60개 요청(서로 다른 Nonce)을 생성하여 게이트웨이 노드들에 분산 전송한다.
  2. 각 노드는 `SLIDING_RL_LUA` 스크립트로 직전 윈도우 카운트(`prev`)와 현재 윈도우 카운트(`curr`)를 조회하고, 경과 시간(`elapsed`)에 따른 가중치 합산치를 계산한다:
     $$\text{estimated} = \left\lfloor \text{prev} \times \frac{10 - \text{elapsed}}{10} \right\rfloor + \text{curr}$$
  3. `estimated <= 50`(`RL_LIMIT`)이면 통과시키고, 초과하면 `429 Too Many Requests`를 반환한다.
- **실행하지 않고 확인하는 결과**:
  - **최초 실행 시 (60건 전송)**:
    ```json
    {
      "status": "flooded",
      "responses": {
        "200": 50,
        "429": 10
      }
    }
    ```
    → 요청이 분산 유입되어도 클러스터 전역 한도 50건만 허용되고 나머지 10건은 `429`로 차단된다.
  - **10초 이내 재실행 시**: 윈도우 한도가 소진되어 `{"status": "flooded", "responses": {"429": 60}}`으로 전건 차단된다.
  - **경계 버스트 실측 비교 (`docs/benchmarks/rate-limit-sliding.md`)**:
    - 윈도우 변경 0.5초 전 50건, 변경 직후 0.5초 후 50건(총 1초간 100건) 전송 시:
    - **`fixed` 모드**: 윈도우 리셋으로 직전 50건과 직후 50건이 모두 통과하여 총 100건이 허용된다.
    - **`sliding` 모드 (기본값)**: 직후 시점에 이전 윈도우 가중치(약 95%)가 유지되어 직전 50건 통과 후 50건은 차단된다. 총 50건만 통과하여 경계 스파이크를 방어한다.
  - **카운터 오염 방지 (Check-Before-Increment)**:
    `SLIDING_RL_LUA` 및 로컬 폴백(`_local_rate_limit`)은 카운터를 사전 증가시키지 않는다. 가중 합산치(`estimated`)를 먼저 검사하여 `estimated < limit`일 때만 원자적으로 카운터를 올린다. 대량 요청이 차단되어도 윈도우 카운터가 증가하지 않으므로, 공격 종료 후 다음 윈도우($W_1$)에서 정상 트래픽이 기아 현상 없이 수용된다.

---

### 시나리오 3: 동일 키 동시 폭주 방어
- **검증 목적**: 캐시 만료 직후나 신규 키에 대해 동일 요청이 유입될 때 백엔드 Policy DB 중복 조회를 방지하고 1회 쿼리로 병합하는지 검증한다.
- **실행 흐름 (`POST /chaos/same_key_burst?node=Node-A&count=50` / `tests/test_singleflight.py`)**:
  1. Control Plane이 동일 키(동일한 `SPIFFE-ID`, `Token`, `Resource`, `Action`)를 가진 요청 50개를 단일 노드 또는 3개 노드에 동시 발송한다.
  2. L1과 L2 캐시 미스가 발생하면 게이트웨이는 `db_loader_task`를 호출한다.
  3. **노드 로컬 단일 비행**:
     - **최초 요청**: `in_flight[comp_key]`에 `asyncio.Future`를 등록하고 `db_loader_task`를 실행하여 Policy DB에 요청을 보낸다(`db_queries_executed +1`).
     - **후속 요청**: `in_flight`의 Future를 확인하고 DB 호출 없이 `await asyncio.shield(fut)`로 대기한다(`singleflight_coalesced +1`).
     - Leader 클라이언트가 연결을 중단해도(`CancelledError`), Single-Flight 활성화(`SINGLEFLIGHT_ENABLED=true`) 시에는 `asyncio.shield`로 보호되어 후속 요청이 정상 응답을 수신한다. 단일 비행 비활성화(`SINGLEFLIGHT_ENABLED=false`) 시에는 불필요한 백엔드 자원 낭비를 막기 위해 DB 조회가 즉시 중단된다.
  4. **클러스터 전역 단일 비행 (`CLUSTER_SINGLEFLIGHT_ENABLED=true`)**:
     - 동일 키 요청이 여러 노드로 분산 유입되어도 Redis 분산 락(`SET NX lock:sf:{key}`)을 선점한 노드만 Policy DB를 조회한다.
     - 락을 얻지 못한 Follower 노드는 L2 캐시를 폴링(기본 25ms 간격)하여 결과를 회수한다.
     - Leader 노드가 비정상 종료되면 Follower 노드가 락을 인계받아 DB 조회를 수행한다.
     - `docs/benchmarks/singleflight-effect.md` 실측 결과, 3개 노드 클러스터에서 동일 키 요청 90건 동시 유입 시 DB 조회가 3회에서 1회로 감소했다.
- **실행하지 않고 확인하는 결과**:
  ```json
  {
    "total_requests": 50,
    "db_queries": 1,
    "coalesced": 49,
    "duration_ms": 12.4
  }
  ```
  → 50개 요청이 동시에 도달해도 DB 쿼리는 1회만 발생하며, 나머지 49개 요청은 첫 번째 쿼리 결과를 공유받아 모두 정상 응답을 받는다.

---

### 시나리오 4: 백엔드 DB 장애 격리 및 서킷 브레이커
- **검증 목적**: Policy DB 장애 시 게이트웨이 스레드 풀이 타임아웃 대기로 고갈되는 연쇄 장애와 상태 진동을 방지하고, 장애 복구 시 안전하게 복귀하는지 검증한다.
- **실행 흐름 (`POST /chaos/db/down` → `/chaos/db/up` / `tests/test_scenarios.py::test_circuit_breaker_trip`)**:
  1. **DB 장애 발생 및 `OPEN` 전환**:
     - Policy DB 장애 시 5회 연속 실패(`CB_FAILURE_THRESHOLD = 5`)가 발생하면 서킷 브레이커는 즉시 `OPEN` 상태로 전환된다.
  2. **`OPEN` 상태에서의 요청 처리**:
     - 캐시 히트(L1/L2) 요청은 정상 응답(`source: "L1"` 또는 `"L2"`)을 반환한다.
     - 캐시 미스 요청은 DB 조회를 차단하고 즉시 저하 허용(`200 OK`, `status: "degraded_allow"`, `source: "Fallback_Degradation"`)으로 응답한다.
     - PoP 공개키 조회 캐시 미스(`/pubkey`)의 경우, 미인가 접근을 막기 위해 저하 허용 대신 **`503 Service Unavailable` [Fail-Closed]**로 안전하게 차단한다.
  3. **`HALF_OPEN` 원자적 단일 프로브 및 자동 복구 (`CB_RESET_TIMEOUT_SEC = 10s`)**:
     - 마지막 실패로부터 10초가 지나면 서킷 브레이커는 `HALF_OPEN` 상태로 전이된다.
     - 동시 유입된 다수의 캐시 미스 요청 중 단 1개의 요청만 프로브로 선점되어 Policy DB 조회를 시도한다. 후속 요청들은 대기 없이 즉시 `degraded_allow`로 처리되어 백엔드 중복 조회를 방지한다.
     - **성공 시**: `cb.record_success()`가 호출되어 즉시 `CLOSED` 상태로 복구되고(실패 카운트 리셋, 프로브 슬롯 반환), 정상 응답을 반환한다.
     - **실패 시**: `HALF_OPEN` 상태에서는 디바운스 없이 단 1회 실패로 즉시 다시 `OPEN` 상태로 전이된다.
  4. **상태 진동 방지 2.0초 디바운스 (`CB_DEBOUNCE_SEC = 2.0s`)**:
     - 서킷 브레이커는 `CLOSED` 상태 전이 직후 2.0초 이내에 도착하는 지연 실패 신호를 무시한다. 복구 직후 서킷이 다시 `OPEN`으로 튀지 않고 안정적으로 `CLOSED`를 유지한다.
  5. **세마포어 벌크헤드 (`MAX_DB_WORKERS = 5`)**:
     - `sync` 모드에서 동시 DB 쿼리가 5개를 초과하여 `0.1초`(`DB_SEMAPHORE_ACQUIRE_TIMEOUT`) 내에 워커 슬롯을 얻지 못하면 `DBOverloadError`를 발생시켜 `503 Service Unavailable - DB Overload`로 Fast-Fail 처리한다.

---

### 시나리오 5: 중앙 Redis 전면 장애 및 로컬 생존 모드
- **검증 목적**: Redis가 중단되었을 때 가용성(Rate Limit 로컬 폴백, L1 캐시)과 보안 정합성(Nonce 검증 Fail-Closed)을 차등 적용하는지 검증한다.
- **실행 흐름 (`POST /chaos/redis/down` / `tests/test_redis_down.py`, `tests/test_rl_fallback.py`)**:
  1. `/chaos/redis/down` 호출 시 각 게이트웨이는 Redis 클라이언트를 응답하지 않는 더미 주소(`127.0.0.1:9999`, 소켓 타임아웃 `0.1s`)로 교체한다. 처리 중이던 인플라이트 요청 보호를 위해 기존 클라이언트는 2.0초 대기 후 종료한다.
  2. **1단계 유량 제한 (로컬 메모리 폴백)**:
     - Redis 연결 실패 시 즉시 `_local_rate_limit()`으로 전환한다.
     - 로컬 메모리(`OrderedDict`, 최대 10,000키 LRU 축출)에서 가중 합산치를 계산하여 노드당 10초에 15건(`RL_FALLBACK_LIMIT = 15`)까지만 허용하고 초과분은 `429 Too Many Requests`로 차단한다.
  3. **2단계 Nonce 리플레이 검사 (`503 Fail-Closed`)**:
     - 1단계를 통과한 요청은 `consume_nonce()` 단계에 도달한다.
     - Nonce 검사는 중복 실행 방지를 위해 중앙 Redis가 필수이므로, 통과시키지 않고 **`503 Service Unavailable - Nonce Check Failed` (Fail-Closed)**를 반환하며 `redis_outage_blocked` 카운터를 증가시킨다.
- **실행하지 않고 확인하는 결과 (`docs/benchmarks/rate-limit-sliding.md`)**:
  - **단일 노드 20개 요청 동시 전송 시 (`tests/test_redis_down.py`)**:
    - 로컬 한도(15건) 초과분 5건은 1단계에서 `429 Too Many Requests`로 차단된다(`rate_limited += 5`).
    - 로컬 한도 내 15건은 2단계 Nonce 검사에서 `503 Service Unavailable - Nonce Check Failed`로 차단된다(`redis_outage_blocked += 15`).
  - **3개 노드 90개 요청 라운드로빈 전송 시 (`tests/test_rl_fallback.py`)**:
    - **Redis 정상 시**: 클러스터 전역 한도 50건 적용 → `200 OK`: 50건, `429`: 40건.
    - **Redis 다운 시**: 노드 3개가 로컬 한도 15건씩 허용(총 45건 Nonce 503 차단) 및 15건씩 차단 → `503`: 45건, `429`: 45건.
  - **대시보드 검증 결과**:
    - Nonce 동시 요청 테스트는 `503 Fail-Closed`, DDoS 테스트는 `429 방어`로 표시되며, 모든 `503` 응답이 `redis_outage_blocked`에 합산되어 Invariant는 `Pass`를 유지한다.

---

### 시나리오 6: 네트워크 격리 및 Redis Streams PEL 긴급 복구
- **검증 목적**: 특정 노드가 네트워크 파티션으로 고립되었을 때 Split-Brain을 막고, 복구 시 고립 기간 동안 누락된 캐시 무효화 이벤트를 반영하여 최종 일관성을 유지하는지 검증한다.
- **실행 흐름 (`POST /chaos/partition/Node-B/isolate` → `/chaos/invalidate` → `/chaos/partition/Node-B/heal` / `tests/test_scenarios.py::test_partition_catchup`)**:
  1. **초기 캐시 적재**: `Node-B`에 요청을 보내 L1 캐시에 정책 데이터를 적재한다(예: `l1_cache_size = 5`).
  2. **노드 격리 (`isolate`)**:
     - `Node-B`의 `is_partitioned` 플래그가 `True`로 전환된다.
     - 격리된 동안 `Node-B`로 유입되는 모든 `/auth` 요청은 미들웨어에서 **`503 Service Unavailable` (`{"status": "error", "message": "Node Isolated"}`)**로 즉시 거부되어 오염된 L1 캐시 사용을 차단한다.
     - `Node-B`의 백그라운드 `stream_listener`도 스트림 읽기를 일시 중단한다.
  3. **고립 중 정책 무효화 발생 (`POST /chaos/invalidate` 4회 실행)**:
     - 가용 노드(`Node-A` 또는 `Node-C`)가 Redis Stream(`invalidation_stream`)에 무효화 이벤트 4건을 발행한다.
     - 가용 노드들은 즉시 수신하여 L1 캐시를 비우지만, 고립된 `Node-B`는 수신하지 못해 `l1_cache_size`가 이전 상태로 남는다.
  4. **네트워크 복구 (`heal`) 및 3단계 동기화**:
     - `/chaos/partition/Node-B/heal` 호출 시 `is_partitioned = False`로 해제되고 DB 상태 확인 백그라운드 태스크가 실행된다.
     - `stream_listener`가 복구를 감지하고 긴급 동기화 루프를 실행한다:
       - **Step A — 자체 미확인 메시지(PEL) 처리 (`ID = "0"`)**: 단절 직전 전달되었으나 `XACK`하지 못한 메시지들을 `XREADGROUP ... COUNT 100`으로 빈 배치가 반환될 때까지 반복 조회하여 캐시를 삭제하고 `XACK`한다.
       - **Step B — 장기 유휴 메시지 자동 회수 (`XAUTOCLAIM`)**: 다른 컨슈머에서 장시간(`STREAM_CLAIM_MIN_IDLE_MS`, 기본 60,000ms) 미확인 상태로 방치된 메시지를 `XAUTOCLAIM`으로 인계받아 캐시 삭제 및 `XACK`를 수행한다.
       - **Step C — 단절 기간 누적 신규 메시지 처리 (`ID = ">"`)**: 단절 동안 스트림에 누적된 메시지들을 `XREADGROUP ... COUNT 100`으로 빈 배치가 반환될 때까지 반복 조회하여 캐시 삭제 및 `XACK`를 완료한다.
- **실행하지 않고 확인하는 결과**:
  - 복구 후 `Node-B`의 `/metrics`를 조회하면 `stats.last_catchup`에 동기화 결과가 기록된다:
    ```json
    "last_catchup": {
      "pending_count": 4,
      "pel_drained": 2,
      "autoclaimed": 1,
      "new_drained": 1,
      "duration_ms": 8,
      "timestamp": 1729320123.456
    }
    ```
  - 무효화 대상 키들이 `Node-B`의 L1 캐시에서 삭제되어 클러스터 캐시 일관성이 복원된다.

---

### 시나리오 7: 실시간 캐시 무효화 스트림 전파
- **검증 목적**: 관리자가 특정 정책이나 공개키를 무효화할 때 클러스터 내 모든 정상 게이트웨이 노드의 L1/L2 캐시가 실시간 동기화 삭제되는지 검증한다.
- **실행 흐름 (`POST /chaos/invalidate` 또는 `POST /chaos/invalidate/{key}`)**:
  1. Control Plane이 가용 게이트웨이 노드를 통해 Redis Stream(`invalidation_stream`)에 `XADD invalidation_stream MAXLEN ~ 10000 key {target_key}` 이벤트를 발행한다.
  2. 각 게이트웨이(`Node-A`, `Node-B`, `Node-C`)는 고유 컨슈머 그룹을 가지므로 동일한 스트림 이벤트를 모든 노드가 브로드캐스트 수신한다.
  3. 각 노드의 `stream_listener`는 메시지에서 키를 추출하여 로컬 L1 및 Redis L2 캐시를 삭제하고 `XACK`를 전송한다.
- **실행하지 않고 확인하는 결과**:
  - 이벤트 발행 즉시 3개 노드의 L1 캐시와 Redis L2 캐시에서 해당 키가 삭제된다. 이후 첫 `/auth` 요청은 Policy DB를 다시 조회(`source: "DB"`)하여 최신 데이터를 반영한다.

---

### 시나리오 8: ECDSA P-256 PoP 서명 검증 (Scope B)
- **검증 목적**: `POP_VERIFY_ENABLED=true` 환경에서 클라이언트가 전송한 ECDSA P-256 서명과 본문 다이제스트를 검증하여 위변조를 차단하고, 리플레이 방지와 서킷 브레이커 연동이 설계대로 동작하는지 확인한다.
- **서명 정규화 규격**:
  - 클라이언트는 Method, Path, 정렬된 Query String, SHA-256 Body Digest, SPIFFE ID, Nonce 6개 필드를 개행 문자(`
`)로 결합하여 개인키(P-256)로 서명(`X-PoP-Signature: <base64>`)한다.
- **세부 검증 케이스**:

| 세부 케이스 | 요청 파라미터 / 변조 내용 | 게이트웨이 처리 동작 | 기대 HTTP 응답 | 관측 메트릭 |
| :--- | :--- | :--- | :--- | :--- |
| **8-1. 정상 서명** | 유효한 개인키로 6개 정규화 필드 서명 | Rate Limit 통과 → Nonce 소진 → 공개키 조회(L1/L2/DB) → 서명 성공 | `200 OK` (`{"status": "allow", "source": "L1\|L2\|DB"}`) | `pop_verified +1` |
| **8-2. 서명 헤더 누락** | `X-PoP-Signature` 미포함 | Nonce 소진 직후 헤더 검사에서 거부 | `400 Bad Request` (`"Missing PoP Signature"`) | `pop_rejected +1` |
| **8-3. 바디 다이제스트 오류** | `X-Body-Digest` 접두사 누락 또는 본문 해시 불일치 | Nonce 소진 직후 SHA-256 해시 검증 실패 시 거부 | `400 Bad Request` (`"Missing or invalid X-Body-Digest header"` 또는 `"Body digest mismatch"`) | `pop_rejected +1` |
| **8-4. 페이로드 위변조 / 타인 키** | 경로나 쿼리 위변조, 또는 타인 개인키로 서명 | 공개키 검증 시 해시 불일치 (`InvalidSignature`) | `401 Unauthorized` (`"Invalid PoP Signature"`) | `pop_rejected +1` |
| **8-5. 미등록 클라이언트 키** | `X-SPIFFE-ID: spiffe://test/nokey` | Policy DB `GET /pubkey`가 `404 Key not found` 반환 | `401 Unauthorized` (`"Unknown Key ID"`) | `pop_rejected +1` |
| **8-6. 리플레이된 Nonce 공격** | 이미 사용된 `X-Nonce` + 서명 전송 | Nonce 검사 단계에서 즉시 차단 (ECDSA 연산 전에 차단하여 CPU 낭비 방지) | `401 Unauthorized` (`"Replay Detected"`) | `nonce_replays +1` (`pop_rejected`는 `0` 유지) |
| **8-7. DB 장애 중 공개키 캐시 미스** | 서킷 `OPEN` 상태에서 캐시에 없는 ID로 요청 | Nonce 소진 후 `/pubkey` DB 폴백 전 `cb.can_execute()`에서 차단 (Fail-Closed) | `503 Service Unavailable` (`"Service Unavailable - Key Fetch Failed"`) | `circuit_blocked +1`, `pubkey_circuit_blocked +1`, `pop_rejected +1` |

---

### 시나리오 9: 정책 거부(DENY) 인가 검증
- **검증 목적**: 권한이 없는 클라이언트(`X-Res: restricted` 또는 `.../deny`)의 요청에 대해 백엔드 Policy DB가 명시적으로 거부(`status: "deny"`)를 반환할 때, 게이트웨이가 이를 올바르게 판별하여 정상 응답(`200 OK`, `status: "deny"`)으로 처리하고 캐싱하는지 검증한다.
- **실행 흐름 (`POST /chaos/policy/deny` / `tests/test_scenarios.py::test_policy_deny_caching`)**:
  1. 클라이언트가 접근이 제한된 리소스(`X-Res: restricted`)로 `GET /auth`를 요청한다.
  2. 캐시 미스 발생 시 게이트웨이는 Policy DB `GET /policy`를 조회한다.
  3. Policy DB는 규칙에 따라 `{"status": "deny", "policy": "DENY"}`를 반환한다.
  4. 게이트웨이는 이를 오류(5xx)나 서킷 실패로 취급하지 않고, 명시적 인가 거부로 판정하여 L1 및 L2 캐시에 `"DENY"`로 적재한다.
  5. 클라이언트에게는 `200 OK` (`{"status": "deny", "policy": "DENY", "source": "DB"}`)를 반환한다.
  6. 동일한 요청이 다시 유입되면 DB를 재조회하지 않고 L1(또는 L2) 캐시에서 즉시 `200 OK` (`source: "L1"`, `policy: "DENY"`)를 반환한다.
- **실행하지 않고 확인하는 결과**:
  ```json
  {
    "status": "deny",
    "policy": "DENY",
    "source": "DB"
  }
  ```
  → `requests` 카운터와 `db_fallbacks` 카운터가 정상 집계되며 서킷 브레이커 실패 카운트는 증가하지 않는다.

---

## 런타임 수학적 불변식

게이트웨이는 시스템 건전성을 보장하기 위해 2대 런타임 불변식을 실시간으로 유지한다.

### 불변식 1: 요청 분류 불변식
`/auth` 엔드포인트에서 처리가 완료된 모든 요청(`requests`)은 아래 7개 상호 배타적 경로의 합과 일치해야 한다:

$$\begin{aligned}
\text{requests} = &\;\text{rate\_limited} \\
&+ \text{nonce\_replays} \\
&+ \text{redis\_outage\_blocked} \\
&+ \text{pop\_rejected} \\
&+ \text{l1\_hits} \\
&+ \text{l2\_hits} \\
&+ \text{db\_fallbacks}
\end{aligned}$$

- **검증**: 단 하나의 요청도 누락되거나 이중 집계되지 않음을 증명한다 ($N = N$).

### 불변식 2: DB 폴백 강한 불변식
L1과 L2 캐시를 모두 미스하여 DB 폴백 단계로 진입한 요청(`db_fallbacks`)은 아래 내부 처리 경로의 합과 일치해야 한다:

$$\begin{aligned}
\text{db\_fallbacks} = &\;\text{db\_queries\_executed} \\
&+ \text{singleflight\_coalesced} \\
&+ \text{l1\_double\_check\_hits} \\
&+ \text{policy\_circuit\_blocked}
\end{aligned}$$

- **검증**: 캐시 미스 요청 중 단일 비행으로 병합되었거나, 더블체크로 구제되었거나, 서킷 브레이커에 의해 차단되었거나, 실제 DB 쿼리를 수행한 건수의 합이 폴백 진입 건수와 일치함을 증명한다.

---

## 웹 대시보드 실시간 카오스 시연

브라우저에서 `http://localhost:8080`에 접속하면 아래 기능을 실시간으로 제어하고 관측할 수 있다:

1. **상단 KPI 카드**: 클러스터 전체의 총 요청 수, L1/L2 캐시 히트율, 2대 불변식 일치 여부(Pass/Fail) 배지 표시.
2. **3개 게이트웨이 노드별 실시간 카드**:
   - `Node-A`, `Node-B`, `Node-C`의 서킷 상태(`CLOSED`, `OPEN`, `HALF_OPEN`).
   - L1 캐시 항목 수, 활성 Single-Flight 수, 처리 지연 시간.
3. **9개 카오스 시나리오 제어 패널**:
   - `1. 트래픽/유량 제어`: Nonce 동시 요청(`POST /chaos/nonce`), DDoS 60건 폭주(`POST /chaos/ddos`), Single-Flight 50건 폭주(`POST /chaos/same_key_burst`).
   - `2. 장애 격리`: DB 중단/복구(`/chaos/db/down|up`), Redis 중단/복구(`/chaos/redis/down|up`) 및 장애 생존성 검증.
   - `3. 네트워크 및 동기화`: Node-B 통신 격리/복구(`/chaos/partition/Node-B/isolate|heal`), 캐시 무효화 이벤트 발행(`/chaos/invalidate`).

---

## 보안 시크릿과 관측성

Control Plane의 `/chaos/*` 엔드포인트와 `/metrics/stream` SSE 스트림, Gateway 및 Policy DB의 `/chaos/*`, `/metrics` 엔드포인트는 `X-Admin-Token` 헤더(`hmac.compare_digest` 상수 시간 비교)로 보호된다.

- `.env.example`을 `.env`로 복사한 뒤, `ADMIN_SECRET_TOKEN` 값을 설정한다.
- **프론트엔드**: 브라우저 탭 종료 시 자동 소멸되는 `sessionStorage`에 토큰이 저장된다.
- **SSE 단일 폴러 팬아웃**: Control Plane은 백그라운드 단일 폴러(`broadcast_metrics_loop`)가 1초 간격으로 3개 게이트웨이의 `/metrics`를 폴링(`asyncio.gather`)하여 구독자 큐로 브로드캐스팅한다.
- **클러스터 불변식 감사 (`GET /chaos/audit`)**: Control Plane의 `GET /chaos/audit` 엔드포인트는 모든 게이트웨이 노드의 2대 불변식 일치성과 캐시 히트율, Single-Flight 병합율, DB 부하 절감 비율을 종합 감사하여 구조화된 JSON 리포트(`overall_invariant_pass: bool`)를 반환한다.

---

## 빠른 시작

1. 환경 변수 파일 생성:
```bash
cp .env.example .env
```
2. `.env` 파일 내부의 `ADMIN_SECRET_TOKEN` 값을 설정한다. 단, 테스트 스크립트와 `conftest.py` 등은 기본적으로 `super-secret-admin` 토큰을 사용하므로 스크립트 구동 시에는 해당 값으로 설정해야 한다.
3. 클러스터 빌드 및 구동:
```bash
docker-compose up -d --build
```
4. **접속 정보**:
   - 대시보드(Control Plane): http://localhost:8080
   - 게이트웨이(Gateway) 노드: http://localhost:8001, 8002, 8003
   - 정책 DB(Policy DB): http://localhost:8000

### 테스트 실행 방법
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .\.venv\Scripts\activate)
pip install -r requirements.txt
pip install -r requirements-dev.txt

# 단위 및 경계 테스트 실행 (Docker 불필요, Mock 기반으로 59개 테스트 즉시 실행)
pytest -v

# E2E 시나리오 테스트 실행 (실제 Docker 클러스터 구동 필요)
pytest -m e2e
```

---

## 알려진 한계점 (Known Limitations & Future Work)

- **근사 알고리즘 오차**: Sliding Window는 가중 합산 근사치이므로 트래픽 분포에 따라 미세한 오차가 발생할 수 있다.
- **클러스터 전역 Single-Flight 조율 오버헤드**:
  - **Follower 추가 지연**: 클러스터 전역 Single-Flight(`CLUSTER_SINGLEFLIGHT_ENABLED=true`)에서 Follower 노드는 Leader의 L2 적재를 폴링(`CLUSTER_SF_POLL_INTERVAL_SEC=25ms`)한다. 이에 따라 평균 12.5ms에서 최대 25ms의 대기 시간과 Redis 네트워크 왕복 지연이 추가된다.
  - **Leader 장애 시 인계 지연**: Leader 노드가 락 획득 후 DB 쿼리 도중 비정상 종료(`SIGKILL` 등)되면, 락 TTL(`CLUSTER_SF_LOCK_TTL_SEC=3.0s`) 또는 타임아웃(`CLUSTER_SF_TIMEOUT_SEC=2.0s`) 만료까지 Follower 노드가 대기한 뒤 락을 인계받는다. 이 과정에서 일시적인 지연이 발생할 수 있다.
  - **Redis 가용성 의존성**: Redis 장애 시 클러스터 분산 락을 사용할 수 없어 프로세스 로컬 Single-Flight로 전환된다. 이 경우 노드당 1회의 DB 조회가 허용되는 저하 모드로 동작한다.
- **성능 실측의 한계**: Single-Flight 성능 측정은 단일 노드 및 3개 노드 클러스터에서 DB 지연을 시뮬레이션하여 측정한 결과다 (`docs/benchmarks/singleflight-effect.md`).
- **PoP 서명의 한계**: Scope B 경량 구현 규격에 따라 Timestamp 및 Host 바인딩, 다중 키 식별을 위한 `kid` 헤더 지원, 하드웨어 Attestation 검증은 지원하지 않는다. 데모 환경에는 단일 데모 키만 등록되어 있다.
- **PoP 테스트 구성**: PoP 기능을 활성화한 시나리오는 클라이언트가 실제로 ECDSA 서명을 생성해야 하므로 `tests/test_pop.py`에서 분리하여 검증한다.
