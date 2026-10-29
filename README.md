# PoP-Sync

네트워크 파티션, Redis 장애, 대규모 DDoS 상황에서 분산 게이트웨이의 복원력을 실시간으로 시연하는 카오스 엔지니어링 데모다.

---

## 주요 기능

### 서킷 브레이커
Policy DB 장애 감지 시 OPEN 상태로 전환하여 연쇄 장애를 차단한다. 2.0초 디바운스로 상태 진동을 제어하며, CLOSED -> OPEN -> HALF_OPEN -> CLOSED 사이클을 안정적으로 관리한다.

### L1/L2 캐시 및 단일 비행
정책 조회는 다단계로 동작한다. 노드 로컬의 L1(OrderedDict LRU, 지연 만료)을 확인하고, 없으면 중앙 Redis(L2)를 조회한다. 모두 캐시 미스일 때 Policy DB를 호출한다.
`SINGLEFLIGHT_ENABLED` 및 `CLUSTER_SINGLEFLIGHT_ENABLED`가 기본 활성화되어 있다. 노드 내부의 `in_flight` Future 병합뿐 아니라, Redis 분산 락(`SET NX lock:sf:{key}`)과 L2 폴링 메커니즘을 통해 클러스터 전역에서도 동일 키 동시 DB 호출을 1회로 병합하여 백엔드 DB 과부하를 방지한다 (단일 노드 실측: docs/benchmarks/singleflight-effect.md, Envoy ext_authz 연동 실측: docs/benchmarks/envoy-ext-authz-benchmark.md).
 
### 스트림 기반 무효화 동기화
네트워크 단절 발생 시 지연되었던 Redis Stream 대기 메시지(PEL)를 누락분부터 연속 적용한다. 이를 통해 최종 일관성을 유지한다.
 
### O(1) 유량 제한 및 Nonce 리플레이 방어
`RL_ALGORITHM` 설정으로 유량 제한 방식을 결정한다(기본값 `sliding`). `sliding` 방식은 Lua 스크립트 기반 가중 합산 근사치로 동작하며, `fixed` 방식은 Redis Pipeline을 사용한다 (경계 버스트 측정 결과: docs/benchmarks/rate-limit-sliding.md). 유량 제한은 노드 단위가 아닌 클라이언트 ID(`SPIFFE ID`) 기준의 클러스터 전역 한도를 적용한다.
Lua 스크립트 기반 Nonce 중복 검증으로 리플레이 공격을 방어한다 (Envoy 에지 차단 실측: docs/benchmarks/envoy-ext-authz-benchmark.md).

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
   ├─ 3. Nonce 중복 검증 (고비용 암호 연산 전에 실행하여 사용 처리)
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
| **1** | **중복 요청 및 리플레이 방어** | `POST /chaos/nonce` | Redis Lua 기반 Nonce 중복 검증 (`NONCE_LUA`) | 1개 노드 `200 OK`, 다른 노드 `401 Replay Detected` (Redis 장애 시 양쪽 `503`) | `nonce_replays +1` (장애 시 `redis_outage_blocked +1`) |
| **2** | **대규모 DDoS 및 경계 버스트 방어** | `POST /chaos/ddos` (60건 동시 폭주) | Check-Before-Increment O(1) 가중 합산 슬라이딩 윈도우 (`50 req / 10s`) | 첫 실행 `200` 50건 / `429` 10건, 연속 실행 시 `429` 60건 차단 | `rate_limited` 증가, `Retry-After: 10` 헤더 |
| **3** | **동일 키 폭주 및 클러스터 전역 단일 비행 병합** | `POST /chaos/same_key_burst` (`count=50`) | Redis 분산 락(`SET NX`) 기반 Leader 선출 + Follower L2 폴링 및 In-Flight Future 병합 (`asyncio.shield`) | 전 노드 요청 모두 `200 OK`, **클러스터 전체에서 실제 DB 쿼리는 1회만 실행** (노드별 중복 쿼리 방지) | 클러스터 전체 `db_queries_executed = 1`, `singleflight_coalesced = N - 1` |
| **4** | **백엔드 DB 장애 격리 및 서킷 브레이커** | `POST /chaos/db/down` → `/chaos/db/up` | 3-State 서킷 브레이커, HALF_OPEN 단일 프로브 원자적 게이팅, 2.0s 디바운스 | 캐시 히트는 `200 OK(L1/L2)`, 캐시 미스는 `200 OK(degraded_allow)` 폴백 | `circuit_state: OPEN`, `circuit_blocked` 증가 |
| **5** | **중앙 Redis 장애 및 로컬 생존 모드** | `POST /chaos/redis/down` → `/chaos/redis/up` | 로컬 Rate Limit 폴백(`15 req / 10s`) + Nonce `503 Fail-Closed` | 노드당 한도(15건) 초과분은 `429`, 통과분(15건)은 Nonce 단계에서 `503` 차단 | `local_rl_size` 활성화, `redis_outage_blocked` 증가, Invariant `PASS` |
| **6** | **네트워크 파티션 및 PEL 긴급 복구** | `POST /chaos/partition/Node-B/isolate` → `heal` | 고립 노드 `503` 차단(Split-Brain 방어) + 복구 시 Stream PEL(`"0"`) 드레인 및 XAUTOCLAIM 유휴 회수 | 단절 중 `/auth`는 `503 Node Isolated`, 복구 즉시 누락 무효화 일괄 반영 | `is_partitioned: true→false`, `last_catchup` 상세 지표 기록 |
| **7** | **실시간 캐시 무효화 스트림 전파** | `POST /chaos/invalidate` | Redis Streams `XADD` (`maxlen~10000`) + 노드별 `XREADGROUP` | 전 노드의 L1 및 L2 캐시에서 대상 정책 키 즉시 삭제 및 `XACK` | 전 노드 `l1_cache_size` 감소 후 재조회 시 `source: "DB"` |
| **8** | **ECDSA P-256 PoP 서명 검증** | `POP_VERIFY_ENABLED=true` | Nonce 검증 통과 후 Query/Body Digest 바인딩 ECDSA 검증 및 `/pubkey` 서킷 연동 | 정상 `200`, 서명 누락/바디 다이제스트 오류 `400`, 위변조/미등록 키 `401`, DB 서킷 OPEN 시 `503` | `pop_verified` / `pop_rejected` 분리 집계 |
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
  - **10초 이내 재실행 시**: 윈도우 한도를 초과하여 `{"status": "flooded", "responses": {"429": 60}}`으로 전건 차단된다.
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
     - 동시 유입된 다수의 캐시 미스 요청 중 요청 1개만 프로브로 선점되어 Policy DB 조회를 시도한다. 후속 요청들은 대기 없이 즉시 `degraded_allow`로 처리되어 백엔드 중복 조회를 방지한다.
     - **성공 시**: `cb.record_success()`가 호출되어 즉시 `CLOSED` 상태로 복구되고(실패 카운트 리셋, 프로브 슬롯 반환), 정상 응답을 반환한다.
     - **실패 시**: `HALF_OPEN` 상태에서는 디바운스 없이 1회 실패 시 다시 `OPEN` 상태로 전이된다.
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
| **8-1. 정상 서명** | 유효한 개인키로 6개 정규화 필드 서명 | Rate Limit 통과 → Nonce 사용 처리 → 공개키 조회(L1/L2/DB) → 서명 성공 | `200 OK` (`{"status": "allow", "source": "L1\|L2\|DB"}`) | `pop_verified +1` |
| **8-2. 서명 헤더 누락** | `X-PoP-Signature` 미포함 | Nonce 사용 처리 직후 헤더 검사에서 거부 | `400 Bad Request` (`"Missing PoP Signature"`) | `pop_rejected +1` |
| **8-3. 바디 다이제스트 오류** | `X-Body-Digest` 접두사 누락 또는 본문 해시 불일치 | Nonce 사용 처리 직후 SHA-256 해시 검증 실패 시 거부 | `400 Bad Request` (`"Missing or invalid X-Body-Digest header"` 또는 `"Body digest mismatch"`) | `pop_rejected +1` |
| **8-4. 페이로드 위변조 / 타인 키** | 경로나 쿼리 위변조, 또는 타인 개인키로 서명 | 공개키 검증 시 해시 불일치 (`InvalidSignature`) | `401 Unauthorized` (`"Invalid PoP Signature"`) | `pop_rejected +1` |
| **8-5. 미등록 클라이언트 키** | `X-SPIFFE-ID: spiffe://test/nokey` | Policy DB `GET /pubkey`가 `404 Key not found` 반환 | `401 Unauthorized` (`"Unknown Key ID"`) | `pop_rejected +1` |
| **8-6. 리플레이된 Nonce 공격** | 이미 사용된 `X-Nonce` + 서명 전송 | Nonce 검사 단계에서 즉시 차단 (ECDSA 연산 전에 차단하여 CPU 낭비 방지) | `401 Unauthorized` (`"Replay Detected"`) | `nonce_replays +1` (`pop_rejected`는 `0` 유지) |
| **8-7. DB 장애 중 공개키 캐시 미스** | 서킷 `OPEN` 상태에서 캐시에 없는 ID로 요청 | Nonce 사용 처리 후 `/pubkey` DB 폴백 전 `cb.can_execute()`에서 차단 (Fail-Closed) | `503 Service Unavailable` (`"Service Unavailable - Key Fetch Failed"`) | `circuit_blocked +1`, `pubkey_circuit_blocked +1`, `pop_rejected +1` |

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

# 단위 및 경계 테스트 실행 (Docker 불필요, Mock 기반으로 93개 테스트 즉시 실행)
pytest -v

# E2E 시나리오 테스트 실행 (실제 Docker 클러스터 구동 필요)
pytest -m e2e
```

---

## 프로덕션 적용 시 고려사항 및 스코프 (Production Considerations & Scope)

### 1) L7 트래픽 프록시 계층 분리 및 PEP 토글 (`UPSTREAM_URL`)
- **PDP 인가 엔진 모드 (기본값)**: `UPSTREAM_URL`이 설정되지 않은 경우, 게이트웨이는 순수 정책 결정 지점(PDP)으로 동작하여 6단계 파이프라인 검증 후 인가 결과 JSON(`200 OK`)만을 반환한다. 이는 Envoy, Nginx 등 엔터프라이즈 L7 프록시의 `ext_authz` 필터와 연동하는 아키텍처에 최적화되어 있다.
- **PEP 비동기 리버스 프록시 모드 (`UPSTREAM_URL` 활성화)**: `UPSTREAM_URL` 환경변수가 설정되면 자체 정책 집행 지점(PEP) 패스스루 모드로 동작한다.
  - 인가 거부(`DENY`) 시에는 Upstream으로 요청을 전달하지 않고 게이트웨이 레벨에서 즉시 `403 Forbidden` JSON을 반환한다.
  - 인가 허용(`allow` 또는 `degraded_allow`) 시에는 Hop-by-hop 헤더 및 내부 관리자 토큰(`x-admin-token`)을 안전하게 제거하고, 검증된 신원 컨텍스트 헤더(`X-Authenticated-SPIFFE-ID`, `X-Policy-Decision`, `X-Auth-Source`)를 주입하여 지연 초기화 커넥션 풀(`httpx.AsyncClient`)을 통해 백엔드 Upstream 서버로 비동기 패스스루 포워딩한다. 별도의 외부 프록시 계층 없이 단독 인프라로 즉시 배포할 수 있다.

### 2) 클라이언트 암호 연산 요구사항
- **PoP 비대칭 서명 검증(`POP_VERIFY_ENABLED=true`)**: ECDSA P-256 비대칭 키 기반 서명 생성 및 검증은 리플레이 및 토큰 탈취 공격을 방지하지만, 클라이언트 측 Web Crypto API 또는 WebAuthn 기반 서명 SDK 도입이 필요하며 클라이언트와 서버 양측의 암호 연산 오버헤드가 발생한다.
- **선별 적용 권장**: 일반 웹 트래픽 전체보다는 금융/결제 API, 제로 트러스트 엔터프라이즈 환경, 대규모 트래픽이 집중되는 주문/티켓팅 등 고보안·고트래픽 API에 선별 적용하는 구성을 권장한다.

### 3) 시스템 가용성 및 다중 리전 동기화 제약
- **Redis SPOF 대응 및 헬스체크**:
  - `USE_REDIS_SENTINEL=true` 활성화 시 Redis Sentinel 클러스터를 통해 마스터 장애 발생 시 자동 승격된 신규 마스터로 동적 재연결하여 단일 장애점(SPOF)을 제거한다.
  - `GET /health` 엔드포인트는 `0.2s` 타임아웃 제한의 Redis `ping()` 검사와 네트워크 파티션 고립 상태를 실시간 판별하는 헬스체크를 수행한다. 장애 감지 시 즉시 `503 Service Unavailable`을 반환하여 상위 GSLB 및 L4/L7 로드밸런서(ALB)가 비정상 노드를 감지하고 Standby 클러스터로 자동 페일오버하도록 보장한다.
- **Multi-Region Active-Active 한계**:
  - Redis Lua 기반 Nonce 중복 검증과 클러스터 분산 락(`SET NX`)은 일관성을 전제한다. 대륙 간 또는 원거리 리전 간 Active-Active 구성 시 네트워크 왕복 지연(RTT 10~50ms+)으로 전체 요청 지연 시간이 증가한다.
  - 따라서 동일 리전 Multi-AZ 환경에서의 Active-Active 구성이나 GSLB 기반 Multi-Region Active-Standby 구성을 권장한다.

---

## Envoy / Nginx 연동 가이드

본 가이드는 프로덕션 환경에서 PoP-Sync를 PDP로 동작시키고 Envoy 및 Nginx를 PEP로 구성하여 제로 트러스트 아키텍처를 구현하는 연동 명세다.

PoP-Sync는 순수 PDP 모드(`UPSTREAM_URL` 미설정 시)에서 `/auth` 엔드포인트를 통해 6단계 검증 파이프라인(유량 제어, 1회용 Nonce 리플레이 방지, ECDSA PoP 서명 검증, 다단계 캐시 및 Policy DB 단일 비행 인가)을 수행한다. 검증 결과에 따라 표준 HTTP 상태 코드(`200 OK`, `401 Unauthorized`, `403 Forbidden`, `429 Too Many Requests`) 및 검증된 신원 컨텍스트 헤더를 PEP 프록시로 반환한다.

---

### 1. 아키텍처 및 신뢰 경계

#### 1.1 아키텍처 및 데이터 흐름

```text
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             Zero-Trust Security Domain                                           │
│                                                                                                                  │
│  [ External / Untrusted ]           [ Ingress PEP Layer ]                          [ Control Plane / PDP ]       │
│  ┌───────────────────────┐          ┌──────────────────────┐                       ┌─────────────────────────┐   │
│  │    Workload Client    │          │  Envoy / Nginx (PEP) │                       │     PoP-Sync (PDP)      │   │
│  │                       │          │                      │                       │                         │   │
│  │ 1. Client TLS Cert    │──(mTLS)─►│ 2. Terminate mTLS    │                       │ 4. 6-Stage Pipeline:    │   │
│  │    (SAN: spiffe://..) │          │    Verify X.509 Cert │                       │    - Per-SPIFFE RateLim │   │
│  │ 2. Authorization      │          │ 3. Header Sanitize   │──(ext_authz /auth)───►│    - Atomic Nonce Check │   │
│  │    X-Nonce            │          │    Extract SAN URI   │                       │    - ECDSA PoP Verify   │   │
│  │    X-PoP-Signature    │          │    Inject X-SPIFFE-ID│                       │    - L1/L2 Cache / DB   │   │
│  │    (X-Body-Digest)    │          │    Buffer Body       │                       │                         │   │
│  └───────────────────────┘          └──────────┬───────────┘                       └────────────┬────────────┘   │
│                                                │                                                │                │
│                                                │◄─────── 200 OK / Context Headers ──────────────┘                │
│                                                │         (X-Authenticated-SPIFFE-ID)                             │
│                                                │         (X-Policy-Decision, X-Auth-Source)                      │
│                                                │         [or 401/403/429 Fast-Block]                             │
│                                                │                                                                 │
│                                                ▼ (Forward with Verified Context)                                 │
│                                     ┌──────────────────────┐                                                     │
│                                     │   Backend Service    │                                                     │
│                                     │   (Target Upstream)  │                                                     │
│                                     │                      │                                                     │
│                                     │ Executes Business    │                                                     │
│                                     │ Logic securely       │                                                     │
│                                     └──────────────────────┘                                                     │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 1.2 신뢰 경계 메커니즘

1. **상호 TLS(mTLS) 인증 종료 및 신원 검증**:
   - 인프라 최외곽의 PEP(Envoy/Nginx)는 클라이언트와의 TCP 연결 시 상호 TLS(mTLS)를 요구(`require_client_certificate: true` / `ssl_verify_client on`)한다.
   - 사설 인증기관(Private CA)이 서명한 X.509 클라이언트 인증서의 유효성을 검증한다. 인증서의 SAN URI(예: `spiffe://example.org/ns/prod/sa/order-service`)에서 워크로드 고유 식별자(SPIFFE ID)를 추출한다.

2. **클라이언트 헤더 위조 차단 및 헤더 정제**:
   - 외부 클라이언트가 상위 권한의 SPIFFE ID를 도용하기 위해 HTTP 요청 헤더에 임의로 `X-SPIFFE-ID: spiffe://example.org/ns/prod/sa/admin`을 조작하여 전송할 수 있다.
   - PEP 계층은 클라이언트가 전송한 임의의 `X-SPIFFE-ID` 헤더를 신뢰하지 않는다. mTLS 핸드셰이크 단계에서 추출 및 검증된 SAN URI 값으로 해당 헤더를 덮어쓴다(OVERWRITE). 이를 통해 하위 PDP 및 백엔드로 전달되는 신원 정보의 무결성을 보장한다.

3. **인가 서브요청 전달**:
   - PEP는 원본 비즈니스 요청의 본문 및 인가 메타데이터 헤더(`Authorization`, `X-Nonce`, 정제된 `X-SPIFFE-ID`, `X-Res`, `X-Act`, `X-PoP-Signature`, `X-Body-Digest`)를 조합하여 PoP-Sync(`/auth`)로 외부 인가 요청을 전송한다.
   - PoP-Sync는 SPIFFE ID 기반 슬라이딩 윈도우 유량 제한, Redis 기반 Nonce 중복 검증, 공개키 기반 PoP ECDSA 서명 검증, 다단계 캐시 인가 정책 조회를 순차적으로 수행한다.

4. **인가 컨텍스트 전파 및 백엔드 보호**:
   - PoP-Sync가 `200 OK`를 반환하면, 응답 헤더로 인가 컨텍스트(`X-Authenticated-SPIFFE-ID`, `X-Policy-Decision`, `X-Auth-Source`)를 PEP로 전달한다.
   - PEP는 이 헤더들을 백엔드 서버(`backend_service`)로 주입하여 전달한다. 백엔드 서비스는 추가 암호 연산이나 DB 재조회 없이 신뢰할 수 있는 주체 정보와 권한을 즉시 활용한다.
   - 인가 거부(`403`), 리플레이 탐지(`401`), 유량 초과(`429`) 발생 시 PEP 레벨에서 즉시 요청을 차단(Fail-Closed)하여 백엔드 자원을 보호한다.

---

### 2. Envoy 설정 (`envoy.yaml`)

Envoy v3 API 규격을 준수하며, `ext_authz` HTTP 필터를 사용하여 PoP-Sync와 연동하는 프로덕션 설정이다. Fail-Closed 정책, 명시적 타임아웃, 원본 요청 본문 바이너리 버퍼링(`with_request_body`), 컨텍스트 헤더 업스트림 전파를 지원한다.

```yaml
# ==============================================================================
# Envoy v3 Production Configuration for PoP-Sync ext_authz (PEP Mode)
# ==============================================================================
admin:
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 9901

static_resources:
  listeners:
    - name: ingress_listener
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 8443
      filter_chains:
        - transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              common_tls_context:
                tls_certificates:
                  - certificate_chain:
                      filename: "/etc/envoy/certs/server.crt"
                    private_key:
                      filename: "/etc/envoy/certs/server.key"
                validation_context:
                  trusted_ca:
                    filename: "/etc/envoy/certs/ca.crt"
                  # 제로 트러스트 mTLS 강제: 유효한 클라이언트 인증서가 없으면 TLS 레벨에서 즉시 연결 종료
                  require_client_certificate: true
          filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: ingress_http
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: protected_service
                      domains: ["*"]
                      # [신뢰 경계 핵심] 클라이언트 헤더 스푸핑 차단:
                      # mTLS 인증서의 SAN URI(%DOWNSTREAM_PEER_URI_SAN%)를 추출하여
                      # 클라이언트가 보낸 임의의 X-SPIFFE-ID를 강제로 덮어쓴다 (OVERWRITE).
                      request_headers_to_add:
                        - header:
                            key: "x-spiffe-id"
                            value: "%DOWNSTREAM_PEER_URI_SAN%"
                          append_action: OVERWRITE_IF_EXISTS_OR_ADD
                      routes:
                        - match:
                            prefix: "/"
                          route:
                            cluster: backend_service
                            timeout: 5.0s
                http_filters:
                  # ------------------------------------------------------------
                  # PoP-Sync External Authorization Filter (HTTP ext_authz)
                  # ------------------------------------------------------------
                  - name: envoy.filters.http.ext_authz
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.ext_authz.v3.ExtAuthz
                      transport_api_version: V3
                      # Fail-Closed 정책: PoP-Sync 장애나 타임아웃 발생 시 요청을 통과시키지 않고 차단
                      failure_mode_allow: false
                      # 인가 서비스 통신 장애 시 클라이언트에 명시적 503 반환
                      status_on_error:
                        code: ServiceUnavailable
                      http_service:
                        server_uri:
                          uri: "http://127.0.0.1:8000/auth"
                          cluster: pop_sync_service
                          timeout: 0.25s # 250ms 명시적 타임아웃으로 빠른 장애 격리(Fast-Fail) 보장
                        authorization_request:
                          # PoP-Sync의 6단계 파이프라인 검증에 필수적인 원본 요청 헤더 전달
                          allowed_headers:
                            patterns:
                              - exact: "authorization"
                              - exact: "x-nonce"
                              - exact: "x-spiffe-id"
                              - exact: "x-res"
                              - exact: "x-act"
                              - exact: "x-pop-signature"
                              - exact: "x-body-digest"
                              - exact: "content-type"
                        authorization_response:
                          # 200 OK 승인 시 PoP-Sync가 발급한 컨텍스트 헤더를 Backend Upstream으로 주입
                          allowed_upstream_headers:
                            patterns:
                              - exact: "x-authenticated-spiffe-id"
                              - exact: "x-policy-decision"
                              - exact: "x-auth-source"
                          # 401/429 차단 시 PoP-Sync의 레이트 리밋 헤더 및 에러 응답을 클라이언트로 투명하게 노출
                          allowed_client_headers_on_error:
                            patterns:
                              - exact: "retry-after"
                              - exact: "content-type"
                      # --------------------------------------------------------
                      # PoP ECDSA 페이로드 서명 검증을 위한 요청 본문 버퍼링 설정
                      # --------------------------------------------------------
                      with_request_body:
                        max_request_bytes: 65536 # 64KB까지 요청 본문을 인메모리 버퍼링하여 PoP-Sync로 전달
                        allow_partial_message: false # 전체 페이로드가 수신되지 않으면 인가 요청을 진행하지 않음
                        pack_as_bytes: true # 바이너리 원본 페이로드를 그대로 보존하여 SHA-256 해시 정합성 보장

                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

  clusters:
    # --------------------------------------------------------------------------
    # PoP-Sync PDP Service Cluster (FastAPI /auth)
    # --------------------------------------------------------------------------
    - name: pop_sync_service
      type: STATIC
      connect_timeout: 0.25s
      load_assignment:
        cluster_name: pop_sync_service
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: 127.0.0.1
                      port_value: 8000
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config:
            http_protocol_options: {} # 고성능 HTTP/1.1 커넥션 풀링 유지
      circuit_breakers:
        thresholds:
          - priority: DEFAULT
            max_connections: 1024
            max_pending_requests: 1024
            max_requests: 2048

    # --------------------------------------------------------------------------
    # Protected Business Backend Service Cluster
    # --------------------------------------------------------------------------
    - name: backend_service
      type: STATIC
      connect_timeout: 0.5s
      load_assignment:
        cluster_name: backend_service
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: 127.0.0.1
                      port_value: 8080
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config:
            http_protocol_options: {}
```

---

### 3. Nginx 설정 (`nginx.conf`)

Nginx 공식 `ngx_http_auth_request_module`을 활용한 프로덕션 구성이다. 내부 서브요청(`/_pop_auth`)을 통한 비동기 인가 검증, `auth_request_set` 기반 컨텍스트 주입, 그리고 Nginx에서 `429 Too Many Requests` 상태 코드가 `500 Internal Server Error`로 바뀌는 현상을 처리하는 에러 인터셉트 구성을 포함한다.

```nginx
# ==============================================================================
# Nginx Production Configuration for PoP-Sync auth_request (PEP Mode)
# ==============================================================================

events {
    worker_connections 2048;
}

http {
    include       mime.types;
    default_type  application/octet-stream;

    # 업스트림 커넥션 풀 정의
    upstream pop_sync_upstream {
        server 127.0.0.1:8000 max_fails=3 fail_timeout=10s;
        keepalive 64; # PDP 인가 서버 커넥션 재사용
    }

    upstream backend_upstream {
        server 127.0.0.1:8080 max_fails=3 fail_timeout=10s;
        keepalive 128; # 백엔드 실서버 커넥션 풀 유지
    }

    server {
        listen 8443 ssl http2;
        server_name api.enterprise.internal;

        # ----------------------------------------------------------------------
        # 1. mTLS 클라이언트 인증서 검증
        # ----------------------------------------------------------------------
        ssl_certificate         /etc/nginx/certs/server.crt;
        ssl_certificate_key     /etc/nginx/certs/server.key;
        ssl_client_certificate  /etc/nginx/certs/ca.crt;
        ssl_verify_client       on;
        ssl_verify_depth        2;

        # ----------------------------------------------------------------------
        # 2. 제로 트러스트 신뢰 경계: 헤더 스푸핑 차단 및 검증 신원 바인딩
        # 클라이언트가 임의로 조작해 전송한 X-SPIFFE-ID 헤더를 차단하고,
        # mTLS 검증이 완료된 X.509 주체 식별자($ssl_client_s_dn 또는 SAN 파싱값)로 강제 바인딩한다.
        # ----------------------------------------------------------------------
        set $sanitized_spiffe_id $ssl_client_s_dn;

        # ----------------------------------------------------------------------
        # 3. 비즈니스 API 메인 엔드포인트 (PEP 실행 지점)
        # ----------------------------------------------------------------------
        location / {
            # PoP-Sync 인가 서브요청 실행
            auth_request /_pop_auth;

            # [인가 컨텍스트 캡처] PoP-Sync가 반환한 응답 헤더 캡처
            auth_request_set $auth_spiffe    $upstream_http_x_authenticated_spiffe_id;
            auth_request_set $auth_decision  $upstream_http_x_policy_decision;
            auth_request_set $auth_source    $upstream_http_x_auth_source;
            auth_request_set $auth_status    $upstream_status;
            auth_request_set $auth_retry     $upstream_http_retry_after;

            # [Nginx 429 상태 코드 복원]
            # auth_request 모듈은 2xx는 통과, 401/403은 해당 코드로 종료하지만,
            # 429(Rate Limit) 등 기타 상태 코드는 500으로 변환된다.
            # 이를 가로채서 PoP-Sync 원본 상태 코드로 복원하기 위해 에러 페이지를 인터셉트한다.
            error_page 401 = @handle_401;
            error_page 403 = @handle_403;
            error_page 500 = @handle_auth_error;

            # 200 OK 승인 시: 백엔드로 검증된 신원 컨텍스트 헤더를 주입하여 프록시 포워딩
            proxy_pass http://backend_upstream;
            proxy_http_version 1.1;
            proxy_set_header Connection "";

            proxy_set_header X-Authenticated-SPIFFE-ID $auth_spiffe;
            proxy_set_header X-Policy-Decision         $auth_decision;
            proxy_set_header X-Auth-Source             $auth_source;

            # 원본 요청 메타데이터 전달
            proxy_set_header Host               $host;
            proxy_set_header X-Real-IP          $remote_addr;
            proxy_set_header X-Forwarded-For    $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto  $scheme;
        }

        # ----------------------------------------------------------------------
        # 4. PoP-Sync PDP 인가 서브요청 전용 내부 엔드포인트
        # ----------------------------------------------------------------------
        location = /_pop_auth {
            internal; # 외부 직접 접근 차단
            proxy_pass http://pop_sync_upstream/auth;
            proxy_http_version 1.1;
            proxy_set_header Connection "";

            # 서브요청 본문 전송 비활성화 (헤더 기반 빠른 인가 판정)
            proxy_pass_request_body off;
            proxy_set_header Content-Length "";

            # 원본 요청 메서드 및 PoP-Sync 6단계 검증 필수 헤더 전달
            proxy_method $request_method;
            proxy_set_header Authorization     $http_authorization;
            proxy_set_header X-Nonce           $http_x_nonce;
            proxy_set_header X-SPIFFE-ID       $sanitized_spiffe_id; # 스푸핑 방지된 검증 신원 전달
            proxy_set_header X-Res             $http_x_res;
            proxy_set_header X-Act             $http_x_act;
            proxy_set_header X-PoP-Signature   $http_x_pop_signature;
            proxy_set_header X-Body-Digest     $http_x_body_digest;
            proxy_set_header X-Original-URI    $request_uri;

            # PDP 타임아웃 제한 (250ms 내 미응답 시 Fail-Closed 차단)
            proxy_connect_timeout 250ms;
            proxy_read_timeout    250ms;
            proxy_send_timeout    250ms;
        }

        # ----------------------------------------------------------------------
        # 5. 상태 코드 매핑 핸들러: 401, 403 및 왜곡된 429 복원 처리
        # ----------------------------------------------------------------------
        location @handle_auth_error {
            # PoP-Sync가 429 Too Many Requests를 반환했으나 Nginx가 500으로 감싼 경우:
            # 캡처된 $auth_status를 판별하여 정확한 429 상태 코드와 Retry-After 헤더를 복원하여 반환
            if ($auth_status = 429) {
                add_header Retry-After $auth_retry always;
                add_header Content-Type application/json always;
                return 429 '{"status":"error","message":"Too Many Requests"}';
            }
            # 그 외 PDP 서버 통신 불가/오류 시 Fail-Closed 503 반환
            add_header Content-Type application/json always;
            return 503 '{"status":"error","message":"External Authorization Service Unavailable"}';
        }

        location @handle_401 {
            add_header Content-Type application/json always;
            return 401 '{"status":"error","message":"Unauthorized or Replay Detected"}';
        }

        location @handle_403 {
            add_header Content-Type application/json always;
            return 403 '{"status":"deny","message":"Access Denied by Policy"}';
        }
    }
}
```

#### Nginx 요청 본문 처리 한계 및 Envoy 권장 사항
> [!WARNING] Nginx `auth_request` 모듈의 요청 본문 누락 제약
> Nginx의 표준 모듈인 `ngx_http_auth_request_module`은 서브요청 생성 시 클라이언트의 HTTP 요청 본문(Request Body)을 서브요청으로 전달하지 못합니다. 따라서 본문 전송을 비활성화(`proxy_pass_request_body off; proxy_set_header Content-Length "";`)해야 합니다.
> 
> 이로 인해 클라이언트 페이로드의 SHA-256 해시를 검증하는 요청 본문 무결성 바인딩 PoP 서명 검증(`X-Body-Digest` 및 Method, Path, Query, Body Digest 정규화 서명)을 Nginx 기본 구성만으로는 수행할 수 없습니다.
> 
> 반면 Envoy는 `ext_authz` HTTP 필터에서 `with_request_body` 설정(`max_request_bytes: 65536`, `pack_as_bytes: true`)을 기본 제공합니다. 원본 요청 본문을 손실 없이 인가 서버로 전달합니다. 따라서 본문 변조 방지 및 페이로드 바인딩 PoP 검증이 필요한 환경에서는 Envoy를 PEP로 사용할 것을 권장합니다.

---

### 4. 연동 검증 curl 테스트

시나리오별 `curl -i` 명령으로 PEP 프록시와 PoP-Sync(PDP) 간 연동 동작을 검증할 수 있습니다.

#### 4.1 정상 인가 요청 (`200 OK`)
유효한 Bearer 인증 토큰과 고유한 1회용 Nonce를 포함하여 요청을 전송합니다. PoP-Sync는 6단계 파이프라인 검증 후 `200 OK`를 반환합니다. PEP 프록시는 이를 수신하여 백엔드로 `X-Authenticated-SPIFFE-ID`, `X-Policy-Decision`, `X-Auth-Source` 헤더를 주입해 전달합니다.

```bash
# 유효한 1회용 Nonce 생성 및 정상 API 호출
TEST_NONCE="nonce-valid-$(date +%s%N)"

curl -i -X GET "https://api.enterprise.internal:8443/api/v1/orders/ORD-7701" \
  --cacert /etc/envoy/certs/ca.crt \
  --cert /etc/envoy/certs/client.crt \
  --key /etc/envoy/certs/client.key \
  -H "Authorization: Bearer valid-user-token-xyz" \
  -H "X-Nonce: ${TEST_NONCE}" \
  -H "X-Res: order" \
  -H "X-Act: read"
```

**기대 HTTP 응답:**
```http
HTTP/1.1 200 OK
Content-Type: application/json
Date: Thu, 29 Oct 2026 01:30:00 GMT
Transfer-Encoding: chunked
X-Authenticated-SPIFFE-ID: spiffe://example.org/ns/prod/sa/order-service
X-Policy-Decision: allow
X-Auth-Source: L1

{
  "order_id": "ORD-7701",
  "status": "APPROVED",
  "resource": "order",
  "message": "Business transaction processed successfully"
}
```
- **검증 포인트**:
  - 클라이언트가 임의의 `X-SPIFFE-ID`를 전달하지 않아도 PEP가 클라이언트 mTLS 인증서의 SAN URI를 추출하여 주입한 뒤 PDP 인가를 통과합니다.
  - 응답 헤더로 `X-Authenticated-SPIFFE-ID` 및 `X-Auth-Source`가 백엔드와 클라이언트로 전달됨을 확인합니다.

---

#### 4.2 리플레이 공격 방어 검증 (`401 Unauthorized`)
동일한 `X-Nonce` 값을 60초 윈도우 내에 2회 연속 전송하여 중복 요청 차단을 검증합니다.

```bash
# 고정 Nonce를 이용한 1회차 호출 (정상 통과)
REPLAY_NONCE="nonce-replay-attack-sample-1234"

echo "=== 1회차 정상 요청 전송 ==="
curl -i -s -o /dev/null -w "1회차 HTTP 응답 코드: %{http_code}\n" -X GET "https://api.enterprise.internal:8443/api/v1/orders/ORD-7701" \
  --cacert /etc/envoy/certs/ca.crt \
  --cert /etc/envoy/certs/client.crt \
  --key /etc/envoy/certs/client.key \
  -H "Authorization: Bearer valid-user-token-xyz" \
  -H "X-Nonce: ${REPLAY_NONCE}"

echo "=== 2회차 리플레이 공격 요청 전송 (동일 Nonce 재사용) ==="
curl -i -X GET "https://api.enterprise.internal:8443/api/v1/orders/ORD-7701" \
  --cacert /etc/envoy/certs/ca.crt \
  --cert /etc/envoy/certs/client.crt \
  --key /etc/envoy/certs/client.key \
  -H "Authorization: Bearer valid-user-token-xyz" \
  -H "X-Nonce: ${REPLAY_NONCE}"
```

**2회차 기대 HTTP 응답:**
```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
Date: Thu, 29 Oct 2026 01:30:01 GMT
Content-Length: 35

{"detail":"Replay Detected"}
```
- **검증 포인트**:
  - PoP-Sync 파이프라인 3단계에서 Redis Lua 스크립트(`NONCE_LUA`)가 `EXISTS == 1`을 감지하여 암호 연산 진입 전에 차단합니다.
  - PEP 프록시는 백엔드로 요청을 전달하지 않고 클라이언트에 즉시 `401 Unauthorized`를 반환합니다.

---

#### 4.3 유량 제한 초과 방어 검증 (`429 Too Many Requests`)
슬라이딩 윈도우 유량 제한 한도를 초과하도록 연속 버스트 요청을 전송합니다.

```bash
# 연속 15회 버스트 요청 전송
echo "=== 슬라이딩 윈도우 유량 초과 버스트 전송 ==="
for i in $(seq 1 15); do
  BURST_NONCE="nonce-burst-$(date +%s%N)-$i"
  curl -i -s -o /dev/null -w "요청 #$i: HTTP %{http_code}\n" -X GET "https://api.enterprise.internal:8443/api/v1/orders/ORD-7701" \
    --cacert /etc/envoy/certs/ca.crt \
    --cert /etc/envoy/certs/client.crt \
    --key /etc/envoy/certs/client.key \
    -H "Authorization: Bearer valid-user-token-xyz" \
    -H "X-Nonce: ${BURST_NONCE}"
done

# 마지막 초과 요청 상세 확인
curl -i -X GET "https://api.enterprise.internal:8443/api/v1/orders/ORD-7701" \
  --cacert /etc/envoy/certs/ca.crt \
  --cert /etc/envoy/certs/client.crt \
  --key /etc/envoy/certs/client.key \
  -H "Authorization: Bearer valid-user-token-xyz" \
  -H "X-Nonce: nonce-burst-exceeded"
```

**유량 초과 시 기대 HTTP 응답:**
```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 10
Date: Thu, 29 Oct 2026 01:30:02 GMT
Content-Length: 53

{"status":"error","message":"Too Many Requests"}
```
- **검증 포인트**:
  - PoP-Sync 파이프라인 2단계에서 Redis 슬라이딩 카운터(`SLIDING_RL_LUA`)가 임계치 초과를 감지하여 `429 Too Many Requests` 및 `Retry-After: 10` 헤더를 반환합니다.
  - Envoy의 `allowed_client_headers_on_error` 및 Nginx의 `@handle_auth_error` 처리가 동작하여 클라이언트에게 `429` 상태 코드와 `Retry-After` 헤더를 전달합니다.

---

### 5. Envoy + ext_authz 성능 벤치마크 요약

Envoy PEP와 PoP-Sync PDP 연동 환경에서 측정한 성능 지표 요약이다. 상세 측정 데이터 및 분석 내용은 `docs/benchmarks/envoy-ext-authz-benchmark.md`를 참고한다.

1. **정상 트래픽 인가 오버헤드 (500 RPS 지속 부하)**:
   - Envoy L7 프록시 홉 오버헤드는 p50 기준 0.42ms이다.
   - L1 캐시 히트 시 PoP-Sync 인가 지연은 0.85ms이며, 총 p50 지연 시간은 1.62ms(p99 1.89ms)이다.
   - 캐시 미스 시 Policy DB 커넥션 풀(`MAX_DB_WORKERS = 5`, 20ms 지연) 세마포어 한계로 처리량이 248.5 RPS로 제한된다.

2. **동일 키 폭주 방어 (Single-Flight)**:
   - 캐시되지 않은 동일 키로 동시 요청 1,000건 유입 시, Single-Flight를 통해 999건을 병합하여 실제 DB 조회를 1건으로 제한한다.
   - p95 지연 시간은 Single-Flight 비활성화 시 4,065.1ms에서 활성화 시 27.3ms로 단축된다.

3. **악의적 공격 에지 차단**:
   - 중복 Nonce 리플레이 공격(1,500건)과 유량 초과 플러드 공격(1,500건)을 Envoy 에지에서 전건 차단한다.
   - 백엔드 도달 건수 0건을 유지하며, 백엔드 CPU 사용률 변동은 0%이다.

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
