# PoP-Sync

네트워크 파티션, Redis 장애, 대규모 DDoS 상황에서 분산 게이트웨이의 복원력(Resilience)을 실시간으로 시연하는 카오스 엔지니어링 데모다.

---

## 주요 기능

### 서킷 브레이커 (Circuit Breaker)
Policy DB 장애 감지 시 OPEN 상태로 전환하여 연쇄 장애를 차단한다. 2.0초의 디바운스 로직으로 Flapping 현상을 제어하며, CLOSED -> OPEN -> HALF_OPEN -> CLOSED 사이클을 안정적으로 관리한다.

### L1/L2 캐시 및 단일 비행 (Cache & Single-Flight)
정책 조회 경로는 다단계로 동작한다. 먼저 노드 본체의 L1 (OrderedDict LRU, 지연 만료)을 확인하고, 없으면 중앙 Redis(L2)를 조회한다. 모두 캐시 미스 시 Policy DB를 직접 호출한다.
SINGLEFLIGHT_ENABLED가 활성화되어 있어, 캐시 미스로 인해 발생하는 동일 키에 대한 동시 DB 호출을 하나로 병합(Coalescing)하여 과부하를 방지한다 (예: 100개 요청이 Rate Limit 50개를 통과해도 DB 호출은 1회만 발생. 효과 검증: docs/benchmarks/singleflight-effect.md).

### 스트림 기반 무효화 동기화 (Stream Catch-up)
네트워크 단절(파티션) 발생 시 지연됐던 Redis Stream의 대기 메시지(PEL)를 누락된 메시지부터 연속으로 긴급 적용한다. 최종 일관성(Eventual Consistency)을 보장한다.

### O(1) 데이터 리밋 & Nonce 리플레이 방어
RL_ALGORITHM 토글을 통해 데이터 리밋 방식을 결정한다(기본값은 sliding). sliding 방식은 Lua 스크립트를 통한 가중 합산 근사치 기반으로 동작하며, fixed 방식은 Redis Pipeline을 사용한다. (경계 버스트 측정 결과: docs/benchmarks/rate-limit-sliding.md). Rate Limit은 노드당 한도가 아닌, 클라이언트 ID(SPIFFE ID) 단위의 클러스터 전역(Global) 한도를 갖는다.
Lua 스크립트 기반 원자적 Nonce 소비로 DDoS와 리플레이 공격을 방어한다.

### PoP 검증 (Proof of Possession - Scope B)
FIDO2 모델에서 영감을 받은 경량화된 ECDSA P-256 서명 검증(Request-digest)을 지원한다 (POP_VERIFY_ENABLED 기본값 False). Policy DB의 키 레지스트리(엔드포인트)를 통해 공개키를 조회하며, 원천 차단 대신 논스 소진(burn)을 통한 재사용 방지를 위해 Nonce 검증 통과 **이후**에 서명 검증을 수행한다. 통계 분류를 위해 누락 및 검증 실패 카운터를 별도 관리한다.

---

## 신뢰 경계 및 계승 (Trust Boundary & Inheritance)

본 게이트웨이는 클라이언트 식별과 Rate Limiting에 X-SPIFFE-ID HTTP 헤더를 의존한다.

- **mTLS 위임**: mTLS 인증은 상위 L4/L7 리버스 프록시(Envoy 등)에서 처리된다고 가정한다. 프록시가 클라이언트 인증서를 검증하고 X-SPIFFE-ID 헤더를 주입하는 구조다.
- **데모 스코프**: 프록시 계층 구현과 mTLS 인증서 파싱은 본 게이트웨이에서는 구현하지 않으며 상위 프록시가 처리한다고 가정한다. 테스트 환경에는 단일 Demo Key만 등록되어 있다. 프록시가 Trust Boundary를 보장한다는 전제하에 분산 시스템 복원력에만 집중한다.

### fido2-pop-gateway 및 pop-mesh 대비 변경 사항
| 계승 / 변경 / 미이식 | 원본 구현체 | 현재 코드베이스 (PoP-Sync) |
| --- | --- | --- |
| **언어/프레임워크** | Java / Reactor | Python / FastAPI |
| **인메모리 캐시** | Caffeine | OrderedDict LRU (지연 만료, Lazy expiry) |
| **무효화 전송 계층** | Redis Pub/Sub | Redis Streams (XREADGROUP 기반 PEL 복구 지원) |
| **데이터 리밋** | Sliding Window Log | 가중 합산 근사치 기반 슬라이딩 카운터 |
| **PoP 서명** | WebAuthn CBOR Envelope | 단순화된 ECDSA P-256 (Canonical Request-digest, Scope B) |

---

## 요청 처리 파이프라인 (`/auth` 단계별 동작 흐름)

클라이언트가 `GET /auth` 엔드포인트를 호출하면 게이트웨이는 아래 **6단계 파이프라인**을 순서대로 수행한다. 어느 단계에서든 요청이 종료되면 즉시 해당 분류 카운터가 1 증가하며, 전체 유입 요청 수(`requests`)와 세부 카운터 합계는 항상 일치한다.

```text
[클라이언트 GET /auth 요청]
   │
   ├─ 0. 미들웨어 격리 검사 ── (노드 파티션 상태) ──> 503 Node Isolated (진입 전 차단, requests 미집계)
   │
   ├─ 1. 필수 헤더 검증 ────── (Authorization / X-Nonce / X-SPIFFE-ID 누락) ──> 401 Missing Headers
   │
   ├─ 2. 유량 제어 (Rate Limit)
   │     ├─ Redis 정상: 클러스터 전역 Sliding/Fixed (50 req / 10s) 초과 ──> 429 Too Many Requests (rate_limited +1)
   │     ├─ Redis 장애: 노드별 로컬 Sliding 폴백 (15 req / 10s) 초과 ────> 429 Too Many Requests (rate_limited +1)
   │     └─ 예기치 못한 내부 예외 ───────────────────────────────────────> 503 Rate Limit Failed (redis_outage_blocked +1)
   │
   ├─ 3. 원자적 Nonce 소진 (Replay Defense)  ※ 고비용 암호 연산(PoP) 전에 실행하여 공격자 Nonce 즉시 소진(Burn)
   │     ├─ Redis 장애/타임아웃 (분산 락 불가 → 데이터 오염 방지) ───────> 503 Nonce Check Failed [Fail-Closed] (redis_outage_blocked +1)
   │     └─ 이미 사용된 Nonce (EXISTS == 1) ─────────────────────────────> 401 Replay Detected (nonce_replays +1)
   │
   ├─ 4. PoP 서명 검증 (POP_VERIFY_ENABLED=true 시 활성화)
   │     ├─ X-PoP-Signature 헤더 누락 ───────────────────────────────────> 400 Missing PoP Signature (pop_rejected +1)
   │     ├─ 공개키 조회 (L1 → L2 → Policy DB /pubkey)
   │     │     ├─ 서킷 브레이커 OPEN 상태 ───────────────────────────────> 503 Key Fetch Failed [Fail-Closed] (circuit_blocked +1, pop_rejected +1)
   │     │     ├─ 미등록 SPIFFE ID (DB 404) ─────────────────────────────> 401 Unknown Key ID (pop_rejected +1)
   │     │     └─ DB 타임아웃/5xx (cb.record_failure) ───────────────────> 503 Key Fetch Failed (pop_rejected +1)
   │     └─ ECDSA P-256 서명 검증 실패 (위변조/타인 키) ─────────────────> 401 Invalid PoP Signature (pop_rejected +1)
   │        (검증 통과 시 pop_verified +1 후 다음 단계 진행)
   │
   ├─ 5. 다단계 캐시 조회 (복합 키: SHA-256("len:sid" + "len:token" + "len:res" + "len:act"))
   │     ├─ L1 (로컬 OrderedDict LRU, TTL 60s) 히트 ─────────────────────> 200 OK [source: "L1"] (l1_hits +1)
   │     └─ L2 (중앙 Redis, TTL 60s) 히트 (L1 적재 후 반환) ─────────────> 200 OK [source: "L2"] (l2_hits +1)
   │
   └─ 6. Policy DB 폴백 및 단일 비행 (L1/L2 모두 캐시 미스 → db_fallbacks +1)
         ├─ 동일 키 In-Flight 존재 (Single-Flight Follower 합류) ────────> Leader 결과 대기 및 공유 (singleflight_coalesced +1)
         ├─ 서킷 브레이커 OPEN 차단 ─────────────────────────────────────> 200 OK [status: "degraded_allow", source: "Fallback_Degradation"] (circuit_blocked +1)
         └─ DB Loader 실행 (Leader)
               ├─ 태스크 진입 직후 L1 더블체크 히트 ─────────────────────> 200 OK [source: "L1"] (l1_double_check_hits +1)
               ├─ DB 세마포어(MAX_DB_WORKERS=5) 0.1s 내 획득 실패 ───────> 503 DB Overload [Fast-Fail] (db_queries_executed +1)
               ├─ Policy DB 응답 성공 (L1/L2 적재, cb.record_success) ───> 200 OK [status: "allow"|"deny", source: "DB"] (db_queries_executed +1)
               └─ Policy DB 타임아웃/503 (cb.record_failure) ────────────> 200 OK [status: "degraded_allow", source: "Fallback_Degradation_PostFail"] (db_queries_executed +1)
```

---

## 카오스 엔지니어링 및 복원력 시나리오 상세

클러스터를 직접 구동하지 않더라도 각 시나리오의 **주입 방법, 내부 동작 메커니즘, 실제 반환되는 HTTP/JSON 응답, 메트릭 변화**를 그대로 파악할 수 있도록 9가지 시나리오를 상세히 기술한다.

### 전체 시나리오 요약 매트릭스

| # | 시나리오명 | 트리거 (API / 설정) | 핵심 방어 메커니즘 | 기대 HTTP 응답 및 상태 | 핵심 관측 메트릭 |
| :-: | :--- | :--- | :--- | :--- | :--- |
| **1** | **중복 요청 및 리플레이 방어** | `POST /chaos/nonce` | Redis Lua 원자적 Nonce 소진 (`NONCE_LUA`) | 1개 노드 `200 OK`, 다른 노드 `401 Replay Detected` (Redis 장애 시 양쪽 `503`) | `nonce_replays +1` (장애 시 `redis_outage_blocked +1`) |
| **2** | **대규모 DDoS 및 경계 버스트 방어** | `POST /chaos/ddos` (60건 동시 폭주) | 비오염 Check-Before-Increment O(1) 가중 합산 슬라이딩 윈도우 (`50 req / 10s`) | 첫 실행 `200` 50건 / `429` 10건, 연속 실행 시 `429` 60건 (100% 차단) | `rate_limited` 증가, `Retry-After: 10` 헤더 |
| **3** | **동일 키 폭주 및 단일 비행 병합** | `POST /chaos/same_key_burst` (`count=50`) | In-Flight Future 병합 (`asyncio.shield`) 및 L1 재활용 | 50건 모두 `200 OK`, **실제 DB 쿼리는 단 1회만 실행** (부하 98% 절감) | `db_queries_executed +1`, `singleflight_coalesced + delta_l1_hits = 49` |
| **4** | **백엔드 DB 장애 격리 및 서킷 브레이커** | `POST /chaos/db/down` → `/chaos/db/up` | 3-State 서킷 브레이커, HALF_OPEN 단일 프로브 원자적 게이팅, 2.0s 디바운스 | 캐시 히트는 `200 OK(L1/L2)`, 캐시 미스는 `200 OK(degraded_allow)` 폴백 | `circuit_state: OPEN`, `circuit_blocked` 증가 |
| **5** | **중앙 Redis 장애 및 로컬 생존 모드** | `POST /chaos/redis/down` → `/chaos/redis/up` | 로컬 Rate Limit 폴백(`15 req / 10s`) + Nonce `503 Fail-Closed` | 노드당 한도(15건) 초과분은 `429`, 통과분(15건)은 Nonce 단계에서 `503` 차단 | `local_rl_size` 활성화, `redis_outage_blocked` 증가, Invariant `PASS` |
| **6** | **네트워크 파티션 및 PEL 긴급 복구** | `POST /chaos/partition/Node-B/isolate` → `heal` | 고립 노드 `503` 차단(Split-Brain 방어) + 복구 시 Stream PEL(`"0"`) 전량 드레인 및 XAUTOCLAIM 유휴 회수 | 단절 중 `/auth`는 `503 Node Isolated`, 복구 즉시 누락 무효화 일괄 반영 | `is_partitioned: true→false`, `last_catchup` 상세 지표 기록 |
| **7** | **실시간 캐시 무효화 스트림 전파** | `POST /chaos/invalidate` | Redis Streams `XADD` (`maxlen~10000`) + 노드별 `XREADGROUP` | 전 노드의 L1 및 L2 캐시에서 대상 정책 키 즉시 삭제 및 `XACK` | 전 노드 `l1_cache_size` 감소 후 재조회 시 `source: "DB"` |
| **8** | **ECDSA P-256 PoP 서명 검증** | `POP_VERIFY_ENABLED=true` | Nonce 선행 소진 후 ECDSA 서명 검증 및 `/pubkey` 서킷 연동 | 정상 `200`, 서명 누락 `400`, 위변조/미등록 키 `401`, DB 서킷 OPEN 시 `503` | `pop_verified` / `pop_rejected` 분리 집계 |
| **9** | **정책 거부(DENY) 인가 검증** | `X-Res: restricted` 또는 `.../deny` | DB `status: "deny"` 명시적 판별 및 `"DENY"` 캐싱 | `200 OK` (`{"status": "deny", "policy": "DENY"}`) 반환 및 L1/L2 캐싱 | 후속 요청 시 L1/L2에서 `"DENY"` 즉시 반환 |

---

### 시나리오 1: 중복 요청 및 리플레이 방어 (Distributed Nonce & Idempotency)
- **검증 목적**: 동일한 일회용 토큰(`X-Nonce`)을 가진 쓰기(`X-Act: write`) 요청이 서로 다른 두 게이트웨이 노드에 동시에 유입될 때(Race Condition), 클러스터 전역에서 **정확히 1회만 승인**되고 나머지는 차단되는지 검증한다.
- **실행 흐름 (`POST /chaos/nonce` / `tests/test_scenarios.py::test_nonce_atomicity`)**:
  1. Control Plane이 새로운 UUID `nonce`를 생성하여 가용한 두 게이트웨이(예: `Node-A`, `Node-B`)에 동일한 헤더(`X-Nonce`, `X-SPIFFE-ID: spiffe://example.org/workload`, `X-Act: write`)로 동시에 `GET /auth` 요청을 발송한다.
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
    → 어느 한 노드는 무조건 `200 OK`, 다른 노드는 무조건 `401 Replay Detected`가 된다.
  - **Redis 장애 상태 (Redis DOWN)**:
    - 분산 락/조율이 불가능한 상태에서 임의로 요청을 통과시키면 이중 결제나 데이터 오염이 발생하므로, 양쪽 노드 모두 **`503 Service Unavailable - Nonce Check Failed` (Fail-Closed)**로 안전하게 차단한다.
  - **메트릭 변화**: 승인된 노드의 `db_fallbacks`(또는 `l1_hits`/`l2_hits`) `+1`, 차단된 노드의 `nonce_replays` `+1`.

---

### 시나리오 2: 대규모 트래픽(DDoS) 및 경계 버스트 방어 (O(1) Sliding-Window Rate Limiter)
- **검증 목적**: 공격자가 다수의 게이트웨이 노드로 트래픽을 분산시켜 공격하더라도 클러스터 전역 한도(`50 req / 10s`)로 즉각 차단(`429`)하고, 고정 윈도우(Fixed Window)의 치명적 약점인 **윈도우 경계 2배 버스트(Boundary Burst)**를 방어하는지 검증한다.
- **실행 흐름 (`POST /chaos/ddos` / `tests/test_scenarios.py::test_rate_limiting`)**:
  1. Control Plane이 단일 공격자 ID(`spiffe://example.org/attacker`)로 60개의 요청(각기 다른 Nonce)을 생성하여 가용한 게이트웨이 노드들에 무작위로 분산해 동시에 전송한다.
  2. 각 노드는 `SLIDING_RL_LUA` 스크립트를 통해 직전 10초 윈도우 카운트(`prev`)와 현재 윈도우 카운트(`curr`)를 조회하고, 현재 윈도우 경과 시간(`elapsed`)에 따른 가중치 합산 추정치를 계산한다:
     $$\text{estimated} = \left\lfloor \text{prev} \times \frac{10 - \text{elapsed}}{10} \right\rfloor + \text{curr}$$
  3. `estimated <= 50`(`RL_LIMIT`)이면 통과시키고, `50`을 초과하면 즉시 `429 Too Many Requests`를 반환한다.
- **실행하지 않고 확인하는 결과**:
  - **최초 DDoS 실행 시 (60건 전송)**:
    ```json
    {
      "status": "flooded",
      "responses": {
        "200": 50,
        "429": 10
      }
    }
    ```
    → 3개 노드로 요청이 흩어져 들어가더라도 노드별 한도가 아닌 **클러스터 전역 한도 50건**만 정확히 허용되고 나머지 10건은 `429`로 차단된다.
  - **10초 이내 연속 클릭 시**: 이미 윈도우 한도가 소진되었으므로 `{"status": "flooded", "responses": {"429": 60}}`으로 **100% 차단**된다.
  - **경계 버스트 실측 비교 (`docs/benchmarks/rate-limit-sliding.md`)**:
    - 10초 윈도우가 바뀌기 **0.5초 전에 50건**, 바뀐 직후 **0.5초 후에 50건**(총 1초간 100건)을 전송했을 때:
    - **`fixed` 모드**: 윈도우가 리셋되면서 직전 50건 + 직후 50건 = **총 100건 통과** (한도의 2배 스파이크 허용).
    - **`sliding` 모드 (기본값)**: 직후 시점에 이전 윈도우 가중치(약 95%)가 유지되어 직전 50건 통과 + 직후 50건 전량 `429` 차단 = **총 50건만 통과** (경계 스파이크 완벽 방어).
  - **429 차단 요청 카운터 비오염 (Check-Before-Increment)**:
    `SLIDING_RL_LUA` 및 로컬 폴백(`_local_rate_limit`)은 카운터를 사전 증가시키지 않고, 이전 및 현재 윈도우 가중 합산치(`estimated`)를 먼저 검사하여 `estimated < limit`인 경우에만 원자적으로 `INCR` 및 만료 시간을 설정한다. 이를 통해 수백~수천 건의 DDoS 버스트 공격이 발생하여 `429`로 전량 차단되더라도 윈도우 카운터가 비대해지지 않아, 공격 종료 직후 다음 윈도우($W_1$)에서 정상 트래픽이 기아 상태(Starvation) 없이 즉시 수용된다.

---

### 시나리오 3: 동일 키 동시 폭주 방어 (Cache Stampede & Single-Flight Coalescing)
- **검증 목적**: 캐시 만료 직후 또는 신규 사용자에 대해 동일한 키로 수십~수백 개의 요청이 동시에 쏟아질 때(Cache Stampede / Thundering Herd), 백엔드 Policy DB로 중복 쿼리가 전달되지 않고 **단 1회의 쿼리로 병합(Coalescing)**되는지 검증한다.
- **실행 흐름 (`POST /chaos/same_key_burst?node=Node-A&count=50` / `tests/test_singleflight.py`)**:
  1. 캐시에 존재하지 않는 새로운 `spiffe://example.org/burst_{uuid}`로 50개의 동시 요청을 `Node-A`에 전송한다.
  2. 50개 요청 모두 Rate Limit(한도 50)와 Nonce 검사를 통과한 뒤 L1/L2 캐시 미스를 겪고 DB 폴백 경로(`db_fallbacks += 50`)에 진입한다.
  3. **최초 1개 요청(Leader)**만 `in_flight[comp_key]`에 `asyncio.Future`를 등록하고 `db_loader_task`를 실행하여 Policy DB에 HTTP 요청을 보낸다(`db_queries_executed += 1`).
  4. **나머지 49개 요청(Followers)**은 `in_flight`에서 해당 Future를 발견하고 DB 호출 없이 `await asyncio.shield(fut)`로 대기(`singleflight_coalesced` 증가)하거나, Leader 응답 직후 채워진 L1 캐시를 읽어(`l1_hits` / `l1_double_check_hits`) 즉시 반환된다.
  5. 만약 대기 중이던 Leader 클라이언트가 연결을 끊더라도(`CancelledError`), DB 조회 태스크는 독립 백그라운드 태스크와 `asyncio.shield`로 보호되어 취소되지 않고 나머지 Follower들에게 결과를 정상 전달한다.
- **실행하지 않고 확인하는 결과**:
  - **API 응답 예시**:
    ```json
    {
      "node": "Node-A",
      "count": 50,
      "delta_db_queries_executed": 1,
      "delta_singleflight_coalesced": 49,
      "delta_l1_hits": 0
    }
    ```
  - **DB 지연 시간(`SIMULATED_DB_LATENCY_SEC`) 및 토글에 따른 실측 비교 (`docs/benchmarks/singleflight-effect.md`)**:
    - **DB 지연 `0.2s ~ 0.5s` 환경**: 50개 요청이 DB 응답 전에 모두 동시에 도착하므로, **Single-Flight ON** 시 `db_queries_executed = 1`, `singleflight_coalesced = 49`가 일관되게 기록된다(**DB 부하 98% 절감**). 반면 **Single-Flight OFF** 시에는 `db_queries_executed = 50`으로 50건 모두가 각각 DB를 호출한다.
    - **기본 DB 지연 `0.02s` 환경**: DB 응답이 매우 빨라 50개 요청이 소켓에 도달하는 도중에 이미 첫 응답이 완료되어 L1 캐시를 채우므로, 초기 도달분(예: 4~10건)은 `singleflight_coalesced`로 병합되고 후속 도달분(40~45건)은 `l1_hits`로 흡수된다. 어느 경우든 **실제 DB 쿼리(`delta_db_queries_executed`)는 단 1회**만 발생한다.

---

### 시나리오 4: 백엔드 Policy DB 장애 격리, 서킷 브레이커 전이 및 디바운스
- **검증 목적**: Policy DB 장애 시 게이트웨이 스레드 풀이 타임아웃 대기로 고갈되는 연쇄 장애(Cascading Failure)를 막고, 복구 시 지연된 실패 신호로 인해 서킷이 다시 열리는 플래핑(Flapping) 현상을 제어하는지 검증한다.
- **실행 흐름 (`POST /chaos/db/down` & `/chaos/db/up` / `tests/test_scenarios.py::test_circuit_breaker`, `tests/test_flapping.py`)**:
  1. **DB 장애 발생 및 `OPEN` 전환**:
     - `/chaos/db/down` 호출 시 Policy DB는 모든 요청에 2.0초 지연 후 `503 DB Outage`를 반환하도록 전환되며, Control Plane이 게이트웨이들의 서킷을 `OPEN`으로 전환한다.
     - 강제 전환 없이 자연 장애가 발생하더라도, 게이트웨이의 DB 타임아웃(`DB_TIMEOUT_SEC = 1.0s`) 실패가 **3회(`CB_MAX_FAILURES = 3`)** 누적되면 서킷이 `CLOSED` → `OPEN`으로 자동 전환된다.
  2. **`OPEN` 상태에서의 요청 유형별 차등 처리**:
     - **L1 / L2 캐시 히트 요청**: 이미 캐싱된 정책이 있으므로 DB 장애와 무관하게 `200 OK` (`source: "L1" | "L2"`)로 1ms 미만 정상 처리된다.
     - **일반 정책 조회 캐시 미스 (`/auth`)**: `cb.can_execute()`가 `False`를 반환하므로 DB를 호출하지 않고(`circuit_blocked += 1`), 서비스 가용성 유지를 위해 즉시 **`200 OK` (`{"status": "degraded_allow", "source": "Fallback_Degradation"}`)**를 반환한다. (서킷이 열리기 직전 실제 DB 호출을 시도했다가 실패한 요청은 `source: "Fallback_Degradation_PostFail"`을 반환한다.)
     - **PoP 공개키 조회 캐시 미스 (`/pubkey`, `POP_VERIFY_ENABLED=true`)**: 공개키 없이는 암호학적 신원 확인이 불가능하므로 `degraded_allow`를 허용하지 않고 즉시 **`503 Service Unavailable - Key Fetch Failed` (Fail-Closed)**로 차단한다(`circuit_blocked += 1`, `pop_rejected += 1`).
  3. **10초 경과 후 `HALF_OPEN` 원자적 단일 프로브 및 자동 복구 (`CB_RESET_TIMEOUT_SEC = 10s`)**:
     - DB가 복구된 후 마지막 실패 시점으로부터 **10초**가 지나면, 다음 캐시 미스 요청에서 서킷이 `OPEN` → `HALF_OPEN`으로 전환되어 DB 호출을 허용한다.
     - **원자적 단일 프로브 게이팅 (`_half_open_probe_in_flight`)**: `HALF_OPEN` 전이 시 동시 유입된 다수의 캐시 미스 요청 중 **오직 단 1건의 시험 요청만 원자적으로 프로브 슬롯을 획득**하여 백엔드 DB를 검증하며, 프로브 진행 중 유입되는 다른 동시 요청들은 `circuit_blocked`로 안전하게 차단되어 복구 중인 DB에 트래픽 쇄도(Thundering Herd)가 발생하는 것을 원천 방지한다.
     - **취소 및 세마포어 과부하 데드락 방어**: 시험 프로브가 세마포어 타임아웃(`DBOverloadError`)이나 클라이언트 코루틴 취소(`asyncio.CancelledError`)를 겪더라도 `release_half_open_probe_if_unrecorded()`를 통해 프로브 슬롯이 즉시 반환되어 데드락 없이 다음 요청이 재시도할 수 있다.
     - **성공 시**: `cb.record_success()`가 호출되어 즉시 `CLOSED` 상태로 완전 복구(`failures = 0`, 프로브 슬롯 반환)되고, 정상 응답(`{"status": "allow", "source": "DB", "policy": "admin"}`)을 반환한다.
     - **실패 시**: `HALF_OPEN` 상태에서는 디바운스를 우회하여 **단 1회 실패만으로도 즉시 다시 `OPEN` 상태로 재개방**된다.
  4. **플래핑(Flapping) 방지 2.0초 디바운스 (`CB_DEBOUNCE_SEC = 2.0s`)**:
     - DB 다운 상태에서 200개의 동시 요청이 유입된 직후 DB를 복구(`CLOSED` 전환)하더라도, 이미 스레드 풀에 들어가 있던 이전 요청들이 뒤늦게 타임아웃 실패를 기록할 수 있다.
     - 서킷 브레이커는 `CLOSED` 상태 전이 직후 **2.0초 이내에 도착하는 지연 실패 신호를 모두 무시(Debounce)**하므로, 복구 직후 서킷이 다시 `OPEN`으로 튕기지 않고 안정적으로 `CLOSED`를 유지한다.
  5. **세마포어 벌크헤드 (`MAX_DB_WORKERS = 5`)**:
     - `sync` 모드에서 동시 DB 쿼리가 5개를 초과하여 `0.1초`(`DB_SEMAPHORE_ACQUIRE_TIMEOUT`) 내에 워커 슬롯을 얻지 못하면 `DBOverloadError`를 발생시켜 `503 Service Unavailable - DB Overload`로 Fast-Fail 처리한다(단순 과부하로 서킷이 열리는 것을 막기 위해 서킷 실패 카운트에는 합산하지 않는다).

---

### 시나리오 5: 중앙 Redis 전면 장애 및 로컬 생존 모드 (Redis Outage & Degraded Mode)
- **검증 목적**: 분산 상태 계층인 Redis가 완전히 중단되었을 때, **가용성(Rate Limit 로컬 폴백, L1 캐시)**과 **보안 정합성(Nonce 검증 Fail-Closed)**을 설계 의도대로 차등 적용하는지 검증한다.
- **실행 흐름 (`POST /chaos/redis/down` / `tests/test_redis_down.py`, `tests/test_rl_fallback.py`)**:
  1. `/chaos/redis/down` 호출 시 각 게이트웨이는 Redis 클라이언트를 응답하지 않는 더미 주소(`127.0.0.1:9999`, 소켓 타임아웃 `0.1s`)로 원자적 핫스왑(`redis_swap_lock`)한다. 이때 처리 중이던 인플라이트 요청이 끊기지 않도록 기존 클라이언트는 **2.0초 대기 후 종료(Graceful Drain)**한다.
  2. **1단계 Rate Limit (로컬 인메모리 폴백 가동)**:
     - `check_rate_limit()`에서 Redis 연결 예외가 발생하면 즉시 `_local_rate_limit()`으로 폴백한다.
     - 각 노드는 로컬 메모리(`OrderedDict`, 최대 10,000키 LRU 축출)에서 현재 윈도우와 직전 윈도우 버킷을 가중 합산하여 **노드당 10초에 15건(`RL_FALLBACK_LIMIT = 15`)**까지만 통과시키고, **16번째 요청부터는 Redis 없이도 즉시 `429 Too Many Requests`로 차단**한다.
  3. **2단계 Nonce 리플레이 검사 (`503 Fail-Closed` 차단)**:
     - 로컬 Rate Limit(15건 이하)을 통과한 요청은 이어지는 `consume_nonce()` 단계에 도달한다.
     - Nonce 검사는 타 노드와의 중복 실행을 막기 위해 반드시 중앙 Redis가 필요하므로, Redis 연결 실패 시 요청을 통과시키지 않고 **`503 Service Unavailable - Nonce Check Failed` (Fail-Closed)**를 반환하며 `redis_outage_blocked` 카운터를 증가시킨다.
- **실행하지 않고 확인하는 결과 (`docs/benchmarks/rate-limit-sliding.md`)**:
  - **단일 노드에 20개 요청 동시 전송 시 (`tests/test_redis_down.py`)**:
    - 20개 요청 중 로컬 한도(15건)를 초과한 **5건은 1단계에서 `429 Too Many Requests`**로 차단된다(`rate_limited += 5`).
    - 로컬 한도 내인 **15건은 1단계를 통과한 뒤 2단계 Nonce 검사에서 `503 Service Unavailable - Nonce Check Failed`**로 차단된다(`redis_outage_blocked += 15`).
    - *(참고: 실측 3회 중 1·2회차는 정확히 `503: 15건 / 429: 5건`이 기록되었으며, 3회차에서 요청이 10초 윈도우 경계에 걸쳐 분산되었을 때만 윈도우당 카운트가 15에 도달하지 않아 `503: 20건 / 429: 0건`이 관측되었다.)*
  - **3개 노드에 90개 요청 라운드로빈 전송 시 (`tests/test_rl_fallback.py`)**:
    - **Redis 정상 시**: 클러스터 전역 한도 50건이 적용되어 **`200 OK`: 50건, `429`: 40건**.
    - **Redis 다운 시**: 노드 3개가 각각 로컬 한도 15건씩을 허용(총 45건 통과 → Nonce 단계에서 503)하고 나머지 15건씩을 차단하여 **`503`: 45건, `429`: 45건**.
  - **대시보드 `Redis 장애 생존성 검증` 버튼 실행 결과**:
    - Nonce 동시 요청 테스트는 `503 Fail-Closed (데이터 오염 방지 안전 차단)`으로, DDoS 테스트는 `429 방어 (로컬 메모리 슬라이딩 윈도우)`로 표시되며, 모든 `503` 응답이 `redis_outage_blocked`에 합산되어 **Invariant 배지는 `Pass` 상태를 유지**한다.

---

### 시나리오 6: 네트워크 단절(Partition) 격리 및 Redis Streams PEL 긴급 복구 (Stream Catch-up)
- **검증 목적**: 특정 노드가 네트워크 파티션으로 고립되었을 때 오염된 캐시로 응답하는 Split-Brain을 막고, 네트워크 복구 시 고립 기간 동안 누락된 모든 캐시 무효화 이벤트를 순서대로 전량 반영(Eventual Consistency)하는지 검증한다.
- **실행 흐름 (`POST /chaos/partition/Node-B/isolate` → `/chaos/invalidate` → `/chaos/partition/Node-B/heal` / `tests/test_scenarios.py::test_partition_catchup`)**:
  1. **초기 캐시 적재**: `Node-B`에 요청을 보내 L1 캐시에 정책 데이터를 적재한다(예: `l1_cache_size = 5`).
  2. **노드 격리 (`isolate`)**:
     - `Node-B`의 `is_partitioned` 플래그가 `True`로 전환된다.
     - 격리된 동안 `Node-B`로 유입되는 모든 `/auth` 요청은 미들웨어에서 즉시 **`503 Service Unavailable` (`{"status": "error", "message": "Node Isolated"}`)**로 거부된다(오염된 L1 캐시 사용 원천 차단).
     - `Node-B`의 백그라운드 `stream_listener`도 스트림 읽기를 일시 중단한다.
  3. **고립 중 정책 무효화 발생 (`POST /chaos/invalidate` 4회 실행)**:
     - 가용 노드(`Node-A` 또는 `Node-C`)가 Redis Stream(`invalidation_stream`)에 무효화 이벤트 4건을 발행한다.
     - `Node-A`, `Node-C`는 즉시 수신하여 L1 캐시를 비우지만, 고립된 `Node-B`는 수신하지 못해 `l1_cache_size`가 이전 값 그대로 남아 있는다(Stale 상태).
  4. **네트워크 복구 (`heal`) 및 3단계 전량 동기화 (Catch-up)**:
     - `/chaos/partition/Node-B/heal` 호출 시 `is_partitioned = False`로 해제되고, DB 상태를 확인하는 `probe_db_health()`가 백그라운드로 1회 실행된다.
     - `stream_listener`가 복구를 감지하고 일반 루프 진입 전에 **긴급 Catch-up 루프**를 실행한다:
       - **Step A — 자체 PEL(미확인 메시지) 전량 드레인 (`ID = "0"`)**: 단절 직전 컨슈머에게 배달되었으나 아직 `XACK`하지 못해 Pending Entries List(PEL)에 남아 있는 메시지들을 `XREADGROUP ... COUNT 100`으로 **빈 배치가 반환될 때까지(`while True`) 반복 조회**하여 L1/L2 캐시를 삭제하고 `XACK`한다(100건 초과 시에도 누락 없이 전량 처리).
       - **Step B — 장기 유휴 메시지 자동 회수 (`XAUTOCLAIM`)**: 다른 컨슈머 또는 이전 프로세스에서 장시간(`STREAM_CLAIM_MIN_IDLE_MS`, 기본 60,000ms) 미확인 상태로 방치된 메시지를 `XAUTOCLAIM`으로 자동 인계받아 캐시 삭제 및 `XACK`를 수행한다.
       - **Step C — 단절 기간 중 누적된 신규 메시지 전량 드레인 (`ID = ">"`)**: 단절 동안 스트림에 새로 쌓여 아직 이 컨슈머 그룹에 전달되지 않은 메시지들을 `XREADGROUP ... COUNT 100`으로 **빈 배치가 반환될 때까지 반복 조회**하여 캐시 삭제 및 `XACK`를 완료한다.
- **실행하지 않고 확인하는 결과**:
  - 복구 후 1초 이내에 `Node-B`의 `/metrics`를 조회하면 `stats.last_catchup`에 동기화 결과가 상세히 기록된다:
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
  - 무효화 대상 키들이 `Node-B`의 L1 캐시에서 모두 삭제되어 클러스터 전체의 캐시 일관성이 복원된다.

---

### 시나리오 7: 실시간 캐시 무효화 스트림 전파 (Pub/Sub Stream Invalidation)
- **검증 목적**: 관리자가 특정 정책이나 공개키를 무효화할 때 클러스터 내 모든 정상 게이트웨이 노드의 L1/L2 캐시가 실시간으로 동기화 삭제되는지 검증한다.
- **실행 흐름 (`POST /chaos/invalidate` 또는 `POST /chaos/invalidate/{key}`)**:
  1. Control Plane이 가용 게이트웨이 노드를 통해 Redis Stream(`invalidation_stream`)에 `XADD invalidation_stream MAXLEN ~ 10000 key {target_key}` 이벤트를 발행한다.
  2. 각 게이트웨이(`Node-A`, `Node-B`, `Node-C`)는 서로 다른 고유 컨슈머 그룹(`gateway_group_Node-A`, `gateway_group_Node-B`, `gateway_group_Node-C`)을 가지므로, 동일한 스트림 이벤트를 **모든 노드가 독립적으로 브로드캐스트 수신**한다.
  3. 각 노드의 `stream_listener`는 메시지에서 `key`를 추출하여 로컬 `l1_cache.delete(key)` 및 Redis `DEL policy:{key}`를 수행한 뒤 `XACK`를 전송한다.
- **실행하지 않고 확인하는 결과**:
  - 이벤트 발행 즉시 3개 노드의 L1 캐시와 Redis L2 캐시에서 해당 키가 삭제되며, 이후 첫 `/auth` 요청은 반드시 Policy DB를 다시 조회(`source: "DB"`)하여 최신 정책을 반영한다.

---

### 시나리오 8: ECDSA P-256 PoP(Proof of Possession) 서명 검증 (Scope B)
- **검증 목적**: `POP_VERIFY_ENABLED=true` 환경에서 클라이언트가 개인키 소유 증명을 위해 제출한 `X-PoP-Signature`를 검증하고, 위변조·키 불일치·리플레이 공격·DB 장애 상황에서 올바르게 차단하는지 검증한다(`tests/test_pop.py`).
- **서명 생성 규격**:
  - **정규화 문자열(Canonical Payload)**: `"{HTTP_METHOD}\n{PATH}\n{X-SPIFFE-ID}\n{X-Nonce}"` (예: `"GET\n/auth\nspiffe://test/valid\n550e8400-e29b-41d4-a716-446655440000"`)
  - **알고리즘 및 인코딩**: ECDSA P-256(`SECP256R1`) + SHA-256 서명 후 Raw `(r || s)` 64바이트(또는 DER)를 URL-safe Base64로 인코딩하여 `X-PoP-Signature` 헤더로 전송.
- **세부 케이스별 실행 결과 (돌려보지 않고 확인 가능한 판정 표)**:

| 세부 케이스 | 요청 조건 | 내부 동작 순서 | 반환 상태 코드 및 응답 메시지 | 증가하는 카운터 |
| :--- | :--- | :--- | :--- | :--- |
| **8-1. 정상 서명** | 유효한 개인키로 현재 요청의 Method, Path, SPIFFE ID, Nonce 서명 | Rate Limit 통과 → Nonce 소진 → 공개키 조회(L1/L2/DB) → 스레드 풀(`pop_executor`)에서 ECDSA 검증 성공 → 정책 조회 | `200 OK` (`{"status": "allow", "policy": "admin"}`) | `pop_verified +1`, `db_fallbacks +1` (또는 `l1/l2_hits +1`) |
| **8-2. 서명 헤더 누락** | `X-PoP-Signature` 미포함 | Nonce 소진 직후 헤더 존재 여부 검사에서 즉시 거부 | `400 Bad Request` (`"Missing PoP Signature"`) | `pop_rejected +1` |
| **8-3. 페이로드 위변조 / 타인 키** | `/other` 경로로 서명 후 `/auth` 호출, 또는 다른 개인키로 서명 | 공개키로 검증 시 해시 불일치 (`InvalidSignature`) | `401 Unauthorized` (`"Invalid PoP Signature"`) | `pop_rejected +1` |
| **8-4. 미등록 클라이언트 키** | `X-SPIFFE-ID: spiffe://test/nokey` | Policy DB `GET /pubkey`가 `404 Key not found` 반환 | `401 Unauthorized` (`"Unknown Key ID"`) | `pop_rejected +1` |
| **8-5. 리플레이된 Nonce 공격** | 이미 사용된 `X-Nonce` + 임의의 서명 전송 | **PoP 서명 검증 전에 Nonce 검사 단계에서 즉시 차단** (고비용 ECDSA 연산을 수행하지 않아 CPU 고갈 DoS 방어) | `401 Unauthorized` (`"Replay Detected"`) | `nonce_replays +1` (`pop_rejected`는 `0` 유지) |
| **8-6. DB 장애 중 공개키 캐시 미스** | 서킷 `OPEN` 상태에서 L1/L2에 없는 신규 ID로 요청 | Nonce는 소진(Burn)된 후, `/pubkey` DB 폴백 전 `cb.can_execute()`에서 즉시 차단 (Fail-Closed) | `503 Service Unavailable` (`"Service Unavailable - Key Fetch Failed"`) | `circuit_blocked +1`, `pop_rejected +1` |

---

### 시나리오 9: 정책 거부(DENY) 인가 검증
- **검증 목적**: Policy DB가 접근 거부(`status: "deny"`)를 반환할 때 게이트웨이가 이를 정상적으로 캐싱하고 인가 거부 상태를 반환하는지 검증한다.
- **실행 흐름 및 결과**:
  - 요청 헤더의 리소스가 `X-Res: restricted`이거나 `X-SPIFFE-ID`가 `/deny`로 끝나면, Policy DB(`GET /policy/{key}`)는 `{"status": "deny", "role": "none"}`을 반환한다.
  - 게이트웨이는 이를 `"DENY"` 문자열로 L1 및 L2 캐시에 저장하고 **`200 OK` (`{"status": "deny", "source": "DB", "policy": "DENY"}`)**를 반환한다.
  - 이후 동일 조건의 후속 요청은 DB를 거치지 않고 L1/L2 캐시에서 즉시 `{"status": "deny", "source": "L1", "policy": "DENY"}`를 반환받는다.

---

## 관측성 및 불변성(Invariant) 수식 검증

시스템은 모든 정상·장애 시나리오에서 카운터 누락이나 중복 집계가 없음을 수학적으로 입증하기 위해 두 가지 **불변식(Invariant)**을 실시간으로 검증한다.

### 1. 전체 요청 분류 불변식 (Request Classification Invariant)
`/auth` 엔드포인트에서 처리가 완료된 모든 요청(`requests`)은 아래 7개 상호 배타적 종료 경로의 합과 정확히 일치해야 한다:

$$\text{requests} = \text{rate\_limited} + \text{nonce\_replays} + \text{pop\_rejected} + \text{l1\_hits} + \text{l2\_hits} + \text{db\_fallbacks} + \text{redis\_outage\_blocked}$$

- 대시보드 각 노드 카드 하단의 `Invariant: Pass (N = N)` 배지가 1초마다 이 등식을 계산하여 표시한다.
- Redis 장애로 인해 Rate Limit 또는 Nonce 단계에서 `503`으로 조기 차단된 요청도 `redis_outage_blocked`에 집계되므로 장애 중에도 항상 `Pass`를 유지한다.

### 2. DB 폴백 강한 불변식 (Strong DB Fallback Invariant)
L1과 L2 캐시를 모두 미스하여 DB 폴백 단계로 진입한 요청(`db_fallbacks`)은 아래 4개 내부 처리 경로의 합과 항상 일치한다:

$$\text{db\_fallbacks} = \text{db\_queries\_executed} + \text{singleflight\_coalesced} + \text{l1\_double\_check\_hits} + \text{circuit\_blocked}$$

- **대시보드 구성**:
  - **상단 Active Faults 바**: 현재 주입된 장애(`Redis: Down`, `Policy DB: Down`, `Node-B: Isolated`) 표시 및 `Reset All Faults (전체 정상 복구)` 원클릭 복구 버튼 제공.
  - **좌측 토폴로지 & 인프라 레이어**: `Node-A`, `Node-B`, `Node-C`의 실시간 RPS, 평균 지연 시간(Avg Lat), L1/L2 Hit, 서킷 브레이커 상태, Invariant 배지, 세부 `Config & Resilience Stats` 드롭다운 및 하단 `cluster-events.log` 실시간 터미널 콘솔.
  - **우측 Chaos Controls (3개 탭)**:
    - `1. 트래픽 및 동시성`: 중복 요청 테스트(`/chaos/nonce`), DDoS 시뮬레이션(`/chaos/ddos`), Same-key Burst 50(`/chaos/same_key_burst`).
    - `2. 인프라 장애 격리`: DB 강제 중단/복구(`/chaos/db/down|up`), Redis 강제 중단/복구(`/chaos/redis/down|up`) 및 `Redis 장애 생존성 검증`.
    - `3. 네트워크 및 동기화`: Node-B 통신 단절/복구(`/chaos/partition/Node-B/isolate|heal`), 캐시 무효화 이벤트 발행(`/chaos/invalidate`).

---

## 보안 시크릿과 관측성

Control Plane의 `/chaos/*` 엔드포인트와 `/metrics/stream` SSE 스트림, 그리고 Gateway 및 Policy DB의 `/chaos/*`, `/metrics` 엔드포인트는 모두 `X-Admin-Token` 헤더(타이밍 공격 방어를 위한 `hmac.compare_digest` 상수 시간 비교 적용)로 보호된다.

- `.env.example`을 `.env`로 복사한 뒤, `ADMIN_SECRET_TOKEN`의 값을 보안 환경에 맞게 강한 토큰으로 교체하거나 로컬 데모 시 그대로 사용한다(공백 전용 토큰은 기동 시 거부됨).
- **프론트엔드**: 브라우저 탭 종료 시 자동 소멸되는 `sessionStorage`에 저장되며 EventSource 연결 및 카오스 명령 호출 시 검증을 거친다.
- **SSE 단일 폴러 팬아웃**: Control Plane은 대시보드 접속자 수와 무관하게 백그라운드 단일 폴러(`broadcast_metrics_loop`)가 1초 간격으로 3개 게이트웨이의 `/metrics`를 동시 폴링(`asyncio.gather`)하여 구독자 큐(`subscribers`)로 팬아웃 브로드캐스팅한다.
- **클러스터 불변식 감사 (`GET /chaos/audit`)**: Control Plane의 `GET /chaos/audit` 엔드포인트(`X-Admin-Token` 인증)는 모든 게이트웨이 노드의 2대 불변식(요청 분류 및 강한 DB 폴백) 일치성과 캐시 히트율, Single-Flight 병합율, DB 부하 절감 비율을 종합 감사하여 구조화된 JSON 리포트(`overall_invariant_pass: bool`)를 즉시 반환한다.

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

# 단위 및 경계 테스트 실행 (Docker 불필요, Mock 기반으로 51개 테스트 즉시 실행)
pytest -v

# E2E 시나리오 테스트 실행 (실제 Docker 클러스터 구동 필요)
pytest -m e2e
```

---

## 알려진 한계점 (Known Limitations & Future Work)

- **근사 알고리즘 오차**: Sliding Window는 단순 가중 합산 근사치이므로 트래픽 분포에 따라 미세한 오차가 발생할 수 있다.
- **노드 간 시각차 (Clock Skew)**: 게이트웨이 로컬 `time.time()`을 기준으로 윈도우 키를 계산하므로, 노드 간 시각차가 클 경우 윈도우 경계 계산에서 불일치가 발생할 수 있다.
- **단일 비행 취소(Cancellation) 동작 차이**: OFF 모드라도 백그라운드 태스크(`asyncio.shield`)에서 DB 조회가 계속 진행되므로, 클라이언트 연결이 끊겨도 DB 호출이 취소되지 않는 차이점이 존재한다 (pre-singleflight 버전과 다름).
- **클러스터 전역 Single-Flight 미지원**: `in_flight` 맵은 각 게이트웨이 프로세스 로컬 메모리에만 존재하므로, 동일 키 요청이 서로 다른 게이트웨이 노드로 분산 유입될 경우 노드별로 각각 1회씩 DB 호출이 발생한다.
- **성능 실측의 한계**: Single-flight 성능 검증은 단일 노드에서 DB 지연을 시뮬레이션하여 3회 반복한 제한적 조건에서 수행되었다 (`docs/benchmarks/singleflight-effect.md`).
- **PoP 서명의 한계**: Scope B 규격에 따라 검증 필드에 Timestamp, Body, Query, Host가 결합되지 않으며(Unbound fields), `kid`(Key-ID) 헤더 지원 및 하드웨어 Attestation 파싱 기능이 없다. 데모 환경에는 단일 Demo Key만 등록되어 있다.
- **PoP 테스트 구성**: PoP 기능(Toggle ON) 시나리오는 클라이언트가 실제로 ECDSA 서명을 생성해야 하므로 기본 `test_scenarios.py`(서명 없이 전송) 환경과 분리되어 `tests/test_pop.py`에서 검증된다.

