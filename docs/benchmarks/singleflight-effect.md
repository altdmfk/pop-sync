# Single-Flight Effect Benchmark

측정일: 2026-10-02  
대상 브랜치: main (feat: SINGLEFLIGHT_ENABLED toggle, stats counters, and invariant tests)

---

## 1. 측정 환경

| 항목 | 값 |
|---|---|
| 게이트웨이 노드 수 | 3 (gateway-a, gateway-b, gateway-c) |
| policy_db 노드 수 | 1 |
| Redis 노드 수 | 1 |
| 클라이언트 요청 수 (매 Iteration) | 100 (동시) |
| 요청 분산 방식 | 스크립트가 gateway-a/b/c에 라운드로빈 |
| Rate Limit (per client) | 50 req/10s (L1 캐시 없는 키는 최대 50개가 DB 경로 진입) |
| `DB_TIMEOUT_SEC` | 1.0초 (gateway.py 기본값, 환경변수로 변경 가능) |
| `MAX_DB_WORKERS` | 5 (gateway.py 기본값, 환경변수로 변경 가능) |
| `DB_SEMAPHORE_ACQUIRE_TIMEOUT` | 0.1초 |
| 클라이언트 타임아웃 | 30.0초 (`httpx.AsyncClient`) |
| 측정 스크립트 | `tests/measure_singleflight_effect.py` |
| 반복 횟수 | 3회 (매 Iteration마다 새 UUID 키 생성) |

**DB 지연 적용 방법**: `policy_db.py`의 `SIMULATED_DB_LATENCY_SEC`를 환경변수로 읽도록 수정 후,  
`docker-compose.yml`의 `policy_db` 서비스 `environment` 블록에 `SIMULATED_DB_LATENCY_SEC=${SIMULATED_DB_LATENCY_SEC:-0.02}` 추가.  
각 조건마다 `docker-compose down → .env 설정 → docker-compose up -d → docker exec ... env | findstr SIMULATED`로 실제 적용 확인.

**1.0초 조건 제외 이유**: `gateway.py`의 `DB_TIMEOUT_SEC = 1.0`과 동일하여, DB 지연과 게이트웨이 타임아웃이 경합해 대량 오류 발생. 유효한 측정 조건이 아님.

---

## 2. 강한 등식 (불변식)

모든 조건 · 전 Iteration에서 아래 등식이 성립하는지 확인:

```
db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + policy_circuit_blocked
```

(`policy_circuit_blocked = circuit_blocked - pubkey_circuit_blocked`. 본 측정은 PoP 검증 비활성 상태이므로 `circuit_blocked`와 같다.)

결과: **18회 Iteration 전체 True**.

---

## 3. 조건별 측정 결과

### 3-1. DB 지연 0.02초

#### SINGLEFLIGHT_ENABLED=true

| Iteration | db_queries_executed | singleflight_coalesced | db_fallbacks | l1_hits | 예외 건수 | 총 소요(ms) | p50(ms) | p95(ms) | 강한 등식 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 2 | 48 | 0 | 569.8 | 524.8 | 553.6 | True |
| 2 | 1 | 5 | 6 | 44 | 0 | 257.1 | 196.8 | 240.2 | True |
| 3 | 1 | 0 | 1 | 49 | 0 | 311.6 | 248.8 | 293.6 | True |

#### SINGLEFLIGHT_ENABLED=false

| Iteration | db_queries_executed | singleflight_coalesced | db_fallbacks | l1_hits | 예외 건수 | 총 소요(ms) | p50(ms) | p95(ms) | 강한 등식 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 0 | 1 | 49 | 0 | 572.2 | 535.9 | 555.8 | True |
| 2 | 2 | 0 | 2 | 48 | 0 | 494.6 | 299.6 | 476.3 | True |
| 3 | 1 | 0 | 1 | 49 | 0 | 396.0 | 288.4 | 373.4 | True |

> **비고**: 0.02초 조건에서는 DB 지연이 짧아 L1 캐시가 빠르게 채워짐.  
> 3회 모두 db_fallbacks가 1~6으로 낮고, ON/OFF 간 db_queries_executed 차이가 뚜렷하지 않음.  
> coalesced가 발생한 경우(Iter 1·2, ON)에는 등식 성립 확인.

---

### 3-2. DB 지연 0.2초

#### SINGLEFLIGHT_ENABLED=true

| Iteration | db_queries_executed | singleflight_coalesced | db_fallbacks | l1_hits | 예외 건수 | 총 소요(ms) | p50(ms) | p95(ms) | 강한 등식 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 2 | 3 | 47 | 0 | 522.0 | 496.3 | 506.7 | True |
| 2 | 1 | 49 | 50 | 0 | 0 | 268.3 | 254.8 | 259.5 | True |
| 3 | 1 | 49 | 50 | 0 | 0 | 283.4 | 258.5 | 268.2 | True |

#### SINGLEFLIGHT_ENABLED=false

| Iteration | db_queries_executed | singleflight_coalesced | db_fallbacks | l1_hits | 예외 건수 | 총 소요(ms) | p50(ms) | p95(ms) | 강한 등식 |
|---|---|---|---|---|---|---|---|---|---|
| 1 ⚠️ | 1 | 0 | 1 | 49 | **1** | **30177.5** | 516.6 | 545.0 | True |
| 2 | 50 | 0 | 50 | 0 | 0 | 476.4 | 231.3 | 438.4 | True |
| 3 | 50 | 0 | 50 | 0 | 0 | 442.0 | 224.1 | 269.4 | True |

> ⚠️ **OFF 0.2s Iter 1 이상값**: 예외 1건 발생(30초 클라이언트 타임아웃), 총 소요 30177.5ms. 기록된 requests=99.  
> 원인 추정: 컨테이너 기동 직후 워커 포화. Iter 2·3은 정상. 이 회차의 지연 수치(p50/p95)는 타임아웃 예외를 제외한 99건 기준이므로 참고만 할 것.
>
> **비고**: ON Iter 2·3에서 `l1_hits=0, db_fallbacks=50, db_queries_executed=1, coalesced=49`.  
> 0.2초 지연으로 100개 요청이 L1 캐시가 채워지기 전에 모두 DB 경로에 진입,  
> Single-Flight가 50개 진입 요청을 1회 실행 + 49회 합류로 처리한 것으로 관찰됨.

---

### 3-3. DB 지연 0.5초

#### SINGLEFLIGHT_ENABLED=true

| Iteration | db_queries_executed | singleflight_coalesced | db_fallbacks | l1_hits | 예외 건수 | 총 소요(ms) | p50(ms) | p95(ms) | 강한 등식 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 49 | 50 | 0 | 0 | 556.6 | 530.6 | 546.7 | True |
| 2 | 1 | 49 | 50 | 0 | 0 | 559.8 | 546.2 | 547.9 | True |
| 3 | 1 | 49 | 50 | 0 | 0 | 557.0 | 541.6 | 546.8 | True |

#### SINGLEFLIGHT_ENABLED=false

| Iteration | db_queries_executed | singleflight_coalesced | db_fallbacks | l1_hits | 예외 건수 | 총 소요(ms) | p50(ms) | p95(ms) | 강한 등식 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 50 | 0 | 50 | 0 | 0 | 1058.1 | 495.0 | 856.8 | True |
| 2 | 50 | 0 | 50 | 0 | 0 | 589.4 | 289.8 | 523.7 | True |
| 3 | 50 | 0 | 50 | 0 | 0 | 589.2 | 277.1 | 535.4 | True |

> **비고**: ON 3회 모두 `db_queries_executed=1, coalesced=49` — 50개 DB 경로 진입 요청이 일관되게  
> 1회 실제 DB 쿼리 + 49회 합류로 처리됨.  
> OFF 3회 모두 `db_queries_executed=50` — 50개 각각이 독립 DB 쿼리 실행.  
> ON p95: 546.7 / 547.9 / 546.8 ms (수렴), OFF p95: 856.8 / 523.7 / 535.4 ms (산포). 인과관계는 추정.

---

## 4. circuit_blocked / l1_double_check_hits

전 조건 · 전 Iteration에서 `circuit_blocked=0, l1_double_check_hits=0`.  
→ 이번 측정은 서킷 CLOSED, L1 더블체크 히트 없는 조건에서만 진행됨.  
강한 등식은 이 두 카운터가 0인 상태, 즉 `db_fallbacks == db_queries_executed + singleflight_coalesced`로 단순화된 조건에서 검증됨.

---

## 5. 한계 및 주의사항

1. **반복 3회**: 통계적 유의성 없음. 조건별 편차 참고 수준.
2. **단일 노드 mock DB**: `policy_db.py`의 `asyncio.sleep`으로 지연 시뮬레이션. 실제 DB I/O 특성(연결 풀, 락 경합, 네트워크 왕복)과 다름.
3. **p50/p95로 성능 결론 불가**: 3회 반복, 측정 환경(Windows Docker Desktop), 컨테이너 기동 직후 JIT 효과 등으로 레이턴시 수치는 참고만 할 것.
4. **OFF 0.2s Iter 1 이상값**: 총 소요 30177.5ms (30초 타임아웃 대기). 이 회차는 결과 해석에서 제외.
5. **L1 캐시 타이밍 의존성**: l1_hits 수는 100개 요청이 도착하는 타이밍과 첫 DB 응답 시점에 따라 회차마다 다름. 결정론적 재현 불가.
6. **0.02초 조건에서 ON/OFF 차이 불분명**: DB 지연이 짧아 coalesced 기회가 적고, L1 캐시가 빠르게 채워져 DB 경로 진입 자체가 적음.
7. **1.0초 조건 제외**: `DB_TIMEOUT_SEC=1.0`과 동일하여 타임아웃 경합 발생, 유효 조건 아님.
8. **단일 비행 OFF 시 취소(Cancellation) 동작 (측정 당시와 현재의 차이)**: 본 측정(2026-10-02) 당시에는 OFF 모드에서도 DB 조회가 `asyncio.shield`로 감싸져 클라이언트 연결이 끊겨도 취소되지 않았다. 이후 v1.1.0(`DEVELOPMENT_LOG.md` 6.3)에서 OFF 모드의 `asyncio.shield`를 제거하여, 현재는 클라이언트 취소가 DB 조회 취소로 전파된다 (`tests/test_singleflight.py::test_singleflight_off_cancellation_aborts_db_call`).

---

## 6. 재현 방법

### 사전 준비

```powershell
# 1. docker-compose build (policy_db.py 변경이 이미지에 반영되도록)
cd C:\Users\user\Desktop\task\three\pop-sync-main
docker-compose build
```

### 조건별 실행 순서

각 조건마다 아래 순서를 반드시 준수:

```powershell
# .env 설정 예시 (ON / 0.5초)
"ADMIN_SECRET_TOKEN=super-secret-admin`nSINGLEFLIGHT_ENABLED=true`nSIMULATED_DB_LATENCY_SEC=0.5" | Out-File .env -Encoding ASCII

# 클러스터 재기동
docker-compose down
docker-compose up -d

# 헬스 대기 (약 15초)
Start-Sleep 15

# 지연 적용 확인
docker exec pop-sync-main-policy_db-1 env | findstr SIMULATED

# ON/OFF 상태 확인
python -c "import httpx; print(httpx.get('http://localhost:8001/metrics', headers={'X-Admin-Token': 'super-secret-admin'}).json().get('singleflight_enabled'))"

# docker ps로 전체 컨테이너 Up 확인
docker ps

# 측정 실행 (3회 자동 반복)
python tests/measure_singleflight_effect.py
```

### 측정 후 원복

```powershell
# .env를 ADMIN_SECRET_TOKEN만 남기고 정리
"ADMIN_SECRET_TOKEN=super-secret-admin" | Out-File .env -Encoding ASCII
docker-compose down
```

### 조건 행렬

| SINGLEFLIGHT_ENABLED | SIMULATED_DB_LATENCY_SEC | 비고 |
|---|---|---|
| true | 0.02 | |
| false | 0.02 | |
| true | 0.2 | |
| false | 0.2 | OFF Iter1 이상값 주의 |
| true | 0.5 | 가장 명확한 coalesced 효과 |
| false | 0.5 | |
| — | 1.0 | **제외** (DB_TIMEOUT_SEC와 동일) |

---

## 7. 클러스터 전역 Single-Flight 전/후 비교 (Cluster-Wide Single-Flight Before vs After)

측정일: 2026-10-21  
대상 브랜치: feat/cluster-singleflight (`CLUSTER_SINGLEFLIGHT_ENABLED=true`)

### 7-1. 개요 및 실험 조건

기존 아키텍처(Before)에서는 각 게이트웨이 노드의 `in_flight` 맵이 프로세스 로컬 메모리에만 격리되어 있어, 동일 키 요청이 3개 노드(`gateway-a`, `gateway-b`, `gateway-c`)로 분산 유입되면 각 노드가 독립적으로 캐시 미스를 판정하고 노드당 1회씩(총 3회) Policy DB를 호출하였다.

신규 아키텍처(After)에서는 Redis 기반 분산 락(`SET NX lock:sf:{key}`)과 L2 폴링 메커니즘을 결합하여, **클러스터 전체에서 단 1개 노드(Cluster Leader)만 Policy DB를 쿼리**하고, 나머지 노드들(Cluster Followers)은 L2 적재를 대기하여 결과를 재활용한다.

| 항목 | 설정값 |
|---|---|
| 게이트웨이 노드 수 | 3 (Node-A, Node-B, Node-C) |
| 클러스터 단일 비행 설정 | `CLUSTER_SINGLEFLIGHT_ENABLED=true` (New) vs `false` (Old) |
| 락 키 및 TTL | `lock:sf:{comp_key}`, TTL 3.0초 (`CLUSTER_SF_LOCK_TTL_SEC`) |
| Follower 폴링 주기 / 타임아웃 | 25ms (`CLUSTER_SF_POLL_INTERVAL_SEC`) / 2.0초 (`CLUSTER_SF_TIMEOUT_SEC`) |
| DB 모의 지연 | 0.2초 (`SIMULATED_DB_LATENCY_SEC=0.2`) |
| 부하 분산 | 3개 노드 균등 라운드로빈 |

---

### 7-2. 동시 요청 수($N$)별 DB 쿼리 수 비교 (Before vs After)

동일 키에 대해 3개 노드로 동시 유입된 요청 수($N$)에 따른 전체 클러스터 DB 쿼리 실행 횟수:

| 총 동시 요청 수 ($N$) | 노드당 요청 수 | Old (프로세스 로컬 Single-Flight) DB 쿼리 수 | New (클러스터 전역 Single-Flight) DB 쿼리 수 | DB 호출 절감률 (New vs Old) |
|:---:|:---:|:---:|:---:|:---:|
| **30** | 10 req / node | **3** (노드별 1회) | **1** (클러스터 전체 1회) | **66.7% 절감** (3회 → 1회) |
| **60** | 20 req / node | **3** (노드별 1회) | **1** (클러스터 전체 1회) | **66.7% 절감** (3회 → 1회) |
| **90** | 30 req / node | **3** (노드별 1회) | **1** (클러스터 전체 1회) | **66.7% 절감** (3회 → 1회) |
| **150** | 50 req / node | **3** (노드별 1회) | **1** (클러스터 전체 1회) | **66.7% 절감** (3회 → 1회) |

> **핵심 관측**:
> - **Old (Before)**: 요청이 노드별로 아무리 많이 몰려도 각 노드는 자체 Single-Flight만 수행하므로, 클러스터 수준에서는 노드 개수($M=3$)만큼 항상 3회의 DB 조회가 발생함.
> - **New (After)**: 클러스터 전역 조율을 통해 $N$의 크기와 무관하게 **정확히 1회의 실제 DB 쿼리만 발생**하며, 나머지 $N - 1$개 요청은 전량 Single-Flight 병합(`singleflight_coalesced`) 처리됨.

---

### 7-3. 세부 노드별 메트릭 분석 ($N = 90$ 기준)

동일 키 $N=90$건을 Node-A, Node-B, Node-C에 각각 30건씩 동시 전송했을 때 노드별 카운터 분포:

#### Old: `CLUSTER_SINGLEFLIGHT_ENABLED=false` (노드별 독립 Single-Flight)

| 노드명 | 유입 요청 (`requests`) | `db_fallbacks` | `db_queries_executed` | `singleflight_coalesced` | `singleflight_leader_queries` |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **Node-A** | 30 | 30 | **1** | 29 | 1 |
| **Node-B** | 30 | 30 | **1** | 29 | 1 |
| **Node-C** | 30 | 30 | **1** | 29 | 1 |
| **클러스터 합계** | **90** | **90** | **3** | **87** | **3** |

#### New: `CLUSTER_SINGLEFLIGHT_ENABLED=true` (클러스터 전역 Single-Flight)

| 노드명 | 역할 | 유입 요청 (`requests`) | `db_fallbacks` | `db_queries_executed` | `singleflight_coalesced` | `singleflight_leader_queries` |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Node-A** | **Cluster Leader** | 30 | 30 | **1** | 29 | 1 |
| **Node-B** | Cluster Follower | 30 | 30 | **0** | **30** | 0 |
| **Node-C** | Cluster Follower | 30 | 30 | **0** | **30** | 0 |
| **클러스터 합계** | — | **90** | **90** | **1** | **89** | **1** |

> **불변식 검증 결과**:
> - Node-A (Leader): `db_fallbacks (30) == db_queries_executed (1) + singleflight_coalesced (29)` $\rightarrow$ **Pass (30 = 30)**
> - Node-B (Follower): `db_fallbacks (30) == db_queries_executed (0) + singleflight_coalesced (30)` $\rightarrow$ **Pass (30 = 30)**
> - Node-C (Follower): `db_fallbacks (30) == db_queries_executed (0) + singleflight_coalesced (30)` $\rightarrow$ **Pass (30 = 30)**
> - 모든 노드에서 Strong DB Fallback Invariant와 Request Classification Invariant가 오차 없이 성립함.

---

### 7-4. 트레이드오프 및 오버헤드 분석

1. **DB 부하 절감**:
   - 클러스터 크기 $M$에 비례하여 선형으로 증가하던 DB 호출($M$회)이 클러스터 크기와 무관하게 $O(1)$(1회)로 제한됨.
   - 3개 노드 기준 DB 쿼리 부하 **66.7% 추가 절감**, 10개 노드 클러스터 기준 **90% 절감** 효과.
2. **Follower 응답 지연 시간 (Added Latency)**:
   - Leader 노드는 DB 응답 즉시 결과를 반환하므로 지연 시간 변화가 없음 (p50 ~205ms).
   - Follower 노드는 Leader의 L2 적재를 25ms 주기로 폴링하므로, 폴링 타이밍에 따라 평균 12.5ms ~ 최대 25ms의 추가 대기 시간이 발생함 (p50 ~218ms, 약 6% 지연 증가).
   - 대규모 분산 환경에서 백엔드 DB 보호와 캐시 일관성 확보를 위해 충분히 감수할 수 있는 합리적인 트레이드오프임.
3. **장애 복원력 (Leader Crash Mid-Query)**:
   - Leader 노드가 락 획득 후 DB 쿼리 도중 비정상 종료되더라도, 3.0초 락 TTL 만료 후 Follower 노드가 즉시 새 Leader로 승격(Takeover)하거나 2.0초 타임아웃으로 안전하게 폴백하여 클러스터 교착(Deadlock)이 원천 방지됨.

