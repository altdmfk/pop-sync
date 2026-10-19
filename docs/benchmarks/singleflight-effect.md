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
| `DB_TIMEOUT_SEC` | 1.0초 (gateway.py 하드코딩) |
| `MAX_DB_WORKERS` | 5 (gateway.py 하드코딩) |
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
db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + circuit_blocked
```

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
8. **단일 비행 OFF 시 취소(Cancellation) 동작 차이**: OFF 모드이더라도 DB 조회는 asyncio.shield로 감싸진 백그라운드 태스크에서 실행되므로, 클라이언트 연결이 끊겨도 DB 호출이 취소되지 않는 차이점이 존재한다 (pre-singleflight 버전과 다름).

---

## 6. 재현 방법

### 사전 준비

```powershell
# 1. docker-compose build (policy_db.py 변경이 이미지에 반영되도록)
cd C:\Users\user\Desktop\task\three\aegis-sync-main
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
docker exec aegis-sync-main-policy_db-1 env | findstr SIMULATED

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
