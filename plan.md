# PoP-Sync 프로젝트 계획 및 로드맵 (Plan & Roadmap)

분산 환경 카오스 엔지니어링 게이트웨이의 버전별 구현 단계, 아키텍처 로드맵, 검증 기준을 관리한다.

---

## 1. 프로젝트 목표 및 핵심 원칙

### 1.1 프로젝트 목적
네트워크 단절(파티션), Redis 인스턴스 장애, 대규모 유량 폭주(DDoS) 등 분산 환경 결함 발생 시 게이트웨이 복원력(Resilience)과 데이터 정합성을 검증·시연하는 카오스 엔지니어링 시스템을 구축한다.

### 1.2 핵심 설계 원칙
- **다단계 캐시 계층화**: L1(로컬 LRU) 및 L2(중앙 Redis) 계층화로 백엔드(Policy DB) 부하를 줄이고, 단일 비행(Single-Flight) 병합으로 캐시 미스 시 동시 쿼리 폭주를 차단함.
- **Fail-Closed 및 격리**: 장애 발생 시 비인가 접근과 리플레이 공격을 방어하기 위해 보수적 보안 원칙(Fail-Closed)을 적용하고, 벌크헤드 및 디바운스 서킷 브레이커로 연쇄 장애를 차단함.
- **최종 일관성(Eventual Consistency)**: 네트워크 파티션 해소 시 Redis Streams 기반 미처리 메시지(PEL) 재처리로 캐시 오염을 복구함.
- **실시간 관측성 및 정합성 검증**: 카오스 주입 콘솔을 구성하고, 전체 유입 요청 수와 세부 처리 분류 카운터 간의 불변성(Invariant) 수식으로 시스템 정합성을 입증함.

---

## 2. 버전별 구현 이력 및 로드맵 (Version Roadmap)

```
[v0.1.0 ~ v0.6.0] ────────> [v0.7.0 (Current)] ────────> [v0.8.0 (Next)] ────────> [v1.0.0 (Backlog)]
 기반 파이프라인 및 코어         1차 코드베이스 감사 및         2차 정밀 개선 백로그           프로덕션 인프라 고도화
 복원력/관측성 구현 완료         동시성/보안 리팩토링 완료      (Invariant/테스트/SSE)         (디스커버리/컨테이너)
 (완료 [x])                     (완료 [x])                    (계획 [ ])                    (계획 [ ])
```

---

### v0.1.0: 기본 분산 환경 및 게이트웨이 파이프라인 [완료]
- [x] **분산 게이트웨이 노드 구성**
  - FastAPI 기반 복수 노드(Node-A, Node-B, Node-C) 환경 구성
  - Docker Compose 기반 게이트웨이, Policy DB, Redis 클러스터 인프라 정의
- [x] **Policy DB 모의 서비스 구축**
  - 사용자 정책(Role, Status: allow/deny) 및 FIDO2 공개키 등록 엔드포인트 구현
  - 지연 시간(Simulated DB Latency) 환경변수 제어 기능 구현
- [x] **기본 요청 인증 파이프라인 수립**
  - 상위 mTLS 프록시(Envoy 가정)에서 주입되는 `X-SPIFFE-ID` 헤더 기반 식별 체계 구축

---

### v0.2.0: 다단계 캐시 및 분산 무효화 동기화 [완료]
- [x] **L1 인메모리 캐시 구현**
  - `OrderedDict` 기반 LRU 캐시 및 지연 만료(Lazy Expiry) 로직 구현
- [x] **L2 분산 캐시 연동**
  - Redis 기반 정책 캐시 연동 및 L1 미스 시 L2 조회 파이프라인 구현
- [x] **스트림 기반 캐시 무효화 (Stream Catch-up)**
  - 정책 변경 시 Redis Stream(`XADD`) 이벤트 발행
  - 게이트웨이 노드별 컨슈머 그룹(`XREADGROUP`) 수신 및 L1 캐시 무효화
  - 파티션 복구 시 미처리 메시지(PEL) 재처리 로직 구현

---

### v0.3.0: 유량 제어 및 리플레이 공격 방어 [완료]
- [x] **전역 Rate Limiter 구현**
  - Fixed Window 방식 구현 (Redis Pipeline 기반)
  - Sliding Window 방식 구현 (Lua 스크립트 기반 가중 합산 근사 알고리즘)
  - `RL_ALGORITHM` 런타임 토글 지원 (sliding / fixed)
- [x] **원자적 Nonce 소비 및 리플레이 공격 방어**
  - 클라이언트 생성 UUID Nonce를 Redis Lua 스크립트로 원자적 검증 및 소비 처리
- [x] **보안 강화 및 해시 충돌 방어**
  - 복합 키 생성 시 `len:value` 접두사 포맷을 적용하여 해시 충돌 가능성 차단
  - 운영/카오스 라우트 대상 `ADMIN_SECRET_TOKEN` 제로 트러스트 인증 적용

---

### v0.4.0: 복원력(Resilience) 및 장애 격리 메커니즘 [완료]
- [x] **서킷 브레이커 (Circuit Breaker) 구축**
  - Policy DB 다운 감지 시 CLOSED -> OPEN -> HALF_OPEN -> CLOSED 상태 전이 구현
  - L1/L2 캐시 히트는 정상 응답, 캐시 미스 시 `degraded_allow` 처리
- [x] **Flapping 방지 디바운스 적용**
  - DB 복구 직후 지연된 과거 타임아웃 신호로 인한 재오픈 방지를 위해 2.0초 디바운스(`CB_DEBOUNCE_SEC`) 적용
- [x] **동시성 안전성 및 리소스 격리 (Bulkhead)**
  - `asyncio.to_thread` 작업 제어용 세마포어(`MAX_DB_WORKERS`) 적용
  - 파티션 제어용 `partition_lock`, Redis 스왑용 `redis_swap_lock` 적용
  - Redis 핫스왑 시 인플라이트 커넥션 보호를 위해 2.0초 대기 후 닫는 지연 종료(Graceful Drain) 적용
- [x] **인가 우회 결함 수정 및 Fail-Closed 원칙 정립**
  - DB 응답 내 `status: "deny"` 명시적 검증 및 캐싱 로직 구현
  - 에러 삼킴(Silent Swallow) 제거 및 장애 시 503 즉각 반환 처리

---

### v0.5.0: Single-Flight 요청 병합 및 ECDSA PoP 서명 검증 [완료]
- [x] **단일 비행(Single-Flight) 요청 병합**
  - 동일 정책 키 동시 DB 조회를 1회로 병합(Coalescing)하여 DB 과부하 차단
  - `SINGLEFLIGHT_ENABLED` 환경변수 토글 지원
- [x] **PoP (Proof of Possession) 서명 검증 구현**
  - ECDSA P-256 서명 검증 로직 구현 (`POP_VERIFY_ENABLED` 토글)
  - Nonce 검증 통과 이후 서명을 검증하도록 순서를 배치하여 타인 Nonce 위조 시 즉각 소진(Burn) 처리

---

### v0.6.0: 실시간 관측성 및 카오스 엔지니어링 대시보드 [완료]
- [x] **실시간 카오스 제어 콘솔 (Control Plane)**
  - Server-Sent Events (SSE) 기반 실시간 상태 브로드캐스팅
  - 네트워크 파티션, DB 다운, Redis 장애, 트래픽 버스트 제어 UI 구성
- [x] **대시보드 메트릭 패널 구성**
  - 불변성(Invariant) 패널, 단일 비행(Single-Flight) 병합 패널, Config/DB 상태 패널 구현
- [x] **통계 정합성(Invariant) 방정식 검증**
  - `requests == rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks`
  - 정상 및 단일 장애 시나리오 검증 수행

---

### v0.7.0: 1차 코드베이스 감사 및 동시성/보안 리팩토링 (Current) [완료]
- [x] **Single-Flight 취소 및 Future 라이프사이클 누수 수정**
  - `db_loader_task` 예외 처리를 `BaseException`으로 확장하여 `asyncio.CancelledError` 감지 시 Follower Future 취소 전파
  - `finally` 블록에서 `in_flight.get(comp_key) is fut` 검증 후 삭제하도록 수정하여 신규 Future 오삭제 방지
- [x] **관리자 인증 토큰 타이밍 공격 방어**
  - `gateway.py` 및 `control_plane.py`의 `X-Admin-Token` 검증 로직에 `hmac.compare_digest` 적용하여 사이드 채널 공격 차단
- [x] **L1Cache 및 CircuitBreaker 캡슐화 개선**
  - `L1Cache` 하드코딩 매직 넘버 제거, 전역 설정 연동, `clear()` 메서드 추가, `delete()` 락 내부 단일 원자 연산 처리
  - `CircuitBreaker`에 `current_state` 읽기 전용 프로퍼티를 구현하여 내부 락 기반 안전한 상태 참조 지원
- [x] **HTTP 및 Redis 커넥션 풀링 구조화 및 테스트 호환성 확보**
  - `get_async_db_client()`, `get_sync_http_client()`, `get_redis_client()`, `get_async_http_client()` 지연 초기화 팩토리 구현
  - `TestClient` 환경에서 lifespan 미기동 시에도 커넥션 풀 접근 가능하도록 보완
  - `control_plane.py`의 `httpx.AsyncClient`에 연결 풀 한도(최대 200) 설정 적용
- [x] **예외 은폐 제거 및 코드베이스 주석 정돈**
  - L2 캐시 조회/적재의 `except Exception: pass` 구문을 제거하고 `logging` 적용
  - 인라인 임포트 정리, ASCII 배너 구분선 및 튜토리얼 주석 제거, 아키텍처 의도 중심 주석으로 전환

---

### v0.8.0: 2차 정밀 개선 백로그 (Next) [계획]
- [ ] **Redis 장애 503 응답 Invariant 카운터 정렬**
  - 현상: Redis 다운 시 Nonce 검증에서 503으로 조기 반환되는 요청이 Invariant 합산 분류에 집계되지 않아 대시보드 방정식 오차 발생
  - 작업: `stats["redis_outage_blocked"]` 카운터를 신설하고 `requests == sum(counters) + redis_outage_blocked` 수식으로 대시보드 및 게이트웨이 정합성 정렬
- [ ] **pytest 테스트 스위트 격리 및 E2E 마커 분리**
  - 현상: `test_scenarios.py`, `test_rl_fallback.py`가 도커 컨테이너 미구동 로컬 환경에서 실행 시 타임아웃 대기 발생
  - 작업: 통합/E2E 테스트 파일에 `@pytest.mark.e2e`를 지정하고 `pytest.ini`에 기본 단위 테스트(`unit`)와 `e2e` 실행 프로파일 분리
- [ ] **Control Plane SSE 브로드캐스트 아키텍처 개선**
  - 현상: SSE 클라이언트 연결마다 1초 주기 폴링 태스크가 독립 실행되어 대시보드 다중 접속 시 게이트웨이 부하 중복 유발
  - 작업: 백그라운드 단일 폴러 태스크가 상태를 수집하고 `asyncio.Queue` 기반 브로드캐스트 팬아웃 방식으로 구조 전환
- [ ] **Scope B PoP 서명 필드 바인딩 강화**
  - 현상: 현재 PoP 서명이 고정 문자열(`pop_signature`) 검증에 머무름
  - 작업: 타임스탬프 허용 오차(예: 30초) 및 HTTP Body SHA-256 해시 바인딩을 추가하여 리플레이 및 변조 방어 심화

---

### v1.0.0: 인프라 고도화 및 운영 환경 준비 (Backlog) [계획]
- [ ] **동적 서비스 디스커버리 (Service Discovery)**
  - 하드코딩된 게이트웨이 노드 목록(Node-A 등) 및 IP를 Consul 또는 DNS 기반 동적 디스커버리로 전환
- [ ] **컨테이너 빌드 최적화 (Multi-stage Dockerfile)**
  - 게이트웨이, Control Plane, Policy DB 단일 빌드 이미지 분리 및 최종 런타임 이미지 경량화
- [ ] **전용 부하 생성 도구 분리**
  - Control Plane 내장 부하 생성기 대신 k6 스크립트 기반 고부하 분산 테스트 환경 구축

---

## 3. 마일스톤 검증 기준 (Acceptance Criteria)

| 검증 영역 | 기준 | 현재 상태 (v0.7.0) |
| :--- | :--- | :---: |
| **코어 파이프라인** | 게이트웨이 3개 노드, Control Plane, Policy DB, Redis 정상 기동 및 통신 | **PASS** |
| **다단계 캐시** | L1 미스 -> L2 미스 -> DB 조회 순 동작 및 파티션 해소 시 PEL 동기화 | **PASS** |
| **단일 비행** | 동일 키 50개 동시 요청 시 DB 조회가 1회로 병합 처리 및 태스크 취소 안전성 확보 | **PASS** |
| **보안 및 유량 제어** | 슬라이딩 윈도우 유량 제한, Nonce 소진, 상수 시간 토큰 비교(`hmac.compare_digest`) | **PASS** |
| **서킷 브레이커** | DB 다운 시 OPEN 전이, degraded 응답, 2.0s 디바운스 플래핑 방지, 프로퍼티 캡슐화 | **PASS** |
| **단위 테스트** | `test_singleflight.py`, `test_rate_limit.py`, `test_pop.py` 전체 통과 (23 passed) | **PASS** |
| **통계 불변성** | 정상 구동 시 Invariant 방정식 일치 (Redis 장애 시 503 집계 분리는 v0.8.0 과제) | **PASS (v0.8.0 연계)** |
| **E2E 테스트 격리** | 단위 테스트와 E2E 테스트 마커 분리 | **TODO (v0.8.0)** |
| **프로덕션 인프라** | 동적 서비스 디스커버리 및 멀티스테이지 Dockerfile 분리 | **TODO (v1.0.0)** |
