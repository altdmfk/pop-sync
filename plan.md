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
[v0.1.0 ~ v0.6.0] ────> [v0.7.0] ────> [v0.8.0] ────> [v0.9.0 (Current)] ────> [v1.0.0 (Backlog)]
 기반 파이프라인 및      1차 코드베이스   2차 리팩토링     3차 감사 백로그 반영       프로덕션 인프라 고도화
 복원력/관측성 구현      동시성/보안      (Invariant/SSE/  (CB/PEL/RL/라이프사이클/   (디스커버리/컨테이너/
 (완료 [x])              (완료 [x])       테스트 격리 [x])  픽스처/문서 복구 [x])      부하 도구 계획 [ ])
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

### v0.7.0: 1차 코드베이스 감사 및 동시성/보안 리팩토링 [완료]
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

### v0.8.0: 2차 정밀 개선 및 테스트 격리 [완료]
- [x] **Redis 장애 503 응답 Invariant 카운터 정렬**
  - `gateway.py`에 `stats["redis_outage_blocked"]` 신설, Nonce 검증 실패(503 Fail-Closed) 시 증가 처리
  - `static/index.html` Invariant RHS 방정식에 `stats.redis_outage_blocked` 합산 추가하여 Redis 다운 시에도 `Pass (LHS == RHS)` 평가되도록 수정
  - `test_invariant_equality_under_redis_503` 단위 테스트 검증 완료
- [x] **pytest 테스트 스위트 격리 및 E2E 마커 분리**
  - `pytest.ini`에 `markers = e2e` 및 `addopts = -m "not e2e"` 등록
  - 외부 도커 클러스터 의존 테스트 모듈/함수에 `@pytest.mark.e2e` 및 `pytestmark = pytest.mark.e2e` 부여
  - `pytest -v` 실행 시 단위 테스트만 실행하고 `pytest -m e2e`로 E2E 스위트 타겟팅 분리
- [x] **Control Plane SSE 브로드캐스트 아키텍처 개선 (Single-Poller Fan-Out)**
  - `control_plane.py`의 클라이언트별 독립 폴링 루프를 제거하고 백그라운드 단일 폴러 태스크(`broadcast_metrics_loop`)로 전환
  - 게이트웨이 폴링 트래픽을 $N$배에서 1회로 고정하고, `subscribers` 집합 내 `asyncio.Queue`로 논블로킹 팬아웃 브로드캐스팅
  - 클라이언트 연결 종료 시 `finally` 블록에서 큐 제거(`discard`)로 메모리 누수 방지
  - `test_control_plane.py` 단위 테스트 검증 완료
- [x] **단위 테스트 전역 상태 격리 및 고가치 경계 테스트 구축**
  - `conftest.py`에 `clean_gateway_state` autouse 픽스처 및 `mock_common_dependencies`를 구축하여 `POP_VERIFY_ENABLED` 누수 방지
  - 서킷 브레이커 2.0초 디바운스 경계 테스트(`CLOSED` -> `OPEN` -> `HALF_OPEN`) 추가
  - Single-Flight 예외 전파 및 `in_flight` 맵 정리(`finally: pop`) 경계 테스트 추가
- [x] **Scope B PoP 서명 필드 바인딩 범위 유지 (아키텍처 경계 명시)**
  - Scope B 규격에 따라 `/auth`는 GET 요청이며 일회용 Nonce 원자적 소진을 전제하므로 Timestamp/Body 비결합(Unbound) 상태를 유지함. (향후 Scope C/POST 확장 시 바인딩 검토 대상)

---

### v0.9.0: 3차 리팩토링 — 감사 백로그 반영 (Current) [완료]
- [x] **서킷 브레이커 HALF_OPEN 즉시 재개방 및 `/pubkey` 연동 (B-01, B-05)**
  - `CircuitBreaker.record_failure()`에서 `HALF_OPEN` 상태일 때 2.0초 디바운스를 우회하여 실패 즉시 `OPEN`으로 재개방하도록 수정
  - PoP 활성화 시 `/pubkey` DB 폴백 경로에 `cb.can_execute()`, `cb.record_success()`, `cb.record_failure()` 및 `sync` 모드 세마포어 타임아웃(`DB_SEMAPHORE_ACQUIRE_TIMEOUT`) 적용
- [x] **로컬 Rate Limit 슬라이딩 윈도우 보존 및 LRU 축출 (B-03)**
  - `cleanup_local_rl`이 직전 윈도우(`now_window - 1`)를 유지하고 `now_window - 2` 이하만 삭제하도록 수정
  - `local_rate_limits`를 `OrderedDict`로 전환하고 `RL_LOCAL_MAX_KEYS` 도달 시 전체 `clear()` 대신 가장 오래된 항목만 `popitem(last=False)`로 축출하도록 개선
- [x] **Stream PEL 전량 드레인, 메시지 헬퍼 통합 및 스트림 상한 설정 (B-02)**
  - 파티션 복구 시 `XREADGROUP ... "0"` 호출을 `while True:` 루프로 감싸 100건 초과 PEL도 전량 드레인한 뒤 `">"`로 전환
  - `stream_listener` 내 3곳의 중복 메시지 처리 루프를 `_process_stream_messages` 헬퍼로 통합하고 `invalidate_cache`의 `xadd`에 `maxlen=10000, approximate=True` 적용
- [x] **백그라운드 태스크 참조 보관 및 안전한 종료 순서 보장 (B-04)**
  - 모듈 전역 `background_tasks` 집합에 모든 백그라운드 태스크를 등록·자동 해제(`discard`)하도록 구성
  - `lifespan` 종료 시 백그라운드 태스크 완료를 먼저 대기한 뒤 HTTP/Redis 클라이언트를 닫고 스레드 풀을 `shutdown(wait=False, cancel_futures=True)`로 종료하도록 수정
- [x] **Redis 스크립트 지연 등록 및 관리자 토큰 검증 강화 (B-07, B-08)**
  - `get_redis_client()`에서 `nonce_script`/`rl_script`를 지연 등록하고 `check_rate_limit`의 `"fixed"` 분기에서도 `get_redis_client().pipeline(...)`을 사용하도록 수정
  - `ADMIN_SECRET_TOKEN` 공백 문자열 거부, Control Plane `fetch_metrics` 동시 폴링(`asyncio.gather`) 및 `raise_for_status()` 적용, `/chaos/auth_check` 및 `policy_db.py:/chaos/db/{state}` 토큰 검증 추가
- [x] **Invariant 카운터 보완, 중복 DOM ID 제거, 픽스처 통합 및 문서 복구 (B-10, B-11, B-12)**
  - `check_rate_limit` 503 예외 시 `redis_outage_blocked` 증가 및 `static/index.html` 중복 DOM ID 제거
  - `conftest.py`의 `mock_common_dependencies`로 테스트 픽스처 일원화, `test_flapping`/`test_rl_node_fallback` E2E 함수 추가(`41 passed, 12 deselected`)
  - 마크다운 제어 문자 오염 복구 및 벤치마크·개발 로그 정합성 현행화 (B-06, B-09는 동작 영향이 없고 변경 범위가 커서 의도적 제외)

---

### v1.0.0: 인프라 고도화 및 운영 환경 준비 (Backlog) [계획]
- [ ] **동적 서비스 디스커버리 (Service Discovery)**
  - 하드코딩된 게이트웨이 노드 목록(Node-A 등) 및 IP를 Consul 또는 DNS 기반 동적 디스커버리로 전환
- [ ] **컨테이너 빌드 최적화 (Multi-stage Dockerfile)**
  - 게이트웨이, Control Plane, Policy DB 단일 빌드 이미지 분리 및 최종 런타임 이미지 경량화
- [ ] **전용 부하 생성 도구 분리**
  - Control Plane 내장 부하 생성기 대신 k6 스크립트 기반 고부하 분산 테스트 환경 구축
- [ ] **분산 고도화 잔여 과제**
  - 서킷 브레이커 `HALF_OPEN` 동시 시험 요청 1건 제한, 클러스터 전역 Single-Flight, Redis Streams `XAUTOCLAIM` 및 미사용 컨슈머 그룹 정리

---

## 3. 마일스톤 검증 기준 (Acceptance Criteria)

| 검증 영역 | 기준 | 현재 상태 (v0.9.0) |
| :--- | :--- | :---: |
| **코어 파이프라인** | 게이트웨이 3개 노드, Control Plane, Policy DB, Redis 정상 기동 및 통신 | **PASS** |
| **다단계 캐시** | L1 미스 -> L2 미스 -> DB 조회 순 동작, 파티션 해소 시 PEL 전량 드레인 및 `xadd maxlen` 적용 | **PASS** |
| **단일 비행** | 동일 키 50개 동시 요청 시 DB 조회가 1회로 병합 처리 및 태스크 취소·종료 안전성 확보 | **PASS** |
| **보안 및 유량 제어** | 슬라이딩 윈도우(직전 윈도우 보존·LRU 축출), Nonce 소진, 전 컴포넌트 `hmac.compare_digest` 인증 | **PASS** |
| **서킷 브레이커** | DB 다운 시 OPEN 전이, `HALF_OPEN` 실패 즉시 재개방, 2.0s 디바운스, `/pubkey` 서킷 연동 | **PASS** |
| **단위 테스트** | 단위/경계 테스트 스위트 전체 통과 (`pytest -v` 기준 41 passed) | **PASS** |
| **통계 불변성** | 정상 및 Rate Limit/Nonce 503 장애 시 Invariant 방정식 100% 일치 (`redis_outage_blocked` 반영) | **PASS** |
| **E2E 테스트 격리** | 단위 테스트와 E2E 테스트 마커 분리 (`addopts = -m "not e2e"`, 12 deselected) | **PASS** |
| **SSE 아키텍처** | Control Plane 동시 폴링(`asyncio.gather`) 메트릭 수집 및 `asyncio.Queue` 팬아웃 브로드캐스팅 | **PASS** |
| **프로덕션 인프라** | 동적 서비스 디스커버리, 멀티스테이지 Dockerfile 분리 및 분산 잔여 과제 | **TODO (v1.0.0)** |
