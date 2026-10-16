# 아키텍처 설계 및 문제 해결 기록 (Development Log)

분산 환경 부하 및 카오스 테스트 과정에서 식별된 병목, 결함, 리팩토링 내역을 기록한다.

---

## 1. 초기 아키텍처 결함 수정 내역

### 1.1 DB 장애 복구 직후 서킷 브레이커 플래핑 (Flapping)
- **현상**: DB 장애 시 스레드 풀에 진입한 요청들이 DB 타임아웃(1.0s) 동안 누적 대기. DB 복구 후 서킷 브레이커를 `CLOSED`로 전환해도, 스레드 풀에서 지연 처리된 이전 요청들이 뒤늦게 타임아웃 실패를 반환하여 서킷 브레이커가 즉시 다시 `OPEN`되는 플래핑 발생.
- **수정**: 상태 전이 직후 지정 시간 동안 실패 집계를 무시하는 2.0s 디바운스(`CB_DEBOUNCE_SEC`) 적용. `DB_TIMEOUT_SEC`(1.0s) 및 컨텍스트 스위칭 시간을 상회하는 마진 확보.

### 1.2 복합 캐시 키 해시 충돌 위험
- **현상**: SPIFFE ID, 토큰, 리소스, 액션 문자열을 단순 결합할 경우 경계 문자열 구성에 따른 SHA-256 해시 충돌 위험 존재.
- **수정**: `generate_composite_key`에 길이 접두사 포맷(`len:value`) 적용. 해시 충돌 원인 제거.

### 1.3 비원자적 상태 스왑 및 관측 부작용
- **현상**: `is_partitioned` 상태 플래그에 락이 없어 동시 읽기/쓰기 시 레이스 컨디션 발생. Redis 클라이언트 교체 시 관련 참조 객체들이 비원자적으로 갱신됨. `/metrics` 조회 시 `check_and_half_open()`을 호출하여 관측 행위가 서킷 브레이커 상태를 변이시키는 부작용 존재.
- **수정**: `partition_lock` 및 `redis_swap_lock`(`asyncio.Lock`) 적용. `/metrics` 경로에서 상태 전이 호출 제거하고 현재 상태만 읽도록 수정.

### 1.4 Policy DB Deny 응답 인가 우회
- **현상**: Policy DB가 `{"status": "deny"}`를 반환해도 `role` 필드만 참조하여 `allow`로 응답. 해당 결과가 L1/L2 캐시에 적재되어 후속 인가 검증이 무력화됨.
- **수정**: DB 응답의 `status` 필드를 검증하여 `deny`일 경우 캐시에 `"DENY"` 적재 및 200 `status: "deny"` 반환.

### 1.5 파티션 복구 시 DB 헬스 체크 프로브 세마포어 우회
- **현상**: 파티션 해소 시 호출되는 `probe_db_health()`가 `db_semaphore`를 거치지 않아 동시 호출 시 동시성 제한(`MAX_DB_WORKERS`) 초과 가능.
- **수정**: `probe_semaphore = asyncio.Semaphore(1)` 적용. 단일 프로브만 실행되도록 제한.

### 1.6 Redis 핫스왑 시 인플라이트 커넥션 중단
- **현상**: 카오스 테스트로 Redis 클라이언트 교체 시 기존 클라이언트를 즉시 `close()`하여 처리 중인 요청에서 500 에러 발생.
- **수정**: 교체된 이전 클라이언트를 백그라운드 태스크에서 2.0s 대기 후 닫는 지연 종료(Graceful Drain) 적용.

### 1.7 슬라이딩 윈도우 유량 제한 도입
- **현상**: 고정 윈도우(Fixed Window) 방식에서 윈도우 경계 시점에 허용량의 2배 트래픽이 통과하는 경계 버스트 현상 관측.
- **수정**: Lua 스크립트 기반 가중 합산 슬라이딩 카운터 근사 알고리즘 적용. 직전 윈도우 잔여 가중치를 반영하여 경계 스파이크 억제.

### 1.8 PoP 서명 검증과 Nonce 소비 순서 정립
- **현상**: FIDO2 모델 기반 ECDSA P-256 서명 검증 도입 시, 고비용 암호학 연산으로 인한 CPU 고갈 DoS 공격 노출 위험.
- **수정**: Nonce 원자적 소비(Redis Lua)를 서명 검증 선행 단계에 배치. 미등록/재사용 Nonce는 암호 연산 진입 전 차단하고, 공격자 요청 발생 시 해당 Nonce를 즉시 소진(Burn) 처리.

---

## 2. 코드베이스 감사 및 동시성/보안 리팩토링 (2026-10-16)

### 2.1 Single-Flight 태스크 취소 및 Future 라이프사이클 누수 수정
- **대상**: `gateway.py:db_loader_task`, `auth`
- **기존 문제**:
  - `db_loader_task`가 `except Exception:`으로만 예외를 처리하여, 코루틴 취소 시 발생하는 `asyncio.CancelledError`(`BaseException` 상속)를 잡지 못함.
  - Leader 태스크 취소 시 `fut`가 완료되지 않은 채 방치되어 `await asyncio.shield(fut)`로 대기하던 Follower 요청들이 영구 행(Hang) 상태에 빠짐.
  - `finally: in_flight.pop(comp_key, None)`이 인스턴스 검증 없이 동작하여, 후속 요청이 등록한 동일 키의 신규 Future를 오삭제하는 맵 오염 가능.
- **수정 내용 및 결과**:
  - `except BaseException as e:`로 변경하고, `asyncio.CancelledError` 감지 시 `fut.cancel()`을 호출하여 Follower에 취소 상태를 즉시 전파.
  - `finally` 블록에서 `if in_flight.get(comp_key) is fut: in_flight.pop(comp_key, None)`으로 인스턴스 일치 여부 확인 후 삭제하도록 수정. 동시성 데드락 및 맵 누수 제거.

### 2.2 관리자 인증 토큰 타이밍 공격(Timing Attack) 방어
- **대상**: `gateway.py:admin_auth_middleware`, `control_plane.py:admin_auth_middleware`
- **기존 문제**: `X-Admin-Token` 헤더 검증 시 `admin_token != ADMIN_SECRET_TOKEN` 일반 문자열 비교 연산자 사용. 문자열 일치 바이트 수에 따라 비교 소요 시간이 달라져 사이드 채널 공격(Timing Attack)에 노출됨.
- **수정 내용 및 결과**: `hmac.compare_digest(admin_token, ADMIN_SECRET_TOKEN)` 상수 시간 비교 함수 적용. 문자열 비교 타이밍 누출 차단.

### 2.3 L1Cache 및 CircuitBreaker 캡슐화 개선
- **대상**: `gateway.py:L1Cache`, `CircuitBreaker`
- **기존 문제**:
  - `L1Cache` 생성자에 매직 넘버(`1000`, `60`)가 하드코딩되었고 `clear()` 메서드가 없어 테스트 코드에서 내부 딕셔너리(`l1_cache.cache.clear()`)를 직접 조작함.
  - `stream_listener`에서 `if key in l1_cache: l1_cache.delete(key)` 호출로 불필요한 2회 연속 락 획득 발생.
  - `CircuitBreaker` 상태 관측 시 `with cb._lock: state = cb.state` 형태로 내부 락 객체에 직접 접근해야 했음.
- **수정 내용 및 결과**:
  - `L1Cache` 기본값을 모듈 전역 설정(`L1_MAX_SIZE`, `L1_TTL_SEC`)과 연동하고 스레드 세이프한 `clear()` 메서드 추가. `delete()`가 락 내부에서 단일 원자 연산으로 동작하도록 수정.
  - `CircuitBreaker`에 `@property def current_state(self) -> str:` 프로퍼티를 추가하여 내부 락으로 보호된 읽기 경로 캡슐화.

### 2.4 HTTP 및 Redis 클라이언트 풀링 구조화 및 테스트 호환성 확보
- **대상**: `gateway.py`, `control_plane.py`
- **기존 문제**:
  - 전역 클라이언트가 FastAPI `lifespan`에서만 초기화되어 `TestClient`나 단위 테스트 실행 시 `None` 참조 에러 발생.
  - `control_plane.py`의 `httpx.AsyncClient` 커넥션 풀이 기본값(100)으로 제한되어 다중 노드 카오스 요청 발생 시 타임아웃 병목 유발.
- **수정 내용 및 결과**:
  - `get_async_db_client()`, `get_sync_http_client()`, `get_redis_client()`, `get_async_http_client()` 지연 초기화(Lazy Initialization) 팩토리 구현. `lifespan` 미실행 환경에서도 커넥션 풀을 반환하여 테스트 호환성 확보.
  - `control_plane.py`의 `httpx.AsyncClient`에 `limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)` 설정 적용. 소켓 고갈 방지.

### 2.5 예외 은폐(Silent Swallow) 제거 및 코드베이스 정돈
- **대상**: `gateway.py:auth`, `policy_db.py`, `.env.example`
- **기존 문제**:
  - L2 캐시 조회 및 적재 시 `except Exception: pass` 구문으로 예외를 삼켜 Redis 이상 발생 시 로그 확인 불가.
  - 라우트 핸들러 내부 인라인 `import binascii` 호출.
  - 튜토리얼 주석(`# Step 1: ...`, `# NORMAL LOOP`), ASCII 배너 구분선(`# ===...===`) 산재.
- **수정 내용 및 결과**:
  - `except Exception: pass` 구문을 제거하고 `logging.warning`, `logging.debug`를 적용하여 장애 원인 추적 가능하도록 변경.
  - 인라인 임포트 최상단 이동, ASCII 배너 제거, 아키텍처 의도(Nonce 선행 검증 사유, PEL 처리 목적 등) 중심 주석으로 교체.

---

## 3. 2차 리팩토링 및 테스트 격리 (v0.8.0, 2026-10-16)

### 3.1 Redis 503 장애 시 Invariant 카운터 정합성 정렬
- **대상 파일 및 함수**: `gateway.py:auth`, `stats`, `static/index.html`
- **기존 문제**: Redis 다운 시 Nonce 검증(`consume_nonce`)에서 503(Fail-Closed)으로 조기 반환된 요청이 `stats["requests"]`에는 반영되었으나 세부 분류 카운터에 집계되지 않아, 대시보드 Invariant 방정식(`requests == sum(classified)`)에 불일치 및 경고 발생.
- **수정 내용 및 결과**: `stats["redis_outage_blocked"]` 카운터를 신설하고 Nonce 검증 실패 예외 블록에서 이를 증가시킴. `static/index.html`의 Invariant RHS 계산식에 `stats.redis_outage_blocked`를 합산하여 Redis 장애 상황에서도 Invariant 방정식이 일치(`PASS`)하도록 수정. 단위 테스트(`test_invariant_equality_under_redis_503`)로 검증함.

### 3.2 Pytest Runner와 E2E 테스트 스위트 격리
- **대상 파일 및 함수**: `pytest.ini`, `tests/test_scenarios.py`, `tests/test_flapping.py`, `tests/test_verify_half_open_and_auth.py`, `tests/stress_test.py`, `tests/test_rl_fallback.py`, `tests/test_rl_node_fallback.py`
- **기존 문제**: 외부 Docker 클러스터 구동을 전제하는 E2E 및 스트레스 테스트가 기본 테스트 스위트에 포함되어, 로컬 환경에서 `pytest` 실행 시 다수의 연결 실패(`ConnectError`) 및 타임아웃 발생.
- **수정 내용 및 결과**: `pytest.ini`에 `e2e` 마커를 등록하고 기본 실행 옵션으로 `addopts = -m "not e2e"` 설정. 라이브 HTTP/클러스터 의존 테스트 파일 및 함수에 `@pytest.mark.e2e` 데코레이터를 적용. `pytest -v` 실행 시 E2E 10건이 자동 제외되고 순수 단위 테스트 29건만 2.1초 내에 통과하며, `pytest -m e2e`로 E2E 스위트만 별도 타겟팅 가능하도록 분리함.

### 3.3 Control Plane SSE 단일 폴러 팬아웃(Fan-Out) 전환
- **대상 파일 및 함수**: `control_plane.py:metrics_stream`, `broadcast_metrics_loop`, `lifespan`, `tests/test_control_plane.py`
- **기존 문제**: SSE 클라이언트(`/metrics/stream`) 연결마다 별도 폴링 루프가 실행되어, 대시보드 접속자 수가 $N$명일 때 게이트웨이 3개 노드를 향한 내부 HTTP 폴링 트래픽이 $3 \times N$건으로 중복 가중됨.
- **수정 내용 및 결과**: `broadcast_metrics_loop` 백그라운드 단일 폴러 태스크를 도입하여 1초 간격으로 게이트웨이 메트릭을 1회만 수집하고 최신 스냅샷을 갱신하도록 변경. 활성 구독자 큐(`subscribers: set[asyncio.Queue]`)로 논블로킹 팬아웃 브로드캐스팅을 수행함. 클라이언트 연결 종료 시 `finally` 블록에서 큐를 제거(`discard`)하여 메모리 누수를 차단함. `test_control_plane.py` 단위 테스트 추가.

### 3.4 단위 테스트 전역 상태 격리 및 경계값 테스트 보강
- **대상 파일 및 함수**: `tests/conftest.py`, `tests/test_pop.py`, `tests/test_singleflight.py`
- **기존 문제**: `test_pop.py` 실행 시 설정된 `POP_VERIFY_ENABLED = True` 플래그가 테스트 종료 후 원복되지 않아 후속 `test_singleflight.py` 실행 시 서명 누락(400 Bad Request)으로 연쇄 실패함. 서킷 브레이커 디바운스 및 Single-Flight 예외 시 맵 정리 경계 테스트 부재.
- **수정 내용 및 결과**: `conftest.py`에 `clean_gateway_state` autouse 픽스처 및 `mock_common_dependencies`를 구축하여 전역 설정/캐시/카운터를 매 테스트 전후로 초기화함. 서킷 브레이커 2.0초 디바운스 전이 경계(`CLOSED` -> `OPEN` -> `HALF_OPEN`), Single-Flight 예외 전파 및 `in_flight` 맵 정리, Redis 장애 503 Invariant 일치성 단위 테스트를 추가함.
