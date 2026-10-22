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
  - `L1Cache` 기본값을 모듈 전역 설정(`L1_MAX_SIZE`, `L1_TTL_SEC`)과 연동하고 스레드 세이프한 `clear()` 메서드 추가. `delete()`가 락 내부에서 단일 원자 연산으로 동작하도록 수정. (단, 테스트 코드의 `l1_cache.cache.clear()` 직접 호출은 일부 남아 있었으며 4.9에서 정정)
  - `CircuitBreaker`에 `@property def current_state(self) -> str:` 프로퍼티를 추가하여 내부 락으로 보호된 읽기 경로 캡슐화.

### 2.4 HTTP 및 Redis 클라이언트 풀링 구조화 및 테스트 호환성 확보
- **대상**: `gateway.py`, `control_plane.py`
- **기존 문제**:
  - 전역 클라이언트가 FastAPI `lifespan`에서만 초기화되어 `TestClient`나 단위 테스트 실행 시 `None` 참조 에러 발생.
  - `control_plane.py`의 `httpx.AsyncClient` 커넥션 풀이 기본값(100)으로 제한되어 다중 노드 카오스 요청 발생 시 타임아웃 병목 유발.
- **수정 내용 및 결과**:
  - `get_async_db_client()`, `get_sync_http_client()`, `get_redis_client()`, `get_async_http_client()` 지연 초기화(Lazy Initialization) 팩토리 구현. `lifespan` 미실행 환경에서도 커넥션 풀을 반환하여 테스트 호환성 확보. (단, `nonce_script`/`rl_script` 지연 등록과 `check_rate_limit`의 `"fixed"` 분기 팩토리 적용은 누락되어 있었으며 4.5에서 정정)
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
- **수정 내용 및 결과**: `pytest.ini`에 `e2e` 마커를 등록하고 기본 실행 옵션으로 `addopts = -m "not e2e"` 설정. 라이브 HTTP/클러스터 의존 테스트 파일 및 함수에 `@pytest.mark.e2e` 데코레이터를 적용. `pytest -v` 실행 시 E2E 10건이 자동 제외되고 순수 단위 테스트 29건만 2.1초 내에 통과하며, `pytest -m e2e`로 E2E 스위트만 별도 타겟팅 가능하도록 분리함. (단, `test_flapping.py`와 `test_rl_node_fallback.py`에는 `test_*` 함수가 없어 pytest 수집 대상에서 빠져 있었으며 4.9에서 정정)

### 3.3 Control Plane SSE 단일 폴러 팬아웃(Fan-Out) 전환
- **대상 파일 및 함수**: `control_plane.py:metrics_stream`, `broadcast_metrics_loop`, `lifespan`, `tests/test_control_plane.py`
- **기존 문제**: SSE 클라이언트(`/metrics/stream`) 연결마다 별도 폴링 루프가 실행되어, 대시보드 접속자 수가 $N$명일 때 게이트웨이 3개 노드를 향한 내부 HTTP 폴링 트래픽이 $3 \times N$건으로 중복 가중됨.
- **수정 내용 및 결과**: `broadcast_metrics_loop` 백그라운드 단일 폴러 태스크를 도입하여 1초 간격으로 게이트웨이 메트릭을 1회만 수집하고 최신 스냅샷을 갱신하도록 변경. 활성 구독자 큐(`subscribers: set[asyncio.Queue]`)로 논블로킹 팬아웃 브로드캐스팅을 수행함. 클라이언트 연결 종료 시 `finally` 블록에서 큐를 제거(`discard`)하여 메모리 누수를 차단함. `test_control_plane.py` 단위 테스트 추가.

### 3.4 단위 테스트 전역 상태 격리 및 경계값 테스트 보강
- **대상 파일 및 함수**: `tests/conftest.py`, `tests/test_pop.py`, `tests/test_singleflight.py`
- **기존 문제**: `test_pop.py` 실행 시 설정된 `POP_VERIFY_ENABLED = True` 플래그가 테스트 종료 후 원복되지 않아 후속 `test_singleflight.py` 실행 시 서명 누락(400 Bad Request)으로 연쇄 실패함. 서킷 브레이커 디바운스 및 Single-Flight 예외 시 맵 정리 경계 테스트 부재.
- **수정 내용 및 결과**: `conftest.py`에 `clean_gateway_state` autouse 픽스처 및 `mock_common_dependencies`를 구축하여 전역 설정/캐시/카운터를 매 테스트 전후로 초기화함. 서킷 브레이커 2.0초 디바운스 전이 경계(`CLOSED` -> `OPEN` -> `HALF_OPEN`), Single-Flight 예외 전파 및 `in_flight` 맵 정리, Redis 장애 503 Invariant 일치성 단위 테스트를 추가함. (단, 당시 `mock_common_dependencies`는 실제 테스트 함수에서 참조되지 않았고 파일별 중복 픽스처가 남아 있었으며 4.9에서 정정)

---

## 4. 3차 리팩토링: 감사 백로그 반영 (v0.9.0, 2026-10-19)

### 4.1 B-01 서킷 브레이커 HALF_OPEN 실패 즉시 재개방
- **대상 파일 및 함수**: `gateway.py:CircuitBreaker.record_failure`, `tests/test_singleflight.py:test_circuit_breaker_debounce_and_half_open_boundary`
- **기존 문제**: `can_execute()` 또는 `check_and_half_open()`를 통해 `OPEN`에서 `HALF_OPEN`으로 전환될 때 `last_transition_time`이 현재 시각으로 갱신되면서, 전환 후 2.0초(`CB_DEBOUNCE_SEC`) 이내에 발생한 `HALF_OPEN` 시험 요청의 실패가 디바운스 조건에 걸려 무시되고 서킷이 `HALF_OPEN` 상태에 머무르는 문제가 있었다.
- **수정 내용 및 결과**: `record_failure()` 진입 시 디바운스 검사 이전에 `self.state == "HALF_OPEN"` 분기를 우선 처리하여, `HALF_OPEN` 상태에서는 디바운스를 우회하고 즉시 `failures`를 증가시킨 뒤 `OPEN`으로 재개방(`last_transition_time`, `last_failure_time` 갱신)하도록 수정했다(`CLOSED` 상태의 2.0초 디바운스는 유지). `test_circuit_breaker_debounce_and_half_open_boundary` 단위 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.2 B-03 로컬 Rate Limit 정리 윈도우/키 축출
- **대상 파일 및 함수**: `gateway.py:cleanup_local_rl`, `gateway.py:_local_rate_limit`, `gateway.py:local_rate_limits`, `tests/test_rate_limit.py`
- **기존 문제**: `cleanup_local_rl`이 `w < now_window` 조건으로 직전 윈도우(`now_window - 1`) 버킷까지 삭제하여 슬라이딩 윈도우 로컬 폴백(`_local_rate_limit`)의 이전 윈도우 가중치 계산이 소실되었고, `_local_rate_limit`에서 `RL_LOCAL_MAX_KEYS` 도달 시 `local_rate_limits.clear()`로 전체 맵을 비워 기존에 제한 중이던 클라이언트의 카운트까지 초기화되는 문제가 있었다.
- **수정 내용 및 결과**: `cleanup_local_rl`의 삭제 조건을 `w < now_window - 1`로 변경하여 현재 윈도우(`now_window`)와 직전 윈도우(`now_window - 1`)를 유지하고 `now_window - 2` 이하만 삭제하도록 수정했다. 또한 `local_rate_limits`를 `OrderedDict`로 변경하고 조회·갱신 시 `move_to_end`를 호출하며, `RL_LOCAL_MAX_KEYS` 도달 시 `popitem(last=False)`로 가장 오래된 키만 축출하도록 수정했다. `test_cleanup_local_rl_compatible_with_new_key_format` 및 `test_local_rl_max_keys_does_not_reset_existing_client` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.3 B-02 Stream PEL 전량 드레인 및 xadd maxlen
- **대상 파일 및 함수**: `gateway.py:stream_listener`, `gateway.py:_process_stream_messages`, `gateway.py:invalidate_cache`, `tests/test_stream_listener.py`
- **기존 문제**: 파티션 복구 시 `stream_listener`가 `XREADGROUP` ID `"0"`(PEL)을 `count=STREAM_CATCHUP_COUNT`(100)로 1회만 호출하여 미확인 메시지가 100건을 초과할 경우 잔여 PEL이 남아 있는 상태에서 `">"` 읽기로 전환되는 문제가 있었다. 또한 `stream_listener` 내 3개 구간에 메시지 처리 루프가 중복되어 있었고, `invalidate_cache`의 `xadd` 호출에 스트림 길이 상한이 없었다.
- **수정 내용 및 결과**: 파티션 복구 시 `XREADGROUP ... {STREAM_NAME: "0"}` 호출을 `while True:` 루프로 감싸 빈 배치가 반환될 때까지 PEL을 전량 드레인한 뒤 `">"` 읽기로 전환하도록 수정했다. 3곳의 중복 메시지 처리 루프는 `_process_stream_messages` 헬퍼 함수로 통합했으며, `invalidate_cache`의 `xadd`에 `maxlen=10000, approximate=True`를 적용했다(단, 컨슈머의 L2 캐시 삭제 `active_redis_client.delete(f"policy:{key}")` 제거 항목은 기존 동작 유지를 위해 미적용하고 `_process_stream_messages` 내 기존 삭제 호출을 유지함). `test_pel_recovery_drains_all_batches_and_switches_to_live`, `test_stream_listener_deduplicated_message_helper`, `test_invalidate_cache_xadd_maxlen_approximate` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.4 B-04 백그라운드 태스크 참조 보관 및 종료 순서
- **대상 파일 및 함수**: `gateway.py:background_tasks`, `gateway.py:lifespan`, `gateway.py:auth`, `gateway.py:set_partition`, `gateway.py:set_redis`, `tests/test_stream_listener.py`
- **기존 문제**: `asyncio.create_task`로 생성된 백그라운드 태스크(`db_loader_task`, `probe_db_health`, `close_old_clients` 등)의 강한 참조(strong reference)를 보관하지 않아 가비지 컬렉션에 의한 조기 소멸 위험이 있었고, `lifespan` 종료 시 백그라운드 태스크 완료를 대기하기 전에 HTTP/Redis 클라이언트를 닫거나 스레드 풀 `shutdown(wait=True)`로 지연될 수 있었다.
- **수정 내용 및 결과**: 모듈 전역 `background_tasks: set[asyncio.Task] = set()`를 도입하고 모든 `asyncio.create_task` 호출부에 `background_tasks.add(task)` 및 `task.add_done_callback(background_tasks.discard)`를 적용했다. `lifespan` 종료 시 `stream_listener`와 `cleanup_local_rl`을 취소한 후 `await asyncio.gather(*tasks, *background_tasks, return_exceptions=True)`로 모든 백그라운드 태스크가 종료될 때까지 대기한 뒤 HTTP/Redis 클라이언트를 닫고, `db_executor`와 `pop_executor`는 `shutdown(wait=False, cancel_futures=True)`로 종료하도록 수정했다. `test_lifespan_awaits_background_tasks_before_closing_clients` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.5 B-08 Redis 스크립트 지연 등록 및 ADMIN_SECRET_TOKEN 공백 검증
- **대상 파일 및 함수**: `gateway.py:get_redis_client`, `gateway.py:check_rate_limit`, `gateway.py:consume_nonce`, `tests/test_rate_limit.py`
- **기존 문제**: `lifespan`을 거치지 않는 테스트 환경에서 `get_redis_client()` 호출 시 `nonce_script`와 `rl_script`가 등록되지 않아 `NoneType` 호출 에러가 발생했고, `check_rate_limit`의 `"fixed"` 분기에서 `get_redis_client()` 대신 전역 `redis_client.pipeline(...)`을 직접 참조했다. 또한 `ADMIN_SECRET_TOKEN`이 공백 문자로만 이루어진 경우 시작 시점 검증을 통과하는 문제가 있었다.
- **수정 내용 및 결과**: `get_redis_client()`에서 Redis 클라이언트 생성 시 `nonce_script`와 `rl_script`를 함께 등록하도록 수정하고, `check_rate_limit`(`"sliding"` 분기 `if rl_script is None: get_redis_client()`, `"fixed"` 분기 `get_redis_client().pipeline(transaction=True)`)와 `consume_nonce`(`if nonce_script is None: get_redis_client()`)가 지연 초기화된 리소스를 사용하도록 보완했다. 또한 모듈 로드 시 `if not ADMIN_SECRET_TOKEN or not ADMIN_SECRET_TOKEN.strip():` 조건으로 공백 전용 토큰을 거부하도록 수정했다. `test_lazy_redis_init_without_lifespan` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.6 B-05 /pubkey 서킷 브레이커 연동
- **대상 파일 및 함수**: `gateway.py:auth`, `tests/test_pop.py`
- **기존 문제**: `POP_VERIFY_ENABLED=True`일 때 L1/L2 캐시 미스 후 Policy DB의 `/pubkey` 엔드포인트를 조회하는 경로가 서킷 브레이커(`cb`)와 연동되지 않아, 서킷이 `OPEN` 상태여도 DB 요청이 전송되고 `/pubkey` 타임아웃이나 5xx 에러가 서킷 실패 카운트에 기록되지 않았다. 또한 `sync` 모드의 `/pubkey` 조회에서 세마포어 획득 타임아웃(`DB_SEMAPHORE_ACQUIRE_TIMEOUT`)이 적용되지 않았다.
- **수정 내용 및 결과**: `/pubkey` L1/L2 미스 시 `cb.can_execute()`를 먼저 확인하여 차단 상태이면 DB 호출 없이 `stats["circuit_state"]`, `stats["circuit_blocked"]`, `stats["pop_rejected"]`를 갱신하고 503(Fail-Closed, 선행 단계에서 Nonce는 이미 소진됨)을 반환하도록 수정했다. DB 조회 성공 시 `cb.record_success()`를 호출하고, 404(401 반환)나 `DBOverloadError`가 아닌 실패 발생 시 `cb.record_failure()`를 호출한 뒤 503을 반환하도록 연동했으며, `sync` 모드에는 `asyncio.wait_for(sem.acquire(), timeout=DB_SEMAPHORE_ACQUIRE_TIMEOUT)`을 적용했다. `test_pop_pubkey_circuit_breaker_and_timeout` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.7 B-07 Control Plane 동시 폴링 및 auth_check 검증
- **대상 파일 및 함수**: `control_plane.py:fetch_metrics`, `control_plane.py:broadcast_metrics_loop`, `control_plane.py:auth_check`, `control_plane.py:chaos_db`, `policy_db.py:set_db_state`, `docker-compose.yml`, `tests/test_control_plane.py`
- **기존 문제**: `fetch_metrics()`가 게이트웨이 노드를 순차 폴링하여 응답 지연이 누적되었고 `resp.raise_for_status()` 호출이 누락되어 있었다. `broadcast_metrics_loop`에서 큐 적재 실패 시 경고 없이 예외를 무시했으며, `/chaos/auth_check`와 `policy_db.py`의 `/chaos/db/{state}` 엔드포인트에 `X-Admin-Token` 검증이 없었다.
- **수정 내용 및 결과**: `fetch_metrics()`가 `asyncio.gather(*reqs, return_exceptions=True)`로 게이트웨이를 동시 폴링하고 `resp.raise_for_status()`를 호출하여 비정상 노드를 `offline` 상태로 격리하도록 수정했다. `broadcast_metrics_loop`에서 `q.put_nowait` 실패 시 `logging.warning` 출력 후 해당 큐를 `subscribers.discard(q)`로 제거하도록 개선했다. 또한 `/chaos/auth_check`와 `policy_db.py:set_db_state`에 `hmac.compare_digest` 기반 `X-Admin-Token` 검증(실패 시 401)을 추가하고, `control_plane.py:chaos_db` 요청 헤더와 `docker-compose.yml`의 `policy_db` 환경변수에 `ADMIN_SECRET_TOKEN`을 연동했다. `test_fetch_metrics_concurrent_and_error_handling`, `test_chaos_auth_check`, `test_policy_db_chaos_auth`, `test_broadcast_metrics_loop_drops_failed_subscriber` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.8 B-10 Invariant 카운터 보완 및 중복 DOM ID 제거
- **대상 파일 및 함수**: `gateway.py:auth`, `static/index.html`, `tests/test_singleflight.py`
- **기존 문제**: `/auth`에서 `check_rate_limit` 호출 중 예외가 발생해 503을 반환할 때 `stats["redis_outage_blocked"]`가 증가하지 않아 `requests`와 분류 카운터 합계 간의 Invariant가 어긋날 수 있었고, `consume_nonce` 블록에서 401 리플레이 예외가 Redis 장애 `try/except` 블록 내부에 섞여 있었다. 또한 `static/index.html`의 노드 카드 템플릿에 중복된 메트릭 DOM ID(`lat-${n}`, `l1-${n}`, `l2-${n}`, `cb-${n}`)와 닫는 `</div>` 태그가 남아 있었다.
- **수정 내용 및 결과**: `check_rate_limit` 예외 블록(503 반환)에서 `stats["redis_outage_blocked"] += 1`을 증가시키고, `consume_nonce`의 Redis 예외 처리(503)와 리플레이 판정(`if not nonce_ok:` -> 401 `nonce_replays += 1`)을 분리했다. `static/index.html`에서는 중복된 메트릭 DOM 요소 및 불필요한 `</div>` 6줄을 제거했다. `test_invariant_equality_under_rate_limit_503` 테스트로 검증했으며, 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.9 B-11 테스트 픽스처 통합
- **대상 파일 및 함수**: `tests/conftest.py:clean_gateway_state`, `tests/conftest.py:mock_common_dependencies`, `tests/test_pop.py`, `tests/test_singleflight.py`, `tests/test_flapping.py:test_flapping`, `tests/test_rl_node_fallback.py:test_rl_node_fallback`
- **기존 문제**: `tests/conftest.py`의 `mock_common_dependencies`가 사용되지 않고 `test_pop.py`와 `test_singleflight.py`에 파일별 중복 픽스처와 `l1_cache.cache.clear()` 직접 호출이 남아 있었다. 또한 `test_flapping.py`와 `test_rl_node_fallback.py`에는 `test_*` 함수가 정의되지 않아 `pytest -m e2e` 실행 시 수집되지 않았다.
- **수정 내용 및 결과**: `conftest.py`의 `clean_gateway_state`와 `mock_common_dependencies`로 상태 초기화(`l1_cache.clear()` 등) 및 공통 Mock 구성을 통합하고, `test_pop.py`(`test_pop_valid_signature` 등)와 `test_singleflight.py`(`test_singleflight_100_requests` 등)가 이를 직접 사용하도록 정리했다. 또한 `test_flapping.py`에 `test_flapping`, `test_rl_node_fallback.py`에 `test_rl_node_fallback` 함수를 추가하여 E2E 수집 대상에 포함시켰다. 최종 `pytest -v` 실행 결과 41 passed(12 deselected)를 확인했다.

### 4.10 B-12 문서 오염 복구
- **대상 파일 및 함수**: `README.md`, `docs/benchmarks/singleflight-effect.md`, `DEVELOPMENT_LOG.md`
- **기존 문제**: `README.md`와 `docs/benchmarks/singleflight-effect.md`에 제어 문자(`0x07`, `0x08`, `0x09`)가 혼입되어 코드 펜스 및 식별자(`test_scenarios.py`, `test_rl_fallback.py`, `asyncio.shield`)가 손상되어 있었고, `singleflight-effect.md`의 `MAX_DB_WORKERS` 표기(`10`)가 `gateway.py`의 실제 기본값(`5`)과 달랐다. 또한 `DEVELOPMENT_LOG.md`의 기존 섹션 일부에 실제 코드와 다른 서술이 있었다.
- **수정 내용 및 결과**: 마크다운 파일의 제어 문자 오염을 전수 제거하고 코드 펜스(` ```bash `)와 식별자를 복구했으며, `README.md`의 단위 테스트 명령을 `pytest -v`, E2E 명령을 `pytest -m e2e`로 갱신했다. `singleflight-effect.md`의 `MAX_DB_WORKERS`를 `5`로 수정하고, `DEVELOPMENT_LOG.md`에 3차 리팩토링 내역과 정정 주석을 반영했다. 기존 테스트 스위트(`test_circuit_breaker_debounce_and_half_open_boundary` 포함 전체)를 실행하여 최종 `pytest -v` 결과 41 passed(12 deselected)를 확인했다.

### 4.11 의도적 미적용 항목
- **B-06 (`DB_MODE` sync 기본값)**: 동작 영향이 없고 변경 범위가 커서 이번 범위에서 제외한다.
- **B-09 (이중 락 구조 및 미사용 반환값)**: 동작 영향이 없고 변경 범위가 커서 이번 범위에서 제외한다.

### 4.12 알려진 잔여 과제
- **서킷 브레이커 HALF_OPEN 동시 시험 요청 1건 제한 미구현**: `HALF_OPEN` 상태에서 별도의 단일 프로브 세마포어를 두지 않아 동시 캐시 미스 요청이 함께 DB를 조회하는 문제가 있었다. (→ 5.1(v1.0.0)에서 원자적 단일 프로브 게이트 `_half_open_probe_in_flight`로 해결 완료)
- **XAUTOCLAIM/미사용 컨슈머 그룹 정리 미구현**: `stream_listener()`는 노드 고유의 컨슈머 그룹(`gateway_group_{node_name}`)에 대해 자신의 PEL(`"0"`)만 재처리하며, 영구 이탈한 다른 컨슈머의 미확인 메시지를 인계받는 `XAUTOCLAIM`이나 미사용 컨슈머 그룹 삭제 로직은 구현되어 있지 않았다. (→ `XAUTOCLAIM`은 5.3(v1.0.0)에서 구현 완료됨; 미사용 컨슈머 그룹 정리는 고정 노드 클러스터 특성상 현행 유지)
- **클러스터 전역 Single-Flight 미지원**: `in_flight` 맵은 각 게이트웨이 프로세스 내부 메모리에만 존재하므로, 동일 키 요청이 서로 다른 게이트웨이 노드로 분산 유입될 경우 노드별로 각각 1회씩 DB 호출이 발생한다. (→ 6.1(v1.1.0)에서 Redis 분산 락 및 L2 폴링 기반 클러스터 전역 Single-Flight로 해결 완료)

---

## 5. 4차 리팩토링: 시나리오 기반 동시성 제어 및 복원력 고도화 (v1.0.0, 2026-10-19)

### 5.1 서킷 브레이커 HALF_OPEN 단일 프로브(Single-Probe) 원자적 게이팅 및 취소 데드락 방어
- **대상 파일 및 함수**: `gateway.py:CircuitBreaker`, `gateway.py:auth`, `gateway.py:db_loader_task`, `gateway.py:get_metrics`, `tests/test_singleflight.py`
- **기존 문제**:
  - `CircuitBreaker.can_execute()`가 `OPEN`에서 `HALF_OPEN`으로 전이된 후, 첫 번째 시험 요청이 DB 결과를 기록하기 전에 유입된 다수의 캐시 미스 요청들이 모두 `can_execute()`를 통과하여 복구 중인 Policy DB에 트래픽이 쇄도하는 문제가 있었다.
- **수정 내용 및 결과**:
  - `CircuitBreaker`에 스레드 세이프 단일 프로브 상태 추적 플래그(`_half_open_probe_in_flight: bool = False`, `is_probe_active` 프로퍼티)를 도입했다.
  - `can_execute()` 내부에서 `OPEN` -> `HALF_OPEN` 전이 시 또는 기존 `HALF_OPEN` 상태에서 `_half_open_probe_in_flight == False`일 때, `self._lock` 보호 아래 원자적으로 `_half_open_probe_in_flight = True`를 마킹하고 단 1건의 시험 요청만 `True`를 반환하도록 게이팅했다.
  - 프로브 실행 중 유입된 후속 동시 요청에는 `(False, "HALF_OPEN")`을 반환하여 Policy DB 호출을 차단하고 `stats["circuit_blocked"]` 및 `stats["db_fallbacks"]`를 증가시켜 기존 강한 DB Fallback Invariant 및 대시보드 Invariant 방정식을 100% 보존했다.
  - `record_success()`, `record_failure()`, `force_state()`, `force_open()`, `force_close()`, `reset()` 시 `_half_open_probe_in_flight`를 `False`로 초기화하도록 보장했다.
  - 시험 프로브가 `DBOverloadError`(세마포어 타임아웃) 또는 코루틴 취소(`asyncio.CancelledError`)를 겪어 성공/실패가 기록되지 않은 채 중단되더라도 데드락이 발생하지 않도록 `finally` 블록에서 `release_half_open_probe()`를 호출하여 슬롯을 즉시 반환하도록 구현했다.
  - `test_half_open_allows_strictly_single_probe_under_concurrency`, `test_half_open_probe_slot_released_on_cancellation_or_overload`, `test_circuit_breaker_single_probe_direct_unit` 단위 테스트로 검증했다.

### 5.2 슬라이딩 윈도우 Rate Limiter 429 카운터 오염 방지 (Check-Before-Increment)
- **대상 파일 및 함수**: `gateway.py:SLIDING_RL_LUA`, `gateway.py:check_rate_limit`, `gateway.py:_local_rate_limit`, `gateway.py:stats`, `gateway.py:auth`, `tests/conftest.py:clean_gateway_state`, `tests/test_rate_limit.py`, `tests/test_singleflight.py`
- **기존 문제**:
  - 기존 Sliding Window Rate Limiter는 429 Too Many Requests로 차단되는 요청에 대해서도 `INCR`를 실행하거나 사후 검사 방식으로 동작하여, DDoS 공격이나 대량 버스트 유입 시 현재 윈도우 카운터가 비정상적으로 비대해졌다. 이로 인해 다음 윈도우($W_1$)의 가중 합산 추정치 $\lfloor \text{prev} \times \frac{10 - \text{elapsed}}{10} \rfloor$가 장시간 제한치를 초과 상태로 유지되어 공격이 중단된 후에도 정상 트래픽이 기아 상태(Starvation)에 빠지는 카운터 오염 문제가 존재했다.
  - Single-Flight 메커니즘에서 백엔드 리더(Leader) 쿼리 발생 빈도와 팔로워(Follower) 병합 효율을 직관적으로 검증할 수 있는 세부 관측성 메트릭이 부재했다.
- **수정 내용 및 결과**:
  - `SLIDING_RL_LUA` 스크립트를 **Check-Before-Increment** 방식으로 전면 개편했다. `prev`와 `curr`를 먼저 읽고 예상 트래픽 `math.floor(prev * ((window - elapsed) / window)) + curr`를 산출하여, `estimated >= limit`인 경우 `INCR` 없이 즉시 `0`(거부)을 반환하고 `estimated < limit`일 때만 `INCR` 및 `EXPIRE`를 수행하고 `1`(허용)을 반환하도록 개선했다. 인자 키 순서 호환을 위해 윈도우 타임스탬프 접미사 자동 판별 로직을 내장했다.
  - 인메모리 로컬 폴백(`_local_rate_limit`) 및 Fixed 윈도우 분기에도 동일한 Check-Before-Increment 방식을 적용하여 Redis 장애/폴백 상황에서도 거부된 요청이 로컬 윈도우 카운터를 오염시키지 않도록 보장했다.
  - Single-Flight 관측성 강화를 위해 `stats`에 `singleflight_leader_queries` 메트릭을 추가하고, `/auth`에서 `db_loader_task`를 기동하는 Leader 요청 시점에 원자적으로 카운트를 증가시켰다 (`conftest.py:clean_gateway_state` 리셋 연동).
  - 2대 수학적 불변식 (`requests == rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks + redis_outage_blocked`, `db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + circuit_blocked`)을 100% 온전히 유지했다.
  - `test_sliding_window_rejected_requests_do_not_pollute_next_window` 및 `test_singleflight_leader_follower_efficiency_observability` 단위 테스트로 검증했다.

### 5.3 Redis Streams XAUTOCLAIM 통합 및 Catch-up 세부 관측 지표 확장
- **대상 파일 및 함수**: `gateway.py:stream_listener`, `gateway.py:STREAM_CLAIM_MIN_IDLE_MS`, `gateway.py:stats`, `tests/test_stream_listener.py`
- **기존 문제**:
  - `stream_listener`가 파티션 복구 시 자신의 노드 컨슈머 PEL(`"0"`)만 드레인하고, 컨슈머 재시작이나 장시간 유휴(Idle) 상태로 방치된 미확인 메시지를 회수하는 `XAUTOCLAIM` 로직이 부재했다.
  - 파티션 복구 완료 후 `stats["last_catchup"]` 메트릭이 총 처리 건수(`pending_count`)만 제공하여 자체 PEL 드레인, 회수 메시지, 신규 메시지 간 세부 구분을 알 수 없었다.
- **수정 내용 및 결과**:
  - `gateway.py`의 `stream_listener` 파티션 복구 시퀀스에 Step B `XAUTOCLAIM`을 신규 통합하여 **3단계 Catch-up 파이프라인**을 구축했다:
    - **Step A**: 자체 PEL 전량 드레인 (`ID = "0"`, `pel_drained`)
    - **Step B**: `STREAM_CLAIM_MIN_IDLE_MS`(기본 60,000ms) 이상 방치된 미확인 메시지 유계 자동 회수 (`XAUTOCLAIM`, `autoclaimed`)
    - **Step C**: 단절 기간 동안 누적된 미전달 신규 메시지 전량 드레인 (`ID = ">"`, `new_drained`)
  - 레거시 Redis 클라이언트 및 Mock 환경과의 호환성을 위해 `AttributeError`, `ResponseError`에 대한 안전 폴백 처리를 적용했다.
  - `stats["last_catchup"]` 텔레메트리를 고도화하여 기존 키(`pending_count`, `duration_ms`, `timestamp`)를 유지하면서 `pel_drained`, `autoclaimed`, `new_drained` 세부 카운트를 추가 제공하도록 개선했다.
  - `test_stream_listener_xautoclaim_and_enriched_catchup_telemetry` 및 `test_stream_listener_xautoclaim_graceful_fallback_without_xautoclaim` 단위 테스트로 검증했다.

### 5.4 클러스터 불변식(Invariant) 자동 감사 엔드포인트 (GET /chaos/audit) 추가
- **대상 파일 및 함수**: `control_plane.py:chaos_audit`, `tests/test_control_plane.py`
- **기존 문제**:
  - 기술 데모 시연 및 자동 검증 시 클러스터 내 모든 게이트웨이 노드의 불변식 일치 여부와 성능 절감 지표(캐시 히트율, Single-Flight 병합율, DB 부하 절감율, 서킷 브레이커 상태)를 단일 호출로 통합 검증할 수 있는 감사 엔드포인트가 부재했다.
- **수정 내용 및 결과**:
  - `control_plane.py`에 `GET /chaos/audit` 엔드포인트를 구현했다(`admin_auth_middleware`에 의해 `X-Admin-Token` 인증 적용).
  - 전 게이트웨이 노드의 `/metrics`를 동시 조회(`fetch_metrics()`)하여 온라인 노드별로 아래 항목을 수학적으로 엄격 검증했다:
    - **Invariant 1 (요청 분류 불변식)**: `requests == rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks + redis_outage_blocked`
    - **Invariant 2 (강한 DB 폴백 불변식)**: `db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + policy_circuit_blocked` (여기서 `policy_circuit_blocked = circuit_blocked - pubkey_circuit_blocked`)
    - **부하 절감 및 관측 지표**: `db_load_reduction_percentage`(Single-Flight + L1 Double-Check + 서킷 차단 절감 비율), `cache_hit_ratio_pct`, `singleflight_coalescing_ratio_pct`, `circuit_state`
  - 전체 온라인 노드가 불변식을 통과했는지 여부를 최상위 `overall_invariant_pass: bool` 필드로 즉시 반환하도록 구현했다.
  - `test_chaos_audit_auth_protection`, `test_chaos_audit_healthy_and_degraded_nodes`, `test_chaos_audit_invariant_mismatch_failure` 단위 테스트로 검증했다.

### 5.5 감사 결함 수정 및 불변식·복원력 무결성 보강 (Audit & Resilience Hardening)
- **`/pubkey` 서킷 브레이커 차단 시 `pubkey_circuit_blocked` 분리 집계를 통한 Invariant 2 정합성 보장 및 `HALF_OPEN` 404 응답 시 정상 복구(`CLOSED`) 처리**:
  - `gateway.py`의 PoP 공개키 조회 경로에서 서킷 브레이커 차단 시 `stats["circuit_blocked"]`뿐만 아니라 `stats["pubkey_circuit_blocked"]`를 독립 카운터로 분리 집계하도록 개선함.
  - 공개키 미스 차단은 정책 DB 폴백 경로(`db_fallbacks`)와 무관하므로, `control_plane.py:chaos_audit`의 Invariant 2 방정식에서 `policy_circuit_blocked = circuit_blocked - pubkey_circuit_blocked`로 차감하여 `db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + policy_circuit_blocked` 정합성을 무결하게 보장함 (`tests/test_pop.py::test_pop_pubkey_circuit_breaker_invariant_2`).
  - 또한 Policy DB가 `/pubkey` 조회에 대해 404(미등록 키)를 반환하는 경우, DB 인프라 자체는 정상 통신 가능한 상태이므로 `cb.record_failure()` 대신 `cb.record_success()`를 호출하여 `HALF_OPEN` 상태에서 `CLOSED`로 정상 복구되도록 처리함 (`tests/test_pop.py::test_pop_pubkey_404_half_open_transitions_to_closed`).
- **`/auth` 헤더 누락(401) 및 클라이언트 연결 종료(`CancelledError`) 시 Invariant 1 무결성 보장**:
  - `request.state.entered_pipeline` 플래그를 도입하여 필수 헤더(`Authorization`, `X-Nonce`, `X-SPIFFE-ID`) 누락으로 401 반환되는 조기 거부 요청이 완료 요청 카운터(`requests`)에 가산되는 현상을 차단함.
  - 클라이언트 연결 조기 종료(`asyncio.CancelledError`)나 미들웨어/헤더 조기 반환 시 불필요한 메트릭 드리프트를 방지하여, Invariant 1 (`requests == rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks + redis_outage_blocked`)의 상호 배타적 분류 합계가 정확히 일치하도록 보장함 (`tests/test_pop.py::test_pop_missing_header_zero_drift_invariant_1`, `tests/test_singleflight.py::test_follower_cancellation_zero_drift_invariant_1`).
- **`SLIDING_RL_LUA` 경계 가중치(`1` permille) 정규화 오차(`>` $\rightarrow$ `>=`) 수정**:
  - `SLIDING_RL_LUA`에서 가중치 인자 정규화 조건식 `if weight >= 1 then weight = weight / 1000.0 end`로 수정함. 기존 `weight > 1` 조건으로 인해 윈도우 99.9% 경과 시점의 잔여 가중치 `1` permille(0.001)이 나눗셈 없이 1.0(100%)으로 오판정되어 정상 요청이 429로 거부되던 정규화 오차를 완전 해소함 (`tests/test_rate_limit.py::test_sliding_window_lua_weight_boundary_999`).
- **Redis Streams 비정상 인코딩(Poison Message) 격리(`XACK`) 및 Catch-up 상한 보호**:
  - `gateway.py`의 `_process_stream_messages`에서 스트림 키 디코딩 실패(손상된 바이트열/Poison Message) 발생 시 경고 로그를 남기고 즉시 `XACK` 처리하여 컨슈머 루프 행(Hang) 및 무한 재시도를 방지함 (`tests/test_stream_listener.py::test_stream_listener_undecodable_stream_key_acked_and_logged`).
  - 파티션 복구 Catch-up 루프에 최대 배치 처리 상한(`max_claim_rounds = 100`, `max_catchup_batches = 100`)을 적용하여 비정상 스트림 쇄도 시 컨슈머 무한 루프 폭주를 원천 차단함.

### 5.6 코드 위생 정리 및 테스트 격리 (Code Hygiene Cleanup)
- **`[HYGIENE-01]`**: `control_plane.py:get_index`에서 인메모리 `index_html_cache`를 우선 반환하여 매 `GET /` 요청마다 발생하던 블로킹 디스크 I/O를 제거하고, 미적재 시에만 파일 읽기 폴백 수행.
- **`[HYGIENE-02]`**: `tests/conftest.py:clean_gateway_state`에서 `stats["last_catchup"]` 6개 필드(`pending_count`, `pel_drained`, `autoclaimed`, `new_drained`, `duration_ms`, `timestamp`) 및 신규 카운터(`pubkey_circuit_blocked` 등)를 전수 리셋하여 테스트 간 텔레메트리 누수를 원천 차단.
- **`[HYGIENE-03]`**: `gateway.py`의 중복 별칭 `release_half_open_probe_if_unrecorded()`를 제거하고 `release_half_open_probe()`로 단일화, `force_cb_state`의 미사용 `request: Request` 매개변수 제거.
- **`[HYGIENE-04]`**: `control_plane.py`의 `TEST_INVALIDATE_KEY` 및 `gateway.py`의 `DUMMY_REDIS_URL`을 `os.getenv` 기본값을 갖는 모듈 전역 상수로 추출.
- 단위 테스트 실행 결과: 총 59개 전건 통과 (`59 passed, 12 deselected`).

---

## 6. 5차 리팩토링: 클러스터 전역 단일 비행 및 보안·복원력 고도화 (v1.1.0, 2026-10-21)

### 6.1 Redis 분산 락 및 L2 폴링 기반 클러스터 전역 Single-Flight 구현
- **대상 파일 및 함수**: `gateway.py:release_cluster_lock`, `gateway.py:db_loader_task`, `gateway.py:get_metrics`, `gateway.py:CLUSTER_SINGLEFLIGHT_ENABLED`, `gateway.py:CLUSTER_SF_LOCK_TTL_SEC`, `gateway.py:CLUSTER_SF_POLL_INTERVAL_SEC`, `gateway.py:CLUSTER_SF_TIMEOUT_SEC`, `.env.example`, `docker-compose.yml`, `docs/cluster-singleflight-design.md`, `docs/benchmarks/singleflight-effect.md`, `tests/test_cluster_singleflight.py`, `tests/test_cluster_singleflight_e2e.py`, `tests/conftest.py:clean_gateway_state`
- **기존 문제**:
  - 기존 Single-Flight는 노드 내부의 `in_flight` 인메모리 딕셔너리(`dict[str, asyncio.Future]`)에서만 요청을 병합했다.
  - 3개 노드(`gateway-a`, `gateway-b`, `gateway-c`) 분산 환경에서 동일 키 요청이 여러 노드로 분산 유입되면 각 노드가 로컬 캐시 미스를 판정했다. 그 결과 노드마다 1회씩(총 3회) Policy DB를 호출하여 백엔드 DB에 중복 쿼리 부하가 발생했다 (`4.12 알려진 잔여 과제` 참조).
- **수정 내용 및 결과**:
  - `gateway.py`에 Redis 분산 락과 L2 캐시 폴링을 결합한 클러스터 전역 Single-Flight(`CLUSTER_SINGLEFLIGHT_ENABLED`, 기본값 `True`)를 구현했다.
    - **L2 선행 조회**: DB 호출 전에 `await r_client.get(f"policy:{comp_key}")`를 확인한다. 다른 노드가 이미 적재했다면 즉시 반환하고 `stats["singleflight_coalesced"] += 1` 및 `stats["singleflight_leader_queries"] -= 1`을 기록한다.
    - **Cluster Leader 선출 (SET NX)**: `await r_client.set(lock_key, lock_token, nx=True, px=CLUSTER_SF_LOCK_TTL_SEC * 1000)` 명령으로 락 선점을 시도한다 (`lock_key = f"lock:sf:{comp_key}"`, `lock_token = f"{node_name}:{uuid.uuid4()}"`, 기본 TTL `3.0s`). 락을 획득한 노드만 Leader가 되어 Policy DB를 조회하고 L1 및 L2(`policy:{comp_key}`)에 결과를 적재한다.
    - **Cluster Follower L2 폴링 및 인계**: 락 선점에 실패한 Follower 노드는 `CLUSTER_SF_POLL_INTERVAL_SEC`(기본 25ms) 간격으로 최대 `CLUSTER_SF_TIMEOUT_SEC`(기본 2.0s) 동안 L2 캐시를 폴링한다. 결과를 확인하면 로컬 `in_flight` Future를 완료한다. Leader 노드가 비정상 종료되어 락이 소멸(`not lock_exists`)하면 대기 중인 Follower가 락을 선점하여 DB 조회를 이어받는다.
    - **펜싱 토큰 원자적 락 해제 (`release_cluster_lock`)**: DB 조회가 끝나면 원자적 Lua 스크립트를 실행한다. 자신이 발급한 고유 `lock_token`과 일치할 때만 락을 삭제한다. `asyncio.shield`로 보호하여 취소 시에도 락이 해제되도록 했다.
    - **Redis 장애 시 로컬 폴백**: Redis 통신 장애가 발생하면 노드 로컬 Single-Flight로 자동 전환하여 가용성을 유지한다.
    - **관측 지표 및 불변식 정합성**: L2 회수 시 `stats["singleflight_coalesced"] += 1`을 기록하고 `stats["singleflight_leader_queries"] -= 1`을 차감한다. 이를 통해 Invariant 1(`requests == sum(classified)`) 및 Invariant 2(`db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + policy_circuit_blocked`) 정합성을 유지한다.
  - `docs/benchmarks/singleflight-effect.md` 실측 결과, 3개 노드 클러스터에서 동일 키 동시 요청 90건 유입 시 전체 DB 호출이 3회에서 1회로 66.7% 감소했다.
  - `tests/test_cluster_singleflight.py` 10개 단위 테스트 및 `tests/test_cluster_singleflight_e2e.py` E2E 테스트로 검증했다.

### 6.2 PoP 정규화 페이로드 Query String 및 Body Digest 바인딩 확장 (Scope B 보안 강화)
- **대상 파일 및 함수**: `gateway.py:generate_canonical_payload`, `gateway.py:auth`, `tests/test_pop.py`, `tests/test_chaos_audit.py`, `README.md`
- **기존 문제**:
  - 기존 Scope B PoP 서명 검증은 Method, Path, Nonce, SPIFFE ID 4개 필드만 서명 페이로드로 바인딩했다.
  - Query String(`?role=admin` 등)이나 요청 본문(`POST /auth` JSON 페이로드)이 중간에서 변조되거나 `X-Body-Digest` 헤더가 조작되어도 서명 검증을 통과하는 취약점이 있었다.
- **수정 내용 및 결과**:
  - `gateway.py:generate_canonical_payload` 및 `gateway.py:verify_pop_signature`의 정규화 규격을 Method, Path, Query String, Body Digest, SPIFFE ID, Nonce 6개 필드로 확장했다.
    - **정규화 문자열 형식**: `f"{method.upper()}\n{path}\n{canonical_query}\n{body_digest}\n{spiffe_id}\n{nonce}"`
    - **Body Digest 규격**: 클라이언트가 전송한 `X-Body-Digest` 헤더(`sha256=<hex_digest>`)와 실제 요청 본문의 SHA-256 다이제스트를 `hmac.compare_digest`로 상수 시간 비교한다. 본문이 비어있으면 `EMPTY_BODY_DIGEST`(`sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`)를 적용한다.
    - **Query 정규화**: 쿼리 파라미터를 알파벳 오름차순으로 정렬(`parse_qsl` → `sorted` → `urlencode`)하여 파라미터 순서 불일치로 인한 오탐을 방지한다.
  - **본문 해시 사전 검증**: `X-Body-Digest` 접두사(`sha256=`)가 없거나 요청 본문과 일치하지 않으면 Nonce를 소진한 후 `400 Bad Request`(`"Missing or invalid X-Body-Digest header"` 또는 `"Body digest mismatch"`)를 반환한다(`pop_rejected += 1`). 고비용 비대칭 암호 연산(ECDSA P-256 verify) 전에 무효 요청을 차단하여 CPU 낭비를 막는다.
  - `tests/test_pop.py`에 바디 다이제스트 누락 및 불일치 400 반환(`test_pop_signature_missing_body_digest_header_returns_400`, `test_pop_signature_body_digest_mismatch_returns_400`, `test_pop_signature_valid_request_with_body_digest_and_query_succeeds`) 및 쿼리 변조 401 차단(`test_pop_signature_query_tampering_rejected`) 테스트를 추가하여 동작을 확인했다.

### 6.3 Single-Flight OFF 모드 클라이언트 취소 시 백엔드 DB 쿼리 중단 정합성 회복
- **대상 파일 및 함수**: `gateway.py:auth_endpoint`, `tests/test_singleflight.py:test_singleflight_disabled_client_cancellation_aborts_db`
- **기존 문제**:
  - `SINGLEFLIGHT_ENABLED=False`(단일 비행 비활성화) 모드에서 DB 조회 코루틴을 `db_loader_task()` 비동기 태스크로 분리하고 `asyncio.shield()`로 감싸 실행했다.
  - 이로 인해 클라이언트 연결이 끊겨도(`CancelledError`) 백엔드 DB 쿼리가 취소되지 않고 실행되었다. 단일 비행 비활성화 모드임에도 불필요한 백엔드 DB 자원이 소모되는 문제가 있었다 (`README.md 알려진 한계점` 참조).
- **수정 내용 및 결과**:
  - `gateway.py:auth_endpoint`의 `singleflight_mode == "off"` 분기에서 `asyncio.shield`를 제거했다. 클라이언트 요청 컨텍스트에서 `await db_loader_task(...)`를 직접 실행하도록 변경했다.
  - 클라이언트가 연결을 끊으면 `asyncio.CancelledError`가 DB 조회 코루틴으로 전파되어 백엔드 호출이 즉시 중단된다.
  - `tests/test_singleflight.py::test_singleflight_disabled_client_cancellation_aborts_db` 단위 테스트를 추가하여 OFF 모드에서 클라이언트 취소 시 DB 조회가 즉시 중단됨을 검증했다.
  - `README.md`의 한계점 목록에서 관련 항목을 제거했다.

### 6.4 Rate Limiter 고정 윈도우 decr 예외 로깅 보강 및 Redis TIME 단일 시간 소스 검증
- **대상 파일 및 함수**: `gateway.py:check_rate_limit`, `tests/test_rate_limit.py:test_fixed_window_decr_rollback_logging`, `tests/conftest.py:mock_common_dependencies`, `docs/benchmarks/rate-limit-sliding.md`, `README.md`
- **기존 문제**:
  - `check_rate_limit`의 고정 윈도우(`fixed`) 방식에서 한도 초과 시 실행되는 `decr` 롤백 블록이 `except Exception: pass`로 예외를 처리했다. 이로 인해 Redis 장애나 타임아웃 발생 시 롤백 실패를 관측하기 어려웠다.
  - 또한 노드 로컬의 `time.time()`을 기준으로 윈도우를 계산하면 노드 간 시계 오차로 인해 윈도우 경계 판단에 편차가 발생할 수 있었다.
- **수정 내용 및 결과**:
  - `check_rate_limit`의 `decr` 롤백 블록에 `logging.debug(f"Rate limit decr rollback failed for {key}: {e}")` 출력을 추가했다.
  - `tests/conftest.py:mock_common_dependencies`에 `m_redis.time` 모의 객체를 추가하여 Redis `TIME` 명령 기반의 시간 일관성 검증을 지원하도록 했다.
  - `tests/test_rate_limit.py::test_fixed_window_decr_rollback_logging` 단위 테스트를 추가하여 롤백 실패 로그 출력을 확인했다.
  - `docs/benchmarks/rate-limit-sliding.md` 및 `README.md`에 Redis `TIME` 단일 시간 소스 사용 시의 트레이드오프(0.2~0.5ms 지연 시간 추가 대비 시계 동기화 효과)를 분석하고 관련 한계점을 정리했다.

### 6.5 9대 카오스 복원력 시나리오 연속 검증 및 수학적 불변식 감사 스위트 확장
- **대상 파일 및 함수**: `tests/test_chaos_audit.py`, `tests/conftest.py:e2e_cluster`, `.github/workflows/ci.yml`
- **기존 문제**:
  - 기존 단위 테스트는 각 시나리오를 파일별로 나누어 검증했다. README에 정리된 9대 카오스 시나리오를 단일 파이프라인에서 순차 실행한 뒤 카운터 오차가 없는지 `GET /chaos/audit`으로 점검하는 회귀 테스트가 없었다.
  - 감사 엔드포인트의 분모 0, 순환소수 반올림, 실수형 카운터 유입 등 경계 조건 검증도 부족했다.
- **수정 내용 및 결과**:
  - `tests/test_chaos_audit.py`를 작성하여 9대 카오스 시나리오를 연속 실행하는 통합 단위 테스트(`test_chaos_audit_back_to_back_9_scenarios_unit`)를 구현했다. 실행 후 Invariant 1과 Invariant 2의 카운터 오차가 0인지, `overall_invariant_pass`가 `True`인지 검증한다.
  - 감사 엔드포인트 수식 경계 테스트 7건(`test_audit_edge_case_zero_denominators`, `test_audit_edge_case_repeating_fractions_rounding`, `test_audit_edge_case_bankers_rounding_halfway`, `test_audit_edge_case_100_percent_saturation_boundary`, `test_audit_edge_case_float_counter_immunity`, `test_sliding_window_rate_limiter_lua_weight_boundary_math`, `test_local_sliding_window_fallback_truncation_edge_case`)을 추가했다.
  - `tests/conftest.py`에 클러스터 E2E 테스트 픽스처(`e2e_cluster`)를 구현하고, E2E 시나리오 테스트(`test_chaos_audit_e2e_cluster_back_to_back_9_scenarios`)를 추가했다.
  - `.github/workflows/ci.yml`의 CI 실행 항목에 `tests/test_chaos_audit.py`를 추가했다.

### 6.6 의도적 미적용 항목
- **PoP kid 헤더 다중 키 파싱 및 하드웨어 Attestation 검증**: Scope B 경량 구현 범위와 데모 환경 제약(단일 키 운용)을 고려하여 이번 범위에서 제외하고 기존 규격을 유지한다.
- **클러스터 Single-Flight Follower 대기 방식의 Redis Pub/Sub 전환**: 25ms L2 폴링 대비 커넥션 오버헤드와 채널 구독 복잡성을 고려하여 폴링 방식을 유지한다.

### 6.7 알려진 잔여 과제
- **클러스터 Single-Flight Follower 폴링 지연**: Follower 노드는 Leader의 L2 적재를 25ms 간격으로 폴링하므로, 캐시 미스 시 Leader 대비 평균 12.5ms에서 최대 25ms의 대기 시간이 추가된다.
- **Leader 비정상 종료 시 인계 대기 시간**: Leader 노드가 락 선점 후 DB 조회 중 비정상 종료(`SIGKILL` 등)되면, 락 TTL(3.0초) 또는 폴링 타임아웃(2.0초) 만료까지 Follower가 대기한 후 락을 인계받으므로 일시적인 지연이 발생할 수 있다.

- 단위 테스트 실행 결과: 총 85개 전건 통과 (`85 passed, 14 deselected`).

---

## 7. 6차 리팩토링: 프로덕션 호환성(PEP 프록시 모드) 및 고가용성(HA) 지원 고도화 (v1.2.0, 2026-10-22)

### 7.1 비동기 리버스 프록시(PEP) 패스스루 토글 모드 구현 (`UPSTREAM_URL`)
- **대상 파일 및 함수**: `gateway.py:auth`, `.env.example`, `tests/test_production_ha.py`
- **기존 문제**: 게이트웨이가 순수 인가 판정(PDP) 결과 JSON만 반환하도록 고정되어 있어, Envoy `ext_authz`와 같은 별도 외부 프록시 없이는 단독으로 백엔드 실서버(Upstream)로 비즈니스 트래픽을 중계(PEP)할 수 없었다.
- **수정 내용 및 결과**: `UPSTREAM_URL` 환경변수 기반 토글 모드를 구현했다. 미설정 시에는 기존 PDP 응답을 100% 유지하며, 설정 시에는 6단계 파이프라인 통과 후 `DENY` 정책은 `403 Forbidden`으로 즉시 차단하고 `allow`/`degraded_allow` 요청만 공유 `httpx.AsyncClient` 풀을 통해 Upstream으로 비동기 패스스루 포워딩한다. 검증된 신원 헤더(`X-Authenticated-SPIFFE-ID`, `X-Policy-Decision`, `X-Auth-Source`)를 주입하며, Upstream 타임아웃(`504`)/연결 실패(`502`)를 Policy DB 서킷 브레이커 및 2대 불변식 카운터와 분리하여 정합성을 보존했다.

### 7.2 GSLB/ALB 페일오버 연동을 위한 딥 헬스체크(`GET /health`) 고도화
- **대상 파일 및 함수**: `gateway.py:health_check`, `tests/conftest.py`, `tests/test_production_ha.py`
- **기존 문제**: `/health` 엔드포인트가 의존 인프라(Redis) 상태나 네트워크 고립 여부와 무관하게 정적 `200 OK`를 반환하여, 중앙 Redis 마비나 노드 고립 시 상위 로드밸런서(GSLB/ALB)가 장애 노드를 감지하고 Standby로 페일오버할 수 없었다.
- **수정 내용 및 결과**: `/health` 호출 시 `is_partitioned` 고립 여부를 우선 확인하고, `0.2s`(`HEALTHCHECK_PING_TIMEOUT_SEC`) 타임아웃 제한을 둔 `redis_client.ping()`을 수행하도록 개선했다. 고립 상태이거나 Redis 응답 불가 시 즉시 `503 Service Unavailable`을 반환하여 상위 계층의 자동 페일오버를 유도한다.

### 7.3 Redis Sentinel 기반 마스터 동적 재연결(HA) 지원
- **대상 파일 및 함수**: `gateway.py:get_redis_client`, `gateway.py:parse_sentinel_hosts`, `.env.example`, `tests/test_production_ha.py`
- **기존 문제**: 단일 `REDIS_URL` 연결만 지원하여 중앙 Redis가 단일 장애점(SPOF)이 될 수 있었고, 마스터 노드 장애 시 자동 승격된 새 마스터로 동적 전환할 수 없었다.
- **수정 내용 및 결과**: `USE_REDIS_SENTINEL`, `REDIS_SENTINEL_HOSTS`, `REDIS_SENTINEL_MASTER_NAME` 설정을 추가하고, 활성화 시 `redis.asyncio.sentinel.Sentinel`의 `master_for()`를 통해 현재 활성 마스터 클라이언트를 동적으로 획득하도록 구현했다. Sentinel 마스터 클라이언트 초기화 시에도 `NONCE_LUA`와 `SLIDING_RL_LUA` 스크립트가 자동 등록되도록 통합했다.

- 단위 테스트 실행 결과: 총 93개 전건 통과 (`93 passed, 14 deselected`).
