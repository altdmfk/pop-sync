# Aegis-Sync

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
Redis Pipeline과 Lua 스크립트를 통해 가중 합산 근사치 기반의 슬라이딩 윈도우(또는 RL_ALGORITHM 토글을 통한 고정 윈도우) 방식 데이터 리밋을 구현했다. (경계 버스트 측정 결과: docs/benchmarks/rate-limit-sliding.md). Rate Limit은 노드당 한도가 아닌, 클라이언트 ID(SPIFFE ID) 단위의 클러스터 전역(Global) 한도를 갖는다.
Lua 스크립트 기반 원자적 Nonce 소비로 DDoS와 리플레이 공격을 방어한다.

### PoP 검증 (Proof of Possession - Scope B)
FIDO2 모델에서 영감을 받은 경량화된 ECDSA P-256 서명 검증(Request-digest)을 지원한다 (POP_VERIFY_ENABLED 기본값 False). Policy DB의 키 레지스트리(엔드포인트)를 통해 공개키를 조회하며, 원천 차단 대신 논스 소진(burn)을 통한 재사용 방지를 위해 Nonce 검증 통과 **이후**에 서명 검증을 수행한다. 통계 분류를 위해 누락 및 검증 실패 카운터를 별도 관리한다.

---

## 신뢰 경계 및 계승 (Trust Boundary & Inheritance)

본 게이트웨이는 클라이언트 식별과 Rate Limiting에 X-SPIFFE-ID HTTP 헤더를 의존한다.

- **mTLS 위임**: mTLS 인증은 상위 L4/L7 리버스 프록시(Envoy 등)에서 처리된다고 가정한다. 프록시가 클라이언트 인증서를 검증하고 X-SPIFFE-ID 헤더를 주입하는 구조다.
- **데모 스코프**: 프록시 계층 구현과 mTLS 인증서 파싱은 이 코드베이스에서 의도적으로 생략했다. 테스트 환경에는 단일 Demo Key만 등록되어 있다. 프록시가 Trust Boundary를 보장한다는 전제하에 분산 시스템 복원력에만 집중한다.

### fido2-pop-gateway 및 aegis-mesh 대비 변경 사항
| 계승 / 변경 / 미이식 | 원본 구현체 | 현재 코드베이스 (Aegis-Sync) |
| --- | --- | --- |
| **언어/프레임워크** | Java / Reactor | Python / FastAPI |
| **인메모리 캐시** | Caffeine | OrderedDict LRU (지연 만료, Lazy expiry) |
| **무효화 전송 계층** | Redis Pub/Sub | Redis Streams (XREADGROUP 기반 PEL 복구 지원) |
| **데이터 리밋** | Sliding Window Log | 가중 합산 근사치 기반 슬라이딩 카운터 |
| **PoP 서명** | WebAuthn CBOR Envelope | 단순화된 ECDSA P-256 (Canonical Request-digest, Scope B) |

---

## 장애 시나리오 동작 원리 (Resilience)

| 장애 유형 | 동작 |
| --- | --- |
| **Redis 다운** | 로컬 폴백이 429(Too Many Requests) 여부를 결정(측정치: 15/5, 15/5, 20/0 차단 발생 - 3회차 미재현). 그러나 이를 통과한 요청도 이어지는 Nonce 검사(Redis 의존)에서 503(Fail Closed)이 발생하므로, Redis 다운 시 /auth는 실질적으로 전체 503 실패로 귀결된다. |
| **DB 다운** | 서킷 브레이커 OPEN 전환. L1/L2 캐시 히트는 정상 응답, 캐시 미스 시 degraded_allow 처리 |
| **네트워크 파티션** | 노드 격리. 해소 시 Redis Stream PEL 긴급 적용으로 L1 캐시 오염 복원 |

---

## 보안 시크릿과 관측성

Control Plane의 /chaos/* 엔드포인트는 X-Admin-Token 헤더로 보호된다. Gateway의 /metrics 도 동일하게 보호된다.

- .env.example을 .env로 복사한 뒤, ADMIN_SECRET_TOKEN의 값을 보안 환경에 맞게 강한 토큰으로 교체하거나 로컬 데모 시 그대로 사용한다.
- **프론트엔드**: 브라우저 탭 종료 시 자동 소멸되는 sessionStorage에 저장되며 EventSource 연결 전 Pre-flight API 검증을 거친다.

---

## 빠른 시작

1. 환경 변수 파일 생성:
`ash
cp .env.example .env
`
2. .env 파일 내부의 ADMIN_SECRET_TOKEN 값을 원하는 토큰(기본 your-secure-token-here)으로 설정.
3. 클러스터 빌드 및 구동:
`ash
docker-compose up -d --build
`
4. **접속 정보**:
   - 대시보드(Control Plane): http://localhost:8080
   - 게이트웨이(Gateway) 노드: http://localhost:8001, 8002, 8003

### 단위 테스트 실행 방법
`ash
python -m venv .venv
source .venv/bin/activate  # (Windows: .\.venv\Scripts\activate)
pip install -r requirements.txt
pip install -r requirements-dev.txt
pytest tests/test_singleflight.py tests/test_rate_limit.py tests/test_pop.py -v
`
> E2E 테스트 스크립트(	est_scenarios.py, 	est_rl_fallback.py 등)는 실제 도커 클러스터 구동이 필요하다.

---

## 알려진 한계점 (Known Limitations & Future Work)

- **근사 알고리즘 오차**: Sliding Window는 단순 가중 합산 근사치이므로 이상값이 발생할 수 있으며, 차단된 429 요청도 카운트에 포함된다.
- **노드 간 시각차 (Clock Skew)**: 노드 간 시각차가 클 경우 윈도우 경계 계산에서 불일치가 발생할 수 있다.
- **단일 비행 취소(Cancellation) 동작 차이**: OFF 모드라도 백그라운드 태스크(syncio.shield)에서 DB 조회가 계속 진행되므로, 클라이언트 연결이 끊겨도 DB 호출이 취소되지 않는 차이점이 존재한다 (pre-singleflight 버전과 다름).
- **성능 실측의 한계**: Single-flight 성능 검증은 단일 노드에서 DB 지연을 시뮬레이션하여 3회 반복한 제한적 조건에서 수행되었다.
- **PoP 서명의 한계**: 검증 필드에 Timestamp, Body, Query, Host가 결합되지 않으며(Unbound fields), kid(Key-ID) 헤더 지원 및 하드웨어 Attestation 파싱 기능이 없다. 테스트에는 단일 Demo Key만 사용된다.
- **PoP 테스트 구성**: PoP 기능(Toggle ON) 시나리오는 클라이언트가 실제로 서명을 생성해야 하므로 기존 	est_scenarios.py (서명 없이 전송) 환경과 분리되어 검증이 제한적이다.
- **테스트 환경 제약**: 단위 테스트는 Redis/DB를 Mock으로 대체하며, 일부 기존 E2E 테스트(컨테이너명 하드코딩, Fixture 누락, pre-singleflight 비교 부재)가 실패하는 제약이 있다. 잘못된 "노드당 한도(per-node)" 주석 등은 현재 버전에서 수정되었다.
