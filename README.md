# Aegis-Sync

네트워크 파티션, Redis 장애, 대규모 DDoS 상황에서 분산 게이트웨이의 복원력(Resilience)을 실시간으로 시연하는 카오스 엔지니어링 데모다.

---

## 주요 기능

### 서킷 브레이커 (Circuit Breaker)
Policy DB 장애 감지 시 `OPEN` 상태로 전환해 연쇄 장애를 차단한다. 2.0초 디바운스 로직으로 Flapping 현상을 제어하며, `CLOSED` -> `OPEN` -> `HALF_OPEN` -> `CLOSED` 사이클을 안정적으로 관리한다.

### L1/L2 캐시 및 로컬 폴백 (Cache & Fallback)
정책 조회 경로는 세 단계로 동작한다. 먼저 노드 본지(L1, `OrderedDict` LRU)를 확인하고, 없으면 중앙 Redis(L2)를 조회한다. 둘 다 놓치면 Policy DB를 직접 호출한다. Redis가 완전히 다운되더라도 L1 히트만으로 서비스를 유지할 수 있다.

### 스트림 기반 데이터 동기화 (Stream Catch-up)
네트워크 단절(파티션) 발생 후 재연결 시, Redis Stream의 펜딩 엔트리 리스트(PEL)와 누락된 메시지를 연속으로 소급 적용한다. 최종 일관성(Eventual Consistency)을 보장한다.

### O(1) 레이트 리미팅 & Nonce 원자성 방어
Redis Pipeline으로 슬라이딩 윈도우 방식의 레이트 리미팅을 구현한다. Redis 다운 시에는 노드별 로컬 폴백 리미터를 적용한다. Lua 스크립트 기반 원자적 Nonce 소비로 DDoS와 리플레이 공격을 방어한다.

---

## 신뢰 경계 (Trust Boundary)

이 게이트웨이는 클라이언트 식별과 Rate Limiting에 `X-SPIFFE-ID` HTTP 헤더를 활용한다.

- **mTLS 위임**: mTLS 인증은 상위 L4/L7 리버스 프록시(Envoy 등)에서 처리된다고 가정한다. 프록시가 클라이언트 인증서를 검증하고 `X-SPIFFE-ID` 헤더에 주입하는 구조다.
- **네트워크 격리 전제**: `gateway.py`는 수신된 `X-SPIFFE-ID` 헤더를 신뢰한다. 따라서 외부 트래픽이 프록시를 우회해 게이트웨이에 직접 도달할 수 없는 격리된 내부망에서만 안전하다.
- **데모 스코프**: 프록시 계층 구현과 mTLS 인증서 파싱은 이 코드베이스에서 의도적으로 생략했다. 프록시가 Trust Boundary를 보장한다는 전제하에 분산 시스템 복원력에만 집중한다.

---

## 비예호서비스 동작 정리

| 장애 유형 | 동작 |
| --- | --- |
| **Redis 다운** | L1 히트는 정상 응답. L1 미스 시 로컬 폴백 레이트 리미터 적용, DB 직접 호출 |
| **DB 다운** | 서킷 브레이커 `OPEN` 전환. L1/L2 캐시 히트는 정상 응답, 캐시 미스 시 `degraded_allow` |
| **네트워크 파티션** | 노드 격리. 합 시 Redis Stream PEL 소급 적용으로 L1 캐시 일관성 복원 |
| **DDoS** | Rate Limiting 429 반환 (노드별 50req/10s, Redis 다운 시 15req/10s) |
| **Nonce 재사용** | Lua 원자적 Redis 연산. Redis 다운 시 write 액션은 Fail Closed |

---

## 보안 및 시크릿 관리

Control Plane의 `/chaos/*` 엔드포인트는 `X-Admin-Token` 헤더로 보호된다. Gateway의 `/metrics`, `/chaos/*`도 동일하게 보호된다.

- **로컬 데모**: `.env` 파일에서 `ADMIN_SECRET_TOKEN`을 설정한다. 기본값 `super-secret-admin`은 로컬 데모 전용이며, 미설정 시 서비스가 시작되지 않는다.
- **프로덕션 주의사항**: 실제 배포 시 `ADMIN_SECRET_TOKEN` 환경 변수를 반드시 강력한 임의 토큰으로 교체해야 한다.
- **설정 방법**: `.env.example`을 `.env`로 복사한 후 값을 수정하면 `docker-compose` 구동 시 자동 적용된다.
- **프론트엔드**: 대시보드 토큰은 브라우저 탭 종료 시 자동 소멸되는 `sessionStorage`에 저장되며, EventSource 연결 전 Pre-flight API 검증을 거친다.

---

## 빠른 시작

```bash
cp .env.example .env
# .env에서 ADMIN_SECRET_TOKEN 설정
docker-compose up --build
```

대시보드: http://localhost:8080

---

## 구조

```
[Control Plane :8080]  <-- 대시보드, 카오스 API
       |
  +----+----+
  |    |    |
[GW-A] [GW-B] [GW-C]  :8001 / :8002 / :8003
  |    |    |
  +----+----+
       |
    [Redis]            스트림, Nonce, Rate Limit
       |
  [Policy DB :8000]    정책 백엔드 (신호 스트레스 시뮬레이션 포함)
```