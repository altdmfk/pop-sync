# Aegis-Sync: 분산 게이트웨이 카오스 엔지니어링 데모 (Distributed Gateway Chaos Engineering Demo)

이 프로젝트는 네트워크 파티션(단절), Redis 중앙 캐시 중단, 대규모 DDoS 공격 등의 극한 상황에서도 스스로를 보호하고 무중단 서비스를 제공하는 **분산 게이트웨이의 복원력(Resilience)**을 실시간으로 시연합니다.

## 주요 기능 및 복원력 메커니즘
- **서킷 브레이커 (Circuit Breaker)**: Policy DB 장애 감지 시 `OPEN` 상태로 전환하여 연쇄 장애를 방지하며, 2.0초의 디바운스(Debounce) 로직을 통해 Flapping 현상을 완벽히 제어합니다.
- **로컬 펄백 (Local Fallback & L1/L2 Cache)**: 중앙 Redis 캐시가 죽더라도 각 게이트웨이 노드가 로컬(L1) 캐시를 활용해 L1-Only 모드로 생존하여 기존 세션의 무중단 서비스를 보장합니다.
- **스트림 기반 데이터 동기화 (Catch-up)**: 네트워크 단절(Partition) 발생 후 재연결 시, Redis Stream을 통해 누락된 무효화 이벤트를 소급 적용하여 최종 일관성(Eventual Consistency)을 맞춥니다.
- **O(1) Rate Limiting & Nonce 원자성 방어**: Sliding window 기반 처리와 Lua Script를 이용한 Redis 원자적 락으로 DDoS 및 중복 요청(Replay)을 방어합니다.

## 신뢰 경계(Trust Boundary) 및 X-SPIFFE-ID (스코프 제한)
이 게이트웨이는 클라이언트 식별 및 Rate Limiting을 위해 `X-SPIFFE-ID` HTTP 헤더에 전적으로 의존합니다.
- **mTLS 위임 처리**: 본 아키텍처는 mTLS 인증이 상위 L4/L7 리버스 프록시(예: Envoy, Nginx)에서 처리됨을 가정합니다. 프록시가 클라이언트 인증서를 검증하고 그 결과를 `X-SPIFFE-ID` 헤더에 주입해야 합니다.
- **네트워크 격리 전제**: `gateway.py`는 수신된 `X-SPIFFE-ID` 헤더를 맹목적으로 신뢰합니다. 따라서 이 게이트웨이는 외부 트래픽이 프록시를 우회하여 직접 도달할 수 없는 완전히 격리된 내부망에 배포되어야만 안전합니다. 외부 클라이언트가 임의의 헤더를 싣고 게이트웨이에 직접 접근할 수 있다면 이 신뢰 모델은 깨집니다.
- **데모 스코프**: 프록시 계층 구현 및 mTLS 인증서 파싱 등은 본 코드베이스에서 의도적으로 생략되었습니다. 이 프로젝트의 핵심은 철저히 **분산 시스템의 복원력 및 카오스 엔지니어링**에 맞춰져 있습니다.
- **파티션 격리 시뮬레이션**: 파티션 격리 시뮬레이션이 활성화된 노드는 실제 네트워크 단절 상황을 충실히 재현하기 위해 CB(서킷 브레이커) 강제 상태 주입을 포함한 모든 관리 API 호출이 차단되도록 설계되었습니다 (503 Service Unavailable).

## 보안 및 시크릿 관리 (Security & Secrets Management)
Control Plane의 주요 엔드포인트(`/chaos/*` 및 `/metrics/stream`)는 `ADMIN_SECRET_TOKEN`을 통해 보호됩니다.
- **로컬 데모 설정**: 기본적으로 로컬 데모와 테스트 편의를 위해 `"super-secret-admin"` 문자열이 사용됩니다.
- **프로덕션 주의사항**: 실제 배포 시에는 반드시 환경 변수(`ADMIN_SECRET_TOKEN`)를 통해 강력한 임의의 토큰으로 덮어써야 합니다.
- **설정 방법**: `.env.example` 파일을 복사하여 `.env`를 생성한 후, 값을 변경하면 `docker-compose` 구동 시 자동 적용됩니다.
- **프론트엔드 보안**: 대시보드 접근 시 사용되는 토큰은 브라우저 탭 종료 시 자동 소멸되는 `sessionStorage`에 안전하게 저장되며, EventSource 연결 전 Pre-flight 검증을 거쳐 불필요한 접근을 방지합니다.
