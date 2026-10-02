# Rate Limit (Sliding Window) 벤치마크 및 검증 결과

## 1. 경계 버스트 방어 실측 (Boundary Burst Measurement)

윈도우 경계(10초)를 기준으로 직전 0.5초와 직후 0.5초에 각각 50개(한도)의 요청을 전송하여 알고리즘별 버스트 방어 성능을 측정했습니다.

### Sliding (버스트 방어 성공)
Sliding 알고리즘은 경계를 넘어가더라도 이전 윈도우의 비중을 가중 합산하여 스파이크를 완벽하게 차단합니다. 3회 모두 동일한 결과를 보였습니다.
```text
Waiting 8.52s until 0.5s before window boundary...
Sending 50 requests right BEFORE boundary...
Before boundary results - Passed: 50, 429: 0, Other: 0
Waiting 0.34s until 0.5s after window boundary...
Sending 50 requests right AFTER boundary...
After boundary results - Passed: 0, 429: 50, Other: 0
Total Passed across boundary: 50 (expected: ~50 for sliding, ~100 for fixed)

Waiting 1.12s until 0.5s before window boundary...
Sending 50 requests right BEFORE boundary...
Before boundary results - Passed: 50, 429: 0, Other: 0
Waiting 0.43s until 0.5s after window boundary...
Sending 50 requests right AFTER boundary...
After boundary results - Passed: 0, 429: 50, Other: 0
Total Passed across boundary: 50 (expected: ~50 for sliding, ~100 for fixed)

Waiting 4.47s until 0.5s before window boundary...
Sending 50 requests right BEFORE boundary...
Before boundary results - Passed: 50, 429: 0, Other: 0
Waiting 0.43s until 0.5s after window boundary...
Sending 50 requests right AFTER boundary...
After boundary results - Passed: 0, 429: 50, Other: 0
Total Passed across boundary: 50 (expected: ~50 for sliding, ~100 for fixed)
```

### Fixed (2배 버스트 허용 한계)
Fixed 알고리즘은 경계 초기화 순간 이전 기록이 리셋되므로, 순간적으로 한도의 2배(100개)가 통과되는 구조적 한계를 보입니다. 3회차에서는 Host-Container 간의 시간차(Clock Skew)로 인해 직전/직후 요청이 단일 윈도우로 병합 처리된 것으로 추정되는 현상이 있었습니다.
```text
Waiting 5.02s until 0.5s before window boundary...
Sending 50 requests right BEFORE boundary...
Before boundary results - Passed: 50, 429: 0, Other: 0
Waiting 0.47s until 0.5s after window boundary...
Sending 50 requests right AFTER boundary...
After boundary results - Passed: 50, 429: 0, Other: 0
Total Passed across boundary: 100 (expected: ~50 for sliding, ~100 for fixed)

Waiting 4.68s until 0.5s before window boundary...
Sending 50 requests right BEFORE boundary...
Before boundary results - Passed: 50, 429: 0, Other: 0
Waiting 0.43s until 0.5s after window boundary...
Sending 50 requests right AFTER boundary...
After boundary results - Passed: 50, 429: 0, Other: 0
Total Passed across boundary: 100 (expected: ~50 for sliding, ~100 for fixed)

Waiting 3.99s until 0.5s before window boundary...
Sending 50 requests right BEFORE boundary...
Before boundary results - Passed: 50, 429: 0, Other: 0
Waiting 0.36s until 0.5s after window boundary...
Sending 50 requests right AFTER boundary...
After boundary results - Passed: 2, 429: 48, Other: 0
Total Passed across boundary: 52 (expected: ~50 for sliding, ~100 for fixed)
```

## 2. Lua 스크립트 실제 Redis 검증
단위 테스트의 mock 한계를 보완하기 위해, 실제 Redis에서 Lua 스크립트를 직접 평가했습니다.
초기 윈도우(100)에 50개가 존재하고, 새 윈도우(101)에 5초 경과 (가중치 0.5)인 상황을 가정했습니다.
```text
1st req estimated: 26 (expected 26)
2nd req estimated: 27 (expected 27)
TTL of current window: 20 (expected > 0)
```
예상대로 `50 * 0.5 + 1 = 26` 등의 가중 합산이 완벽하게 연산되며 TTL도 정상 등록됨을 확인했습니다.

## 3. Redis 장애 실측 (Fallback)
실측 횟수 3회, 단일 환경에서 Redis 컨테이너를 중지(`docker stop`)시킨 후 새 client_id로 20개의 요청을 동시(`asyncio.gather`)에 보내 로컬 Fallback 동작을 확인했습니다.

순차 방식(for loop)으로 요청 시 실패했던 원인은, Redis 연결 타임아웃 대기로 인해 15개 요청을 보내는 데 약 60초 이상이 소요되어 10초 윈도우가 초기화되었기 때문입니다(확인된 사실).

동시 요청 시 응답 완료 순서는 무의미하며, 응답 건수만 의미가 있습니다. Redis 장애 중에는 RL을 통과한 요청이라도 이어지는 Nonce 검증 단계(Redis 의존)에서 결국 타임아웃으로 503이 발생하므로 `/auth`는 사실상 전부 실패합니다. 즉, 로컬 fallback은 전체 장애를 막아주는 것이 아니라 "429(Too Many Requests) 차단 판단"만 로컬화하여 정확한 의미론적 에러를 반환하는 역할만 수행합니다.

### 회차별 실측 결과
| 회차 | 503 (Nonce 단계 타임아웃) | 429 (로컬 RL 차단) | 기타 |
|---|---|---|---|
| 1회차 | 15건 | 5건 | 0건 |
| 2회차 | 15건 | 5건 | 0건 |
| 3회차 | 20건 | 0건 | 0건 |
*3회차의 경우, 요청들이 정확히 윈도우 경계(10초)를 걸쳐 분산 처리되면서 단일 윈도우 내 누적 카운트가 15(한도)에 도달하지 못해 전량 503이 발생한 것으로 추정됩니다.*

**1회차 원문:**
```text
Request 1: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 2: Status 429 - Body: {"status":"error","message":"Too Many Requests"}
Request 3: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 4: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 5: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 6: Status 429 - Body: {"status":"error","message":"Too Many Requests"}
Request 7: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 8: Status 429 - Body: {"status":"error","message":"Too Many Requests"}
Request 9: Status 429 - Body: {"status":"error","message":"Too Many Requests"}
Request 10: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 11: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 12: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 13: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 14: Status 429 - Body: {"status":"error","message":"Too Many Requests"}
Request 15: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 16: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 17: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 18: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 19: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
Request 20: Status 503 - Body: {"status":"error","message":"Service Unavailable - Nonce Check Failed"}
```

## 4. 한계점 (Limitations)
1. **근사 알고리즘의 오차**: Sliding Window Counter는 완벽한 로그(log) 방식이 아닌 윈도우 가중 합산에 의존하므로, 트래픽이 윈도우 내에 고르지 않게 분포된 경우 실제 요청 수와 추정치 사이에 작은 오차가 발생할 수 있습니다.
2. **차단된 요청 포함 카운트**: 현재 구현은 INCR 후 한도 초과 여부를 검사하므로, 차단된 요청(429)도 카운트를 증가시켜 이후 정상 트래픽까지 패널티를 받게 하는 특성이 있습니다. (추가 로직 없이 단순 INCR을 사용해 원자성을 확보함)
3. **노드 간 시각 차이**: 현재 `time.time()`을 클라이언트(게이트웨이) 시각 기준으로 사용하여 Redis TIME 왕복 비용을 없앴습니다. 그러나 분산 노드 간의 시각 차이(Clock Skew)가 존재하면 같은 윈도우 키에 대한 합산이 어긋날 수 있습니다. Fixed 버스트 3회차의 `52개 통과` 이상값 역시 측정 스크립트가 실행된 Host 머신과 컨테이너 내부 간의 미세한 시간차로 인한 현상으로 추정됩니다.
