# Aegis-Sync: Distributed Gateway Chaos Engineering Demo

This project demonstrates a distributed gateway architecture emphasizing robustness against network partitions, Redis outages, and DDoS attacks via rate-limiting and fail-closed nonce verifications.

## Trust Boundary & X-SPIFFE-ID Architecture (Scope Limitations)
The gateway relies on the `X-SPIFFE-ID` HTTP header for identity-based rate limiting and access control. 
- **Delegated mTLS Termination**: This architecture assumes that mTLS termination is handled by an upstream L4/L7 reverse proxy (e.g., Envoy, Nginx). The proxy validates the client certificate and injects the SAN (Subject Alternative Name) into the `X-SPIFFE-ID` header.
- **Network Isolation Assumption**: The `X-SPIFFE-ID` header is blindly trusted by `gateway.py`. For this assumption to be secure, the gateway must be deployed in an isolated internal network where external traffic cannot directly reach it and bypass the reverse proxy. If external clients can send arbitrary headers directly to the gateway, this trust model is broken.
- **Demo Scope**: The implementation of the aforementioned proxy layer and the actual parsing of mTLS certificates are intentionally omitted from this codebase. The focus here is strictly on distributed resilience (e.g., Local Fallback, Circuit Breaker, Chaos Scenarios).
- **Partition Isolation**: 파티션 격리 시뮬레이션 중인 노드는 CB 강제 상태 주입을 포함한 모든 관리 API 호출도 차단되며, 이는 네트워크 단절 상황을 충실히 재현하기 위한 의도된 설계다.

## Security & Secrets Management
This project requires an `ADMIN_SECRET_TOKEN` to protect the Control Plane (`/chaos/*` and `/metrics/stream` endpoints).
- **Demo Defaults**: By default, the codebase and tests fall back to `"super-secret-admin"`. This is strictly for local demonstration and development convenience.
- **Production Warning**: In any actual deployment, you MUST override this with a strong, securely generated token via the `ADMIN_SECRET_TOKEN` environment variable.
- **How to Configure**: Copy `.env.example` to `.env` and replace `your-secure-token-here` with your actual token. `docker-compose` is configured to pick up variables from this `.env` file automatically.
- **Note on Git History**: You may notice `"super-secret-admin"` hardcoded in the early Git commit history. As this is not a real production secret but a placeholder for local demonstrations, this is not considered a security breach. However, actual secrets must never be committed.
