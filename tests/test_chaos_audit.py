import asyncio
import base64
import os
import sys
import uuid
import time
import subprocess
from unittest.mock import patch, AsyncMock
import pytest
from httpx import AsyncClient, ASGITransport
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

import gateway
import control_plane

ADMIN_SECRET_TOKEN = os.getenv("ADMIN_SECRET_TOKEN", "super-secret-admin")


from gateway import generate_canonical_payload

def generate_ecdsa_keypair():
    """Generate P-256 keypair for PoP testing."""
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).decode()
    return priv, pub


def sign_pop_payload(priv, method, path, spiffe_id, nonce, query="", body_digest=None):
    """Sign normalized canonical payload with ECDSA P-256."""
    canonical = generate_canonical_payload(
        method,
        path,
        query=query,
        body_digest=body_digest,
        spiffe_id=spiffe_id,
        nonce=nonce,
    )
    sig = priv.sign(canonical, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(sig)
    raw_sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return base64.urlsafe_b64encode(raw_sig).decode().rstrip("=")


# ==============================================================================
# SECTION 1: UNIT / IN-PROCESS REPRESENTATIVE MIX OF ALL 9 CHAOS SCENARIOS
# ==============================================================================

@pytest.mark.asyncio
async def test_chaos_audit_back_to_back_9_scenarios_unit(mock_common_dependencies, monkeypatch):
    """
    Runs a representative mix of all 9 chaos scenarios described in README's
    '전체 시나리오 요약 매트릭스' back-to-back through the gateway pipeline in-process,
    then evaluates GET /chaos/audit to assert overall_invariant_pass == True.

    Exercises:
      Scenario 1: Nonce Replay Defense (401 Replay Detected -> nonce_replays +1)
      Scenario 2: O(1) Sliding-Window Rate Limiting (429 Too Many Requests -> rate_limited +1)
      Scenario 3: Same Key Burst & Single-Flight Coalescing (db_queries_executed=1, singleflight_coalesced=N-1)
      Scenario 4: DB Outage & Circuit Breaker Degradation (OPEN -> 200 degraded_allow -> circuit_blocked +1)
      Scenario 5: Redis Outage & Fail-Closed Degraded Mode (503 Service Unavailable -> redis_outage_blocked +1)
      Scenario 6: Network Partition & Heal (503 Node Isolated -> heal)
      Scenario 7: Real-Time Cache Invalidation Stream (L1 delete -> cache miss DB fallback)
      Scenario 8: ECDSA P-256 PoP Verification (valid 200, missing 400, tampered 401, unknown key 401, pubkey CB 503)
      Scenario 9: Policy DENY Authorization (DB deny -> 'DENY' cached -> L1 hit)

    Validates:
      - Invariant 1: requests == rate_limited + nonce_replays + pop_rejected + l1_hits + l2_hits + db_fallbacks + redis_outage_blocked
      - Invariant 2: db_fallbacks == db_queries_executed + singleflight_coalesced + l1_double_check_hits + (circuit_blocked - pubkey_circuit_blocked)
      - Audit endpoint flags: overall_invariant_pass == True with zero counter drift.
    """
    m_rl, m_nonce, m_redis, m_db = mock_common_dependencies

    # Configure delayed DB call for singleflight concurrency testing
    async def delayed_db(comp_key, sid, res):
        await asyncio.sleep(0.04)
        return {"status": "allow", "role": "admin"}
    m_db.side_effect = delayed_db

    transport = ASGITransport(app=gateway.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:

        # ----------------------------------------------------------------------
        # Scenario 1: Nonce Replay Defense
        # First request consumes nonce successfully; second request with same nonce is rejected.
        # ----------------------------------------------------------------------
        nonce_s1 = f"nonce-s1-{uuid.uuid4()}"
        headers_s1 = {
            "Authorization": "Aegis token-s1",
            "X-SPIFFE-ID": "spiffe://test/s1-replay",
            "X-Nonce": nonce_s1,
            "X-Act": "write"
        }
        m_nonce.return_value = True
        resp_s1_first = await client.get("/auth", headers=headers_s1)
        assert resp_s1_first.status_code == 200

        m_nonce.return_value = False
        resp_s1_second = await client.get("/auth", headers=headers_s1)
        assert resp_s1_second.status_code == 401
        assert "Replay Detected" in resp_s1_second.json().get("message", "")
        assert gateway.stats["nonce_replays"] >= 1
        m_nonce.return_value = True

        # ----------------------------------------------------------------------
        # Scenario 2: DDoS & Boundary Burst Rate Limiting
        # When rate limit threshold is exceeded, requests are rejected with 429.
        # ----------------------------------------------------------------------
        m_rl.return_value = False
        headers_s2 = {
            "Authorization": "Aegis token-s2",
            "X-SPIFFE-ID": "spiffe://example.org/attacker",
            "X-Nonce": f"nonce-s2-{uuid.uuid4()}"
        }
        resp_s2 = await client.get("/auth", headers=headers_s2)
        assert resp_s2.status_code == 429
        assert resp_s2.headers.get("Retry-After") == str(gateway.RL_WINDOW_SEC)
        assert gateway.stats["rate_limited"] >= 1
        m_rl.return_value = True

        # ----------------------------------------------------------------------
        # Scenario 3: Same Key Burst & Single-Flight Coalescing
        # Burst of concurrent requests for identical key coalesces into 1 DB query.
        # ----------------------------------------------------------------------
        spiffe_s3 = f"spiffe://example.org/burst_{uuid.uuid4()}"
        db_before = gateway.stats["db_queries_executed"]
        coalesced_before = gateway.stats["singleflight_coalesced"]

        tasks_s3 = [
            client.get("/auth", headers={
                "Authorization": "Bearer token-s3",
                "X-SPIFFE-ID": spiffe_s3,
                "X-Nonce": f"nonce-s3-{i}-{uuid.uuid4()}"
            })
            for i in range(10)
        ]
        results_s3 = await asyncio.gather(*tasks_s3)
        for r in results_s3:
            assert r.status_code == 200

        delta_db = gateway.stats["db_queries_executed"] - db_before
        delta_coal = gateway.stats["singleflight_coalesced"] - coalesced_before
        assert delta_db == 1
        assert delta_coal == 9

        # ----------------------------------------------------------------------
        # Scenario 4: Policy DB Outage & Circuit Breaker Degradation
        # Tripping circuit breaker returns 200 degraded_allow and increments circuit_blocked.
        # ----------------------------------------------------------------------
        gateway.cb.force_state("OPEN")
        headers_s4 = {
            "Authorization": "Bearer token-s4",
            "X-SPIFFE-ID": f"spiffe://example.org/cb_{uuid.uuid4()}",
            "X-Nonce": f"nonce-s4-{uuid.uuid4()}"
        }
        resp_s4 = await client.get("/auth", headers=headers_s4)
        assert resp_s4.status_code == 200
        assert resp_s4.json().get("status") == "degraded_allow"
        assert gateway.stats["circuit_blocked"] >= 1
        gateway.cb.force_state("CLOSED")

        # ----------------------------------------------------------------------
        # Scenario 5: Central Redis Outage & Local Survival Mode
        # When Redis fails during Nonce consumption, request is rejected with 503 Fail-Closed.
        # ----------------------------------------------------------------------
        m_nonce.side_effect = Exception("Redis Connection Lost (Simulated Outage)")
        headers_s5 = {
            "Authorization": "Bearer token-s5",
            "X-SPIFFE-ID": f"spiffe://example.org/redis-down-{uuid.uuid4()}",
            "X-Nonce": f"nonce-s5-{uuid.uuid4()}"
        }
        resp_s5 = await client.get("/auth", headers=headers_s5)
        assert resp_s5.status_code == 503
        assert gateway.stats["redis_outage_blocked"] >= 1
        m_nonce.side_effect = None
        m_nonce.return_value = True

        # ----------------------------------------------------------------------
        # Scenario 6: Network Partition & Heal
        # When node is partitioned, /auth requests fail-fast with 503 Node Isolated.
        # ----------------------------------------------------------------------
        resp_iso = await client.post("/chaos/partition/isolate", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        assert resp_iso.status_code == 200
        resp_part = await client.get("/auth", headers={
            "Authorization": "Bearer token-s6",
            "X-SPIFFE-ID": "spiffe://test/part",
            "X-Nonce": f"nonce-part-{uuid.uuid4()}"
        })
        assert resp_part.status_code == 503
        assert "Node Isolated" in resp_part.json().get("message", "")
        resp_heal = await client.post("/chaos/partition/heal", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        assert resp_heal.status_code == 200

        # ----------------------------------------------------------------------
        # Scenario 7: Real-Time Cache Invalidation Stream & L2 Cache Hit
        # Populating L1/L2, invalidating via /chaos/invalidate/{key}, and refetching.
        # ----------------------------------------------------------------------
        comp_key_s7 = gateway.generate_composite_key("spiffe://test/inv", "token-s7", "resource", "read")
        gateway.l1_cache.set(comp_key_s7, "admin")
        # Cache hit
        resp_hit = await client.get("/auth", headers={
            "Authorization": "Bearer token-s7",
            "X-SPIFFE-ID": "spiffe://test/inv",
            "X-Nonce": f"nonce-hit-{uuid.uuid4()}"
        })
        assert resp_hit.status_code == 200
        assert resp_hit.json().get("source") == "L1"
        assert gateway.stats["l1_hits"] >= 1

        # L2 Cache hit verification
        comp_key_l2 = gateway.generate_composite_key("spiffe://test/l2-hit", "token-l2", "resource", "read")
        async def mock_l2_get(k):
            if k == f"policy:{comp_key_l2}":
                return b"admin"
            return None
        m_redis.get = AsyncMock(side_effect=mock_l2_get)
        resp_l2 = await client.get("/auth", headers={
            "Authorization": "Bearer token-l2",
            "X-SPIFFE-ID": "spiffe://test/l2-hit",
            "X-Nonce": f"nonce-l2-{uuid.uuid4()}"
        })
        assert resp_l2.status_code == 200
        assert resp_l2.json().get("source") == "L2"
        assert gateway.stats["l2_hits"] >= 1
        m_redis.get = AsyncMock(return_value=None)

        # L1 Double-Check Hit verification
        with patch.object(gateway.l1_cache, "get", side_effect=[None, "admin"]):
            resp_dc = await client.get("/auth", headers={
                "Authorization": "Bearer token-dc",
                "X-SPIFFE-ID": f"spiffe://test/l1-dc-{uuid.uuid4()}",
                "X-Nonce": f"nonce-dc-{uuid.uuid4()}"
            })
            assert resp_dc.status_code == 200
            assert resp_dc.json().get("source") == "L1"
            assert gateway.stats["l1_double_check_hits"] >= 1

        # Invalidate
        resp_inv = await client.post(f"/chaos/invalidate/{comp_key_s7}", headers={"X-Admin-Token": ADMIN_SECRET_TOKEN})
        assert resp_inv.status_code == 200
        # Post-invalidation cache miss
        gateway.l1_cache.delete(comp_key_s7)
        resp_miss = await client.get("/auth", headers={
            "Authorization": "Bearer token-s7",
            "X-SPIFFE-ID": "spiffe://test/inv",
            "X-Nonce": f"nonce-miss-{uuid.uuid4()}"
        })
        assert resp_miss.status_code == 200

        # ----------------------------------------------------------------------
        # Scenario 8: ECDSA P-256 PoP Verification
        # Valid signature, missing signature, tampered signature, unknown key, and pubkey CB open.
        # ----------------------------------------------------------------------
        monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
        priv, pub = generate_ecdsa_keypair()
        monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

        # 8-1: Valid PoP Signature
        spiffe_pop = "spiffe://test/pop-valid"
        nonce_pop_valid = f"nonce-pop-{uuid.uuid4()}"
        sig_valid = sign_pop_payload(priv, "GET", "/auth", spiffe_pop, nonce_pop_valid)
        resp_pop_ok = await client.get("/auth", headers={
            "Authorization": "Bearer token-pop",
            "X-SPIFFE-ID": spiffe_pop,
            "X-Nonce": nonce_pop_valid,
            "X-PoP-Signature": sig_valid
        })
        assert resp_pop_ok.status_code == 200
        assert gateway.stats["pop_verified"] >= 1

        # 8-2: Missing PoP Signature (400)
        resp_pop_missing = await client.get("/auth", headers={
            "Authorization": "Bearer token-pop",
            "X-SPIFFE-ID": spiffe_pop,
            "X-Nonce": f"nonce-pop-{uuid.uuid4()}"
        })
        assert resp_pop_missing.status_code == 400
        assert gateway.stats["pop_rejected"] >= 1

        # 8-3: Tampered PoP Signature (401)
        nonce_pop_tamp = f"nonce-pop-{uuid.uuid4()}"
        sig_tampered = sign_pop_payload(priv, "GET", "/tampered_path", spiffe_pop, nonce_pop_tamp)
        resp_pop_tamp = await client.get("/auth", headers={
            "Authorization": "Bearer token-pop",
            "X-SPIFFE-ID": spiffe_pop,
            "X-Nonce": nonce_pop_tamp,
            "X-PoP-Signature": sig_tampered
        })
        assert resp_pop_tamp.status_code == 401

        # 8-4: Unknown Key ID (401)
        nonce_pop_nokey = f"nonce-pop-{uuid.uuid4()}"
        sig_nokey = sign_pop_payload(priv, "GET", "/auth", "spiffe://test/nokey", nonce_pop_nokey)
        resp_pop_nokey = await client.get("/auth", headers={
            "Authorization": "Bearer token-pop",
            "X-SPIFFE-ID": "spiffe://test/nokey",
            "X-Nonce": nonce_pop_nokey,
            "X-PoP-Signature": sig_nokey
        })
        assert resp_pop_nokey.status_code == 401

        # 8-5: Pubkey Circuit Breaker Open (503 Key Fetch Failed)
        gateway.cb.force_state("OPEN")
        gateway.l1_cache.delete("pubkey:spiffe://test/cb-pubkey")
        nonce_pop_cb = f"nonce-pop-{uuid.uuid4()}"
        sig_cb = sign_pop_payload(priv, "GET", "/auth", "spiffe://test/cb-pubkey", nonce_pop_cb)
        resp_pop_cb = await client.get("/auth", headers={
            "Authorization": "Bearer token-pop",
            "X-SPIFFE-ID": "spiffe://test/cb-pubkey",
            "X-Nonce": nonce_pop_cb,
            "X-PoP-Signature": sig_cb
        })
        assert resp_pop_cb.status_code == 503
        assert gateway.stats["pubkey_circuit_blocked"] >= 1
        gateway.cb.force_state("CLOSED")
        monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", False)

        # ----------------------------------------------------------------------
        # Scenario 9: Policy DENY Authorization
        # Policy DB returns status: "deny" -> gateway caches "DENY" -> second hit from L1.
        # ----------------------------------------------------------------------
        m_db.side_effect = None
        m_db.return_value = {"status": "deny", "role": "none"}
        spiffe_deny = f"spiffe://test/workload/deny_{uuid.uuid4()}"
        resp_deny1 = await client.get("/auth", headers={
            "Authorization": "Bearer token-deny",
            "X-SPIFFE-ID": spiffe_deny,
            "X-Nonce": f"nonce-deny-{uuid.uuid4()}"
        })
        assert resp_deny1.status_code == 200
        assert resp_deny1.json().get("status") == "deny"
        assert resp_deny1.json().get("policy") == "DENY"

        # Subsequent request served from L1 cache as "DENY"
        resp_deny2 = await client.get("/auth", headers={
            "Authorization": "Bearer token-deny",
            "X-SPIFFE-ID": spiffe_deny,
            "X-Nonce": f"nonce-deny-{uuid.uuid4()}"
        })
        assert resp_deny2.status_code == 200
        assert resp_deny2.json().get("status") == "deny"
        assert resp_deny2.json().get("source") == "L1"

    # ==========================================================================
    # AUDIT INVARIANT ASSERTIONS ACROSS CUMULATIVE ACCOUNTING
    # ==========================================================================
    snapshot_stats = dict(gateway.stats)

    # Verify that representative mix touched ALL core invariant counters
    assert snapshot_stats["rate_limited"] >= 1
    assert snapshot_stats["nonce_replays"] >= 1
    assert snapshot_stats["pop_rejected"] >= 1
    assert snapshot_stats["l1_hits"] >= 1
    assert snapshot_stats["l2_hits"] >= 1
    assert snapshot_stats["db_fallbacks"] >= 1
    assert snapshot_stats["redis_outage_blocked"] >= 1
    assert snapshot_stats["db_queries_executed"] >= 1
    assert snapshot_stats["singleflight_coalesced"] >= 1
    assert snapshot_stats["l1_double_check_hits"] >= 1
    assert snapshot_stats["circuit_blocked"] >= 1
    assert snapshot_stats["pubkey_circuit_blocked"] >= 1

    # Simulate multi-node cluster metrics reported to control_plane
    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "circuit_state": gateway.cb.current_state,
            "stats": snapshot_stats
        },
        "Node-B": {
            "node": "Node-B",
            "status": "online",
            "circuit_state": "CLOSED",
            "stats": {
                "requests": 10,
                "rate_limited": 2,
                "nonce_replays": 1,
                "pop_rejected": 0,
                "l1_hits": 3,
                "l2_hits": 0,
                "db_fallbacks": 4,
                "redis_outage_blocked": 0,
                "db_queries_executed": 1,
                "singleflight_coalesced": 3,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0,
            }
        },
        "Node-C": {
            "node": "Node-C",
            "status": "online",
            "circuit_state": "CLOSED",
            "stats": {
                "requests": 0, "rate_limited": 0, "nonce_replays": 0, "pop_rejected": 0,
                "l1_hits": 0, "l2_hits": 0, "db_fallbacks": 0, "redis_outage_blocked": 0,
                "db_queries_executed": 0, "singleflight_coalesced": 0, "l1_double_check_hits": 0,
                "circuit_blocked": 0, "pubkey_circuit_blocked": 0
            }
        }
    }

    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics
        cp_transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=cp_transport, base_url="http://controlplane") as cp_client:
            audit_resp = await cp_client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert audit_resp.status_code == 200
            audit_data = audit_resp.json()

            # PRIMARY ASSERTION
            assert audit_data["overall_invariant_pass"] is True
            assert audit_data["online_nodes_count"] == 3

            node_a_audit = audit_data["nodes"]["Node-A"]
            assert node_a_audit["invariant_pass"] is True
            assert node_a_audit["invariant_1"]["pass"] is True
            assert node_a_audit["invariant_2"]["pass"] is True
            assert node_a_audit["invariant_1"]["requests"] == node_a_audit["invariant_1"]["classified_sum"]
            assert node_a_audit["invariant_2"]["db_fallbacks"] == node_a_audit["invariant_2"]["resolved_sum"]

            node_b_audit = audit_data["nodes"]["Node-B"]
            assert node_b_audit["invariant_pass"] is True
            assert node_b_audit["invariant_1"]["pass"] is True
            assert node_b_audit["invariant_2"]["pass"] is True


# ==============================================================================
# SECTION 2: FLOAT AND ROUNDING EDGE CASES IN AUDIT COMPUTATION & SLIDING WINDOW
# ==============================================================================

@pytest.mark.asyncio
async def test_audit_edge_case_zero_denominators():
    """
    Edge Case 1: Zero Denominators on Cold or Idle Nodes.

    Explanation:
      Immediately after startup, or on idle nodes in a cold cluster:
        - requests == 0 -> cache_hit_ratio could trigger ZeroDivisionError without guard.
        - db_fallbacks == 0 -> db_load_reduction_ratio and singleflight_coalescing_ratio
          could trigger ZeroDivisionError without guard.
      The audit implementation explicitly guards:
        ratio = round(...) if denominator > 0 else 0.0
      Both Invariants evaluate 0 == 0 -> True.
    """
    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "stats": {
                "requests": 0, "rate_limited": 0, "nonce_replays": 0, "pop_rejected": 0,
                "l1_hits": 0, "l2_hits": 0, "db_fallbacks": 0, "redis_outage_blocked": 0,
                "db_queries_executed": 0, "singleflight_coalesced": 0,
                "l1_double_check_hits": 0, "circuit_blocked": 0, "pubkey_circuit_blocked": 0
            }
        }
    }
    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics
        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            assert data["overall_invariant_pass"] is True
            node = data["nodes"]["Node-A"]
            assert node["cache_hit_ratio"] == 0.0
            assert node["cache_hit_ratio_pct"] == 0.0
            assert node["db_load_reduction_ratio"] == 0.0
            assert node["db_load_reduction_percentage"] == 0.0
            assert node["singleflight_coalescing_ratio"] == 0.0
            assert node["singleflight_coalescing_ratio_pct"] == 0.0
            assert node["invariant_1"]["pass"] is True
            assert node["invariant_2"]["pass"] is True


@pytest.mark.asyncio
async def test_audit_edge_case_repeating_fractions_rounding():
    """
    Edge Case 2: IEEE 754 Non-Terminating Binary Fractions (1/3, 5/7).

    Explanation:
      Fractions such as 1/3 (0.3333333333333333...) and 5/7 (0.7142857142857143...)
      cannot be represented exactly in binary floating point.
      The audit endpoint rounds:
        - Ratio: round(value, 4) -> 0.3333, 0.7143
        - Percentage: round(value * 100.0, 2) -> 33.33, 71.43
      This test ensures that repeating decimals are cleanly rounded without precision drift.
    """
    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "stats": {
                # 1 hit out of 3 requests -> 1/3
                "requests": 3,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 1,
                "l2_hits": 0,
                "db_fallbacks": 2,
                "redis_outage_blocked": 0,
                "db_queries_executed": 2,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0
            }
        },
        "Node-B": {
            "node": "Node-B",
            "status": "online",
            "stats": {
                # 5 coalesced out of 7 db_fallbacks -> 5/7
                "requests": 7,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 0,
                "l2_hits": 0,
                "db_fallbacks": 7,
                "redis_outage_blocked": 0,
                "db_queries_executed": 2,
                "singleflight_coalesced": 5,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0
            }
        }
    }
    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics
        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            assert data["overall_invariant_pass"] is True
            node_a = data["nodes"]["Node-A"]
            assert node_a["cache_hit_ratio"] == 0.3333
            assert node_a["cache_hit_ratio_pct"] == 33.33

            node_b = data["nodes"]["Node-B"]
            assert node_b["singleflight_coalescing_ratio"] == 0.7143
            assert node_b["singleflight_coalescing_ratio_pct"] == 71.43
            assert node_b["db_load_reduction_ratio"] == 0.7143
            assert node_b["db_load_reduction_percentage"] == 71.43


@pytest.mark.asyncio
async def test_audit_edge_case_bankers_rounding_halfway():
    """
    Edge Case 3: Python Built-in Banker's Rounding (Round-Half-to-Even).

    Explanation:
      Python's round() rounds halfway values to the nearest EVEN integer:
        - 1 / 800 * 100.0 = 0.125 -> round(0.125, 2) == 0.12 (since 2 is even).
        - 3 / 800 * 100.0 = 0.375 -> round(0.375, 2) == 0.38 (since 8 is even, rounding up from 7).
      This test surfaces the round-half-to-even behavior so clients relying on
      standard round-half-up (which would expect 0.13) understand the exact output contract.
    """
    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "stats": {
                # 1 hit out of 800 requests -> percentage = 0.125%
                "requests": 800,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 1,
                "l2_hits": 0,
                "db_fallbacks": 799,
                "redis_outage_blocked": 0,
                "db_queries_executed": 799,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0
            }
        },
        "Node-B": {
            "node": "Node-B",
            "status": "online",
            "stats": {
                # 3 hits out of 800 requests -> percentage = 0.375%
                "requests": 800,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 3,
                "l2_hits": 0,
                "db_fallbacks": 797,
                "redis_outage_blocked": 0,
                "db_queries_executed": 797,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0
            }
        }
    }
    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics
        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            node_a = data["nodes"]["Node-A"]
            # 0.125 rounds to 0.12 (banker's round-half-to-even)
            assert node_a["cache_hit_ratio_pct"] == 0.12

            node_b = data["nodes"]["Node-B"]
            # 0.375 rounds to 0.38 (banker's round-half-to-even)
            assert node_b["cache_hit_ratio_pct"] == 0.38


@pytest.mark.asyncio
async def test_audit_edge_case_100_percent_saturation_boundary():
    """
    Edge Case 4: 100.0% Saturation Upper Boundary.

    Explanation:
      When all requests hit cache (l1_hits == requests) or all DB fallbacks are coalesced
      (singleflight_coalesced == db_fallbacks), the ratios must be exactly 1.0 and
      the percentages must be exactly 100.0 without floating-point overflow exceeding 100.
    """
    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "stats": {
                "requests": 500,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 500,
                "l2_hits": 0,
                "db_fallbacks": 0,
                "redis_outage_blocked": 0,
                "db_queries_executed": 0,
                "singleflight_coalesced": 0,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0
            }
        },
        "Node-B": {
            "node": "Node-B",
            "status": "online",
            "stats": {
                "requests": 100,
                "rate_limited": 0,
                "nonce_replays": 0,
                "pop_rejected": 0,
                "l1_hits": 0,
                "l2_hits": 0,
                "db_fallbacks": 100,
                "redis_outage_blocked": 0,
                "db_queries_executed": 0,
                "singleflight_coalesced": 100,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0
            }
        }
    }
    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics
        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            node_a = data["nodes"]["Node-A"]
            assert node_a["cache_hit_ratio"] == 1.0
            assert node_a["cache_hit_ratio_pct"] == 100.0

            node_b = data["nodes"]["Node-B"]
            assert node_b["singleflight_coalescing_ratio"] == 1.0
            assert node_b["singleflight_coalescing_ratio_pct"] == 100.0
            assert node_b["db_load_reduction_ratio"] == 1.0
            assert node_b["db_load_reduction_percentage"] == 100.0


@pytest.mark.asyncio
async def test_audit_edge_case_float_counter_immunity():
    """
    Edge Case 5: Immunity of Invariant Accounting to Floating-Point Latency Fields.

    Explanation:
      Gateway's stats dictionary includes floating-point accumulators:
        stats["latency_sum"] (float, e.g. 0.00234...)
        stats["total_latency_ms"] (float)
      Due to binary floating-point representation (e.g. 0.1 + 0.2 = 0.30000000000000004),
      floating-point sums accumulate rounding errors over time.
      However, Invariant 1 and Invariant 2 are constructed purely from integer discrete
      accounting counters (requests, rate_limited, db_fallbacks, singleflight_coalesced, etc.).
      This test verifies that presence of large, irregular float values in latency fields
      does not interfere with the integer equality comparison of Invariants.
    """
    mock_metrics = {
        "Node-A": {
            "node": "Node-A",
            "status": "online",
            "stats": {
                "requests": 10,
                "rate_limited": 1,
                "nonce_replays": 1,
                "pop_rejected": 1,
                "l1_hits": 2,
                "l2_hits": 1,
                "db_fallbacks": 4,
                "redis_outage_blocked": 0,
                "db_queries_executed": 1,
                "singleflight_coalesced": 3,
                "l1_double_check_hits": 0,
                "circuit_blocked": 0,
                "pubkey_circuit_blocked": 0,
                # Floating-point jitter injected into latency counters
                "latency_sum": 0.30000000000000004,
                "total_latency_ms": 300.00000000000006
            }
        }
    }
    with patch("control_plane.fetch_metrics", new_callable=AsyncMock) as m_fetch:
        m_fetch.return_value = mock_metrics
        transport = ASGITransport(app=control_plane.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/chaos/audit", headers={"X-Admin-Token": control_plane.ADMIN_SECRET_TOKEN})
            assert resp.status_code == 200
            data = resp.json()

            assert data["overall_invariant_pass"] is True
            node = data["nodes"]["Node-A"]
            assert node["invariant_1"]["pass"] is True
            assert node["invariant_2"]["pass"] is True
            # Invariants remain exact integer equalities
            assert node["invariant_1"]["requests"] == 10
            assert node["invariant_1"]["classified_sum"] == 10


def test_sliding_window_rate_limiter_lua_weight_boundary_math():
    """
    Edge Case 6: Sliding Window Rate Limiter Weight Boundaries in Permille & Normalization.

    Explanation:
      In gateway.check_rate_limit:
        elapsed = now % RL_WINDOW_SEC
        weight  = int(round((1 - elapsed / RL_WINDOW_SEC) * 1000))
      And inside Lua (SLIDING_RL_LUA):
        local weight = tonumber(ARGV[2]) or 0
        if weight >= 1 then weight = weight / 1000.0 end
        local estimated = math.floor(prev * weight) + curr

      This test checks the exact boundary math:
        1. elapsed = 0.0s  -> weight = 1000 permille -> 1.0 in Lua -> prev * 1.0 = prev (100% prev weight)
        2. elapsed = 5.0s  -> weight = 500 permille  -> 0.5 in Lua -> prev * 0.5 (50% prev weight)
        3. elapsed = 9.99s -> weight = 1 permille    -> 0.001 in Lua -> math.floor(50 * 0.001) = 0
        4. elapsed = 9.999s-> weight = 0 permille    -> 0.0 in Lua -> prev * 0.0 = 0 (0% prev weight)

      Because math.floor() is applied, the estimated count is strictly integer-bounded,
      preventing fractional rate-limiting decisions from corrupting stats["rate_limited"].
    """
    import math
    RL_WINDOW_SEC = 10

    # 1. Start of window (elapsed = 0.0)
    elapsed_0 = 0.0
    weight_permille_0 = int(round((1 - elapsed_0 / RL_WINDOW_SEC) * 1000))
    assert weight_permille_0 == 1000
    norm_0 = weight_permille_0 / 1000.0
    assert norm_0 == 1.0
    assert math.floor(50 * norm_0) == 50

    # 2. Mid window (elapsed = 5.0)
    elapsed_mid = 5.0
    weight_permille_mid = int(round((1 - elapsed_mid / RL_WINDOW_SEC) * 1000))
    assert weight_permille_mid == 500
    norm_mid = weight_permille_mid / 1000.0
    assert norm_mid == 0.5
    assert math.floor(50 * norm_mid) == 25

    # 3. 99.9% elapsed (elapsed = 9.99)
    elapsed_999 = 9.99
    weight_permille_999 = int(round((1 - elapsed_999 / RL_WINDOW_SEC) * 1000))
    assert weight_permille_999 == 1
    norm_999 = weight_permille_999 / 1000.0
    assert norm_999 == 0.001
    assert math.floor(50 * norm_999) == 0

    # 4. Near boundary (elapsed = 9.999)
    elapsed_end = 9.999
    weight_permille_end = int(round((1 - elapsed_end / RL_WINDOW_SEC) * 1000))
    assert weight_permille_end == 0


def test_local_sliding_window_fallback_truncation_edge_case():
    """
    Edge Case 7: In-Memory Sliding Window Fallback Integer Truncation (int() vs math.floor()).

    Explanation:
      In gateway._local_rate_limit:
        elapsed   = now % RL_WINDOW_SEC
        weight    = 1.0 - (elapsed / RL_WINDOW_SEC)
        estimated = int(prev_count * weight) + curr_count
      In Python, int(x) truncates floats towards zero. For positive numbers,
      int(x) is mathematically equivalent to math.floor(x).
      This test validates that floating-point values near 1.0 or near 0.0 truncate
      cleanly to integers without generating non-integral results or unexpected boundaries.
    """
    RL_WINDOW_SEC = 10
    prev_count = 15
    curr_count = 0

    # Exact start
    weight_start = 1.0 - (0.0 / RL_WINDOW_SEC)
    est_start = int(prev_count * weight_start) + curr_count
    assert est_start == 15

    # Halfway
    weight_half = 1.0 - (5.0 / RL_WINDOW_SEC)
    est_half = int(prev_count * weight_half) + curr_count
    assert est_half == 7  # int(7.5) == 7

    # 99.9% elapsed
    weight_near_end = 1.0 - (9.99 / RL_WINDOW_SEC)
    est_near_end = int(prev_count * weight_near_end) + curr_count
    assert est_near_end == 0  # int(15 * 0.001) == 0


# ==============================================================================
# SECTION 3: END-TO-END LIVE CLUSTER TEST
# ==============================================================================

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_chaos_audit_e2e_cluster_back_to_back_9_scenarios(e2e_cluster):
    """
    E2E integration test:
      1. Reuses or starts the cluster components via e2e_cluster fixture
         (Policy DB, Gateways A/B/C, Control Plane).
      2. Runs a representative mix of the 9 chaos scenarios back-to-back over HTTP.
      3. Calls GET /chaos/audit and asserts overall_invariant_pass == True.
    """
    cp_url = e2e_cluster["control_plane"]
    node_urls = e2e_cluster["gateways"]
    admin_headers = {"X-Admin-Token": e2e_cluster["admin_token"]}

    async with AsyncClient(timeout=30.0) as client:

            # ------------------------------------------------------------------
            # Scenario 1: Nonce Replay
            # ------------------------------------------------------------------
            resp_s1 = await client.post(f"{cp_url}/chaos/nonce", headers=admin_headers)
            assert resp_s1.status_code == 200

            # ------------------------------------------------------------------
            # Scenario 2: DDoS Burst
            # ------------------------------------------------------------------
            resp_s2 = await client.post(f"{cp_url}/chaos/ddos", headers=admin_headers)
            assert resp_s2.status_code == 200

            # ------------------------------------------------------------------
            # Scenario 3: Same Key Burst (Single-Flight)
            # ------------------------------------------------------------------
            resp_s3 = await client.post(f"{cp_url}/chaos/same_key_burst?node=Node-A&count=20", headers=admin_headers)
            assert resp_s3.status_code == 200

            # ------------------------------------------------------------------
            # Scenario 4: Policy DB Outage & Recovery
            # ------------------------------------------------------------------
            await client.post(f"{cp_url}/chaos/db/down", headers=admin_headers)
            # Send cache-miss request to Node-A
            await client.get(f"{node_urls['Node-A']}/auth", headers={
                "Authorization": "Bearer test-token",
                "X-SPIFFE-ID": f"spiffe://example.org/cb-e2e-{uuid.uuid4()}",
                "X-Nonce": f"nonce-cb-{uuid.uuid4()}"
            })
            await client.post(f"{cp_url}/chaos/db/up", headers=admin_headers)

            # ------------------------------------------------------------------
            # Scenario 5: Central Redis Outage & Recovery
            # ------------------------------------------------------------------
            await client.post(f"{cp_url}/chaos/redis/down", headers=admin_headers)
            await client.get(f"{node_urls['Node-A']}/auth", headers={
                "Authorization": "Bearer test-token",
                "X-SPIFFE-ID": f"spiffe://example.org/redis-e2e-{uuid.uuid4()}",
                "X-Nonce": f"nonce-redis-{uuid.uuid4()}"
            })
            await client.post(f"{cp_url}/chaos/redis/up", headers=admin_headers)

            # ------------------------------------------------------------------
            # Scenario 6: Partition Node-B & Heal
            # ------------------------------------------------------------------
            await client.post(f"{cp_url}/chaos/partition/Node-B/isolate", headers=admin_headers)
            resp_iso = await client.get(f"{node_urls['Node-B']}/auth", headers={
                "Authorization": "Bearer test-token",
                "X-SPIFFE-ID": f"spiffe://example.org/part-{uuid.uuid4()}",
                "X-Nonce": f"nonce-part-{uuid.uuid4()}"
            })
            assert resp_iso.status_code == 503
            await client.post(f"{cp_url}/chaos/partition/Node-B/heal", headers=admin_headers)

            # ------------------------------------------------------------------
            # Scenario 7: Real-Time Cache Invalidation
            # ------------------------------------------------------------------
            resp_inv = await client.post(f"{cp_url}/chaos/invalidate", headers=admin_headers)
            assert resp_inv.status_code == 200

            # ------------------------------------------------------------------
            # Scenario 8: PoP Requests
            # ------------------------------------------------------------------
            # Missing PoP signature when header not supplied
            resp_pop_miss = await client.get(f"{node_urls['Node-A']}/auth", headers={
                "Authorization": "Bearer test-token",
                "X-SPIFFE-ID": "spiffe://test/workload",
                "X-Nonce": f"nonce-pop-{uuid.uuid4()}"
            })
            assert resp_pop_miss.status_code in [200, 400]

            # ------------------------------------------------------------------
            # Scenario 9: Policy DENY
            # ------------------------------------------------------------------
            resp_deny = await client.get(f"{node_urls['Node-A']}/auth", headers={
                "Authorization": "Bearer test-token",
                "X-SPIFFE-ID": f"spiffe://example.org/deny-{uuid.uuid4()}",
                "X-Nonce": f"nonce-deny-{uuid.uuid4()}",
                "X-Res": "restricted"
            })
            assert resp_deny.status_code == 200
            assert resp_deny.json().get("status") == "deny"

            await asyncio.sleep(0.5)

            # ------------------------------------------------------------------
            # FINAL AUDIT ASSERTION
            # ------------------------------------------------------------------
            audit_resp = await client.get(f"{cp_url}/chaos/audit", headers=admin_headers)
            assert audit_resp.status_code == 200
            audit_data = audit_resp.json()

            assert audit_data["overall_invariant_pass"] is True
            assert audit_data["online_nodes_count"] >= 1
            for name, n_data in audit_data["nodes"].items():
                if n_data.get("status") == "online":
                    assert n_data["invariant_pass"] is True
                    assert n_data["invariant_1"]["pass"] is True
                    assert n_data["invariant_2"]["pass"] is True
