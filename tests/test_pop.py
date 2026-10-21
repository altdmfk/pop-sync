import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
import uuid
import base64
import hashlib
from unittest.mock import patch, AsyncMock

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature

from gateway import app, l1_cache, stats, stats_lock, POP_EXECUTOR_WORKERS, generate_canonical_payload
import gateway

def generate_keypair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).decode()
    return priv, pub

def sign_payload(priv, method, path, spiffe_id, nonce, query="", body=b"", body_digest=None):
    if body_digest is None:
        body_digest = hashlib.sha256(body).hexdigest()
    elif body_digest.startswith("sha256="):
        body_digest = body_digest.split("=", 1)[1]
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

@pytest.fixture
def empty_body_digest():
    return f"sha256={hashlib.sha256(b'').hexdigest()}"

@pytest.mark.asyncio
async def test_pop_valid_signature(mock_common_dependencies, monkeypatch, empty_body_digest):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)
    
    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce)
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
            "X-Body-Digest": empty_body_digest,
        })
        
        assert res.status_code == 200
        assert stats["pop_verified"] == 1
        assert stats["pop_rejected"] == 0
        assert stats["db_fallbacks"] == 1
        assert stats["requests"] == stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] + stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] + stats["redis_outage_blocked"]

@pytest.mark.asyncio
async def test_pop_tampered_payload(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)
    
    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/other", spiffe_id, nonce)
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64
        })
        
        assert res.status_code == 401
        assert "Invalid PoP Signature" in res.text
        assert stats["pop_rejected"] == 1
        assert stats["pop_verified"] == 0
        assert stats["requests"] == stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] + stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] + stats["redis_outage_blocked"]

@pytest.mark.asyncio
async def test_pop_wrong_key(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    wrong_priv, wrong_pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)
    
    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(wrong_priv, "GET", "/auth", spiffe_id, nonce)
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64
        })
        
        assert res.status_code == 401
        assert "Invalid PoP Signature" in res.text
        assert stats["pop_rejected"] == 1

@pytest.mark.asyncio
async def test_pop_missing_header(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": "spiffe://test/valid",
            "X-Nonce": str(uuid.uuid4())
        })
        
        assert res.status_code == 400
        assert "Missing PoP Signature" in res.text
        assert stats["pop_rejected"] == 1
        assert stats["requests"] == stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] + stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] + stats["redis_outage_blocked"]

@pytest.mark.asyncio
async def test_pop_unknown_key(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    
    spiffe_id = "spiffe://test/nokey"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce)
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64
        })
        
        assert res.status_code == 401
        assert "Unknown Key ID" in res.text
        assert stats["pop_rejected"] == 1

@pytest.mark.asyncio
async def test_pop_replayed_nonce(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    m_rl, m_nonce, m_redis, m_db = mock_common_dependencies
    m_nonce.return_value = False  # Simulate replay
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": "spiffe://test/valid",
            "X-Nonce": "replayed",
            "X-PoP-Signature": "fake"
        })
        
        assert res.status_code == 401
        assert "Replay Detected" in res.text
        assert stats["nonce_replays"] == 1
        assert stats["pop_rejected"] == 0
        assert stats["requests"] == stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] + stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] + stats["redis_outage_blocked"]

@pytest.mark.asyncio
async def test_pop_toggle_off(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", False)
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": "spiffe://test/valid",
            "X-Nonce": str(uuid.uuid4())
        })
        
        assert res.status_code == 200
        assert stats["pop_rejected"] == 0
        assert stats["pop_verified"] == 0

@pytest.mark.asyncio
async def test_pop_stream_invalidation(mock_common_dependencies):
    pubkey_key = "pubkey:spiffe://test/valid"
    l1_cache.set(pubkey_key, "fake_pem")
    assert l1_cache.get(pubkey_key) == "fake_pem"
    
    l1_cache.delete(pubkey_key)
    assert l1_cache.get(pubkey_key) is None


@pytest.mark.asyncio
async def test_pop_pubkey_circuit_breaker_and_timeout(mock_common_dependencies, monkeypatch):
    """
    Verify:
    1. Circuit OPEN -> /pubkey lookup on L1/L2 miss fast-fails without calling DB,
       nonce is still burned, and the request is rejected (not allowed).
    2. DB timeout during /pubkey lookup increments cb.failures.
    """
    import httpx
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    m_rl, m_nonce, m_redis, m_db = mock_common_dependencies
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/cb-open"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce)

    gateway.cb.force_state("OPEN")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
        })

        assert res.status_code == 503
        assert m_db.get.await_count == 0, "DB must not be called when circuit is OPEN"
        assert m_nonce.await_count == 1, "Nonce must still be burned before PoP check"
        assert stats["pop_rejected"] == 1
        assert stats["pop_verified"] == 0
        assert stats["requests"] == (
            stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] +
            stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )

    gateway.cb.reset()
    gateway.cb.force_state("CLOSED")
    assert gateway.cb.failures == 0

    m_db.get = AsyncMock(side_effect=httpx.TimeoutException("DB pubkey timeout"))
    spiffe_id_2 = "spiffe://test/cb-timeout"
    nonce_2 = str(uuid.uuid4())
    sig_b64_2 = sign_payload(priv, "GET", "/auth", spiffe_id_2, nonce_2)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res2 = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id_2,
            "X-Nonce": nonce_2,
            "X-PoP-Signature": sig_b64_2,
        })

        assert res2.status_code == 503
        assert gateway.cb.failures == 1
        assert stats["pop_rejected"] == 2


@pytest.mark.asyncio
async def test_pop_pubkey_circuit_breaker_invariant_2(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/cb-pubkey-inv2"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce)

    gateway.cb.force_state("OPEN")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
        })
        assert res.status_code == 503
        assert stats["circuit_blocked"] == 1
        assert stats["pubkey_circuit_blocked"] == 1
        assert stats["pop_rejected"] == 1
        assert stats["db_fallbacks"] == 0

        classified_sum = (
            stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] +
            stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )
        assert stats["requests"] == classified_sum

        policy_circuit_blocked = stats["circuit_blocked"] - stats["pubkey_circuit_blocked"]
        resolved_sum = (
            stats["db_queries_executed"] + stats["singleflight_coalesced"] +
            stats["l1_double_check_hits"] + policy_circuit_blocked
        )
        assert stats["db_fallbacks"] == resolved_sum


@pytest.mark.asyncio
async def test_pop_missing_header_zero_drift_invariant_1(mock_common_dependencies):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        r1 = await client.get("/auth", headers={
            "X-SPIFFE-ID": "spiffe://test/missing-auth",
            "X-Nonce": str(uuid.uuid4()),
        })
        assert r1.status_code == 401
        assert stats["requests"] == 0

        r2 = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": "spiffe://test/missing-nonce",
        })
        assert r2.status_code == 401
        assert stats["requests"] == 0

        r3 = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-Nonce": str(uuid.uuid4()),
        })
        assert r3.status_code == 401
        assert stats["requests"] == 0

        classified_sum = (
            stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] +
            stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )
        assert stats["requests"] == classified_sum


@pytest.mark.asyncio
async def test_pop_pubkey_404_half_open_transitions_to_closed(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/nokey"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce)

    gateway.cb.force_state("HALF_OPEN")
    assert gateway.cb.current_state == "HALF_OPEN"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
        })
        assert res.status_code == 401
        assert "Unknown Key ID" in res.text
        assert gateway.cb.current_state == "CLOSED"
        assert gateway.cb.failures == 0
        assert gateway.cb.is_probe_active is False


@pytest.mark.asyncio
async def test_pop_signature_invalid_length_mod_4_equals_1(mock_common_dependencies, monkeypatch):
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    invalid_sig = "abcde"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": invalid_sig,
        })
        assert res.status_code == 401
        assert "Invalid PoP Signature" in res.text
        assert stats["pop_rejected"] == 1


@pytest.mark.asyncio
async def test_pop_query_tampering_regression(mock_common_dependencies, monkeypatch, empty_body_digest):
    """
    Regression test (Task 4):
    Sign a request for /auth?amount=10, then replay the same signature against
    /auth?amount=10000 (same method, path, spiffe-id, nonce).
    Assert that verification fails (401 Invalid PoP Signature), closing the
    query-string tampering gap.
    """
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    # Sign for /auth?amount=10
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce, query="amount=10")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Replay the same signature against /auth?amount=10000
        res = await client.get("/auth?amount=10000", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
            "X-Body-Digest": empty_body_digest,
        })

        assert res.status_code == 401
        assert "Invalid PoP Signature" in res.text
        assert stats["pop_rejected"] == 1
        assert stats["pop_verified"] == 0
        assert stats["requests"] == (
            stats["rate_limited"] + stats["nonce_replays"] + stats["pop_rejected"] +
            stats["l1_hits"] + stats["l2_hits"] + stats["db_fallbacks"] +
            stats["redis_outage_blocked"]
        )


@pytest.mark.asyncio
async def test_pop_query_param_valid_signature(mock_common_dependencies, monkeypatch, empty_body_digest):
    """Verify that a request with query parameters succeeds when signed correctly."""
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce, query="amount=10")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth?amount=10", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
            "X-Body-Digest": empty_body_digest,
        })

        assert res.status_code == 200
        assert stats["pop_verified"] == 1
        assert stats["pop_rejected"] == 0


@pytest.mark.asyncio
async def test_pop_body_digest_mismatch(mock_common_dependencies, monkeypatch):
    """Verify that a mismatched X-Body-Digest header fails with 400 Body digest mismatch."""
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    tampered_digest = "1" * 64
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce, body_digest=tampered_digest)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
            "X-Body-Digest": f"sha256={tampered_digest}",
        })

        assert res.status_code == 400
        assert "Body digest mismatch" in res.text
        assert stats["pop_rejected"] == 1


@pytest.mark.asyncio
async def test_pop_body_digest_invalid_format(mock_common_dependencies, monkeypatch):
    """Verify that a malformed X-Body-Digest header without sha256= prefix fails with 400."""
    monkeypatch.setattr(gateway, "POP_VERIFY_ENABLED", True)
    priv, pub = generate_keypair()
    monkeypatch.setattr(gateway, "TEST_PUBLIC_KEY", pub, raising=False)

    spiffe_id = "spiffe://test/valid"
    nonce = str(uuid.uuid4())
    sig_b64 = sign_payload(priv, "GET", "/auth", spiffe_id, nonce)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        res = await client.get("/auth", headers={
            "Authorization": "Bearer token1",
            "X-SPIFFE-ID": spiffe_id,
            "X-Nonce": nonce,
            "X-PoP-Signature": sig_b64,
            "X-Body-Digest": "invalid_digest_without_prefix",
        })

        assert res.status_code == 400
        assert "Missing or invalid X-Body-Digest header" in res.text
        assert stats["pop_rejected"] == 1

def test_generate_canonical_payload_explicit_args_validation():
    # 0 args: valid (method, path + kwargs)
    payload0 = generate_canonical_payload('GET', '/auth', query='foo=1')
    assert b'GET\n/auth\nfoo=1\n' in payload0

    # 2 args: valid (method, path, spiffe_id, nonce)
    payload2 = generate_canonical_payload('GET', '/auth', 'spiffe://user', 'nonce-1')
    assert b'GET\n/auth\n\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nspiffe://user\nnonce-1' == payload2

    # 4 args: valid (method, path, query, body_digest, spiffe_id, nonce)
    payload4 = generate_canonical_payload('GET', '/auth', 'q=1', 'sha256=digest', 'spiffe://user', 'nonce-1')
    assert b'GET\n/auth\nq=1\ndigest\nspiffe://user\nnonce-1' == payload4

    # 1 arg: INVALID -> raises TypeError
    with pytest.raises(TypeError) as exc1:
        generate_canonical_payload('GET', '/auth', 'single_extra_arg')
    assert 'generate_canonical_payload() accepts 2, 4, or 6 positional arguments (3 given)' in str(exc1.value)

    # 3 args: INVALID -> raises TypeError
    with pytest.raises(TypeError) as exc3:
        generate_canonical_payload('GET', '/auth', 'arg1', 'arg2', 'arg3')
    assert 'generate_canonical_payload() accepts 2, 4, or 6 positional arguments (5 given)' in str(exc3.value)

    # 5 args: INVALID -> raises TypeError
    with pytest.raises(TypeError) as exc5:
        generate_canonical_payload('GET', '/auth', '1', '2', '3', '4', '5')
    assert 'generate_canonical_payload() accepts 2, 4, or 6 positional arguments (7 given)' in str(exc5.value)

