import asyncio
import httpx
import uuid
import pytest

pytestmark = pytest.mark.e2e


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_cluster_singleflight_e2e_burst(e2e_cluster):
    """
    Integration test asserting that for a burst of concurrent requests across
    Node-A, Node-B, and Node-C for the same composite key, Policy DB is queried exactly once.
    Uses the shared e2e_cluster fixture for environment orchestration.
    """
    node_urls = e2e_cluster["gateways"]
    admin_token = e2e_cluster["admin_token"]

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Fetch baseline metrics from all 3 nodes
        before_metrics = {}
        for name, url in node_urls.items():
            resp = await client.get(f"{url}/metrics", headers={"X-Admin-Token": admin_token})
            assert resp.status_code == 200
            before_metrics[name] = resp.json()["stats"]

        # Generate unique key for this burst
        spiffe_id = f"spiffe://example.org/e2e-cluster-sf-{uuid.uuid4()}"
        token = f"token-{uuid.uuid4()}"
        target_list = list(node_urls.values())

        # Send burst of 30 concurrent requests distributed across Node-A, Node-B, Node-C
        tasks = []
        for i in range(30):
            target_url = target_list[i % len(target_list)]
            headers = {
                "Authorization": f"Bearer {token}",
                "X-SPIFFE-ID": spiffe_id,
                "X-Nonce": f"e2e-nonce-{uuid.uuid4()}"
            }
            tasks.append(client.get(f"{target_url}/auth", headers=headers))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            assert not isinstance(r, Exception), f"Request failed: {r}"
            assert r.status_code == 200, f"Non-200 status: {r.status_code}"
            assert r.json().get("policy") == "admin"

        await asyncio.sleep(0.2)

        # Fetch post-burst metrics
        after_metrics = {}
        for name, url in node_urls.items():
            resp = await client.get(f"{url}/metrics", headers={"X-Admin-Token": admin_token})
            assert resp.status_code == 200
            after_metrics[name] = resp.json()["stats"]

        delta_db = sum(
            after_metrics[name]["db_queries_executed"] - before_metrics[name]["db_queries_executed"]
            for name in node_urls
        )
        delta_coalesced = sum(
            after_metrics[name]["singleflight_coalesced"] - before_metrics[name]["singleflight_coalesced"]
            for name in node_urls
        )

        # PRIMARY ASSERTION: Across all nodes in the cluster, Policy DB was queried EXACTLY ONCE!
        assert delta_db == 1, f"Expected exactly 1 DB query across cluster, got {delta_db}"
        print(f"E2E Cluster Single-Flight Verified: DB Queries={delta_db}, Coalesced={delta_coalesced}")
