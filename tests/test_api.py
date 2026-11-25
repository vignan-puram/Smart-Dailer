"""HTTP API end-to-end through FastAPI's TestClient (in-memory database)."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("SMARTDIALER_DB_PATH", ":memory:")
    from app.main import app
    with TestClient(app) as c:
        yield c


def _setup(client, agents=5, borrowers=50, mode="PROGRESSIVE"):
    assert client.post("/agents", json={"count": agents}).status_code == 201
    assert client.post("/campaigns", json={"id": "c1", "name": "Demo", "mode": mode}).status_code == 201
    assert client.post("/campaigns/c1/borrowers", json={"count": borrowers}).status_code == 201
    assert client.post("/campaigns/c1/start").json()["status"] == "RUNNING"


def test_health(client):
    body = client.get("/health").json()
    assert body["status"] == "ok" and body["safety_mode"] == "STRICT"


def test_root_redirects_to_swagger_docs(client):
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"] == "/docs"
    assert client.get("/").status_code == 200          # followed: Swagger UI page


def test_progressive_flow_end_to_end(client):
    _setup(client)
    cycle = client.post("/dialer/cycle").json()
    assert cycle[0]["decision"]["decision"] == "APPROVED"
    assert cycle[0]["decision"]["approved"] == 5
    assert cycle[0]["recommendation"]["explanation"]
    calls = client.get("/calls").json()["calls"]
    assert len(calls) == 5 and all(c["binding"] == "BOUND" for c in calls)
    detail = client.get(f"/calls/{calls[0]['id']}").json()
    assert detail["id"] == calls[0]["id"]
    client.post("/time/advance", json={"seconds": 600})
    m = client.get("/metrics").json()
    assert m["connected_total"] > 0 and m["abandoned_total"] == 0
    decisions = client.get("/safety/decisions").json()
    assert sum(decisions["summary"].values()) >= 1
    assert "capacity" in decisions["decisions"][0]


def test_predictive_mode_switch_and_safety_is_reported(client):
    _setup(client, agents=10, borrowers=500)
    assert client.put("/campaigns/c1/mode", json={"mode": "PREDICTIVE"}).json()["mode"] == "PREDICTIVE"
    cycle = client.post("/dialer/cycle").json()[0]
    assert cycle["recommendation"]["engine"] == "PREDICTIVE"
    assert cycle["decision"]["approved"] <= cycle["decision"]["requested"]


def test_agent_status_changes_and_validation(client):
    _setup(client, agents=2)
    assert client.post("/agents/agent-00000/status", json={"status": "PAUSED"}).status_code == 200
    assert client.get("/agents", params={"status": "PAUSED"}).json()["agents"][0]["id"] == "agent-00000"
    assert client.post("/agents/agent-00000/status", json={"status": "CONNECTED"}).status_code == 422
    assert client.post("/agents/nope/status", json={"status": "PAUSED"}).status_code == 404
    client.post("/dialer/cycle")
    lost = client.post("/agents/agent-00001/disappear").json()
    assert lost["agent"]["status"] == "OFFLINE"


def test_provider_outage_and_health(client):
    _setup(client)
    assert client.post("/providers/provider_a/outage", json={"active": True}).json()["in_outage"] is True
    health = {h["provider"]: h for h in client.get("/providers/health").json()}
    assert health["provider_a"]["in_outage"] is True
    assert client.post("/providers/unknown/outage", json={"active": True}).status_code == 404


def test_input_validation(client):
    assert client.post("/agents", json={"count": 0}).status_code == 422
    assert client.post("/campaigns", json={"id": "bad id!", "name": "x"}).status_code == 422
    assert client.post("/time/advance", json={"seconds": -1}).status_code == 422


def test_recovery_and_failure_endpoints(client):
    _setup(client)
    assert "report" in client.post("/recovery/run").json()
    r = client.post("/failures/duplicate_events").json()
    assert r
    assert client.post("/failures/nonexistent").status_code == 404


def test_reset(client):
    _setup(client)
    client.post("/reset")
    assert client.get("/campaigns").json() == []
