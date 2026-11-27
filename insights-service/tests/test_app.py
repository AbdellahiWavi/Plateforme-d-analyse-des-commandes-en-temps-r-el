import httpx
import pytest
from fastapi.testclient import TestClient

import app as app_module

METRICS = {
    "kpis": {"total_orders": 3, "revenue": 2700.0, "average_order_value": 900.0},
    "top_products": [{"product": "Laptop", "orders": 3, "quantity": 6, "revenue": 2700.0}],
    "history_hours": 2,
    "current_hour": {"hour": "2026-11-25T14", "orders": 1},
    "trend": {"status": "insufficient_data", "window_hours": 6, "history_hours": 2, "required_hours": 12},
    "peak_hour": None,
    "anomalies": {"status": "insufficient_data", "points": 2, "required": 6, "anomalies": []},
    "forecast": {"status": "insufficient_data", "points": 2, "required": 6},
    "customer_segments": {"status": "ok", "customers": 1,
                          "segments": {"new": {"customers": 1, "revenue": 2700.0, "revenue_share_pct": 100.0}}},
}


@pytest.fixture
def ctx(monkeypatch):
    stored = []
    calls = {"groq": 0}
    monkeypatch.setattr(app_module, "collect_metrics", lambda: dict(METRICS))
    monkeypatch.setattr(app_module, "save_report", lambda r: stored.append(r))
    monkeypatch.setattr(app_module, "latest_report", lambda: stored[-1] if stored else None)
    monkeypatch.setattr(app_module, "list_reports", lambda limit: stored[-limit:][::-1])
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "")
    monkeypatch.setattr(app_module, "last_refresh", None)

    def fake_groq(metrics):
        calls["groq"] += 1
        return "Résumé généré par le LLM"

    return {"stored": stored, "calls": calls, "fake_groq": fake_groq}


@pytest.fixture
def client():
    with TestClient(app_module.app) as c:
        yield c


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_metrics_endpoint_does_not_call_llm(client, ctx):
    r = client.get("/insights/metrics")
    assert r.status_code == 200
    assert r.json()["kpis"]["total_orders"] == 3
    assert ctx["calls"]["groq"] == 0


def test_insights_without_key_uses_fallback(client, ctx):
    r = client.get("/insights")
    body = r.json()
    assert r.status_code == 200
    assert body["llm"]["source"] == "fallback"
    assert "GROQ_API_KEY" in body["llm"]["error"]
    assert "3 commande(s)" in body["report"]
    assert len(ctx["stored"]) == 1


def test_insights_returns_cached_report(client, ctx):
    first = client.get("/insights").json()
    second = client.get("/insights").json()
    assert first["generated_at"] == second["generated_at"]
    assert len(ctx["stored"]) == 1


def test_insights_with_groq(client, ctx, monkeypatch):
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "test-key")
    monkeypatch.setattr(app_module, "ask_groq", ctx["fake_groq"])
    body = client.get("/insights").json()
    assert body["llm"]["source"] == "groq"
    assert body["report"] == "Résumé généré par le LLM"


def test_groq_failure_falls_back(client, ctx, monkeypatch):
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "test-key")

    def boom(metrics):
        raise httpx.ConnectError("network down")

    monkeypatch.setattr(app_module, "ask_groq", boom)
    body = client.get("/insights").json()
    assert body["llm"]["source"] == "fallback"
    assert body["llm"]["error"] == "ConnectError"
    assert "test-key" not in str(body)


def test_scheduler_skips_when_nothing_changed(ctx, monkeypatch):
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "test-key")
    monkeypatch.setattr(app_module, "ask_groq", ctx["fake_groq"])
    app_module.generate_report(force=True)
    app_module.generate_report()  # même total de commandes : pas de nouvel appel
    assert ctx["calls"]["groq"] == 1
    assert len(ctx["stored"]) == 1


def test_scheduler_retries_llm_after_a_fallback(ctx, monkeypatch):
    app_module.generate_report(force=True)  # sans clé : fallback
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "test-key")
    monkeypatch.setattr(app_module, "ask_groq", ctx["fake_groq"])
    report = app_module.generate_report()
    assert report["llm"]["source"] == "groq"


def test_refresh_has_cooldown(client, ctx):
    assert client.post("/insights/refresh").status_code == 200
    assert client.post("/insights/refresh").status_code == 429


def test_analytics_down_returns_503(client, ctx, monkeypatch):
    def down():
        raise httpx.ConnectError("analytics down")

    monkeypatch.setattr(app_module, "collect_metrics", down)
    assert client.get("/insights/metrics").status_code == 503
    assert client.get("/insights").status_code == 503


def test_history(client, ctx):
    client.post("/insights/refresh")
    assert len(client.get("/insights/history?limit=5").json()) == 1
    assert client.get("/insights/history?limit=0").status_code == 422


# ---------- appel Groq (requête et analyse de la réponse) ----------
class FakeResponse:
    def __init__(self, status_code=200, content="  Bonjour  ", headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=None, response=self)

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


def configure_groq(monkeypatch, effort=""):
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "k")
    monkeypatch.setattr(app_module, "GROQ_MODEL", "openai/gpt-oss-20b")
    monkeypatch.setattr(app_module, "GROQ_REASONING_EFFORT", effort)


def test_ask_groq_builds_request_and_parses_answer(monkeypatch):
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, payload=json, headers=headers)
        return FakeResponse()

    configure_groq(monkeypatch, effort="low")
    monkeypatch.setattr(app_module.httpx, "post", fake_post)

    assert app_module.ask_groq(METRICS) == "Bonjour"
    assert captured["url"].endswith("/openai/v1/chat/completions")
    assert captured["payload"]["model"] == "openai/gpt-oss-20b"
    assert captured["payload"]["reasoning_effort"] == "low"
    assert captured["headers"]["Authorization"] == "Bearer k"
    assert "Laptop" in captured["payload"]["messages"][1]["content"]


def test_ask_groq_omits_reasoning_effort_when_empty(monkeypatch):
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["payload"] = json
        return FakeResponse()

    configure_groq(monkeypatch, effort="")
    monkeypatch.setattr(app_module.httpx, "post", fake_post)
    app_module.ask_groq(METRICS)
    assert "reasoning_effort" not in captured["payload"]


def test_ask_groq_retries_after_rate_limit(monkeypatch):
    answers = [FakeResponse(429, headers={"retry-after": "0"}), FakeResponse()]
    configure_groq(monkeypatch)
    monkeypatch.setattr(app_module.httpx, "post", lambda *a, **k: answers.pop(0))
    monkeypatch.setattr(app_module.time, "sleep", lambda s: None)
    assert app_module.ask_groq(METRICS) == "Bonjour"
    assert answers == []


def test_ask_groq_empty_answer_is_treated_as_unavailable(monkeypatch):
    configure_groq(monkeypatch)
    monkeypatch.setattr(app_module.httpx, "post", lambda *a, **k: FakeResponse(content=""))
    with pytest.raises(app_module.LLMUnavailable):
        app_module.ask_groq(METRICS)


def test_ask_groq_http_error_falls_back_with_status(ctx, monkeypatch):
    configure_groq(monkeypatch)
    monkeypatch.setattr(app_module.httpx, "post", lambda *a, **k: FakeResponse(400))
    report = app_module.generate_report(force=True)
    assert report["llm"]["source"] == "fallback"
    assert report["llm"]["error"] == "HTTPStatusError (HTTP 400)"
