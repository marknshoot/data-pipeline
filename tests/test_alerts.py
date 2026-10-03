"""Tests for the failure-alert callback.

Alerting is the one piece of the pipeline that runs when everything else is already
broken, so the interesting cases are the awkward ones: a missing attribute, an
unreachable webhook, no webhook configured at all. It must never raise -- an alerting
bug must not replace the real exception with a confusing one.
"""

from __future__ import annotations

import alerts
import pytest
import requests
from alerts import alert_on_failure, format_alert, send_webhook


class _Dag:
    dag_id = "data_quality"


class FakeTaskInstance:
    dag = _Dag()
    task_id = "reconcile"
    run_id = "manual__2026-10-03T04:07:01"
    try_number = 2
    max_tries = 1


def context(**overrides) -> dict:
    base = {
        "task_instance": FakeTaskInstance(),
        "exception": RuntimeError("row-count reconciliation found a mismatch"),
    }
    base.update(overrides)
    return base


# ------------------------------------------------------------------ formatting


def test_format_alert_names_the_dag_task_and_run():
    message = format_alert(context())

    assert message.startswith("[ALERT] ")
    assert "data_quality.reconcile" in message
    assert "manual__2026-10-03T04:07:01" in message
    assert "RuntimeError: row-count reconciliation found a mismatch" in message


def test_format_alert_counts_retries_as_attempts():
    """`max_tries` counts retries, so 1 retry means 2 attempts -- not "2/1"."""
    assert "attempt=2/2" in format_alert(context())


def test_format_alert_prefers_an_explicit_reason():
    message = format_alert(context(reason="timed out", exception=None))
    assert "timed out" in message
    assert "RuntimeError" not in message


def test_format_alert_survives_a_bare_context():
    """Airflow's context is not guaranteed to carry everything we read."""
    message = format_alert({})
    assert message.startswith("[ALERT] ")
    assert "failed" in message


def test_format_alert_without_try_numbers():
    class NoTries:
        dag = _Dag()
        task_id = "clean"
        run_id = "r1"

    message = format_alert({"task_instance": NoTries(), "exception": ValueError("boom")})
    assert "attempt" not in message
    assert "ValueError: boom" in message


# ------------------------------------------------------------------- delivery


def test_alert_on_failure_without_a_webhook_only_logs(monkeypatch, capsys):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    called = []
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: called.append(a))

    alert_on_failure(context())

    assert called == []
    assert "[ALERT]" in capsys.readouterr().err


def test_alert_on_failure_posts_a_payload_both_chat_tools_accept(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://example.test/hook")
    captured = {}

    class Response:
        status_code = 204
        text = ""

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs.get("data")
        return Response()

    monkeypatch.setattr(alerts.requests, "post", fake_post)
    alert_on_failure(context())

    assert captured["url"] == "https://example.test/hook"
    # Discord reads `content`, Slack reads `text`; each ignores the other's key.
    assert '"content"' in captured["json"]
    assert '"text"' in captured["json"]
    assert "data_quality.reconcile" in captured["json"]


def test_alert_on_failure_never_raises_when_the_webhook_is_down(monkeypatch, capsys):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://example.test/hook")

    def explode(*_args, **_kwargs):
        raise requests.ConnectionError("no route to host")

    monkeypatch.setattr(alerts.requests, "post", explode)

    # The task's own failure is the important one; alerting must not mask it.
    alert_on_failure(context())
    assert "[ALERT]" in capsys.readouterr().err


def test_send_webhook_reports_an_http_error_without_raising(monkeypatch, capsys):
    class Response:
        status_code = 500
        text = "server exploded"

    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: Response())
    send_webhook("hello", "https://example.test/hook")

    assert "500" in capsys.readouterr().err


def test_alert_on_failure_survives_a_broken_formatter(monkeypatch, capsys):
    def explode(_context):
        raise KeyError("nope")

    monkeypatch.setattr(alerts, "format_alert", explode)
    alert_on_failure(context())

    assert "could not format failure alert" in capsys.readouterr().err


@pytest.mark.parametrize("url", ["", None])
def test_alert_on_failure_treats_an_empty_url_as_unset(monkeypatch, url, capsys):
    if url is None:
        monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    else:
        monkeypatch.setenv("ALERT_WEBHOOK_URL", url)
    called = []
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: called.append(a))

    alert_on_failure(context())

    assert called == []
    assert "[ALERT]" in capsys.readouterr().err
