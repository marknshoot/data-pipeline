"""Alerting for failed Airflow tasks.

A failure callback that always emits one structured log line and *optionally* POSTs it
to a webhook. Log-only is the default, so the pipeline works with no configuration and
nothing silently depends on a URL being set.

Two rules this module follows:

* **It never raises.** An alerting failure must not replace the task's own exception
  with a confusing one, so every path is guarded.
* **It reports what a human needs.** Which DAG, which task, which run, how many tries
  are left, and the exception -- not a bare "task failed".

Set ``ALERT_WEBHOOK_URL`` to a Discord or Slack incoming webhook to get the same
message there. The payload carries both ``content`` (Discord) and ``text`` (Slack);
each ignores the other's key, so one shape works for both.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

import requests

WEBHOOK_TIMEOUT_SECONDS = 10


def format_alert(context: dict[str, Any]) -> str:
    """Render the failure as one line that stands on its own in a log or a chat."""
    instance = context.get("task_instance")
    dag_id = getattr(getattr(instance, "dag", None), "dag_id", None) or context.get("dag", "")
    task_id = getattr(instance, "task_id", "")
    run_id = getattr(instance, "run_id", "")

    # `max_tries` counts *retries*, so the last attempt number is max_tries + 1.
    # Rendering the raw pair as "2/1" reads like a bug in the alert itself.
    try_number = getattr(instance, "try_number", None)
    max_tries = getattr(instance, "max_tries", None)
    attempt = ""
    if isinstance(try_number, int):
        attempt = f" attempt={try_number}"
        if isinstance(max_tries, int):
            attempt += f"/{max_tries + 1}"

    exception = context.get("exception")
    reason = context.get("reason") or (
        f"{type(exception).__name__}: {exception}" if exception else ""
    )

    return f"[ALERT] {dag_id}.{task_id} failed (run={run_id}{attempt})" + (
        f" -- {reason}" if reason else ""
    )


def send_webhook(message: str, url: str) -> None:
    """POST the alert, never raising: the task's failure is the important one."""
    try:
        response = requests.post(
            url,
            data=json.dumps({"content": message, "text": message}),
            headers={"Content-Type": "application/json"},
            timeout=WEBHOOK_TIMEOUT_SECONDS,
        )
        if response.status_code >= 400:
            print(
                f"alert webhook returned {response.status_code}: {response.text[:200]}",
                file=sys.stderr,
            )
    except requests.RequestException as exc:
        print(f"alert webhook failed: {exc}", file=sys.stderr)


def alert_on_failure(context: dict[str, Any]) -> None:
    """Airflow ``on_failure_callback``."""
    try:
        message = format_alert(context)
    except Exception as exc:  # noqa: BLE001 - alerting must not mask the real failure
        print(f"could not format failure alert: {exc}", file=sys.stderr)
        return

    print(message, file=sys.stderr, flush=True)

    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        send_webhook(message, url)
