"""Capture portfolio screenshots of the running UI (Metabase + Airflow).

Run against a live stack:
    make up-core && make up-bi && make metabase-setup && make up-orch
    make screenshots

Writes PNGs into docs/img/. Playwright is pulled in on demand so it is not a
project dependency. Run as a module (not by path) so the repo root is importable:
    uv run --with playwright==1.63.0 python -m scripts.capture_screenshots
"""

from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

from generators.config import get_settings
from metabase.provision import Metabase

OUT_DIR = Path(__file__).resolve().parents[1] / "docs" / "img"
VIEWPORT = {"width": 1600, "height": 1000}
# Metabase draws charts after the network goes quiet; give the renderer a moment.
RENDER_SETTLE_MS = 7000


def _slug(name: str) -> str:
    return name.lower().replace(" ", "-")


def _expand_to_content(page: Page, max_height: int = 4000) -> None:
    """Grow the viewport to fit the page.

    ``full_page=True`` is not enough here: Metabase scrolls inside an inner
    container rather than the document, so a full-page screenshot still stops at
    the viewport. Expanding the viewport makes the whole dashboard render.
    """
    height = page.evaluate(
        """() => {
            // Prefer real content: the bottom edge of the last dashcard. Metabase
            // reserves grid rows and has containers much taller than the cards, so
            // a naive max(scrollHeight) leaves a wide empty band under the dashboard.
            let bottom = 0;
            for (const el of document.querySelectorAll('[data-testid="dashcard"]')) {
                const rect = el.getBoundingClientRect();
                bottom = Math.max(bottom, rect.bottom + window.scrollY);
            }
            if (bottom > 0) return bottom;
            let max = document.body.scrollHeight;
            for (const el of document.querySelectorAll('*')) {
                if (el.scrollHeight > max) max = el.scrollHeight;
            }
            return max;
        }"""
    )
    target = min(max(int(height) + 80, VIEWPORT["height"]), max_height)
    page.set_viewport_size({"width": VIEWPORT["width"], "height": target})
    page.wait_for_timeout(1500)


def capture_metabase(page: Page, base_url: str) -> list[Path]:
    settings = get_settings()
    metabase = Metabase(base_url)
    metabase.ensure_setup(settings)

    dashboards = metabase.get("/api/dashboard")
    rows = dashboards["data"] if isinstance(dashboards, dict) else dashboards

    written: list[Path] = []
    # Log in through the UI so the session cookie is set for page loads.
    page.goto(f"{base_url}/auth/login", wait_until="networkidle")
    page.locator('input[name="email"], input[type="email"], #email').first.fill(
        settings.metabase_admin_email
    )
    page.locator('input[name="password"], input[type="password"], #password').first.fill(
        settings.metabase_admin_password
    )
    page.locator('button[type="submit"]').first.click()
    page.wait_for_url(f"{base_url}/**", timeout=60_000)
    page.wait_for_timeout(2000)

    for dashboard in rows:
        page.goto(f"{base_url}/dashboard/{dashboard['id']}", wait_until="networkidle")
        page.wait_for_timeout(RENDER_SETTLE_MS)
        _expand_to_content(page)
        target = OUT_DIR / f"metabase-{_slug(dashboard['name'])}.png"
        page.screenshot(path=str(target))
        written.append(target)
        print(f"  captured {target.name}  ({dashboard['name']})")
    return written


def capture_airflow(page: Page, base_url: str) -> list[Path]:
    written: list[Path] = []
    # Reset the viewport: the Metabase capture grows it to fit a tall dashboard.
    page.set_viewport_size(VIEWPORT)

    page.goto(f"{base_url}/dags", wait_until="networkidle")
    page.wait_for_timeout(RENDER_SETTLE_MS // 2)
    target = OUT_DIR / "airflow-dags.png"
    page.screenshot(path=str(target))
    written.append(target)
    print(f"  captured {target.name}  (DAG list)")

    for dag_id in ("warehouse_build", "oltp_extract", "clean_events"):
        # Airflow 3 renders the DAG graph on the DAG detail page itself; there is no
        # /dags/<id>/graph route (that 404s).
        page.goto(f"{base_url}/dags/{dag_id}", wait_until="networkidle")
        page.wait_for_timeout(RENDER_SETTLE_MS // 2)
        target = OUT_DIR / f"airflow-{dag_id.replace('_', '-')}.png"
        page.screenshot(path=str(target))
        written.append(target)
        print(f"  captured {target.name}  ({dag_id} graph)")
    return written


def main() -> int:
    settings = get_settings()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1)
        page = context.new_page()

        print("Metabase:")
        try:
            capture_metabase(page, settings.metabase_url)
        except Exception as exc:  # noqa: BLE001 - report and continue to the next target
            print(f"  metabase capture failed: {str(exc)[:200]}", file=sys.stderr)

        print("Airflow:")
        try:
            capture_airflow(page, "http://localhost:8080")
        except Exception as exc:  # noqa: BLE001
            print(f"  airflow capture failed: {str(exc)[:200]}", file=sys.stderr)

        browser.close()

    print(f"\nwrote screenshots to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
