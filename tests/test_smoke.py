"""Placeholder test so CI/pytest has something to collect until real modules land."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_compose_file_exists() -> None:
    assert (ROOT / "docker-compose.yml").is_file()


def test_env_example_has_no_real_secrets() -> None:
    text = (ROOT / ".env.example").read_text()
    assert "change_me" in text
