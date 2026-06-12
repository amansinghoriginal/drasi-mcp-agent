"""Tests for drasi_mcp_agent.config: defaults, env overrides, use_llm toggle."""

from __future__ import annotations

import pytest

from drasi_mcp_agent.config import (
    DEFAULT_ANTHROPIC_MODEL,
    DEFAULT_APP_HOST,
    DEFAULT_APP_PORT,
    DEFAULT_CALLBACK_URL,
    DEFAULT_EVENT_NAME,
    DEFAULT_MCP_BEARER,
    DEFAULT_MCP_URL,
    DEFAULT_SUB_TTL_MS,
    Settings,
    load_settings,
)

# Every environment variable config reads, so a single fixture can scrub them.
_CONFIG_ENV_VARS = (
    "MCP_URL",
    "MCP_BEARER",
    "EVENT_NAME",
    "CALLBACK_URL",
    "APP_HOST",
    "APP_PORT",
    "SUB_TTL_MS",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_API_KEY",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each test from a pristine environment (no inherited config vars)."""
    for name in _CONFIG_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_defaults_when_env_empty() -> None:
    settings = load_settings()
    assert settings == Settings(
        mcp_url=DEFAULT_MCP_URL,
        mcp_bearer=DEFAULT_MCP_BEARER,
        event_name=DEFAULT_EVENT_NAME,
        callback_url=DEFAULT_CALLBACK_URL,
        app_host=DEFAULT_APP_HOST,
        app_port=DEFAULT_APP_PORT,
        ttl_ms=DEFAULT_SUB_TTL_MS,
        anthropic_model=DEFAULT_ANTHROPIC_MODEL,
        use_llm=False,
    )


def test_settings_is_frozen() -> None:
    settings = load_settings()
    with pytest.raises(AttributeError):
        settings.app_port = 9999  # type: ignore[misc]


def test_env_overrides_all_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_URL", "http://mcp.example:9000/mcp")
    monkeypatch.setenv("MCP_BEARER", "secret-token")
    monkeypatch.setenv("EVENT_NAME", "other-query.changed")
    monkeypatch.setenv("CALLBACK_URL", "https://agent.example/hook")
    monkeypatch.setenv("APP_HOST", "0.0.0.0")
    monkeypatch.setenv("APP_PORT", "9001")
    monkeypatch.setenv("SUB_TTL_MS", "120000")
    monkeypatch.setenv("ANTHROPIC_MODEL", "claude-opus-4-1")

    settings = load_settings()

    assert settings.mcp_url == "http://mcp.example:9000/mcp"
    assert settings.mcp_bearer == "secret-token"
    assert settings.event_name == "other-query.changed"
    assert settings.callback_url == "https://agent.example/hook"
    assert settings.app_host == "0.0.0.0"
    assert settings.app_port == 9001
    assert settings.ttl_ms == 120000
    assert settings.anthropic_model == "claude-opus-4-1"


def test_int_fields_are_typed_int(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_PORT", "8080")
    monkeypatch.setenv("SUB_TTL_MS", "30000")
    settings = load_settings()
    assert isinstance(settings.app_port, int)
    assert isinstance(settings.ttl_ms, int)
    assert settings.app_port == 8080
    assert settings.ttl_ms == 30000


def test_invalid_int_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_PORT", "not-a-number")
    with pytest.raises(ValueError, match="APP_PORT"):
        load_settings()


def test_blank_env_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # A present-but-empty value should not clobber the default.
    monkeypatch.setenv("MCP_URL", "")
    monkeypatch.setenv("APP_PORT", "   ")
    settings = load_settings()
    assert settings.mcp_url == DEFAULT_MCP_URL
    assert settings.app_port == DEFAULT_APP_PORT


def test_use_llm_true_when_api_key_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-xxx")
    assert load_settings().use_llm is True


def test_use_llm_false_when_api_key_absent() -> None:
    assert load_settings().use_llm is False


def test_use_llm_false_when_api_key_blank(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "   ")
    assert load_settings().use_llm is False
