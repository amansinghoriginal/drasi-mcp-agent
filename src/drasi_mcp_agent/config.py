"""Process configuration for the drasi-mcp-agent.

A single immutable :class:`Settings` snapshot read from the environment at
startup. See ``docs/ARCHITECTURE.md`` (config.py) for the binding contract.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Environment variable names (kept as constants so other modules / tests can
# reference them without restating string literals).
ENV_MCP_URL = "MCP_URL"
ENV_MCP_BEARER = "MCP_BEARER"
ENV_EVENT_NAME = "EVENT_NAME"
ENV_CALLBACK_URL = "CALLBACK_URL"
ENV_APP_HOST = "APP_HOST"
ENV_APP_PORT = "APP_PORT"
ENV_SUB_TTL_MS = "SUB_TTL_MS"
ENV_ANTHROPIC_MODEL = "ANTHROPIC_MODEL"
ENV_ANTHROPIC_API_KEY = "ANTHROPIC_API_KEY"

# Defaults (mirror the demo topology in docs/ARCHITECTURE.md).
DEFAULT_MCP_URL = "http://127.0.0.1:8090/mcp"
DEFAULT_MCP_BEARER = "devtoken"
DEFAULT_EVENT_NAME = "high-value-orders.changed"
DEFAULT_CALLBACK_URL = "http://127.0.0.1:8001/mcp-events/webhook"
DEFAULT_APP_HOST = "127.0.0.1"
DEFAULT_APP_PORT = 8001
DEFAULT_SUB_TTL_MS = 60000
DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-4-6"


@dataclass(frozen=True)
class Settings:
    """Immutable configuration snapshot for the agent process."""

    mcp_url: str
    mcp_bearer: str
    event_name: str
    callback_url: str
    app_host: str
    app_port: int
    ttl_ms: int
    anthropic_model: str
    use_llm: bool


def _env_str(name: str, default: str) -> str:
    """Return the environment value for ``name``, falling back to ``default``.

    An empty / whitespace-only value is treated as unset so a blank env var
    does not silently override a sensible default.
    """
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value


def _env_int(name: str, default: int) -> int:
    """Return the integer environment value for ``name`` or ``default``.

    Raises ``ValueError`` (with the offending name) on a non-integer value so a
    typo surfaces at startup rather than as a confusing failure later.
    """
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def load_settings() -> Settings:
    """Build :class:`Settings` from ``os.environ``, applying defaults.

    ``use_llm`` is ``True`` iff a non-empty ``ANTHROPIC_API_KEY`` is present;
    when absent the agent loop completes against the ``echo-llm`` component.
    """
    api_key = os.environ.get(ENV_ANTHROPIC_API_KEY)
    use_llm = bool(api_key and api_key.strip())

    return Settings(
        mcp_url=_env_str(ENV_MCP_URL, DEFAULT_MCP_URL),
        mcp_bearer=_env_str(ENV_MCP_BEARER, DEFAULT_MCP_BEARER),
        event_name=_env_str(ENV_EVENT_NAME, DEFAULT_EVENT_NAME),
        callback_url=_env_str(ENV_CALLBACK_URL, DEFAULT_CALLBACK_URL),
        app_host=_env_str(ENV_APP_HOST, DEFAULT_APP_HOST),
        app_port=_env_int(ENV_APP_PORT, DEFAULT_APP_PORT),
        ttl_ms=_env_int(ENV_SUB_TTL_MS, DEFAULT_SUB_TTL_MS),
        anthropic_model=_env_str(ENV_ANTHROPIC_MODEL, DEFAULT_ANTHROPIC_MODEL),
        use_llm=use_llm,
    )
