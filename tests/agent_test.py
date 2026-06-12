"""Tests for the ``[agent]`` modules: ``agent.py`` and ``activation.py``.

No Dapr runtime, no network, no API key. Constructing a real ``DurableAgent``
bootstraps from a live Dapr sidecar (it blocks on a health check), so:

* :func:`format_task` and :func:`build_llm` are tested directly,
* :func:`build_agent`'s LLM selection is tested with a substituted
  ``DurableAgent`` that just records the kwargs it was handed,
* :func:`install` is tested against duck-typed fakes for the FastAPI app,
  the runner and the activation context — asserting it wires the schedule
  callback, mounts the route, and registers the lifecycle handlers.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import drasi_mcp_agent.agent as agent_mod
from dapr_agents.llm import DaprChatClient
from dapr_agents.llm.anthropic import AnthropicChatClient
from dapr_agents.tool import AgentTool
from drasi_mcp_agent.activation import WEBHOOK_PATH, install
from drasi_mcp_agent.agent import (
    AGENT_NAME,
    build_agent,
    build_llm,
    format_task,
    summarize_change,
)
from drasi_mcp_agent.config import Settings, load_settings
from drasi_mcp_agent.mcp_events.wire import EventOccurrence
from drasi_mcp_agent.state import AgentEventState

# --- fixtures / helpers ------------------------------------------------------


def make_settings(*, use_llm: bool = False) -> Settings:
    """A complete :class:`Settings` snapshot, independent of the environment."""
    return Settings(
        mcp_url="http://127.0.0.1:8090/mcp",
        mcp_bearer="devtoken",
        event_name="high-value-orders.changed",
        callback_url="http://127.0.0.1:8001/mcp-events/webhook",
        app_host="127.0.0.1",
        app_port=8001,
        ttl_ms=60000,
        anthropic_model="claude-sonnet-4-6",
        use_llm=use_llm,
    )


def occurrence(data: dict[str, Any], *, event_id: str = "evt_1") -> EventOccurrence:
    return EventOccurrence(
        event_id=event_id,
        name="high-value-orders.changed",
        timestamp="2026-06-11T16:00:00Z",
        data=data,
        cursor="cursor_1",
    )


ADDED = occurrence(
    {"changeType": "added", "after": {"id": 42, "customer": "alice", "total": 5000}},
    event_id="evt_added",
)
UPDATED = occurrence(
    {
        "changeType": "updated",
        "before": {"id": 7, "customer": "bob", "total": 900},
        "after": {"id": 7, "customer": "bob", "total": 1500},
    },
    event_id="evt_updated",
)
DELETED = occurrence(
    {"changeType": "deleted", "before": {"id": 9, "customer": "carol", "total": 1200}},
    event_id="evt_deleted",
)


# --- format_task -------------------------------------------------------------


def test_format_task_added_uses_after_row() -> None:
    task = format_task(ADDED)
    assert "(ADDED)" in task
    assert "order 42" in task
    assert "customer alice" in task
    assert "total 5000" in task


def test_format_task_updated_uses_after_row() -> None:
    task = format_task(UPDATED)
    assert "(UPDATED)" in task
    assert "order 7" in task
    assert "customer bob" in task
    # The *current* state (after), not the pre-image (before).
    assert "total 1500" in task
    assert "total 900" not in task


def test_format_task_deleted_uses_before_row() -> None:
    task = format_task(DELETED)
    assert "(DELETED)" in task
    assert "order 9" in task
    assert "customer carol" in task
    assert "total 1200" in task


def test_format_task_tolerates_missing_fields() -> None:
    # Verified-but-malformed body: no changeType, no after/before. Must not raise.
    task = format_task(occurrence({"unexpected": True}))
    assert "(CHANGED)" in task
    assert "order ?" in task
    assert "customer ?" in task


# --- summarize_change tool ---------------------------------------------------


def test_summarize_change_is_a_tool_and_confirms() -> None:
    assert isinstance(summarize_change, AgentTool)
    # The underlying function still returns its confirmation string.
    assert summarize_change.func("added", "order 42 entered") == (
        "Recorded added change: order 42 entered"
    )


# --- build_llm / build_agent LLM selection -----------------------------------


def test_build_llm_uses_echo_when_no_key() -> None:
    llm = build_llm(make_settings(use_llm=False))
    assert isinstance(llm, DaprChatClient)
    assert llm.component_name == agent_mod.ECHO_LLM_COMPONENT


def test_build_llm_uses_anthropic_when_key_present() -> None:
    llm = build_llm(make_settings(use_llm=True))
    assert isinstance(llm, AnthropicChatClient)
    assert llm.model == "claude-sonnet-4-6"


def test_build_llm_selection_follows_env(monkeypatch) -> None:
    # env -> Settings.use_llm -> llm type, without a real key or Dapr.
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert isinstance(build_llm(load_settings()), DaprChatClient)

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-dummy-not-used")
    assert isinstance(build_llm(load_settings()), AnthropicChatClient)


class _FakeDurableAgent:
    """Records the kwargs ``build_agent`` hands the DurableAgent constructor."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


def test_build_agent_wires_echo_llm_no_tools(monkeypatch) -> None:
    # No key → echo LLM, and NO tools (echo cannot form a valid tool call).
    monkeypatch.setattr(agent_mod, "DurableAgent", _FakeDurableAgent)
    built = build_agent(make_settings(use_llm=False))
    assert isinstance(built, _FakeDurableAgent)
    assert built.kwargs["name"] == AGENT_NAME
    assert isinstance(built.kwargs["llm"], DaprChatClient)
    assert built.kwargs["tools"] == []


def test_build_agent_picks_anthropic_and_tools_when_use_llm(monkeypatch) -> None:
    monkeypatch.setattr(agent_mod, "DurableAgent", _FakeDurableAgent)
    built = build_agent(make_settings(use_llm=True))
    assert isinstance(built.kwargs["llm"], AnthropicChatClient)
    assert summarize_change in built.kwargs["tools"]


# --- install / activation hook ----------------------------------------------


class _FakeRunner:
    """Captures ``runner.run(...)`` calls and returns a fixed instance id."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run(self, agent: Any, *, payload: dict[str, Any], wait: bool) -> str:
        self.calls.append({"agent": agent, "payload": payload, "wait": wait})
        return "wf-instance-1"


class _FakeRouter:
    """Stand-in for Starlette's router lifecycle lists (>=1.3 has no
    app.add_event_handler; handlers are appended to these lists)."""

    def __init__(self) -> None:
        self.on_startup: list[Any] = []
        self.on_shutdown: list[Any] = []


class _FakeApp:
    """Minimal FastAPI stand-in: records routes and lifecycle handlers."""

    def __init__(self) -> None:
        self.routes: list[dict[str, Any]] = []
        self.router = _FakeRouter()
        self.state = SimpleNamespace()

    def add_api_route(self, path: str, endpoint: Any, *, methods: list[str]) -> None:
        self.routes.append({"path": path, "endpoint": endpoint, "methods": methods})


class _FakeAgent:
    """Captures the single activation hook ``install`` registers."""

    def __init__(self) -> None:
        self.hook: Any = None

    def add_activation(self, callback: Any) -> None:
        self.hook = callback


def test_install_returns_state_and_registers_hook() -> None:
    agent = _FakeAgent()
    st = install(agent, make_settings())
    assert isinstance(st, AgentEventState)
    assert callable(agent.hook)


def test_hook_without_app_is_a_noop() -> None:
    agent = _FakeAgent()
    st = install(agent, make_settings())
    ctx = SimpleNamespace(app=None, runner=_FakeRunner(), agent=object())

    assert agent.hook(ctx) is None
    # No app → no schedule sink wired (receiver would 503 / ask retry).
    assert st.schedule is None


async def test_hook_with_app_wires_route_schedule_and_lifecycle() -> None:
    agent = _FakeAgent()
    st = install(agent, make_settings())
    app = _FakeApp()
    runner = _FakeRunner()
    sentinel_agent = object()
    ctx = SimpleNamespace(app=app, runner=runner, agent=sentinel_agent)

    assert agent.hook(ctx) is None

    # 1) webhook route mounted as POST at the documented path.
    route = next(r for r in app.routes if r["path"] == WEBHOOK_PATH)
    assert route["methods"] == ["POST"]

    # 2) lifecycle handlers registered (subscription started on startup).
    assert len(app.router.on_startup) == 1
    assert len(app.router.on_shutdown) == 1

    # 3) schedule callback wired and delegates to runner.run(..., wait=False).
    assert st.schedule is not None
    instance_id = await st.schedule(ADDED)
    assert instance_id == "wf-instance-1"
    call = runner.calls[0]
    assert call["agent"] is sentinel_agent
    assert call["wait"] is False
    assert call["payload"] == {"task": format_task(ADDED)}
