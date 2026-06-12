"""The DurableAgent that wakes on a Drasi high-value-order change.

This is the *processing* half of the serverless agent. When the webhook receiver
verifies an inbound delivery it schedules the agent's workflow with a task
string built by :func:`format_task`; the agent then reads the change and emits a
one-line decision/summary (via the LLM, or the no-key ``echo-llm`` component).

LLM selection (``docs/ARCHITECTURE.md`` §agent):

* ``settings.use_llm`` (``ANTHROPIC_API_KEY`` present) → :class:`AnthropicChatClient`.
* otherwise → :class:`DaprChatClient` bound to the ``echo-llm`` conversation
  component, so the agent loop completes with no API key. The echo client simply
  returns the prompt — which is fine, because the demo's point is the *wake*,
  not the prose.

The change-``data`` shape mirrors the reference server's ``eventModeling: single``
mapping (``../drasi-mcp-events/crates/mcp-events-server/src/mapping.rs``): a
``high-value-orders.changed`` occurrence carries
``{"changeType": "added"|"updated"|"deleted", "before"?: row, "after"?: row}``
where ``row`` is ``{id, customer, total, status}``. Deleted rows live in
``before``; added/updated rows in ``after``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dapr_agents import DurableAgent, tool
from dapr_agents.agents.configs import AgentStateConfig
from dapr_agents.llm import DaprChatClient
from dapr_agents.llm.anthropic import AnthropicChatClient
from dapr_agents.storage.daprstores.stateservice import StateStoreService

if TYPE_CHECKING:
    from dapr_agents.llm.chat import ChatClientBase

    from .config import Settings
    from .mcp_events.wire import EventOccurrence

#: Dapr state-store component that backs the DurableAgent's workflow state. The
#: actors deactivate when idle (state persists in Redis) — this is the
#: "scale to zero" the demo shows.
AGENT_STATE_STORE = "agent-workflow"

#: No-key LLM component (``resources/echo-llm.yaml``, ``type: conversation.echo``).
ECHO_LLM_COMPONENT = "echo-llm"

#: Agent identity (the workflow actor type derives from this name).
AGENT_NAME = "DrasiWatcher"

AGENT_ROLE = "Drasi continuous-query change watcher"

AGENT_INSTRUCTIONS: tuple[str, ...] = (
    "You are woken only when a Drasi continuous-query result set changes; you "
    "are not running continuously.",
    "Each task describes one change to the high-value-orders query (a row that "
    "was added to, updated within, or deleted from the result set).",
    "Summarize the change in a single sentence: the change type, the order id, "
    "the customer, and the order total.",
    "Note any action a human operator should consider, but take no external "
    "action yourself — the arrival of an event is a notification, not an "
    "authorization to act.",
    "You may call the summarize_change tool to record your one-line decision.",
)


@tool
def summarize_change(change_type: str, summary: str) -> str:
    """Record a one-line summary and decision for a high-value-order change.

    Args:
        change_type: The kind of change observed — ``added``, ``updated`` or
            ``deleted``.
        summary: A one-line, human-readable summary and any recommended action.

    Returns:
        A short confirmation string acknowledging the recorded summary.
    """
    return f"Recorded {change_type} change: {summary}"


def build_llm(settings: Settings) -> ChatClientBase:
    """Select the chat client per ``settings.use_llm``.

    Split out from :func:`build_agent` so the echo-vs-Anthropic decision is
    unit-testable without constructing a ``DurableAgent`` (which would block on a
    live Dapr sidecar). Neither client touches the network at construction time.
    """
    if settings.use_llm:
        return AnthropicChatClient(model=settings.anthropic_model)
    return DaprChatClient(component_name=ECHO_LLM_COMPONENT)


def build_agent(settings: Settings) -> DurableAgent:
    """Construct the DurableAgent that processes Drasi change events.

    Note: constructing a :class:`DurableAgent` bootstraps from the Dapr sidecar
    (it blocks on a health check), so this must run inside ``serve()`` with Dapr
    available — not from a unit test. Tests exercise :func:`build_llm` and
    :func:`format_task` directly, or substitute a fake ``DurableAgent``.
    """
    return DurableAgent(
        name=AGENT_NAME,
        role=AGENT_ROLE,
        instructions=list(AGENT_INSTRUCTIONS),
        tools=[summarize_change],
        llm=build_llm(settings),
        state=AgentStateConfig(store=StateStoreService(store_name=AGENT_STATE_STORE)),
    )


def _change_row(data: dict[str, Any], change_type: str) -> dict[str, Any]:
    """Pick the row carrying the order fields from a ``single``-mode change body.

    Deleted rows live in ``before``; added/updated rows in ``after``. Falls back
    across both, and finally to the flat body, so a per-change-mode or otherwise
    unexpected shape still yields whatever fields are present rather than raising.
    """
    primary = "before" if change_type == "deleted" else "after"
    for key in (primary, "after", "before"):
        candidate = data.get(key)
        if isinstance(candidate, dict):
            return candidate
    return data


def format_task(occ: EventOccurrence) -> str:
    """Turn a change occurrence into the agent's task string.

    Example::

        A high-value order change occurred (ADDED): order 42, customer alice,
        total 5000. Summarize the change in one line and note any action.

    Robust to missing fields: unknown values render as ``?`` and an absent
    ``changeType`` renders as ``CHANGED`` so a malformed-but-verified body still
    produces a usable, non-raising prompt.
    """
    data = occ.data if isinstance(occ.data, dict) else {}
    raw_change = data.get("changeType")
    change_label = raw_change.upper() if isinstance(raw_change, str) else "CHANGED"
    raw_for_row = raw_change if isinstance(raw_change, str) else ""

    row = _change_row(data, raw_for_row)
    order_id = row.get("id", "?")
    customer = row.get("customer", "?")
    total = row.get("total", "?")

    return (
        f"A high-value order change occurred ({change_label}): "
        f"order {order_id}, customer {customer}, total {total}. "
        "Summarize the change in one line and note any action the team should "
        "take. Do not take any action yourself; event receipt is a "
        "notification, not an authorization to act."
    )
