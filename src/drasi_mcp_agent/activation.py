"""Activation wiring: mount the webhook route and run the subscription lifecycle.

:func:`install` registers a single activation hook on the :class:`DurableAgent`.
When the agent is hosted via ``AgentRunner.serve(...)`` the runner builds one
:class:`~dapr_agents.types.activation.ActivationContext` and calls the hook
*before* uvicorn starts. The hook:

1. sets :attr:`AgentEventState.schedule` to a coroutine that schedules the
   DurableAgent workflow (``ctx.runner.run(..., wait=False)``) — this is how the
   receiver wakes the agent;
2. mounts the raw ``POST /mcp-events/webhook`` route on the FastAPI app (a raw
   ``Request`` handler is required because the Standard Webhooks signature is
   computed over the *raw* body bytes);
3. registers a FastAPI ``startup`` handler that builds the MCP Events client and
   :class:`SubscriptionManager` and kicks off the subscription, plus a
   ``shutdown`` handler that tears them down.

Startup-vs-listening ordering (load-bearing — see SPEC-FINDINGS):
``events/subscribe`` blocks while the server POSTs a ``verification`` control
envelope *back* to this agent's own ``/mcp-events/webhook`` and waits for the
challenge echo. The reference server runs that handshake **once, with no
retries** (``../drasi-mcp-events/.../webhook/challenge.rs``). But uvicorn fires
FastAPI ``startup`` handlers *before* it creates the listening socket, so a
callback issued from inside ``startup`` would hit a closed port → the single
challenge fails → ``events/subscribe`` returns ``-32015``. Therefore the startup
handler does not ``await mgr.start()`` inline; it schedules it as a background
task that runs once the event loop is serving and the socket is up. This honors
the architecture's stated intent ("subscription happens AFTER uvicorn is
listening") against the real server's no-retry verification.

Ownership: this module is ``[agent]``-owned and imports the real symbols from the
``[foundation]``/``[receiver]`` modules; it does not reimplement them.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import httpx

from .agent import format_task
from .mcp_events.client import McpEventsClient, McpProtocolError, McpRpcError
from .receiver import make_webhook_route
from .state import AgentEventState
from .subscription import NoWebhookEventError, SubscriptionManager

if TYPE_CHECKING:
    from dapr_agents import DurableAgent
    from dapr_agents.types import ActivationContext

    from .config import Settings
    from .mcp_events.wire import EventOccurrence

logger = logging.getLogger(__name__)

#: ``app.state`` attribute names for the objects the shutdown handler tears down.
_CLIENT_ATTR = "mcp_events_client"
_MANAGER_ATTR = "subscription_manager"
_START_TASK_ATTR = "subscription_start_task"

#: The webhook callback path. Must match ``settings.callback_url``'s path and the
#: receiver's documented mount point.
WEBHOOK_PATH = "/mcp-events/webhook"

#: Failures from ``SubscriptionManager.start()`` we log (rather than crash the
#: server) when the subscription is started in the background: a JSON-RPC error,
#: a framing fault, a transport fault, or "no webhook-capable event".
_START_ERRORS = (McpRpcError, McpProtocolError, httpx.HTTPError, NoWebhookEventError)


def install(agent: DurableAgent, settings: Settings) -> AgentEventState:
    """Build the shared state and register the activation hook.

    Returns the :class:`AgentEventState` so callers (and tests) hold the same
    instance the receiver and subscription manager share. The hook itself only
    runs later, when the agent is hosted.
    """
    st = AgentEventState()

    def hook(ctx: ActivationContext) -> None:
        # No FastAPI app means a non-HTTP host (subscribe()/workflow()/run()).
        # The webhook receiver and the subscription loop both require an HTTP
        # endpoint to receive deliveries and the verification callback, so there
        # is nothing to wire — bail out (per ARCHITECTURE.md and the
        # ActivationContext contract: branch on ``app is None``).
        if ctx.app is None:
            logger.warning(
                "activation: no FastAPI app on the context; webhook delivery "
                "requires serve() — not mounting the receiver or subscription"
            )
            return None

        async def schedule(occ: EventOccurrence) -> str:
            """Wake the agent: schedule its workflow for this change, non-blocking.

            Returns the workflow instance id; the receiver logs it and acks fast.
            """
            return await ctx.runner.run(
                ctx.agent,
                payload={"task": format_task(occ)},
                wait=False,
            )

        st.schedule = schedule

        # Raw Request handler (not a parsed-model route): the signature is over
        # the raw body bytes, so the body must not be re-serialized.
        ctx.app.add_api_route(
            WEBHOOK_PATH,
            make_webhook_route(st),
            methods=["POST"],
        )

        app = ctx.app

        async def on_startup() -> None:
            """Build the client + manager and start subscribing in the background.

            See the module docstring for why the subscribe is deferred to a task
            rather than awaited inline: the self-verification callback must reach
            a socket that uvicorn has not opened yet at ``startup`` time.
            """
            client = McpEventsClient(settings.mcp_url, settings.mcp_bearer)
            mgr = SubscriptionManager(client, st, settings)
            setattr(app.state, _CLIENT_ATTR, client)
            setattr(app.state, _MANAGER_ATTR, mgr)
            setattr(app.state, _START_TASK_ATTR, asyncio.create_task(_start(mgr)))
            logger.info("subscription start scheduled (deferred until serving)")

        async def on_shutdown() -> None:
            """Cancel any in-flight start, stop the refresh loop, close the client."""
            task = getattr(app.state, _START_TASK_ATTR, None)
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            mgr = getattr(app.state, _MANAGER_ATTR, None)
            if mgr is not None:
                await mgr.stop()
            client = getattr(app.state, _CLIENT_ATTR, None)
            if client is not None:
                await client.aclose()

        # Starlette >=1.3 removed FastAPI.add_event_handler; append directly to
        # the router's lifecycle lists (still honored by the lifespan runner).
        app.router.on_startup.append(on_startup)
        app.router.on_shutdown.append(on_shutdown)
        return None

    agent.add_activation(hook)
    return st


async def _start(mgr: SubscriptionManager) -> None:
    """Run ``mgr.start()``, logging (not raising) on subscription failure.

    Raising here would only crash a background task; the agent's HTTP surface
    (and the demo's ``/agent/run`` path) should stay up even if the MCP server is
    briefly unreachable. The refresh loop, once started, retries on its own.
    """
    try:
        await mgr.start()
    except _START_ERRORS as exc:
        logger.error("initial subscription failed: %s", exc)
