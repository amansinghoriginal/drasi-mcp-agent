"""Process entry point for the Drasi MCP webhook agent.

Wires the three halves together and hands control to the DurableAgent runner:

* :func:`load_settings` reads the environment snapshot,
* :func:`build_agent` constructs the DurableAgent (LLM, tools, workflow state),
* :func:`install` registers the activation hook that mounts the webhook route and
  drives the subscription lifecycle,
* ``AgentRunner().serve(...)`` hosts the agent over FastAPI/uvicorn and blocks.

Run under a Dapr sidecar (see ``deploy/run-demo.sh``)::

    dapr run --app-id drasi-agent --app-port 8001 --resources-path resources \\
        -- uv run python -m drasi_mcp_agent.app
"""

from __future__ import annotations

import logging

from dapr_agents import AgentRunner

from .activation import install
from .agent import build_agent
from .config import load_settings

logger = logging.getLogger(__name__)


def main() -> None:
    """Build, wire, and serve the agent (blocks until the process is stopped)."""
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    logger.info(
        "starting drasi-mcp-agent: mcp_url=%s event=%s callback=%s use_llm=%s",
        settings.mcp_url,
        settings.event_name,
        settings.callback_url,
        settings.use_llm,
    )
    agent = build_agent(settings)
    install(agent, settings)
    AgentRunner().serve(agent, host=settings.app_host, port=settings.app_port)


if __name__ == "__main__":
    main()
