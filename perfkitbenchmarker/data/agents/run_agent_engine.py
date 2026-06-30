"""Sends a prompt to an agent remotely deployed on Vertex AI Agent Engine.

This is usually called during PKB's Run stage.
"""

import argparse
import asyncio
import importlib
import os
from typing import Any

from absl import logging
import common_utils
import vertexai
import yaml

logging.set_verbosity(logging.INFO)
logging.set_stderrthreshold(logging.INFO)


class RunAgentEnginePrompt[AgentConfigT](
    common_utils.PromptConfig[AgentConfigT]
):
  """Configuration for running an agent remotely."""

  agent_engine_id: str


def _import_agent_module(agent: str, framework: str) -> Any:
  """Imports the agent module based on configuration."""
  module_name = f"{agent}_{framework}"
  return importlib.import_module(module_name)


async def run_remote_agent[AgentConfigT](
    config: RunAgentEnginePrompt[AgentConfigT], module: Any
) -> None:
  """Runs the agent remotely on Vertex AI Agent Engine."""

  project_id: str | None = os.environ.get("GOOGLE_CLOUD_PROJECT")
  location: str | None = os.environ.get("GOOGLE_CLOUD_LOCATION")

  if not project_id or not location:
    raise ValueError(
        "GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION env vars must be set"
    )

  vertexai.init(project=project_id, location=location)
  client: Any = vertexai.Client()

  print(f"Fetching remote agent: {config.agent_engine_id}...")
  deployed_ref: Any = client.agent_engines.get(name=config.agent_engine_id)

  if not config.prompt:
    raise ValueError("Prompt cannot be empty. Please provide a valid prompt.")

  handler_class: Any = module.AgentHandler
  handler = handler_class()
  endpoint = handler.create_endpoint(deployed_ref)
  response_text: Any = await handler.execute(endpoint, config)

  # Export results (empty metrics for now as it's remote execution)
  if config.output_dir:
    handler.export_results(config.output_dir, response_text, {})
  else:
    raise ValueError("output_dir must be set for remote execution.")


def main() -> None:
  parser = argparse.ArgumentParser(
      description="Run agent remotely on Vertex AI Agent Engine."
  )
  parser.add_argument(
      "--config_file",
      type=str,
      required=True,
      help="Path to the YAML configuration file.",
  )
  args = parser.parse_args()

  with open(args.config_file, "r") as f:
    raw_config = yaml.safe_load(f)

  base_config = common_utils.BasePromptConfig(**raw_config)
  module = _import_agent_module(base_config.agent, base_config.framework)
  config = RunAgentEnginePrompt[module.AgentHandler.AGENT_CONFIG_CLS](
      **raw_config
  )
  asyncio.run(run_remote_agent(config, module))


if __name__ == "__main__":
  main()
