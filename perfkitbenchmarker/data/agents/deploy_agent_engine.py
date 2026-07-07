"""Deploys an agent to Vertex AI Agent Engine.

This is usually called on Agent Service's creation.
"""

import argparse
import asyncio
import importlib
import os
import time
from typing import Any

import common_utils
import pydantic
import vertexai
import yaml

os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "1"
if "GOOGLE_CLOUD_PROJECT" not in os.environ:
  raise ValueError("GOOGLE_CLOUD_PROJECT environment variable must be set")
if "GOOGLE_CLOUD_LOCATION" not in os.environ:
  raise ValueError("GOOGLE_CLOUD_LOCATION environment variable must be set")
os.environ["GOOGLE_API_USE_CLIENT_CERTIFICATE"] = "false"


class BaseDeploymentConfig(pydantic.BaseModel):
  """Basic configuration to determine agent and framework."""

  agent: str
  framework: str


class DeploymentConfig[AgentConfigT](BaseDeploymentConfig):
  """Full configuration for agent deployment."""

  staging_bucket: str
  run_uri: str
  agent_config: AgentConfigT
  initial_prompt: str | None = None


def _import_agent_module(agent: str, framework: str) -> Any:
  """Imports the agent module."""
  module_name = f"{agent}_{framework}"
  return importlib.import_module(module_name)


async def _measure_time_to_ready[AgentConfigT](
    remote_agent: Any,
    handler: Any,
    config: DeploymentConfig[AgentConfigT],
) -> float | None:
  """Measures the time to first chunk."""
  print("Sending initial prompt...")
  endpoint = handler.create_endpoint(remote_agent)
  prompt_config = common_utils.PromptConfig[
      AgentConfigT
  ].create_for_initial_prompt(config)
  first_chunk_time = None
  try:
    async for _ in endpoint.stream_execute(prompt_config=prompt_config):
      if first_chunk_time is None:
        first_chunk_time = time.monotonic()
    return first_chunk_time
  except Exception as e:  # pylint: disable=broad-exception-caught
    # Since the agent has already been created, it's better to let this script
    # finish normally, so the resource is marked as created and can be cleaned
    # up normally.
    print(f"Error measuring time to ready: {e}")
    return None


def run_deployment[AgentConfigT](
    config: DeploymentConfig[AgentConfigT], module: Any
) -> None:
  """Deploys the agent to Vertex AI Agent Engine."""
  agent = config.agent
  framework = config.framework
  staging_bucket = config.staging_bucket

  handler_class: Any = module.AgentHandler
  handler = handler_class()
  agent_to_deploy = handler.get_deployable_agent(
      config.agent_config, config.run_uri
  )

  vertexai.init(
      project=os.environ["GOOGLE_CLOUD_PROJECT"],
      location=os.environ["GOOGLE_CLOUD_LOCATION"],
  )

  display_name = f"{agent}_{framework}_agent"
  print(f"Deploying {display_name} to Vertex AI Agent Engine...")
  client = vertexai.Client()

  wheel_name = f"dist/{agent}_{framework}-0.1.0-py3-none-any.whl"
  if not os.path.exists(wheel_name):
    raise ValueError(f"Wheel file not found at {wheel_name}")

  deploy_config = {
      "staging_bucket": staging_bucket,
      "requirements": [f"./{wheel_name}"],
      "extra_packages": [wheel_name],
      "display_name": display_name,
  }

  create_start = time.monotonic()
  remote_agent = client.agent_engines.create(
      agent=agent_to_deploy,
      config=deploy_config,
  )
  create_time = time.monotonic() - create_start
  print(f"Time to Create: {create_time}")

  print("Successfully deployed Agent Engine!")
  print(f"Resource name: {remote_agent.api_resource.name}")

  if config.initial_prompt:
    first_chunk_time = asyncio.run(
        _measure_time_to_ready(remote_agent, handler, config)
    )
    if first_chunk_time is not None:
      ready_time = first_chunk_time - create_start
      print(f"Time to Ready: {ready_time}")


def main() -> None:
  parser = argparse.ArgumentParser(
      description="Deploy agents to Vertex AI Agent Engine."
  )
  parser.add_argument(
      "--config_file",
      type=str,
      required=True,
      help=(
          "Path to the YAML configuration file containing staging_bucket,"
          " agent, and framework."
      ),
  )
  args = parser.parse_args()

  with open(args.config_file, "r") as f:
    raw_config = yaml.safe_load(f)

  base_config = BaseDeploymentConfig(**raw_config)
  module = _import_agent_module(base_config.agent, base_config.framework)
  config = DeploymentConfig[module.AgentHandler.AGENT_CONFIG_CLS](**raw_config)
  run_deployment(config, module)


if __name__ == "__main__":
  main()
