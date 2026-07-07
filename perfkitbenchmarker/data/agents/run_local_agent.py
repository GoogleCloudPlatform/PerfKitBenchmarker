"""Generic script to run agent locally."""

import argparse
import asyncio
from collections.abc import Sequence
import importlib
from typing import Any

from absl import logging
import common_utils
from google.adk.telemetry import setup
from opentelemetry.sdk import trace
from opentelemetry.sdk.trace import export
from opentelemetry.sdk.trace.export import in_memory_span_exporter
import yaml

logging.set_verbosity(logging.INFO)
logging.set_stderrthreshold(logging.INFO)


class RunLocalPrompt[AgentConfigT](common_utils.PromptConfig[AgentConfigT]):
  """Configuration for running an agent locally."""

  pass


def _import_agent_module(agent: str, framework: str) -> Any:
  """Imports the agent module based on configuration."""
  if framework != "adk":
    raise ValueError(f"Unsupported framework: {framework}")
  module_name = f"{agent}_{framework}"
  return importlib.import_module(module_name)


def calculate_generic_metrics(
    spans: Sequence[trace.ReadableSpan],
) -> dict[str, Any]:
  """Calculates generic metrics like LLM and Tool latency from spans."""
  llm_latency = 0.0
  tool_latency = 0.0

  for span in spans:
    attributes = getattr(span, "attributes", None) or getattr(
        span, "labels", {}
    )
    op_name = attributes.get("gen_ai.operation.name")

    start = span.start_time
    end = span.end_time
    duration_sec = (
        (end - start).total_seconds()
        if hasattr(start, "timestamp")
        else (end - start) / 1e9
    )

    if op_name == "generate_content":
      llm_latency += duration_sec
    elif op_name == "execute_tool":
      tool_latency += duration_sec

  return {"otel_llm_latency": llm_latency, "otel_tool_latency": tool_latency}


async def run_local_agent[AgentConfigT](
    config: RunLocalPrompt[AgentConfigT], module: Any
) -> None:
  """Runs the agent locally."""
  span_exporter = in_memory_span_exporter.InMemorySpanExporter()
  hooks = setup.OTelHooks(
      span_processors=[export.SimpleSpanProcessor(span_exporter)]
  )
  setup.maybe_set_otel_providers(otel_hooks_to_setup=[hooks])

  handler_class: Any = module.AgentHandler
  handler = handler_class()
  agent_app = handler.get_local_agent(config.agent_config, config.run_uri)
  endpoint = handler.create_endpoint(agent_app)

  response_text: Any = await handler.execute(endpoint, config)

  spans = span_exporter.get_finished_spans()
  generic_metrics = calculate_generic_metrics(spans)

  handler.export_results(config.output_dir, response_text, generic_metrics)


def main() -> None:
  parser = argparse.ArgumentParser(description="Run agent locally.")
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
  config = RunLocalPrompt[module.AgentHandler.AGENT_CONFIG_CLS](**raw_config)
  asyncio.run(run_local_agent(config, module))


if __name__ == "__main__":
  main()
