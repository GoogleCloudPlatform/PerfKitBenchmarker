"""Trivial agent benchmark which translates to pirate-speak.."""

from collections.abc import Iterable
from typing import Any, override

from absl import flags
from perfkitbenchmarker import ai_agent_benchmark_helper as agent
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'pirate_agent'
BENCHMARK_CONFIG = """
pirate_agent:
  description: >
    Pirate Agent benchmark.
  flags:
    gcloud_scopes: cloud-platform
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
  ai_agent_service:
    cloud: GCP
    deployment_type: agent_engine
    agent: pirate_agent
    framework: adk
    model: gemini-3-flash-preview
"""


FLAGS = flags.FLAGS


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


class PirateAgent(agent.BaseAgent):
  """Agent for translating to pirate."""

  @override
  def GetPrompts(self) -> Iterable[agent.Prompt]:
    """Generates prompts for the agent."""
    prompt_text = 'Hello, world! Can you tell me a story about cloud computing?'
    return [agent.Prompt(id='default', session_id='default', text=prompt_text)]

  @property
  @override
  def agent_name(self):
    return 'pirate_agent'

  @override
  def UploadValidatorScript(self) -> None:
    pass

  @override
  def RunValidationLogic(
      self,
      prompt: agent.Prompt,
      results: agent.PromptResults,
  ) -> tuple[float, list[sample.Sample]]:
    return 1.0, []


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Configures the VM before running."""
  PirateAgent.GetOrCreateFromSpec(spec).Prepare()


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark."""
  return PirateAgent.GetOrCreateFromSpec(spec).Run()


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup the resources."""
  PirateAgent.GetOrCreateFromSpec(spec).Cleanup()
