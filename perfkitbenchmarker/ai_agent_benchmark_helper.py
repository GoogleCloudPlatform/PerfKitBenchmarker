"""Base class and utilities for agents."""

import abc
import dataclasses
import json
import logging
import os
import time
from typing import Any, Iterable, Self

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker.resources import ai_agent_service

_FRAMEWORK = flags.DEFINE_enum(
    'agentic_framework',
    'adk',
    ['adk'],
    'Which agentic framework to measure.',
)

_MODEL = flags.DEFINE_string(
    'agent_model',
    'gemini-3-flash-preview',
    'Which model to use under the framework.',
)

_MODEL_LOCATION = flags.DEFINE_string(
    'agent_model_location',
    None,
    'Which location to use for the model under the framework. If not set,'
    ' defaults to the VM region.',
)


@dataclasses.dataclass
class Prompt:
  """Prompt configuration and state."""

  id: str
  session_id: str
  text: str
  user_id: str = 'pkb'
  agent_config: dict[str, Any] | None = None
  abort_on_validation_failure: bool = True


@dataclasses.dataclass
class PromptResults:
  metrics: dict[str, float | None]
  artifacts: dict[str, str]


class MissingArtifactError(Exception):
  """Raised when a required artifact is missing."""


def _FetchOutputFromObjectStorage(
    agent_service: ai_agent_service.BaseAiAgentService, output_dir: str
) -> PromptResults:
  local_output_path = os.path.join(temp_dir.GetRunDirPath(), 'output')
  local_results_path = os.path.join(local_output_path, 'results.json')
  agent_service.storage_service.Copy(
      os.path.join(output_dir, 'results.json'), os.path.join(local_results_path)
  )
  with open(local_results_path) as f:
    results_dict = json.load(f)
  return PromptResults(
      metrics=results_dict['metrics'],
      artifacts=results_dict['artifacts'],
  )


class BaseAgent(abc.ABC):
  """Base class for agents."""

  @classmethod
  def GetOrCreateFromSpec(cls, spec: benchmark_spec.BenchmarkSpec) -> Self:
    # This is ugly, but allows us to save state across stages.
    agent = getattr(spec, 'agentic_workload', None)
    if agent is None:
      agent = cls(spec)
      spec.agentic_workload = agent  # pyrefly: ignore[missing-attribute]
    return agent

  def __init__(self, spec: benchmark_spec.BenchmarkSpec):
    self.spec = spec
    self.client_vm = spec.vms[0]
    self.agent_service = spec.ai_agent_service
    self.framework = _FRAMEWORK.value
    self.model = _MODEL.value
    self.model_location = _MODEL_LOCATION.value
    self.metrics_metadata = {
        'framework': self.framework,
        'model': self.model,
        'agent': self.agent_name,
        'agent_deployment': self.agent_service.DEPLOYMENT_TYPE,  # pyrefly: ignore[missing-attribute]
    }

  def BeforeCreateAgent(self) -> None:
    """Hook for subclasses to run stuff before Agent Service creation."""

  def UploadValidatorScript(self) -> None:
    """Uploads validator script to Client VM."""
    validator_rel_path = (
        f'agents/{self.agent_name}/validator/{self.agent_name}_validator.py'
    )
    self.client_vm.RemoteCommand('mkdir -p validator')
    self.client_vm.PushDataFile(
        data.ResourcePath(validator_rel_path),
        os.path.join('validator', os.path.basename(validator_rel_path)),
    )

  def PostPrepare(self) -> None:
    """Hook for actions after Agent creation and other common steps."""

  def Prepare(self) -> None:
    """Configures the VM before running."""
    self.agent_service.agent_config = self._GetDefaultAgentConfig()  # pyrefly: ignore[missing-attribute]

    self.BeforeCreateAgent()
    self.spec.always_call_cleanup = True  # pyrefly: ignore[missing-attribute]
    self.agent_service.Create()  # pyrefly: ignore[missing-attribute]
    self.UploadValidatorScript()
    self.PostPrepare()

  def Run(self) -> list[sample.Sample]:
    """Runs the benchmark."""
    raw_samples = self._RunWorkload()
    return self.ProcessSamples(raw_samples)

  def _RunWorkload(self) -> list[sample.Sample]:
    """Iterates over prompts and executes/validates them."""
    samples = []
    for prompt in self.GetPrompts():
      results, exec_samples = self.ExecutePrompt(prompt)
      samples.extend(exec_samples)
      val_samples = self.ValidatePrompt(prompt, results)
      samples.extend(val_samples)

      success_score = 0.0
      for s in val_samples:
        if s.metric == 'success':
          success_score = s.value
          break

      if prompt.abort_on_validation_failure and success_score < 1.0:
        break

    return samples

  def ProcessSamples(self, samples: list[sample.Sample]) -> list[sample.Sample]:
    """A hook to reduce, filter, or aggregate metrics."""
    return samples

  def _GetDefaultAgentConfig(self) -> dict[str, Any]:
    """Returns the default agent configuration."""
    return {
        'model': self.model,
    }

  def ExecutePrompt(
      self, prompt: Prompt
  ) -> tuple[PromptResults, list[sample.Sample]]:
    """Resolves config, executes prompt, and fetches results."""
    agent_config = (
        prompt.agent_config
        if prompt.agent_config is not None
        else self._GetDefaultAgentConfig()
    )
    output_path = os.path.join(self.agent_service.base_dir, 'output', prompt.id)  # pyrefly: ignore[missing-attribute]

    start_time = time.monotonic()
    self.agent_service.Execute(  # pyrefly: ignore[missing-attribute]
        output_path,
        prompt=prompt.text,
        agent_config=agent_config,
        session_id=prompt.session_id,
        user_id=prompt.user_id,
    )
    job_wall_time = time.monotonic() - start_time

    prompt_results = _FetchOutputFromObjectStorage(
        self.agent_service, output_path  # pyrefly: ignore[bad-argument-type]
    )

    samples = []
    metadata = self.metrics_metadata.copy()
    metadata['prompt_id'] = prompt.id
    if 'model' in agent_config:
      metadata['model'] = agent_config['model']

    samples.append(
        sample.Sample('job_wall_time', job_wall_time, 'seconds', metadata)
    )

    if 'otel_llm_latency' in prompt_results.metrics:
      samples.append(
          sample.Sample(
              'llm_latency',
              prompt_results.metrics['otel_llm_latency'],
              'seconds',
              metadata,
          )
      )
    if 'otel_tool_latency' in prompt_results.metrics:
      samples.append(
          sample.Sample(
              'tool_latency',
              prompt_results.metrics['otel_tool_latency'],
              'seconds',
              metadata,
          )
      )
    if 'generation_wall_time' in prompt_results.metrics:
      samples.append(
          sample.Sample(
              'generation_wall_time',
              prompt_results.metrics['generation_wall_time'],
              'seconds',
              metadata,
          )
      )
    return prompt_results, samples

  def _GetScratchDir(self, prompt: Prompt) -> str:
    """Centralized environment manager controls this sterile path."""
    return f'/tmp/agentic_scratch/{prompt.id}'

  def DownloadArtifact(
      self,
      prompt: Prompt,
      results: PromptResults,
      artifact_key: str,
      filename: str | None = None,
  ) -> str:
    """Downloads an artifact to the scratch directory and returns its full path.

    This helper automates path isolation (preventing collisions across prompt
    runs) and allows optional filename deduction from the artifact's remote
    path.

    Args:
      prompt: The Prompt context triggering this download. Used to isolate
        scratch space.
      results: The results descriptor containing the pointers published by the
        agent run.
      artifact_key: The semantic identifier of the artifact (e.g. 'answer').
      filename: Optional target filename on the VM. If None, the filename is
        extracted automatically from the remote object's path basename.

    Returns:
      The absolute path where this artifact resides on the Client VM.

    Raises:
      MissingArtifactError: If the specified artifact_key is absent in Results
      Registry.
    """
    if filename is None:
      if not results.artifacts or not results.artifacts.get(artifact_key):
        raise MissingArtifactError(
            f'No {artifact_key} generated for validation.'
        )
      remote_path = results.artifacts[artifact_key]
      filename = os.path.basename(remote_path)

    scratch_dir = self._GetScratchDir(prompt)
    self.client_vm.RemoteCommand(f'mkdir -p {scratch_dir}')
    local_path = os.path.join(scratch_dir, filename)
    self.FetchArtifact(results, artifact_key, local_path)
    return local_path

  def ValidatePrompt(
      self, prompt: Prompt, results: PromptResults
  ) -> list[sample.Sample]:
    """Wraps the validation lifecycle."""
    metadata = self.metrics_metadata.copy()
    metadata['prompt_id'] = prompt.id
    if prompt.agent_config and 'model' in prompt.agent_config:
      metadata['model'] = prompt.agent_config['model']

    try:
      grade, extra_samples = self.RunValidationLogic(prompt, results)
    except MissingArtifactError as e:
      logging.error(e)
      return [sample.Sample('success', 0.0, '', metadata)]

    for s in extra_samples:
      s.metadata['prompt_id'] = prompt.id

    extra_samples.append(sample.Sample('success', grade, '', metadata))
    return extra_samples

  @abc.abstractmethod
  def GetPrompts(self) -> Iterable[Prompt]:
    """Generates prompts for the agent."""
    raise NotImplementedError

  @abc.abstractmethod
  def RunValidationLogic(
      self, prompt: Prompt, results: PromptResults
  ) -> tuple[float, list[sample.Sample]]:
    """Executes agent-specific validation logic on the client VM."""
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def agent_name(self) -> str:
    """Returns the agent name."""
    raise NotImplementedError

  def FetchArtifact(
      self,
      prompt_results: PromptResults,
      artifact_key: str,
      local_path: str,
  ) -> None:
    """Fetches an artifact from object storage to the client VM."""
    if not prompt_results.artifacts or not prompt_results.artifacts.get(
        artifact_key
    ):
      raise MissingArtifactError(f'No {artifact_key} generated for validation.')

    object_storage_path = prompt_results.artifacts[artifact_key]

    # TODO(odiego): Make this cloud agnostic.
    self.client_vm.RemoteCommand(
        f'gcloud storage cp {object_storage_path} {local_path}'
    )

  def Cleanup(self):
    """Hook for cleanup. Deletes Agent Service resource by default."""
    self.agent_service.Delete()  # pyrefly: ignore[missing-attribute]
