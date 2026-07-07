"""Coding Agent Agentic framework benchmark.

Tests the performance (latency, correctness) of agentic frameworks (like ADK)
via coding tasks (SWE-bench).
"""

from collections.abc import Iterable
import json
import logging
import os
import textwrap
from typing import Any, override
import urllib.parse

from absl import flags
from perfkitbenchmarker import ai_agent_benchmark_helper as agent
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'coding_agent'
BENCHMARK_CONFIG = """
coding_agent:
  description: >
    Coding Agent Agentic framework benchmark.
  flags:
    gcloud_scopes: cloud-platform
  vm_groups:
    clients:
      vm_spec:
        GCP:
          machine_type: n2-standard-32
          zone: us-central1-f
      vm_count: 1
  ai_agent_service:
    cloud: GCP
    deployment_type: agent_engine
    agent: coding_agent
    framework: adk
    model: gemini-3-flash-preview
"""

BENCHMARK_DATA = {}

FLAGS = flags.FLAGS


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


class CodingAgent(agent.BaseAgent):
  """Agent for SWE-bench-like tasks."""

  _SWEBENCH_DATUM_PATH = 'agents/coding_agent/prepare/swebench_datum.json'

  @override
  def GetPrompts(self) -> Iterable[agent.Prompt]:
    """Generates prompts for the agent."""
    datum_path = data.ResourcePath(self._SWEBENCH_DATUM_PATH)
    with open(datum_path, 'r') as f:
      datum = json.load(f)

    prompt_id = 'default'
    output_dir = os.path.join(self.agent_service.base_dir, 'output', prompt_id)  # pyrefly: ignore[missing-attribute]
    prompt_text = self._ConstructPrompt(datum, output_dir)
    return [agent.Prompt(id=prompt_id, session_id=prompt_id, text=prompt_text)]

  @property
  @override
  def agent_name(self) -> str:
    return 'coding_agent'

  def _ConstructPrompt(self, datum: dict[str, Any], output_dir: str) -> str:
    """Constructs the prompt for the agent."""
    repo = datum.get('repo')
    base_commit = datum.get('base_commit')
    env_setup_commit = datum.get('environment_setup_commit', base_commit)
    problem_statement = datum['problem_statement']

    parsed_url = urllib.parse.urlparse(output_dir)
    bucket_name = parsed_url.netloc
    blob_prefix = parsed_url.path.strip('/')

    return textwrap.dedent(f"""
      Before solving the issue, you MUST set up the environment by performing the following steps in order:
      1. Configure git dummy user name and email:
         - `git config --global user.name "Agent"`
         - `git config --global user.email "agent@google.com"`
      2. Clone the repository `https://github.com/{repo}.git` into a directory named `workspace_repo`.
         - Example: `git clone https://github.com/{repo}.git workspace_repo`
      3. Check out the environment setup commit `{env_setup_commit}`.
         - Use `run_shell_command` with `cwd='workspace_repo'`.
      4. Set up a virtual environment named `venv` inside `workspace_repo` and install dependencies.
      5. Check out the base commit `{base_commit}` for the actual agent run.

      Remember that, you can write unit tests for debugging purpose, but you should not include
      these tests in the final patch submission mentioned below.

      After you have solved the issue and made a commit, generate a diff patch of your changes and upload it to GCS:
         - Create a patch file, e.g., `fix.patch`, containing the diff of your changes compared to the base commit.
         - Upload it to GCS using `upload_to_gcs` tool with:
           - bucket_name: '{bucket_name}'
           - source_file_name: 'fix.patch'
           - destination_blob_name: '{blob_prefix}/fix.patch'

      Solve the following issue:

      {problem_statement}
    """).lstrip()

  @override
  def PostPrepare(self) -> None:
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand('sudo apt-get install -y python3-venv')

    # Datum path pushed to VM for validation
    datum_path = data.ResourcePath(self._SWEBENCH_DATUM_PATH)
    self.client_vm.RemoteCommand('mkdir -p prepare')
    self.client_vm.PushDataFile(datum_path, 'prepare/swebench_datum.json')

  @override
  def RunValidationLogic(
      self,
      prompt: agent.Prompt,
      results: agent.PromptResults,
  ) -> tuple[float, list[sample.Sample]]:
    """Executes agent-specific validation logic on the client VM."""
    validator_vm_path = f'validator/{self.agent_name}_validator.py'
    success = 0.0
    samples = []
    patch_vm_path = self.DownloadArtifact(prompt, results, 'resulting_patch')
    try:
      # The validator needs the datum to know which repo/tests to run
      stdout, _ = self.client_vm.RemoteCommand(
          f'python3 {validator_vm_path} --patch_path {patch_vm_path}'
          ' --datum_path prepare/swebench_datum.json'
      )
      for line in stdout.splitlines():
        if line.startswith('PASSED:'):
          success = 1.0
          break
        if line.startswith('FAILED:'):
          break
      else:
        logging.error('Failed to parse validator output.')
    except errors.VirtualMachine.RemoteCommandError:
      logging.exception('Validation failed.')

    return success, samples


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Configures the VM before running."""
  CodingAgent.GetOrCreateFromSpec(spec).Prepare()


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark."""
  return CodingAgent.GetOrCreateFromSpec(spec).Run()


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleans up the resources."""
  CodingAgent.GetOrCreateFromSpec(spec).Cleanup()
