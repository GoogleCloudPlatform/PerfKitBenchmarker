"""Data Science Agentic framework benchmark.

Tests the performance (latency, correctness) of agentic frameworks (like ADK)
via data science workloads.
"""

from collections.abc import Iterable
import logging
import os
from typing import Any, override
import urllib

from absl import flags
from perfkitbenchmarker import ai_agent_benchmark_helper as agent
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'data_science_agent'
BENCHMARK_CONFIG = """
data_science_agent:
  description: >
    Data Science Agentic framework benchmark.
  flags:
    gcloud_scopes: cloud-platform
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
  ai_agent_service:
    agent: data_science
"""


FLAGS = flags.FLAGS


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


class DataScienceAgent(agent.BaseAgent):
  """Agent for Data Analytics' tasks."""

  _MOCK_DATA_GEN_PATH = 'agents/data_science/prepare/mock_data_gen.py'

  def _ConstructPrompt(self, prompt_id: str):
    parsed_url = urllib.parse.urlparse(self.agent_service.base_dir)  # pyrefly: ignore[missing-attribute]
    data_bucket = parsed_url.netloc
    data_prefix = os.path.join(parsed_url.path, 'data')
    output_prefix = os.path.join(parsed_url.path, 'output', prompt_id)

    return f"""
    Find the most popular product among users aged 20-30 in the 'North' region.
    The data is split across two databases:

    1. SQLite database (queried via `query_sqlite`):
       - `users` table: `id`, `name`, `age`, `region`
       - `orders` table: `id`, `user_id`, `order_date`

    2. DuckDB database (queried via `query_duckdb`):
       - `order_details` table: `order_id`, `product_id`, `quantity`
       - `products` table: `id`, `name`, `category`

    You MUST use the multi-agent system as follows:
    1. Let these values be:
      - DATA_PREFIX: {data_prefix}
      - DATA_BUCKET: {data_bucket}
      - OUTPUT_PREFIX: {output_prefix}
    2. Instruct `db_expert` to use `download_from_gcs` to download the database
       files from GCS using the bucket and prefix found:
       - bucket: <value of DATA_BUCKET>,
         blob: <value of DATA_PREFIX>/logistics_analytical.db,
         file: logistics_analytical.db
       - bucket: <value of DATA_BUCKET>,
         blob: <value of DATA_PREFIX>/logistics_transactional.db,
         file: logistics_transactional.db
    3. Delegate to `db_expert` to query both databases and find the most popular
       product and its quantity.
    4. Delegate to `ds_analyst` to write the answer in the format 'product name:
       <name>, quantity: <total_quantity>' to
       'adk_data_science_results/answer.txt' using `python_repl`.
    5. FINALLY, the Supervisor MUST instruct `db_expert` to use `upload_to_gcs`
       to upload the result file back to GCS using the same bucket and prefix:
       - local path: adk_data_science_results/answer.txt,
       - storage bucket: <value of DATA_BUCKET>,
       - storage path: <value of OUTPUT_PREFIX>/answer.txt

    If the database files are already present, you don't need to download them
    again.

    At the very end of your final response (Supervisor's final message), you
    MUST output the bucket name and prefix used in the following format:
    GCS_BUCKET: <bucket_name>
    GCS_PREFIX: <prefix>
    """

  @override
  def PostPrepare(self) -> None:
    # install mock data gen script pre-requisites
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand('pip install duckdb')
    resource_path = data.ResourcePath(self._MOCK_DATA_GEN_PATH)
    self.client_vm.RemoteCommand('mkdir -p prepare')
    self.client_vm.PushDataFile(
        resource_path, os.path.join('prepare', 'mock_data_gen.py')
    )

    # run mock data gen
    self.client_vm.RemoteCommand(
        'python3 prepare/mock_data_gen.py --data_dir data'
    )

    # upload outputs
    remote_data_dir = os.path.join(self.agent_service.base_dir, 'data')  # pyrefly: ignore[missing-attribute]
    # TODO(odiego): Don't depend on gsutil explicitly
    self.client_vm.RemoteCommand(
        'gcloud storage cp data/logistics_analytical.db'
        f' {os.path.join(remote_data_dir, "logistics_analytical.db")}'
    )
    self.client_vm.RemoteCommand(
        'gcloud storage cp data/logistics_transactional.db'
        f' {os.path.join(remote_data_dir, "logistics_transactional.db")}'
    )

  @override
  def GetPrompts(self) -> Iterable[agent.Prompt]:
    """Generates prompts for the agent."""
    prompt_text = self._ConstructPrompt('default')
    return [agent.Prompt(id='default', session_id='default', text=prompt_text)]

  @property
  @override
  def agent_name(self):
    return 'data_science'

  @override
  def RunValidationLogic(
      self,
      prompt: agent.Prompt,
      results: agent.PromptResults,
  ) -> tuple[float, list[sample.Sample]]:
    validator_vm_path = f'validator/{self.agent_name}_validator.py'
    success = 0.0
    vm_answer_path = self.DownloadArtifact(prompt, results, 'answer')
    try:
      logging.info("Cat'ing Data Science Agent generated answer:")
      self.client_vm.RemoteCommand(f'cat {vm_answer_path}')
      stdout, _ = self.client_vm.RemoteCommand(
          f'python3 {validator_vm_path} --answer {vm_answer_path}'
      )
      for line in stdout.splitlines():
        if line.startswith('PASSED:'):
          success = 1.0
          break
        if line.startswith('FAILED:'):
          break
      else:
        logging.error('Failed to parse generated answers.')
    except errors.VirtualMachine.RemoteCommandError:
      logging.exception('Data science agent answer validation failed!')

    return success, []


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Configures the VM before running."""
  DataScienceAgent.GetOrCreateFromSpec(spec).Prepare()


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark."""
  return DataScienceAgent.GetOrCreateFromSpec(spec).Run()


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup the resources."""
  DataScienceAgent.GetOrCreateFromSpec(spec).Cleanup()
