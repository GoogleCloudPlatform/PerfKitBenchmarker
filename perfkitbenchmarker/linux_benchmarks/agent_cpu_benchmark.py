"""Agent CPU benchmark (Swe-bench) for PerfKitBenchmarker."""

import json
import logging
from typing import Any
import urllib.error
import urllib.request

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util as gcp_util

BENCHMARK_NAME = 'agent_cpu'
BENCHMARK_CONFIG = """
agent_cpu:
  description: Solves a SWE-bench problem on a VM using an LLM
  flags:
    gcloud_scopes: cloud-platform
  vm_groups:
    default:
      os_type: ubuntu2404
      vm_spec:
        GCP:
          machine_type: c4a-standard-4
          zone: us-central1-a
"""

WORKLOAD_TEMPLATE = 'agent_cpu_workload.py.j2'
WORKLOAD_SCRIPT = 'agent_cpu_workload.py'
WORKLOAD_LOG = 'agent_workload_live.log'

flags.DEFINE_string(
    'swebench_task_id',
    'django__django-11299',
    'SWE-bench task instance identifier.',
)

FLAGS = flags.FLAGS


@vm_util.Retry(max_retries=10, retryable_exceptions=(urllib.error.HTTPError,))
def FetchSwebenchTaskSpec(task_id: str) -> dict[str, Any]:
  """Fetches task specifications programmatically from HuggingFace REST API."""
  url = (
      'https://datasets-server.huggingface.co/filter?'
      'dataset=princeton-nlp/SWE-bench&config=default&split=test&'
      f'where=%22instance_id%22%3D%27{task_id}%27'
  )
  request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
  with urllib.request.urlopen(request) as response:
    rows = json.loads(response.read().decode())['rows']
    return rows[0]['row']


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns the benchmark configuration dictionary."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: Any) -> None:
  """Prepares the VM environment for running the agent CPU workload."""
  vm = benchmark_spec.vms[0]
  task_spec = FetchSwebenchTaskSpec(FLAGS.swebench_task_id)

  repo_path = task_spec['repo']
  repo_name = repo_path.split('/')[-1]
  repo_url = f'https://github.com/{repo_path}.git'
  base_commit = task_spec['base_commit']

  vm.InstallPackages('python3-pip python3-venv python3-dev git')
  vm.RemoteCommand(
      'pip install google-genai sqlparse asgiref tblib --break-system-packages'
  )
  vm.RemoteCommand(
      f'rm -rf {repo_name} && git clone {repo_url} {repo_name} && cd'
      f' {repo_name} && git checkout {base_commit}'
  )


def Run(benchmark_spec: Any) -> list[sample.Sample]:
  """Runs the agent CPU workload and collects metrics."""
  vm = benchmark_spec.vms[0]
  task_spec = FetchSwebenchTaskSpec(FLAGS.swebench_task_id)
  repo_name = task_spec['repo'].split('/')[-1]

  context = {
      'task_id': FLAGS.swebench_task_id,
      'repo_directory': repo_name,
      'problem_statement': task_spec['problem_statement'],
      'gcp_project': FLAGS.project or gcp_util.GetDefaultProject(),
      'gcp_location': 'us-east1',  # Better availablility than us-central1
      'model_name': 'gemini-2.5-flash',
      'max_turns': 50,
  }
  vm.RenderTemplate(
      data.ResourcePath(WORKLOAD_TEMPLATE), WORKLOAD_SCRIPT, context
  )
  vm.RemoteCommand(f'cat {WORKLOAD_SCRIPT}')
  tail_cmd = (
      f'ssh -o StrictHostKeyChecking=no -i {vm.ssh_private_key} '
      f'-p {vm.ssh_port} {vm.user_name}@{vm.ip_address} '
      f'"tail -f {WORKLOAD_LOG}"'
  )
  logging.info(
      'To monitor live agent output, run in a new terminal:\n%s', tail_cmd
  )
  workload_log_stdout = vm.RemoteCommand(
      f'python3 -u {WORKLOAD_SCRIPT} | tee {WORKLOAD_LOG}'
  )
  vm.PullFile(vm_util.GetTempDir(), WORKLOAD_LOG)
  vm.PullFile(vm_util.GetTempDir(), WORKLOAD_SCRIPT)
  # TODO(user): Add unique error for resource exhausted
  if '429 RESOURCE_EXHAUSTED' in workload_log_stdout:
    raise errors.Error(
        'Resource exhausted. Please try again later. Please refer to'
        ' https://cloud.google.com/vertex-ai/generative-ai/docs/error-code-429'
    )
  stdout, _ = vm.RemoteCommand('cat telemetry_results.json')
  sample_metadata = json.loads(stdout)
  sample_metadata['task_id'] = FLAGS.swebench_task_id
  sample_metadata['repo'] = task_spec['repo']
  sample_metadata['base_commit'] = task_spec['base_commit']
  sample_metadata['vertex_region'] = context['gcp_location']
  sample_metadata['model_name'] = context['model_name']

  return [
      sample.Sample(
          'wall_time',
          float(sample_metadata['elapsed_wall_time_seconds']),
          'seconds',
          sample_metadata,
      ),
      sample.Sample(
          'cpu_time',
          float(sample_metadata['total_cpu_seconds']),
          'seconds',
          sample_metadata,
      ),
      sample.Sample(
          'turns',
          int(sample_metadata['turns_taken']),
          'turns',
          sample_metadata,
      ),
  ]


def Cleanup(benchmark_spec: Any) -> None:
  """Cleans up the VM after benchmark completion."""
  vm = benchmark_spec.vms[0]
  vm.RemoteCommand('rm -rf *')
