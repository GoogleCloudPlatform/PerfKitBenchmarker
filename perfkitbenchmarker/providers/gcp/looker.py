# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module containing class for Looker EDW service."""

import copy
import json
import os
import shlex
from typing import Any, override

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.linux_packages import mcp_toolbox_for_db

FLAGS = flags.FLAGS

LOOKER_BASE_URL = flags.DEFINE_string(
    'looker_base_url',
    None,
    'The base URL of the Looker instance (e.g. https://instance.looker.com).',
)
LOOKER_MODEL_NAME = flags.DEFINE_string(
    'looker_model_name',
    None,
    'Looker model name for executing SQL queries.',
)
LOOKER_CLIENT_ID = flags.DEFINE_string(
    'looker_client_id',
    None,
    'Looker API Client ID.',
)
LOOKER_CA_CLIENT = flags.DEFINE_enum(
    'looker_ca_client',
    'looker_data_agent',
    ['looker_data_agent', 'claude'],
    'The conversational analytics client to use for Looker.',
)
LOOKER_CA_DATA_AGENT = flags.DEFINE_string(
    'looker_ca_data_agent',
    None,
    'The full resource name or ID of the Conversational Analytics data agent'
    ' to use for Looker.',
)
LOOKER_CA_CLAUDE_MODEL = flags.DEFINE_string(
    'looker_ca_claude_model',
    '',
    'Model to use for Claude Client in Conversational Analytics benchmarking.',
)

LOOKER_PYTHON_CLIENT_FILE = 'looker_python_driver.py'
LOOKER_CA_CLIENT_FILE = 'looker_ca_driver.py'
LOOKER_PYTHON_CLIENT_DIR = 'edw/looker/clients/python'
CLAUDE_PYTHON_DRIVER_FILE = 'claude_python_driver.py'
CLAUDE_CLIENT_SYSTEM_PROMPT = (
    'You are a helpful assistant. Use the available Looker MCP tools to'
    ' explore the LookML model, generate the SQL query (e.g. using'
    ' query_sql), and run the query to fetch the data (e.g. using query). Do'
    ' not attempt to run raw SQL directly or use external SDK scripts;'
    ' always execute queries through Looker MCP tools.\n\n'
    'You must generate your final response in JSON format. The JSON object'
    ' must contain the following keys:\n'
    '- "thoughts": A list of strings representing your thoughts or'
    ' reasoning steps.\n'
    '- "generated_sql": The SQL query you generated to answer the user\'s'
    ' question.\n'
    '- "retrieved_data": The data retrieved from running the query via'
    ' Looker MCP tools.\n'
    '- "text_answer": A text summary answering the user\'s question based on'
    ' the retrieved data.'
)
LOOKER_CLIENT_SECRET_FILE = 'looker_client_secret.txt'


def _LoadLookerClientSecret(
    secret_file: str = LOOKER_CLIENT_SECRET_FILE,
) -> str:
  """Load Looker client secret from file."""
  secret_path = data.ResourcePath(secret_file)
  with open(secret_path, 'r') as f:
    secret = f.read().strip()
  if not secret:
    raise ValueError(f'Looker client secret file at {secret_path} is empty.')
  return secret


class PythonClientInterface(edw_service.EdwClientInterface):
  """Python Client Interface class for Looker."""

  def __init__(
      self,
      base_url: str | None,
      model_name: str | None,
      client_id: str | None = None,
  ):
    super().__init__()
    self.base_url = base_url
    self.model_name = model_name
    self.client_id = client_id
    self.benchmark_name: str | None = None

  @override
  def SetProvisionedAttributes(self, benchmark_spec):
    super().SetProvisionedAttributes(benchmark_spec)
    self.benchmark_name = benchmark_spec.name

  @override
  def GetMetadata(self) -> dict[str, str]:
    """Get the Metadata attributes for the Client Interface."""
    return {
        'client': 'PYTHON',
        'looker_base_url': str(self.base_url),
        'looker_model_name': str(self.model_name),
    }

  def Prepare(self, package_name: str) -> None:
    """Prepare the client vm to execute query."""
    assert self.client_vm is not None

    self.client_vm.InstallPreprovisionedBenchmarkData(
        self.benchmark_name, [LOOKER_CLIENT_SECRET_FILE], '.'
    )

    # Install dependencies for driver
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand(
        'sudo apt-get -qq update && DEBIAN_FRONTEND=noninteractive sudo apt-get'
        ' -qq install python3.12-venv'
    )
    self.client_vm.RemoteCommand('python3 -m venv .venv')
    self.client_vm.RemoteCommand(
        'source .venv/bin/activate && pip install looker-sdk absl-py'
    )

    # Push driver script and python driver lib to client vm
    self.client_vm.PushDataFile(
        os.path.join(LOOKER_PYTHON_CLIENT_DIR, LOOKER_PYTHON_CLIENT_FILE)
    )
    self.client_vm.PushDataFile(
        os.path.join(
            edw_service.EDW_PYTHON_DRIVER_LIB_DIR,
            edw_service.EDW_PYTHON_DRIVER_LIB_FILE,
        )
    )

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
  ) -> tuple[float, dict[str, Any]]:
    """Execute a query and return performance details."""
    cmd = (
        f'.venv/bin/python {LOOKER_PYTHON_CLIENT_FILE} single'
        f' --base_url={self.base_url} --model_name={self.model_name}'
        f' --client_id={self.client_id}'
        f' --query_file={query_name}'
    )
    if print_results:
      cmd += ' --print_results'
    assert self.client_vm is not None
    stdout, _ = self.client_vm.RobustRemoteCommand(cmd)
    details = copy.copy(self.GetMetadata())
    details.update(json.loads(stdout)['details'])
    return json.loads(stdout)['query_wall_time_in_secs'], details


class ConversationalAnalyticsClientInterface(
    edw_service.BaseConversationalAnalyticsClientInterface,
    PythonClientInterface,
):
  """Conversational Analytics Client Interface subclassing PythonClientInterface."""

  def __init__(
      self,
      base_url: str | None,
      model_name: str | None,
      client_id: str | None = None,
  ):
    super().__init__(
        base_url=base_url, model_name=model_name, client_id=client_id
    )

  @property
  def fetches_results_immediately(self) -> bool:
    return True

  @override
  def Prepare(self, package_name: str) -> None:
    """Prepare the client vm by installing looker-sdk and pushing driver."""
    assert self.client_vm is not None
    # PythonClientInterface.Prepare should be called here to install
    # python dependencies and push common python driver files.
    super().Prepare(package_name)

    # Push Looker CA driver script from looker directory
    self.client_vm.PushDataFile(
        os.path.join(LOOKER_PYTHON_CLIENT_DIR, LOOKER_CA_CLIENT_FILE)
    )

  @override
  def _GetConversationalAnalyticsCommand(self, remote_query_file: str) -> str:
    """Return the CLI command to execute the driver script."""
    cmd = (
        f'.venv/bin/python {LOOKER_CA_CLIENT_FILE} single '
        f'--base_url={self.base_url} '
        f'--client_id={self.client_id} '
        f'--agent_id={LOOKER_CA_DATA_AGENT.value} '
        f'--print_results --query_file={remote_query_file}'
    )
    return cmd


# TODO(shuninglin): A lot of duplication with
# bigquery.ClaudeConversationalAnalyticsClientInterface, send a follow-up CL to
# refactor.
class ClaudeConversationalAnalyticsClientInterface(
    edw_service.BaseClaudeConversationalAnalyticsClientInterface,
):
  """ClaudeConversationalAnalyticsClientInterface for Looker."""

  def __init__(
      self,
      base_url: str | None,
      model_name: str | None,
      client_id: str | None = None,
  ):
    super().__init__()
    self.base_url = base_url
    self.model_name = model_name
    self.client_id = client_id
    self.python_client_interface = PythonClientInterface(
        base_url=base_url, model_name=model_name, client_id=client_id
    )
    self.claude_dir: str | None = None
    self.mcp_toolbox_path: str | None = None
    self.benchmark_name: str | None = None

  @override
  def SetProvisionedAttributes(
      self, benchmark_spec: bm_spec.BenchmarkSpec
  ) -> None:
    super().SetProvisionedAttributes(benchmark_spec)
    self.python_client_interface.SetProvisionedAttributes(benchmark_spec)
    self.benchmark_name = benchmark_spec.name

  def _ConstructClaudeSystemPrompt(self) -> str:
    """Construct the system prompt for Claude."""
    prompt = CLAUDE_CLIENT_SYSTEM_PROMPT
    if edw_service.CA_DATASET.value == 'ecomm':
      prompt += (
          '\nPlease only use Looker model "thelook_adwords" and Looker Explore'
          ' "events" as data source.'
      )
    elif edw_service.CA_DATASET.value == 'call_center':
      prompt += (
          '\nPlease only use Looker model "call_center" and Looker Explore'
          ' "transcript" as data source.'
      )
    return prompt

  def GetMcpConfig(self) -> str:
    if not self.mcp_toolbox_path:
      raise RuntimeError('mcp_toolbox_path is not set.')
    client_secret = _LoadLookerClientSecret()
    env = {
        'LOOKER_BASE_URL': self.base_url,
        'LOOKER_CLIENT_ID': self.client_id,
        'LOOKER_CLIENT_SECRET': client_secret,
        'LOOKER_VERIFY_SSL': 'true',
    }
    config = {
        'mcpServers': {
            'looker-toolbox': {
                'command': self.mcp_toolbox_path,
                'args': ['--prebuilt', 'looker', '--stdio'],
                'env': env,
            }
        }
    }
    return json.dumps(config)

  def GetClaudeDir(self) -> str:
    if not self.claude_dir:
      raise RuntimeError('claude_dir is not set.')
    return self.claude_dir

  @property
  def fetches_results_immediately(self) -> bool:
    return True

  @override
  def _GetQueryFileName(self, query_name: str) -> str:
    """Generate a filename from a query name inside Claude directory."""
    base_filename = os.path.basename(super()._GetQueryFileName(query_name))
    return os.path.join(self.GetClaudeDir(), base_filename)

  @override
  def InstallSdk(self) -> None:
    """Install the Claude Code SDK in the venv."""
    assert self.client_vm is not None
    self.client_vm.RemoteCommand(
        'source .venv/bin/activate && pip install claude-agent-sdk'
        ' python-dotenv'
    )

  @override
  def Prepare(self, package_name: str = '') -> None:
    """Prepare the client VM for Claude conversational analytics."""
    assert self.client_vm is not None

    home_dir = self.client_vm.RemoteCommand('echo $HOME')[0].strip()
    self.claude_dir = os.path.join(home_dir, 'claude_ca')
    self.client_vm.RemoteCommand(f'mkdir -p {self.claude_dir}')

    self.mcp_toolbox_path = mcp_toolbox_for_db.Install(self.client_vm)

    # Call python_client_interface.Prepare to setup common Python dependencies.
    self.python_client_interface.Prepare(package_name)

    # Call BaseClaude...Prepare to install Claude SDK and setup .mcp.json
    super().Prepare(package_name)

    # Push Claude driver script to claude_dir
    self.client_vm.PushDataFile(
        os.path.join(
            edw_service.EDW_PYTHON_DRIVER_LIB_DIR,
            CLAUDE_PYTHON_DRIVER_FILE,
        ),
        os.path.join(self.claude_dir, CLAUDE_PYTHON_DRIVER_FILE),
    )

    # Copy edw_python_driver_lib.py to claude_dir
    self.client_vm.RemoteCommand(
        f'cp {os.path.join(home_dir, edw_service.EDW_PYTHON_DRIVER_LIB_FILE)}'
        f' {self.claude_dir}'
    )

    # Install .env file to claude_dir
    if not self.benchmark_name:
      raise ValueError('benchmark_name not set, cannot install .env for Claude')
    self.client_vm.InstallPreprovisionedBenchmarkData(
        self.benchmark_name, ['.env'], self.claude_dir
    )

  @override
  def _GetConversationalAnalyticsCommand(self, remote_query_file: str) -> str:
    claude_dir = self.GetClaudeDir()

    cmd = (
        f'cd {claude_dir} && '
        'source ../.venv/bin/activate && '
        f'python3 {CLAUDE_PYTHON_DRIVER_FILE} single '
        f'--system_prompt={shlex.quote(self._ConstructClaudeSystemPrompt())} '
        '--allowed_tools=mcp__looker-toolbox__* '
    )

    if LOOKER_CA_CLAUDE_MODEL.value:
      cmd += f'--model={LOOKER_CA_CLAUDE_MODEL.value} '

    cmd += f'--print_results --query_file={remote_query_file}'
    return cmd


class Looker(edw_service.EdwService):
  """Object representing a Looker service."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'looker'
  QUERY_SET = 'looker'
  client_interface: PythonClientInterface

  def __init__(self, edw_service_spec):
    super().__init__(edw_service_spec)
    self.base_url: str | None = LOOKER_BASE_URL.value
    self.model_name: str | None = LOOKER_MODEL_NAME.value
    self.client_id: str | None = LOOKER_CLIENT_ID.value
    self.client_interface = PythonClientInterface(
        base_url=self.base_url,
        model_name=self.model_name,
        client_id=self.client_id,
    )

  def GetMetadata(self) -> dict[str, str]:
    """Return a dictionary of the metadata for the Looker service."""
    basic_data = super().GetMetadata()
    basic_data['looker_base_url'] = str(self.base_url)
    basic_data['looker_model_name'] = str(self.model_name)
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data

  @override
  def GetConversationalAnalyticsClientInterface(
      self,
  ) -> edw_service.BaseConversationalAnalyticsClientInterface:
    """Return the Conversational Analytics Client Interface instance."""
    if LOOKER_CA_CLIENT.value == 'claude':
      return ClaudeConversationalAnalyticsClientInterface(
          base_url=self.base_url,
          model_name=self.model_name,
          client_id=self.client_id,
      )
    return ConversationalAnalyticsClientInterface(
        base_url=self.base_url,
        model_name=self.model_name,
        client_id=self.client_id,
    )

  def IsUserManaged(self, edw_service_spec) -> bool:
    return True

  def _Create(self):
    raise NotImplementedError

  def _Exists(self):
    return True

  def _Delete(self):
    raise NotImplementedError
