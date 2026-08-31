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
from typing import Any, override

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import provider_info

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
LOOKER_CA_DATA_AGENT = flags.DEFINE_string(
    'looker_ca_data_agent',
    None,
    'The full resource name or ID of the Conversational Analytics data agent'
    ' to use for Looker.',
)

LOOKER_PYTHON_CLIENT_FILE = 'looker_python_driver.py'
LOOKER_CA_CLIENT_FILE = 'looker_ca_driver.py'
LOOKER_PYTHON_CLIENT_DIR = 'edw/looker/clients/python'
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
