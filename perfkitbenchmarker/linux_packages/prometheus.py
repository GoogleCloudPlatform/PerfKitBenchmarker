# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing a function to install Prometheus."""

import logging
import os
import re
from typing import List, TYPE_CHECKING

from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import kvrocks_server

if TYPE_CHECKING:
  from perfkitbenchmarker import linux_virtual_machine  # pylint: disable=g-import-not-at-top

PROMETHEUS_VERSION = '2.53.1'
PROMETHEUS_DIR = 'prometheus'
PROMETHEUS_CONF_JINJA = (
    'third_party/py/perfkitbenchmarker/data/kvrocks/prometheus.yaml.jinja2'
)


def Install(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Installs Prometheus on the VM."""
  vm.Install('wget')
  vm.RemoteCommand(f'mkdir -p {PROMETHEUS_DIR}')
  vm.RemoteCommand(f'sudo mkdir -p /{PROMETHEUS_DIR}')
  vm.RemoteCommand(f'chmod +rw {PROMETHEUS_DIR}')
  vm.RemoteCommand(f'sudo chmod a+rwx /{PROMETHEUS_DIR}')
  vm.RemoteCommand(
      'wget'
      f' https://github.com/prometheus/prometheus/releases/download/v{PROMETHEUS_VERSION}/prometheus-{PROMETHEUS_VERSION}.linux-amd64.tar.gz'
      f' -O {PROMETHEUS_DIR}/prometheus.tar.gz'
  )
  vm.RemoteCommand(
      f'tar -xzf {PROMETHEUS_DIR}/prometheus.tar.gz -C {PROMETHEUS_DIR} '
      '--strip-components=1'
  )
  vm.RemoteCommand(f'rm {PROMETHEUS_DIR}/prometheus.tar.gz')


def ConfigureAndStart(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine',
    server_vms: List['linux_virtual_machine.BaseLinuxVirtualMachine'],
) -> None:
  """Renders the config and starts Prometheus."""
  vm.PushDataFile(PROMETHEUS_CONF_JINJA, PROMETHEUS_DIR)

  kvrocks_instances = []
  for server_vm in server_vms:
    for i in range(1, kvrocks_server.NUM_KV_INSTANCES + 1):
      kvrocks_instances.append({
          'ip_address': server_vm.internal_ip,
          'port': kvrocks_server.INSTANCE_TO_PORT_MAP[i],
      })

  template_data = {
      'kvrocks_instances': kvrocks_instances,
      'kvrocks_exporter': {
          'ip_address': vm.internal_ip  # Exporter runs on the client VM
      },
  }
  conf_file = os.path.join(PROMETHEUS_DIR, 'prometheus.yml')
  vm.RenderTemplate(PROMETHEUS_CONF_JINJA, conf_file, template_data)

  # Start Prometheus
  vm.RemoteCommand(
      f'nohup {PROMETHEUS_DIR}/prometheus --config.file={conf_file} '
      '> /tmp/prometheus.log 2>&1 &'
  )


def WaitUntilHealthy(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine', timeout: int = 50
) -> None:
  """Waits for Prometheus to become healthy."""

  @vm_util.Retry(
      poll_interval=10,
      timeout=timeout,
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,),
  )
  def _WaitUntilHealthy():
    vm.RemoteCommand('curl -sSf http://localhost:9090/-/healthy')

  try:
    _WaitUntilHealthy()
    logging.info('Prometheus is healthy.')
  except vm_util.TimeoutExceededRetryError as e:
    raise errors.Benchmarks.PrepareException(
        f'Prometheus did not become healthy within {timeout}s.'
    ) from e


def _ParsePromtoolOutput(output: str) -> float | None:
  """Parses the output of promtool query instant."""
  # Example output for a metric:
  # "{map[]} => (2025-08-19 07:23:29.213532742 +0000 UTC, 12345.67)"
  match = re.search(r'\s+UTC,\s+([0-9.]+)\)', output)
  if match:
    return float(match.group(1))
  # Example output for a ratio: "0.12345"
  try:
    return float(output)
  except ValueError:
    logging.warning('Could not parse promtool output: %s', output)
    return None


def RunQuery(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine',
    query: str,
) -> float | None:
  """Runs a promtool query on a VM and returns the parsed result."""

  @vm_util.Retry(
      max_retries=3,
      poll_interval=5,
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,),
  )
  def _RunQuery():
    cmd = (
        f'{PROMETHEUS_DIR}/promtool query instant http://localhost:9090'
        f" '{query}'"
    )
    stdout, _ = vm.RemoteCommand(cmd)
    return _ParsePromtoolOutput(stdout.strip())

  try:
    return _RunQuery()
  except vm_util.RetriesExceededRetryError:
    logging.error('Promtool query failed after 3 attempts.')
    return None
