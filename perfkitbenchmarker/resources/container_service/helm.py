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
"""Methods to run helm commands against a cluster."""

import dataclasses
from typing import Any

from absl import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

_HELM = flags.DEFINE_string('helm', 'helm', 'Path to helm tool')


def RunHelmCommand(command: list[str], **kwargs: Any) -> tuple[str, str, int]:
  """Run a helm command with kubeconfig automatically injected."""
  kwargs = vm_util.IncrementStackLevel(**kwargs)

  # Ensure --kubeconfig is passed to Helm
  cmd = [
      _HELM.value,
  ]

  # Inject kubeconfig if available
  if 'kubeconfig' in FLAGS and FLAGS.kubeconfig:
    cmd.extend(['--kubeconfig', FLAGS.kubeconfig])

  cmd.extend(command)

  return vm_util.IssueCommand(cmd, **kwargs)


@dataclasses.dataclass
class HelmChart:
  """A single Helm chart to install via InstallCharts.

  Attributes:
    release_name: The Helm release name, e.g. 'dynamo-crds'.
    chart_ref: The chart reference: a repo URL, packaged .tgz URL, or local
      path.
    namespace: The Kubernetes namespace to install into.
    set_values: Extra --set key=value pairs to pass to `helm upgrade`.
    wait: Whether to pass --wait so the install blocks until resources are
      ready.
    atomic: Whether to pass --atomic so a failed install is rolled back.
  """

  release_name: str
  chart_ref: str
  namespace: str = 'default'
  set_values: dict[str, str] = dataclasses.field(default_factory=dict)
  wait: bool = True
  atomic: bool = True


def InstallCharts(charts: list[HelmChart]) -> None:
  """Installs (or upgrades) a list of Helm charts, in order."""
  for chart in charts:
    command = [
        'upgrade',
        '--install',
        chart.release_name,
        chart.chart_ref,
        '--namespace',
        chart.namespace,
    ]
    for key, value in chart.set_values.items():
      command.extend(['--set', f'{key}={value}'])
    if chart.wait:
      command.append('--wait')
    if chart.atomic:
      command.append('--atomic')
    RunHelmCommand(command)
