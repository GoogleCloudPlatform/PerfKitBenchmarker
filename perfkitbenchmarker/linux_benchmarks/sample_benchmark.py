# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Sample benchmark.

This benchmark demonstrates the use of preprovisioned data.

TODO(deitz): Expand this benchmark so that it describes how to add new
benchmarks to PerfKitBenchmarker and demonstrates more features.
"""

import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'sample'
BENCHMARK_CONFIG = """
sample:
  description: Runs a sample benchmark.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

# This benchmark uses preprovisioned data. Before running this benchmark on a
# given cloud, you must add the file 'sample_preprovisioned_data.txt' to that
# cloud by following the cloud-specific instructions. To create this file, run
# the following command:
#   echo "1234567890" > sample_preprovisioned_data.txt
#
# The value in this dict is the sha256sum hash for the file.
#
# The benchmark defines the constant BENCHMARK_DATA to contain the name of the
# file (which is prepended by the benchmark name followed by an underscore)
# mapped to the sha256sum hash. This ensures that when we change versions of the
# benchmark data or binaries, we update the code.
BENCHMARK_DATA = {
    'preprovisioned_data.txt':
        '4795a1c2517089e4df569afd77c04e949139cf299c87f012b894fccf91df4594'}


def GetConfig(user_config):
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepares the VMs and other resources for running the benchmark.

  This is a good place to download binaries onto the VMs, create any data files
  needed for a benchmark run, etc.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME, BENCHMARK_DATA,
                                        vm_util.VM_TMP_DIR)

  stdout, _ = vm.RemoteCommand('cat %s' % (posixpath.join(
      vm_util.VM_TMP_DIR, 'preprovisioned_data.txt')))
  assert stdout.strip() == '1234567890'


def Run(benchmark_spec):
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  del benchmark_spec
  # This is a sample benchmark that produces no meaningful data, but we return
  # a single sample as an example.
  metadata = dict()
  metadata['sample_metadata'] = 'sample'
  return [sample.Sample('sample_metric', 123.456, 'sec', metadata)]


def Cleanup(benchmark_spec):
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  del benchmark_spec
