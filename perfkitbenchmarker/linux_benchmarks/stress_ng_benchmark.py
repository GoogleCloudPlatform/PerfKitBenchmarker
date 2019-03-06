# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs stress-ng.

From the stress-ng ubuntu documentation:
stress-ng will stress test a computer system in various selectable ways.
It was designed to exercise various physical subsystems of a computer as
well as the various operating system kernel interfaces. stress-ng also has
a wide range of CPU specific stress tests that exercise floating point,
integer, bit manipulation and control flow.

stress-ng manpage:
http://manpages.ubuntu.com/manpages/xenial/man1/stress-ng.1.html
"""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample


FLAGS = flags.FLAGS


BENCHMARK_NAME = 'stress_ng'
BENCHMARK_CONFIG = """
stress_ng:
  description: Runs stress-ng
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_50_gb
"""

flags.DEFINE_integer('stress_ng_duration', 10,
                     'Number of seconds to run the test.')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Installs stress-ng on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.InstallPackages('stress-ng')


def _ParseStressngResult(metadata, output):
  """Returns stress-ng data as samples for each stressor.

  Sample output eg:
  stress-ng: info:  [14369] dispatching hogs: 16 cpu
  stress-ng: info:  [14369] cache allocate: default cache size: 56320K
  stress-ng: info:  [14369] successful run completed in 10.05s
  ... stressor  bogo ops real time  usr time  sys time   bogo ops/s   bogo ops/s
  ...                   (secs)    (secs)    (secs)   (real time) (usr+sys time)
  ... cpu          26966     10.02    160.20      0.01      2690.74       168.32
  Args:
    metadata: metadata of the sample.
    output: the output of the stress-ng benchmark.


  """
  samples = []
  output_list = output.splitlines()
  output_matrix = [i.split() for i in output_list]
  assert output_matrix[2][-4] == 'bogo' and output_matrix[2][-3] == 'ops/s'
  assert output_matrix[3][-4] == '(real' and output_matrix[3][-3] == 'time)'

  for line in output_matrix[4:]:
    name = line[3]
    value = float(line[-2])  # parse bogo ops/s (real time)
    samples.append(sample.Sample(
        metric=name,
        value=value,
        unit='bogus_ops_sec',  # bogus operations per second
        metadata=metadata))

  return samples


def Run(benchmark_spec):
  """Runs stress-ng on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]

  metadata = {
      'duration_sec': FLAGS.stress_ng_duration,
      'threads': vm.num_cpus
  }

  cmd = ('stress-ng --class cpu,cpu-cache,memory --sequential {numthreads} '
         '--metrics-brief -t {duration}'.format(
             numthreads=vm.num_cpus,
             duration=FLAGS.stress_ng_duration))
  stdout, _ = vm.RobustRemoteCommand(cmd)

  return _ParseStressngResult(metadata, stdout)


def Cleanup(benchmark_spec):
  """Cleans up stress-ng from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Uninstall('stress-ng')
