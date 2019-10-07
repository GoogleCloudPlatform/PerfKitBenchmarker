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
"""OpenFOAM Benchmark.

OpenFOAM is a C++ toolbox for the development of customized numerical solvers,
and pre-/post-processing utilities for the solution of continuum mechanics
problems, most prominently including computational fluid dynamics.
https://openfoam.org/

This benchmark runs a motorbike simulation that is popularly used to measure
scalability of OpenFOAM across multiple cores. Since this is a complex
computation, make sure to use a compute-focused machine-type that has multiple
cores before attempting to run.

# TODO(liubrandon): Add support for multiple vms.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import logging
import posixpath
import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import openfoam


_DEFAULT_CASE = 'motorbike'
_CASE_PATHS = {
    'motorbike': 'tutorials/incompressible/simpleFoam/motorBike'
}
assert _DEFAULT_CASE in _CASE_PATHS


FLAGS = flags.FLAGS
flags.DEFINE_enum(
    'openfoam_case', _DEFAULT_CASE,
    sorted(list(_CASE_PATHS.keys())), 'Name of the Openfoam case to run.')


BENCHMARK_NAME = 'openfoam'
BENCHMARK_CONFIG = """
openfoam:
  description: Runs a Openfoam benchmarks.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: c2-standard-8
          zone: us-east1-c
          boot_disk_size: 100
        Azure:
          machine_type: Standard_F8s_v2
          zone: eastus2
        AWS:
          machine_type: c5.2xlarge
          zone: us-east-1f
          boot_disk_size: 100
      os_type: ubuntu1604
      vm_count: null
"""
_BENCHMARK_RUN_PATH = '$HOME/Openfoam/${USER}-7/run'


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
  vm.Install('openfoam')
  vm.RemoteCommand('mkdir -p %s' % _BENCHMARK_RUN_PATH)
  vm.RemoteCommand('cp -r {case_path} {run_path}'.format(
      case_path=posixpath.join(openfoam.OPENFOAM_ROOT,
                               _CASE_PATHS[FLAGS.openfoam_case]),
      run_path=_BENCHMARK_RUN_PATH))


def _AsSeconds(input_time):
  """Convert time from formatted string to seconds.

  Input format: 4m1.419s

  Args:
    input_time: The time to parse to a float.

  Returns:
    A float representing the time in seconds.
  """
  parsed = time.strptime(input_time, '%Mm%S.%fs')
  return datetime.timedelta(minutes=parsed.tm_min,
                            seconds=parsed.tm_sec).total_seconds()


def _GetSample(line):
  """Parse a single output line into a performance sample.

  Input format:
    real    4m1.419s

  Args:
    line: A single line from the Openfoam timing output.

  Returns:
    A single performance sample, with times in ms.
  """
  runtime_category, runtime_output = line.split()
  runtime_seconds = _AsSeconds(runtime_output)
  logging.info('Runtime of %s seconds from [%s, %s]',
               runtime_seconds, runtime_category, runtime_output)
  runtime_category = 'time_' + runtime_category
  return sample.Sample(runtime_category, runtime_seconds, 'seconds')


def _GetSamples(output):
  """Parse the output and return performance samples.

  Output is in the format:
    real    4m1.419s
    user    23m11.198s
    sys     0m25.274s

  Args:
    output: The output from running the Openfoam benchmark.

  Returns:
    A list of performance samples.
  """
  return [_GetSample(line) for line in output.strip().splitlines()]


def _GetOpenfoamVersion(vm):
  """Get the installed OpenFOAm version from the vm."""
  return vm.RemoteCommand('source {}/etc/bashrc && echo $WM_PROJECT_VERSION'
                          .format(openfoam.OPENFOAM_ROOT))[0]


def _GetOpenmpiVersion(vm):
  """Get the installed OpenMPI version from the vm."""
  return vm.RemoteCommand('mpirun -version')[0].split()[3]


def Run(benchmark_spec):
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for the OpenFOAM benchmark.

  Returns:
    A list of performance samples.
  """
  vm = benchmark_spec.vms[0]
  run_cmd = [
      'source %s/etc/bashrc' % openfoam.OPENFOAM_ROOT,
      'cd %s/motorBike' % _BENCHMARK_RUN_PATH,
      './Allclean',
      'time ./Allrun'
  ]
  _, run_output = vm.RemoteCommand(' && '.join(run_cmd))
  common_metadata = {
      'case': FLAGS.openfoam_case,
      'openfoam_version': _GetOpenfoamVersion(vm),
      'openmpi_version': _GetOpenmpiVersion(vm)
  }
  samples = _GetSamples(run_output)
  for result in samples:
    result.metadata.update(common_metadata)
  return samples


def Cleanup(benchmark_spec):
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for the OpenFOAM benchmark.
  """
  del benchmark_spec
