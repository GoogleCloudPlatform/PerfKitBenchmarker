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

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import posixpath
import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import openfoam


_DEFAULT_CASE = 'motorbike'
_CASE_PATHS = {
    'motorbike': 'tutorials/incompressible/simpleFoam/motorBike',
}
assert _DEFAULT_CASE in _CASE_PATHS

FLAGS = flags.FLAGS
flags.DEFINE_enum(
    'openfoam_case', _DEFAULT_CASE,
    sorted(list(_CASE_PATHS.keys())),
    'Name of the OpenFOAM case to run.')
flags.DEFINE_list('openfoam_dimensions', ['20_8_8'], 'Dimensions of the case.')
flags.DEFINE_integer(
    'openfoam_num_threads', None,
    'The number of threads to run OpenFOAM with.')
flags.DEFINE_string(
    'openfoam_mpi_mapping', 'core:SPAN',
    'Mpirun process mapping to use as arguments to "mpirun --map-by".')
flags.DEFINE_enum(
    'openfoam_decomp_method', 'scotch',
    ['scotch', 'hierarchical', 'simple'],
    'Decomposition method to use in decomposePar. See: '
    'https://cfd.direct/openfoam/user-guide/v7-running-applications-parallel/')
flags.DEFINE_integer(
    'openfoam_max_global_cells', 200 * 1000 * 1000,
    'The maximum number of refinement cells to use in snappHexMeshDict. See: '
    'https://cfd.direct/openfoam/user-guide/v6-snappyhexmesh/')

BENCHMARK_NAME = 'openfoam'
_BENCHMARK_ROOT = '$HOME/OpenFOAM/run'
BENCHMARK_CONFIG = """
openfoam:
  description: Runs an OpenFOAM benchmark.
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
          boot_disk_size: 100
        AWS:
          machine_type: c5.2xlarge
          zone: us-east-1f
          boot_disk_size: 100
      os_type: ubuntu1604
      vm_count: 2
      disk_spec:
        GCP:
          disk_type: nfs
          nfs_managed: False
          mount_point: {path}
        Azure:
          disk_type: nfs
          nfs_managed: False
          mount_point: {path}
        AWS:
          disk_type: nfs
          nfs_managed: False
          mount_point: {path}
""".format(path=_BENCHMARK_ROOT)
_MACHINE_FILE = posixpath.join(_BENCHMARK_ROOT, 'MACHINEFILE')
_RUN_SCRIPT = 'Allrun'
_BLOCK_MESH_DICT = 'system/blockMeshDict'
_DECOMPOSE_DICT = 'system/decomposeParDict'
_SNAPPY_HEX_MESH_DICT = 'system/snappyHexMeshDict'

_TIME_RE = re.compile(r"""(\d+)m       # The minutes part
                          (\d+)\.\d+s  # The seconds part """, re.VERBOSE)

_SSH_CONFIG_CMD = ('echo "LogLevel ERROR\nHost *\n  IdentitiesOnly yes\n" | '
                   'tee -a $HOME/.ssh/config')


def GetConfig(user_config):
  """Returns the configuration of a benchmark."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['num_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.num_vms
  return config


@flags.validator('openfoam_dimensions')
def _CheckDimensions(dimensions_list):
  # throws InvalidValue if an entry is not correct
  for dimensions in dimensions_list:
    _ParseDimensions(dimensions)
  return True


def _ParseDimensions(dimensions):
  """Parse and validate an individual dimensions entry.

  Args:
    dimensions: String formatted as "_" separated integers like: '80_20_20'.

  Returns:
    Parsed dimensions like: '80 20 20'.

  Raises:
    errors.Config.InvalidValue: If input dimensions are incorrectly formatted.

  """
  dimensions = dimensions.split('_')
  if not all(value.isdigit() for value in dimensions):
    raise errors.Config.InvalidValue(
        'Expected list of ints separated by "_" in --openfoam_dimensions '
        'but received %s.' % dimensions)
  return ' '.join(dimensions)


def Prepare(benchmark_spec):
  """Prepares the VMs and other resources for running the benchmark.

  This is a good place to download binaries onto the VMs, create any data files
  needed for a benchmark run, etc.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(lambda vm: vm.Install('openfoam'), vms)
  # Allow ssh access to other vms.
  vm_util.RunThreaded(lambda vm: vm.AuthenticateVm(), vms)
  # Avoids printing ssh warnings and prevents too many auth errors.
  vm_util.RunThreaded(lambda vm: vm.RemoteCommand(_SSH_CONFIG_CMD), vms)
  # Tell mpirun about other nodes.
  hpc_util.CreateMachineFile(vms, remote_path=_MACHINE_FILE)


def _AsSeconds(input_time):
  """Convert time from formatted string to seconds.

  Input format: 200m1.419s
  Should return 1201

  Args:
    input_time: The time to parse to an integer.

  Returns:
    An integer representing the time in seconds.
  """
  match = _TIME_RE.match(input_time)
  assert match, 'Time "{}" does not match format "{}"'.format(input_time,
                                                              _TIME_RE.pattern)
  minutes, seconds = match.group(1, 2)
  return int(minutes) * 60 + int(seconds)


def _GetSample(line):
  """Parse a single output line into a performance sample.

  Input format:
    real    4m1.419s

  Args:
    line: A single line from the OpenFOAM timing output.

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
    output: The output from running the OpenFOAM benchmark.

  Returns:
    A list of performance samples.
  """
  return [_GetSample(line) for line in output.strip().splitlines()]


def _GetOpenfoamVersion(vm):
  """Get the installed OpenFOAM version from the vm."""
  return vm.RemoteCommand('echo $WM_PROJECT_VERSION')[0].rstrip()


def _GetOpenmpiVersion(vm):
  """Get the installed OpenMPI version from the vm."""
  return vm.RemoteCommand('mpirun -version')[0].split()[3].rstrip()


def _GetWorkingDirPath():
  """Get the base directory name of the case being run."""
  case_dir_name = posixpath.basename(_CASE_PATHS[FLAGS.openfoam_case])
  return posixpath.join(_BENCHMARK_ROOT, case_dir_name)


def _GetPath(openfoam_file):
  """Get the absolute path to the file in the working directory."""
  return posixpath.join(_GetWorkingDirPath(), openfoam_file)


def _SetDictEntry(vm, key, value, dict_file_name):
  """Sets an entry in an OpenFOAM dictionary file.

  Args:
    vm: The VM to set the entry on.
    key: String; name of the key to set (like hierarchicalCoeffs.n).
    value: String; the value to set.
    dict_file_name: String; name of the file to set the specified entry. This
      file should be in the working directory. Example: system/snappyHexMeshDict
  """
  vm.RemoteCommand('foamDictionary -entry {key} -set "{value}" {file}'.format(
      key=key,
      value=value,
      file=_GetPath(dict_file_name)))


def _UseMpi(vm, num_processes, mapping):
  """Configure OpenFOAM to use MPI if running with more than 1 VM.

  This function looks for the word "runParallel" in the run script and replaces
  it with an mpirun command.

  Args:
    vm: The worker VM to use MPI on.
    num_processes: An integer representing the total number of processes for the
      MPI job.
    mapping: A string for the mpirun --map-by flag.
  """
  run_script = _GetPath(_RUN_SCRIPT)
  vm_util.ReplaceText(
      vm, 'runParallel', 'mpirun '
      '-hostfile {machinefile} '
      '-mca btl ^openib '
      '--map-by {mapping} '
      '-np {num_processes}'.format(
          machinefile=_MACHINE_FILE,
          mapping=mapping,
          num_processes=num_processes), run_script, '|')
  vm_util.ReplaceText(vm, '^mpirun.*', '& -parallel', run_script)


def _RunCase(master_vm, dimensions):
  """Runs the case with the given dimensions.

  Args:
    master_vm: The vm to run the case commands on. If using the default NFS
      server, it doesn't actually matter which vm this is.
    dimensions: A string of the dimensions to run with. Like "100 24 24".

  Returns:
    A list of performance samples for the given dimensions.
  """
  dims_entry = ('( hex ( 0 1 2 3 4 5 6 7 ) ( {dimensions} ) '
                'simpleGrading ( 1 1 1 ) )').format(
                    dimensions=_ParseDimensions(dimensions))
  _SetDictEntry(master_vm, 'blocks', dims_entry, _BLOCK_MESH_DICT)

  run_command = ' && '.join(
      ['cd %s' % _GetWorkingDirPath(), './Allclean', 'time ./Allrun'])
  _, run_output = master_vm.RemoteCommand(run_command)
  results = _GetSamples(run_output)
  # Update every run with run-specific metadata.
  for result in results:
    result.metadata['dimensions'] = dimensions
  return results


def Run(benchmark_spec):
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for the OpenFOAM benchmark.

  Returns:
    A list of performance samples.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  num_vms = len(vms)

  # Run configuration metadata:
  num_cpus_available = num_vms * master_vm.NumCpusForBenchmark()
  num_cpus_to_use = FLAGS.openfoam_num_threads or num_cpus_available // 2
  case_name = FLAGS.openfoam_case
  mpi_mapping = FLAGS.openfoam_mpi_mapping
  decomp_method = FLAGS.openfoam_decomp_method
  max_global_cells = FLAGS.openfoam_max_global_cells
  openfoam_version = _GetOpenfoamVersion(master_vm)
  openmpi_version = _GetOpenmpiVersion(master_vm)
  common_metadata = {
      'case_name': case_name,
      'decomp_method': decomp_method,
      'max_global_cells': max_global_cells,
      'mpi_mapping': mpi_mapping,
      'openfoam_version': openfoam_version,
      'openmpi_version': openmpi_version,
      'total_cpus_available': num_cpus_available,
      'total_cpus_used': num_cpus_to_use,
  }
  logging.info('Running %s case on %s/%s cores on %s vms', case_name,
               num_cpus_to_use, num_cpus_available, num_vms)
  logging.info('Common metadata: %s', common_metadata)

  # Copy the run directory.
  master_vm.RemoteCommand('cp -r {case_path} {destination}'.format(
      case_path=posixpath.join(openfoam.OPENFOAM_ROOT, _CASE_PATHS[case_name]),
      destination=_BENCHMARK_ROOT))

  # Configure common parameters.
  _SetDictEntry(master_vm, 'method', decomp_method, _DECOMPOSE_DICT)
  _SetDictEntry(master_vm, 'numberOfSubdomains', num_cpus_to_use,
                _DECOMPOSE_DICT)
  _SetDictEntry(master_vm, 'hierarchicalCoeffs.n',
                '({} 1 1)'.format(num_cpus_to_use),
                _DECOMPOSE_DICT)
  _SetDictEntry(master_vm, 'castellatedMeshControls.maxGlobalCells',
                max_global_cells, _SNAPPY_HEX_MESH_DICT)
  _UseMpi(master_vm, num_cpus_to_use, mpi_mapping)

  # Run and gather samples.
  samples = []
  for dimensions in FLAGS.openfoam_dimensions:
    results = _RunCase(master_vm, dimensions)
    # Update every case run with common metadata.
    for result in results:
      result.metadata.update(common_metadata)
    samples.extend(results)
  return samples


def Cleanup(benchmark_spec):
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for the OpenFOAM benchmark.
  """
  del benchmark_spec
