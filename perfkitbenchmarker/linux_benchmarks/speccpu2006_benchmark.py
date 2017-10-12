# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs SPEC CPU2006.

From the SPEC CPU2006 documentation:
"The SPEC CPU 2006 benchmark is SPEC's next-generation, industry-standardized,
CPU-intensive benchmark suite, stressing a system's processor, memory subsystem
and compiler."

SPEC CPU2006 homepage: http://www.spec.org/cpu2006/
"""

import itertools
import logging
import os
import posixpath
import re
import tarfile

from operator import mul
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages


FLAGS = flags.FLAGS

_SPECINT_BENCHMARKS = frozenset([
    'perlbench', 'bzip2', 'gcc', 'mcf', 'gobmk', 'hmmer', 'sjeng',
    'libquantum', 'h264ref', 'omnetpp', 'astar', 'xalancbmk'])
_SPECFP_BENCHMARKS = frozenset([
    'bwaves', 'gamess', 'milc', 'zeusmp', 'gromacs', 'cactusADM',
    'leslie3d', 'namd', 'dealII', 'soplex', 'povray', 'calculix',
    'GemsFDTD', 'tonto', 'lbm', 'wrf', 'sphinx3'])
_SPECCPU_SUBSETS = frozenset(['int', 'fp', 'all'])

flags.DEFINE_enum(
    'benchmark_subset', 'int',
    _SPECFP_BENCHMARKS | _SPECINT_BENCHMARKS | _SPECCPU_SUBSETS,
    'Used by the PKB speccpu2006 benchmark. Specifies a subset of SPEC CPU2006 '
    'benchmarks to run.')
flags.DEFINE_string(
    'runspec_config', 'linux64-x64-gcc47.cfg',
    'Used by the PKB speccpu2006 benchmark. Name of the cfg file to use as the '
    'SPEC CPU2006 config file provided to the runspec binary via its --config '
    'flag. If the benchmark is run using the cpu2006-1.2.iso file, then the '
    'cfg file must be placed in the local PKB data directory and will be '
    'copied to the remote machine prior to executing runspec. See README.md '
    'for instructions if running with a repackaged cpu2006v1.2.tgz file.')
flags.DEFINE_integer(
    'runspec_iterations', 3,
    'Used by the PKB speccpu2006 benchmark. The number of benchmark iterations '
    'to execute, provided to the runspec binary via its --iterations flag.')
flags.DEFINE_string(
    'runspec_define', '',
    'Used by the PKB speccpu2006 benchmark. Optional comma-separated list of '
    'SYMBOL[=VALUE] preprocessor macros provided to the runspec binary via '
    'repeated --define flags. Example: numa,smt,sse=SSE4.2')
flags.DEFINE_boolean(
    'runspec_enable_32bit', False,
    'Used by the PKB speccpu2006 benchmark. If set, multilib packages will be '
    'installed on the remote machine to enable use of 32-bit SPEC CPU2006 '
    'binaries. This may be useful when running on memory-constrained instance '
    'types (i.e. less than 2 GiB memory/core), where 64-bit execution may be '
    'problematic.')
flags.DEFINE_boolean(
    'runspec_keep_partial_results', False,
    'Used by the PKB speccpu2006 benchmark. If set, the benchmark will report '
    'an aggregate score even if some of the SPEC CPU2006 component tests '
    'failed with status "NR". Available results will be saved, and PKB samples '
    'will be marked with a metadata value of partial=true. If unset, partial '
    'failures are treated as errors.')
flags.DEFINE_boolean(
    'runspec_estimate_spec', False,
    'Used by the PKB speccpu2006 benchmark. If set, the benchmark will report '
    'an estimated aggregate score even if SPEC CPU2006 did not compute one. '
    'This usually occurs when --runspec_iterations is less than 3.  '
    '--runspec_keep_partial_results is also required to be set. Samples will be'
    'created as estimated_SPECint(R)_rate_base2006 and '
    'estimated_SPECfp(R)_rate_base2006.  Available results will be saved, '
    'and PKB samples will be marked with a metadata value of partial=true. If '
    'unset, SPECint(R)_rate_base2006 and SPECfp(R)_rate_base2006 are listed '
    'in the metadata under missing_results.')

BENCHMARK_NAME = 'speccpu2006'
BENCHMARK_CONFIG = """
speccpu2006:
  description: Runs SPEC CPU2006
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_50_gb
"""

_BENCHMARK_SPECIFIC_VM_STATE_ATTR = 'speccpu2006_vm_state'
_MOUNT_DIR = 'cpu2006_mnt'
_SPECCPU2006_DIR = 'cpu2006'
_SPECCPU2006_ISO = 'cpu2006-1.2.iso'
_SPECCPU2006_TAR = 'cpu2006v1.2.tgz'
_TAR_REQUIRED_MEMBERS = 'cpu2006', 'cpu2006/bin/runspec'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required input files are present."""
  try:
    # Peeking into the tar file is slow. If running in stages, it's
    # reasonable to do this only once and assume that the contents of the
    # tar file will not change between stages.
    _CheckTarFile(FLAGS.runspec_config,
                  examine_members=stages.PROVISION in FLAGS.run_stage)
  except data.ResourceNotFound:
    _CheckIsoAndCfgFile(FLAGS.runspec_config)


def _CheckTarFile(runspec_config, examine_members):
  """Searches for the tar file and performs preliminary checks on its format.

  Args:
    runspec_config: string. User-specified name of the config file that is
        expected to be in the tar file.
    examine_members: boolean. If True, this function will examine the tar file's
        members to verify that certain required members are present.

  Raises:
    data.ResourcePath: If the tar file cannot be found.
    errors.Benchmarks.PrepareException: If the tar file does not contain a
        required member.
    errors.Config.InvalidValue: If the tar file is found, and runspec_config is
        not a valid file name.
  """
  tar_file_path = data.ResourcePath(_SPECCPU2006_TAR)
  logging.info('Found tar file at %s. Skipping search for %s.', tar_file_path,
               _SPECCPU2006_ISO)
  if posixpath.basename(runspec_config) != runspec_config:
    raise errors.Config.InvalidValue(
        'Invalid runspec_config value: {0}{1}When running speccpu2006 with a '
        'tar file, runspec_config cannot specify a file in a sub-directory. '
        'See README.md for information about running speccpu2006 with a tar '
        'file.'.format(runspec_config, os.linesep))
  if not examine_members:
    return

  with tarfile.open(tar_file_path, 'r') as tf:
    members = tf.getnames()
  cfg_member = 'cpu2006/config/{0}'.format(runspec_config)
  required_members = itertools.chain(_TAR_REQUIRED_MEMBERS, [cfg_member])
  missing_members = set(required_members).difference(members)
  if missing_members:
    raise errors.Benchmarks.PrepareException(
        'The following files were not found within {tar}:{linesep}{members}'
        '{linesep}This is an indication that the tar file is formatted '
        'incorrectly. See README.md for information about the expected format '
        'of the tar file.'.format(
            linesep=os.linesep, tar=tar_file_path,
            members=os.linesep.join(sorted(missing_members))))


def _CheckIsoAndCfgFile(runspec_config):
  """Searches for the iso file and cfg file.

  Args:
    runspec_config: string. Name of the config file to provide to runspec.

  Raises:
    data.ResourcePath: If one of the required files could not be found.
  """
  # Search for the iso.
  try:
    data.ResourcePath(_SPECCPU2006_ISO)
  except data.ResourceNotFound:
    logging.error(
        '%(iso)s not found. To run the speccpu2006 benchmark, %(iso)s must be '
        'in the perfkitbenchmarker/data directory (or one of the specified '
        'data directories if the --data_search_paths flag is used). Visit '
        'https://www.spec.org/cpu2006/ to learn more about purchasing %(iso)s.',
        {'iso': _SPECCPU2006_ISO})
    raise

  # Search for the cfg.
  try:
    data.ResourcePath(runspec_config)
  except data.ResourceNotFound:
    logging.error(
        '%s not found. To run the speccpu2006 benchmark, the config file '
        'specified by the --runspec_config flag must be in the '
        'perfkitbenchmarker/data directory (or one of the specified data '
        'directories if the --data_search_paths flag is used). Visit '
        'https://www.spec.org/cpu2006/docs/runspec.html#about_config to learn '
        'more about config files.', runspec_config)
    raise


class _SpecCpu2006SpecificState(object):
  """State specific to this benchmark that must be preserved between PKB stages.

  An instance of this class is attached to the VM as an attribute and is
  therefore preserved as part of the pickled BenchmarkSpec between PKB stages.

  Each attribute represents a possible file or directory that may be created on
  the remote machine as part of running the benchmark.

  Attributes:
    cfg_file_path: Optional string. Path of the cfg file on the remote machine.
    iso_file_path: Optional string. Path of the iso file on the remote machine.
    mount_dir: Optional string. Path where the iso file is mounted on the
        remote machine.
    spec_dir: Optional string. Path of a created directory on the remote machine
        where the SPEC files are stored.
    tar_file_path: Optional string. Path of the tar file on the remote machine.
  """
  def __init__(self):
    self.cfg_file_path = None
    self.iso_file_path = None
    self.mount_dir = None
    self.spec_dir = None
    self.tar_file_path = None


def Prepare(benchmark_spec):
  """Installs SPEC CPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  speccpu_vm_state = _SpecCpu2006SpecificState()
  setattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR, speccpu_vm_state)
  vm.Install('wget')
  vm.Install('build_tools')
  vm.Install('fortran')
  if FLAGS.runspec_enable_32bit:
    vm.Install('multilib')
  vm.Install('numactl')
  scratch_dir = vm.GetScratchDir()
  vm.RemoteCommand('chmod 777 {0}'.format(scratch_dir))
  speccpu_vm_state.spec_dir = posixpath.join(scratch_dir, _SPECCPU2006_DIR)
  try:
    _PrepareWithTarFile(vm, speccpu_vm_state)
  except data.ResourceNotFound:
    _PrepareWithIsoFile(vm, speccpu_vm_state)


def _PrepareWithTarFile(vm, speccpu_vm_state):
  """Prepares the VM to run using the tar file.

  Args:
    vm: BaseVirtualMachine. Recipient of the tar file.
    speccpu_vm_state: _SpecCpu2006SpecificState. Modified by this function to
        reflect any changes to the VM that may need to be cleaned up.
  """
  scratch_dir = vm.GetScratchDir()
  local_tar_file_path = data.ResourcePath(_SPECCPU2006_TAR)
  speccpu_vm_state.tar_file_path = posixpath.join(scratch_dir, _SPECCPU2006_TAR)
  vm.PushFile(local_tar_file_path, scratch_dir)
  vm.RemoteCommand('cd {dir} && tar xvfz {tar}'.format(dir=scratch_dir,
                                                       tar=_SPECCPU2006_TAR))
  speccpu_vm_state.cfg_file_path = posixpath.join(
      speccpu_vm_state.spec_dir, 'config', FLAGS.runspec_config)


def _PrepareWithIsoFile(vm, speccpu_vm_state):
  """Prepares the VM to run using the iso file.

  Copies the iso to the VM, mounts it, and extracts the contents. Copies the
  config file to the VM. Runs the SPEC install.sh script on the VM.

  Args:
    vm: BaseVirtualMachine. Recipient of the iso file.
    speccpu_vm_state: _SpecCpu2006SpecificState. Modified by this function to
        reflect any changes to the VM that may need to be cleaned up.
  """
  scratch_dir = vm.GetScratchDir()

  # Make cpu2006 directory on the VM.
  vm.RemoteCommand('mkdir {0}'.format(speccpu_vm_state.spec_dir))

  # Copy the iso to the VM.
  local_iso_file_path = data.ResourcePath(_SPECCPU2006_ISO)
  speccpu_vm_state.iso_file_path = posixpath.join(scratch_dir, _SPECCPU2006_ISO)
  vm.PushFile(local_iso_file_path, scratch_dir)

  # Extract files from the iso to the cpu2006 directory.
  speccpu_vm_state.mount_dir = posixpath.join(scratch_dir, _MOUNT_DIR)
  vm.RemoteCommand('mkdir {0}'.format(speccpu_vm_state.mount_dir))
  vm.RemoteCommand('sudo mount -t iso9660 -o loop {0} {1}'.format(
      speccpu_vm_state.iso_file_path, speccpu_vm_state.mount_dir))
  vm.RemoteCommand('cp -r {0}/* {1}'.format(speccpu_vm_state.mount_dir,
                                            speccpu_vm_state.spec_dir))
  vm.RemoteCommand('chmod -R 777 {0}'.format(speccpu_vm_state.spec_dir))

  # Copy the cfg to the VM.
  local_cfg_file_path = data.ResourcePath(FLAGS.runspec_config)
  cfg_file_name = os.path.basename(local_cfg_file_path)
  speccpu_vm_state.cfg_file_path = posixpath.join(
      speccpu_vm_state.spec_dir, 'config', cfg_file_name)
  vm.PushFile(local_cfg_file_path, speccpu_vm_state.cfg_file_path)

  # Run SPEC CPU2006 installation.
  install_script_path = posixpath.join(speccpu_vm_state.spec_dir, 'install.sh')
  vm.RobustRemoteCommand('yes | {0}'.format(install_script_path))


def _ExtractScore(stdout, vm, keep_partial_results, estimate_spec):
  """Extracts the SPEC(int|fp) score from stdout.

  Args:
    stdout: stdout from running RemoteCommand.
    vm: The vm instance where SPEC CPU2006 was run.
    keep_partial_results: A boolean indicating whether partial results should
        be extracted in the event that not all benchmarks were successfully
        run. See the "runspec_keep_partial_results" flag for more info.

  Sample input for SPECint:
      ...
      ...
      =============================================
      400.perlbench    9770        417       23.4 *
      401.bzip2        9650        565       17.1 *
      403.gcc          8050        364       22.1 *
      429.mcf          9120        364       25.1 *
      445.gobmk       10490        499       21.0 *
      456.hmmer        9330        491       19.0 *
      458.sjeng       12100        588       20.6 *
      462.libquantum  20720        468       44.2 *
      464.h264ref     22130        700       31.6 *
      471.omnetpp      6250        349       17.9 *
      473.astar        7020        482       14.6 *
      483.xalancbmk    6900        248       27.8 *
       Est. SPECint(R)_base2006              22.7

  Sample input for SPECfp:
      ...
      ...
      =============================================
      410.bwaves      13590        717      19.0  *
      416.gamess      19580        923      21.2  *
      433.milc         9180        480      19.1  *
      434.zeusmp       9100        600      15.2  *
      435.gromacs      7140        605      11.8  *
      436.cactusADM   11950       1289       9.27 *
      437.leslie3d     9400        859      10.9  *
      444.namd         8020        504      15.9  *
      447.dealII      11440        409      28.0  *
      450.soplex       8340        272      30.6  *
      453.povray       5320        231      23.0  *
      454.calculix     8250        993       8.31 *
      459.GemsFDTD    10610        775      13.7  *
      465.tonto        9840        565      17.4  *
      470.lbm         13740        365      37.7  *
      481.wrf         11170        788      14.2  *
      482.sphinx3     19490        668      29.2  *
       Est. SPECfp(R)_base2006              17.5

  Returns:
      A list of sample.Sample objects.
  """
  results = []

  re_begin_section = re.compile('^={1,}')
  re_end_section = re.compile(r'Est. (SPEC.*_base2006)\s*(\S*)')
  result_section = []
  in_result_section = False

  # Extract the summary section
  for line in stdout.splitlines():
    if in_result_section:
      result_section.append(line)
    # search for begin of result section
    match = re.search(re_begin_section, line)
    if match:
      assert not in_result_section
      in_result_section = True
      continue
    # search for end of result section
    match = re.search(re_end_section, line)
    if match:
      assert in_result_section
      spec_name = str(match.group(1))
      try:
        spec_score = float(match.group(2))
      except ValueError:
        # Partial results may get reported as '--' instead of a number.
        spec_score = None
      in_result_section = False
      # remove the final SPEC(int|fp) score, which has only 2 columns.
      result_section.pop()

  metadata = {'num_cpus': vm.num_cpus,
              'runspec_config': FLAGS.runspec_config,
              'runspec_iterations': str(FLAGS.runspec_iterations),
              'runspec_enable_32bit': str(FLAGS.runspec_enable_32bit),
              'runspec_define': FLAGS.runspec_define}

  missing_results = []
  scores = []

  for benchmark in result_section:
    # Skip over failed runs, but count them since they make the overall
    # result invalid.
    if 'NR' in benchmark:
      logging.warning('SPEC CPU2006 missing result: %s', benchmark)
      missing_results.append(str(benchmark.split()[0]))
      continue
    # name, ref_time, time, score, misc
    name, _, _, score_str, _ = benchmark.split()
    score_float = float(score_str)
    scores.append(score_float)
    results.append(sample.Sample(str(name), score_float, '', metadata))

  if spec_score is None:
    missing_results.append(spec_name)

  if missing_results:
    if keep_partial_results:
      metadata['partial'] = 'true'
      metadata['missing_results'] = ','.join(missing_results)
    else:
      raise errors.Benchmarks.RunError(
          'speccpu2006: results missing, see log: ' + ','.join(missing_results))

  if spec_score is not None:
    results.append(sample.Sample(spec_name, spec_score, '', metadata))
  elif estimate_spec:
    estimated_spec_score = _GeometricMean(scores)
    results.append(sample.Sample('estimated_' + spec_name,
                                 estimated_spec_score, '', metadata))

  return results


def _GeometricMean(arr):
  "Calculates the geometric mean of the array."
  return reduce(mul, arr) ** (1.0 / len(arr))


def _ParseOutput(vm, spec_dir):
  """Retrieves the SPEC CPU2006 output from the VM and parses it.

  Args:
    vm: The vm instance where SPEC CPU2006 was run.
    spec_dir: string. Path of the directory on the remote machine where the SPEC
        files, including binaries and logs, are located.

  Returns:
    A list of samples to be published (in the same format as Run() returns).
  """
  results = []

  log_files = []
  # FIXME(liquncheng): Only reference runs generate SPEC scores. The log
  # id is hardcoded as 001, which might change with different runspec
  # parameters. SPEC CPU2006 will generate different logs for build, test
  # run, training run and ref run.
  if FLAGS.benchmark_subset in _SPECINT_BENCHMARKS | set(['int', 'all']):
    log_files.append('CINT2006.001.ref.txt')
  if FLAGS.benchmark_subset in _SPECFP_BENCHMARKS | set(['fp', 'all']):
    log_files.append('CFP2006.001.ref.txt')

  for log in log_files:
    stdout, _ = vm.RemoteCommand('cat %s/result/%s' % (spec_dir, log),
                                 should_log=True)
    results.extend(_ExtractScore(
        stdout, vm, FLAGS.runspec_keep_partial_results or (
            FLAGS.benchmark_subset not in _SPECCPU_SUBSETS),
        FLAGS.runspec_estimate_spec))

  return results


def Run(benchmark_spec):
  """Runs SPEC CPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]
  speccpu_vm_state = getattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR)
  num_cpus = vm.num_cpus

  runspec_flags = [
      ('config', posixpath.basename(speccpu_vm_state.cfg_file_path)),
      ('tune', 'base'), ('size', 'ref'), ('rate', num_cpus),
      ('iterations', FLAGS.runspec_iterations)]
  if FLAGS.runspec_define:
    for runspec_define in FLAGS.runspec_define.split(','):
      runspec_flags.append(('define', runspec_define))
  runspec_cmd = 'runspec --noreportable {flags} {subset}'.format(
      flags=' '.join('--{0}={1}'.format(k, v) for k, v in runspec_flags),
      subset=FLAGS.benchmark_subset)

  cmd = ' && '.join((
      'cd {0}'.format(speccpu_vm_state.spec_dir), '. ./shrc', './bin/relocate',
      '. ./shrc', 'rm -rf result', runspec_cmd))
  vm.RobustRemoteCommand(cmd)
  logging.info('SPEC CPU2006 Results:')
  return _ParseOutput(vm, speccpu_vm_state.spec_dir)


def Cleanup(benchmark_spec):
  """Cleans up SPEC CPU2006 from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  speccpu_vm_state = getattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR, None)
  if speccpu_vm_state:
    if speccpu_vm_state.mount_dir:
      try:
        vm.RemoteCommand('sudo umount {0}'.format(speccpu_vm_state.mount_dir))
      except errors.VirtualMachine.RemoteCommandError:
        # Even if umount failed, continue to clean up.
        logging.exception('umount failed.')
    targets = ' '.join(p for p in speccpu_vm_state.__dict__.values() if p)
    vm.RemoteCommand('rm -rf {0}'.format(targets))
