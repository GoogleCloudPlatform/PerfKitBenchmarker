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

"""Module to install, uninstall, and parse results for SPEC CPU 2006 and 2017.
"""

import itertools
import logging
import os
import posixpath
import re

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker.linux_packages import build_tools


FLAGS = flags.FLAGS


flags.DEFINE_string(
    'runspec_config', 'linux64-x64-gcc47.cfg',
    'Used by the PKB speccpu benchmarks. Name of the cfg file to use as the '
    'SPEC CPU config file provided to the runspec binary via its --config '
    'flag. If the benchmark is run using an .iso file, then the '
    'cfg file must be placed in the local PKB data directory and will be '
    'copied to the remote machine prior to executing runspec/runcpu. '
    'See README.md for instructions if running with a repackaged .tgz file.')
flags.DEFINE_string(
    'runspec_build_tool_version', None,
    'Version of gcc/g++/gfortran. This should match runspec_config. Note, if '
    'neither runspec_config and runspec_build_tool_version is set, the test '
    'install gcc/g++/gfortran-4.7, since that matches default config version. '
    'If runspec_config is set, but not runspec_build_tool_version, default '
    'version of build tools will be installed. Also this flag only works with '
    'debian.')
flags.DEFINE_integer(
    'runspec_iterations', 3,
    'Used by the PKB speccpu benchmarks. The number of benchmark iterations '
    'to execute, provided to the runspec binary via its --iterations flag.')
flags.DEFINE_string(
    'runspec_define', '',
    'Used by the PKB speccpu benchmarks. Optional comma-separated list of '
    'SYMBOL[=VALUE] preprocessor macros provided to the runspec binary via '
    'repeated --define flags. Example: numa,smt,sse=SSE4.2')
flags.DEFINE_boolean(
    'runspec_enable_32bit', False,
    'Used by the PKB speccpu benchmarks. If set, multilib packages will be '
    'installed on the remote machine to enable use of 32-bit SPEC CPU '
    'binaries. This may be useful when running on memory-constrained instance '
    'types (i.e. less than 2 GiB memory/core), where 64-bit execution may be '
    'problematic.')
flags.DEFINE_boolean(
    'runspec_keep_partial_results', False,
    'Used by the PKB speccpu benchmarks. If set, the benchmark will report '
    'an aggregate score even if some of the SPEC CPU component tests '
    'failed with status "NR". Available results will be saved, and PKB samples '
    'will be marked with a metadata value of partial=true. If unset, partial '
    'failures are treated as errors.')
flags.DEFINE_boolean(
    'runspec_estimate_spec', False,
    'Used by the PKB speccpu benchmarks. If set, the benchmark will report '
    'an estimated aggregate score even if SPEC CPU did not compute one. '
    'This usually occurs when --runspec_iterations is less than 3.  '
    '--runspec_keep_partial_results is also required to be set. Samples will be'
    'created as estimated_SPECint(R)_rate_base and '
    'estimated_SPECfp(R)_rate_base.  Available results will be saved, '
    'and PKB samples will be marked with a metadata value of partial=true. If '
    'unset, SPECint(R)_rate_base20** and SPECfp(R)_rate_base20** are listed '
    'in the metadata under missing_results.')


_VM_STATE_ATTR = 'speccpu_vm_state'


def _CheckTarFile(vm, runspec_config, examine_members, speccpu_vm_state):
  """Performs preliminary checks on the format of tar file downloaded on vm.

  Args:
    vm: virtual machine
    runspec_config: String. User-specified name of the config file that is
        expected to be in the tar file.
    examine_members: Boolean. If True, this function will examine the tar file's
        members to verify that certain required members are present.
    speccpu_vm_state: SpecInstallConfigurations. Install configurations.

  Raises:
    errors.Benchmarks.PrepareException: If the tar file does not contain a
        required member.
    errors.Config.InvalidValue: If the tar file is found, and runspec_config is
        not a valid file name.
  """
  if posixpath.basename(runspec_config) != runspec_config:
    raise errors.Config.InvalidValue(
        'Invalid runspec_config value: {0}{1}When running speccpu with a '
        'tar file, runspec_config cannot specify a file in a sub-directory. '
        'See README.md for information about running speccpu with a tar '
        'file.'.format(runspec_config, os.linesep))
  if not examine_members:
    return

  scratch_dir = vm.GetScratchDir()
  cfg_member = '{0}/config/{1}'.format(speccpu_vm_state.base_spec_dir,
                                       runspec_config)
  required_members = itertools.chain(speccpu_vm_state.required_members,
                                     [cfg_member])
  missing_members = []
  for member in required_members:
    stdout, _ = vm.RemoteCommand(
        'cd {scratch_dir} && (test -f {member} || test -d {member}) ; echo $?'
        .format(scratch_dir=scratch_dir, member=member))
    if stdout.strip() != '0':
      missing_members.append(member)

  if missing_members:
    raise errors.Benchmarks.PrepareException(
        'The following files were not found within tar file:{linesep}{members}'
        '{linesep}This is an indication that the tar file is formatted '
        'incorrectly. See README.md for information about the expected format '
        'of the tar file.'.format(
            linesep=os.linesep,
            members=os.linesep.join(sorted(missing_members))))


def _CheckIsoAndCfgFile(runspec_config, spec_iso):
  """Searches for the iso file and cfg file.

  Args:
    runspec_config: String. Name of the config file to provide to runspec.
    spec_iso: String. Location of spec iso file.

  Raises:
    data.ResourcePath: If one of the required files could not be found.
  """
  # Search for the iso.
  try:
    data.ResourcePath(spec_iso)
  except data.ResourceNotFound:
    logging.error(
        '%(iso)s not found. To run the speccpu benchmark, %(iso)s must be '
        'in the perfkitbenchmarker/data directory (or one of the specified '
        'data directories if the --data_search_paths flag is used). Visit '
        'https://www.spec.org/ to learn more about purchasing %(iso)s.',
        {'iso': spec_iso})
    raise

  # Search for the cfg.
  try:
    data.ResourcePath(runspec_config)
  except data.ResourceNotFound:
    logging.error(
        '%s not found. To run the speccpu benchmark, the config file '
        'specified by the --runspec_config flag must be in the '
        'perfkitbenchmarker/data directory (or one of the specified data '
        'directories if the --data_search_paths flag is used). Visit '
        'https://www.spec.org/cpu2006/docs/runspec.html#about_config to learn '
        'more about config files.', runspec_config)
    raise


class SpecInstallConfigurations(object):
  """Configs for SPEC CPU run that must be preserved between PKB stages.

  Specifies directories to look for install files and tracks install locations.

  An instance of this class is attached to the VM as an attribute and is
  therefore preserved as part of the pickled BenchmarkSpec between PKB stages.

  Each attribute represents a possible file or directory that may be created on
  the remote machine as part of running the benchmark.

  Attributes:
    benchmark_name: String. Either speccpu2006 or speccpu2017.
    cfg_file_path: Optional string. Path of the cfg file on the remote machine.
    base_mount_dir: Optional string. Base directory where iso file is mounted.
    mount_dir: Optional string. Path where the iso file is mounted on the
        remote machine.
    base_spec_dir: Optional string. Base directory where spec files are located.
    spec_dir: Optional string. Path of a created directory on the remote machine
        where the SPEC files are stored.
    base_iso_file_path: Optional string. Base directory of iso file.
    iso_file_path: Optional string. Path of the iso file on the remote machine.
    base_tar_file_path: Optional string. Base directory of tar file.
    tar_file_path: Optional string. Path of the tar file on the remote machine.
    required_members: List. File components that must exist for spec to run.
    log_format: String. Logging format of this spec run.
  """

  def __init__(self):
    self.benchmark_name = None
    self.cfg_file_path = None
    self.base_mount_dir = None
    self.mount_dir = None
    self.base_spec_dir = None
    self.spec_dir = None
    self.base_iso_file_path = None
    self.iso_file_path = None
    self.base_tar_file_path = None
    self.tar_file_path = None
    self.required_members = None
    self.log_format = None


def Install(vm, speccpu_vm_state):
  """Installs SPEC CPU2006 or 2017 on the target vm.

  Args:
    vm: Vm on which speccpu is installed.
    speccpu_vm_state: SpecInstallConfigurations. Install configuration for spec.
  """
  setattr(vm, _VM_STATE_ATTR, speccpu_vm_state)
  vm.Install('wget')
  vm.Install('fortran')
  vm.Install('build_tools')

  # If using default config files and runspec_build_tool_version is not set,
  # install 4.7 gcc/g++/gfortan. If either one of the flag is set, we assume
  # user is smart
  if not FLAGS['runspec_config'].present or FLAGS.runspec_build_tool_version:
    build_tool_version = FLAGS.runspec_build_tool_version or '4.7'
    build_tools.Reinstall(vm, version=build_tool_version)
  if FLAGS.runspec_enable_32bit:
    vm.Install('multilib')
  vm.Install('numactl')
  scratch_dir = vm.GetScratchDir()
  vm.RemoteCommand('chmod 777 {0}'.format(scratch_dir))
  speccpu_vm_state.spec_dir = posixpath.join(scratch_dir,
                                             speccpu_vm_state.base_spec_dir)
  try:
    _PrepareWithPreprovisionedTarFile(vm, speccpu_vm_state)
    _CheckTarFile(vm, FLAGS.runspec_config,
                  stages.PROVISION in FLAGS.run_stage,
                  speccpu_vm_state)
  except errors.Setup.BadPreprovisionedDataError:
    _CheckIsoAndCfgFile(FLAGS.runspec_config, speccpu_vm_state.iso_dir)
    _PrepareWithIsoFile(vm, speccpu_vm_state)


def _PrepareWithPreprovisionedTarFile(vm, speccpu_vm_state):
  """Prepares the VM to run using tar file in preprovisioned cloud.

  Args:
    vm: BaseVirtualMachine. Vm on which the tar file is installed.
    speccpu_vm_state: SpecInstallConfigurations. Install configuration for spec.
  """
  scratch_dir = vm.GetScratchDir()
  vm.InstallPreprovisionedBenchmarkData(speccpu_vm_state.benchmark_name,
                                        [speccpu_vm_state.base_tar_file_path],
                                        scratch_dir)
  vm.RemoteCommand('cd {dir} && tar xvfz {tar}'.format(
      dir=scratch_dir, tar=speccpu_vm_state.base_tar_file_path))
  speccpu_vm_state.cfg_file_path = posixpath.join(
      speccpu_vm_state.spec_dir, 'config', FLAGS.runspec_config)


def _PrepareWithIsoFile(vm, speccpu_vm_state):
  """Prepares the VM to run using the iso file.

  Copies the iso to the VM, mounts it, and extracts the contents. Copies the
  config file to the VM. Runs the SPEC install.sh script on the VM.

  Args:
    vm: BaseVirtualMachine. Recipient of the iso file.
    speccpu_vm_state: SpecInstallConfigurations. Modified by this function to
        reflect any changes to the VM that may need to be cleaned up.
  """
  scratch_dir = vm.GetScratchDir()

  # Make cpu2006 or cpu2017 directory on the VM.
  vm.RemoteCommand('mkdir {0}'.format(speccpu_vm_state.spec_dir))

  # Copy the iso to the VM.
  local_iso_file_path = data.ResourcePath(speccpu_vm_state.iso_file_path)
  speccpu_vm_state.iso_file_path = posixpath.join(
      scratch_dir, speccpu_vm_state.iso_file_path)
  vm.PushFile(local_iso_file_path, scratch_dir)

  # Extract files from the iso to the cpu2006 or cpu2017 directory.
  speccpu_vm_state.mount_dir = posixpath.join(
      scratch_dir, speccpu_vm_state.mount_dir)
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

  # Run SPEC CPU2006 or 2017 installation.
  install_script_path = posixpath.join(speccpu_vm_state.spec_dir, 'install.sh')
  vm.RobustRemoteCommand('yes | {0}'.format(install_script_path))


def _ExtractScore(stdout, vm, keep_partial_results, runspec_metric):
  """Extracts the SPEC(int|fp) score from stdout.

  Args:
    stdout: String. stdout from running RemoteCommand.
    vm: The vm instance where SPEC CPU was run.
    keep_partial_results: Boolean. True if partial results should
        be extracted in the event that not all benchmarks were successfully
        run. See the "runspec_keep_partial_results" flag for more info.
    runspec_metric: String. Indicates whether this is spec speed or rate run.

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
  speccpu_vm_state = getattr(vm, _VM_STATE_ATTR, None)
  re_begin_section = re.compile('^={1,}')
  re_end_section = re.compile(speccpu_vm_state.log_format)
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
      if runspec_metric == 'speed':
        spec_name += ':speed'
      try:
        spec_score = float(match.group(2))
      except ValueError:
        # Partial results may get reported as '--' instead of a number.
        spec_score = None
      in_result_section = False
      # remove the final SPEC(int|fp) score, which has only 2 columns.
      result_section.pop()

  metadata = {
      'runspec_config': FLAGS.runspec_config,
      'runspec_iterations': str(FLAGS.runspec_iterations),
      'runspec_enable_32bit': str(FLAGS.runspec_enable_32bit),
      'runspec_define': FLAGS.runspec_define,
      'runspec_metric': runspec_metric
  }

  missing_results = []
  scores = []

  for benchmark in result_section:
    # Skip over failed runs, but count them since they make the overall
    # result invalid.
    if 'NR' in benchmark:
      logging.warning('SPEC CPU missing result: %s', benchmark)
      missing_results.append(str(benchmark.split()[0]))
      continue
    # name, ref_time, time, score, misc
    name, _, _, score_str, _ = benchmark.split()
    if runspec_metric == 'speed':
      name += ':speed'
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
          'speccpu: results missing, see log: ' + ','.join(missing_results))

  if spec_score is not None:
    results.append(sample.Sample(spec_name, spec_score, '', metadata))
  elif FLAGS.runspec_estimate_spec:
    estimated_spec_score = _GeometricMean(scores)
    results.append(sample.Sample('estimated_' + spec_name,
                                 estimated_spec_score, '', metadata))

  return results


def _GeometricMean(arr):
  """Calculates the geometric mean of the array."""
  product = 1
  for val in arr:
    product *= val
  return product ** (1.0 / len(arr))


def ParseOutput(vm, log_files, is_partial_results, runspec_metric):
  """Retrieves the SPEC CPU output from the VM and parses it.

  Args:
    vm: Vm. The vm instance where SPEC CPU was run.
    log_files: String. Path of the directory on the remote machine where the
        SPEC files, including binaries and logs, are located.
    is_partial_results: Boolean. True if the output is partial result.
    runspec_metric: String. Indicates whether this is spec speed or rate run.

  Returns:
    A list of samples to be published (in the same format as Run() returns).
  """
  speccpu_vm_state = getattr(vm, _VM_STATE_ATTR, None)
  results = []

  for log in log_files:
    spec_dir = speccpu_vm_state.spec_dir
    stdout, _ = vm.RemoteCommand('cat %s/result/%s' % (spec_dir, log),
                                 should_log=True)
    results.extend(_ExtractScore(
        stdout, vm, FLAGS.runspec_keep_partial_results or is_partial_results,
        runspec_metric))

  return results


def Run(vm, benchmark_subset, version_specific_parameters=None):
  """Runs SPEC CPU on the target vm.

  Args:
    vm: Vm. The vm on which speccpu will run.
    benchmark_subset: List. Subset of the benchmark to run.
    version_specific_parameters: List. List of parameters for specific versions.

  Returns:
    A list of sample.Sample objects.
  """
  speccpu_vm_state = getattr(vm, _VM_STATE_ATTR, None)
  runspec_flags = [
      ('config', posixpath.basename(speccpu_vm_state.cfg_file_path)),
      ('tune', 'base'), ('size', 'ref'),
      ('iterations', FLAGS.runspec_iterations)]
  if FLAGS.runspec_define:
    for runspec_define in FLAGS.runspec_define.split(','):
      runspec_flags.append(('define', runspec_define))
  fl = ' '.join('--{0}={1}'.format(k, v) for k, v in runspec_flags)

  if version_specific_parameters:
    fl += ' '.join(version_specific_parameters)

  runspec_cmd = 'runspec --noreportable {flags} {subset}'.format(
      flags=fl, subset=benchmark_subset)

  cmd = ' && '.join((
      'cd {0}'.format(speccpu_vm_state.spec_dir), '. ./shrc', './bin/relocate',
      '. ./shrc', 'rm -rf result', runspec_cmd))
  vm.RobustRemoteCommand(cmd)


def Uninstall(vm):
  """Cleans up SPECCPU from the target vm.

  Args:
    vm: The vm on which SPECCPU is uninstalled.
  """
  speccpu_vm_state = getattr(vm, _VM_STATE_ATTR, None)
  if speccpu_vm_state:
    if speccpu_vm_state.mount_dir:
      try:
        vm.RemoteCommand('sudo umount {0}'.format(speccpu_vm_state.mount_dir))
      except errors.VirtualMachine.RemoteCommandError:
        # Even if umount failed, continue to clean up.
        logging.exception('umount failed.')
    targets = ' '.join(p for p in speccpu_vm_state.__dict__.values() if p)
    vm.RemoteCommand('rm -rf {0}'.format(targets))
