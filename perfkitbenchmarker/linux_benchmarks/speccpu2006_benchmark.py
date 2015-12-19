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

"""Runs SpecCPU2006.

From SpecCPU2006's documentation:
The SPEC CPU2006 benchmark is SPEC's industry-standardized, CPU-intensive
benchmark suite, stressing a system's processor, memory subsystem and compiler.

SpecCPU2006 homepage: http://www.spec.org/cpu2006/
"""

import logging
import posixpath
import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

flags.DEFINE_enum('benchmark_subset', 'int', ['int', 'fp', 'all'],
                  'specify a subset of benchmarks to run: int, fp, all')

flags.DEFINE_string('runspec_config', 'linux64-x64-gcc47.cfg',
                    'name of the cpu2006 configuration to use (runspec --config'
                    ' argument)')

flags.DEFINE_integer('runspec_iterations', 3,
                     'number of benchmark iterations to execute - default 3 '
                     '(runspec --iterations argument)')

flags.DEFINE_string('runspec_define', '',
                    'optional comma separated list of preprocessor macros: '
                    'SYMBOL[=VALUE] - e.g. numa,smt,sse=SSE4.2 (runspec '
                    '--define arguments)')

flags.DEFINE_boolean('runspec_enable_32bit', default=False,
                     help='setting this flag will result in installation of '
                     'multilib packages to enable use of 32-bit cpu2006 '
                     'binaries (useful when running on memory constrained '
                     'instance types where 64-bit execution may be problematic '
                     ' - i.e. < 1.5-2GB/core)')

flags.DEFINE_boolean('runspec_keep_partial_results', False,
                     'speccpu will report an aggregate score even if some of '
                     'the component tests failed with a "NR" status. If this '
                     'flag is set to true, save the available results and '
                     'mark metadata with partial=true. If unset, partial '
                     'failures are treated as errors.')

BENCHMARK_NAME = 'speccpu2006'
BENCHMARK_CONFIG = """
speccpu2006:
  description: Run Spec CPU2006
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

SPECCPU2006_TAR = 'cpu2006v1.2.tgz'
SPECCPU2006_DIR = 'cpu2006'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(SPECCPU2006_TAR)


def Prepare(benchmark_spec):
  """Install SpecCPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('prepare SpecCPU2006 on %s', vm)
  vm.Install('wget')
  vm.Install('build_tools')
  vm.Install('fortran')
  if (FLAGS.runspec_enable_32bit):
    vm.Install('multilib')
  vm.Install('numactl')
  try:
    local_tar_file_path = data.ResourcePath(SPECCPU2006_TAR)
  except data.ResourceNotFound as e:
    logging.error('Please provide %s under perfkitbenchmarker/data directory '
                  'before running SpecCPU2006 benchmark.', SPECCPU2006_TAR)
    raise errors.Benchmarks.PrepareException(str(e))
  vm.tar_file_path = posixpath.join(vm.GetScratchDir(), SPECCPU2006_TAR)
  vm.spec_dir = posixpath.join(vm.GetScratchDir(), SPECCPU2006_DIR)
  vm.RemoteCommand('chmod 777 %s' % vm.GetScratchDir())
  vm.PushFile(local_tar_file_path, vm.GetScratchDir())
  vm.RemoteCommand('cd %s && tar xvfz %s' % (vm.GetScratchDir(),
                                             SPECCPU2006_TAR))


def ExtractScore(stdout, vm, keep_partial_results):
  """Exact the Spec (int|fp) score from stdout.

  Args:
    stdout: stdout from running RemoteCommand.
    vm: The vm instance where Spec CPU2006 was run.
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

  metadata = {'num_cpus': vm.num_cpus}
  metadata.update(vm.GetMachineTypeDict())

  missing_results = []

  for benchmark in result_section:
    # Skip over failed runs, but count them since they make the overall
    # result invalid.
    if 'NR' in benchmark:
      logging.warning('SpecCPU2006 missing result: %s', benchmark)
      missing_results.append(str(benchmark.split()[0]))
      continue
    # name, ref_time, time, score, misc
    name, _, _, score, _ = benchmark.split()
    results.append(sample.Sample(str(name), float(score), '', metadata))

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

  return results


def ParseOutput(vm):
  """Parses the output from Spec CPU2006.

  Args:
    vm: The vm instance where Spec CPU2006 was run.

  Returns:
    A list of samples to be published (in the same format as Run() returns).
  """
  results = []

  log_files = []
  # FIXME(liquncheng): Only reference runs generate SPEC scores. The log
  # id is hardcoded as 001, which might change with different runspec
  # parameters. Spec CPU 2006 will generate different logs for build, test
  # run, training run and ref run.
  if FLAGS.benchmark_subset in ('int', 'all'):
    log_files.append('CINT2006.001.ref.txt')
  if FLAGS.benchmark_subset in ('fp', 'all'):
    log_files.append('CFP2006.001.ref.txt')

  for log in log_files:
    stdout, _ = vm.RemoteCommand('cat %s/result/%s' % (vm.spec_dir, log),
                                 should_log=True)
    results.extend(ExtractScore(stdout, vm, FLAGS.runspec_keep_partial_results))

  return results


def Run(benchmark_spec):
  """Run SpecCPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('SpecCPU2006 running on %s', vm)
  num_cpus = vm.num_cpus
  iterations = ' --iterations=' + repr(FLAGS.runspec_iterations) if \
               FLAGS.runspec_iterations != 3 else ''
  defines = ' --define ' + ' --define '.join(FLAGS.runspec_define.split(','))\
            if FLAGS.runspec_define != '' else ''
  cmd = ('cd %s; . ./shrc; ./bin/relocate; . ./shrc; rm -rf result; '
         'runspec --config=%s --tune=base '
         '--size=ref --noreportable --rate %s%s%s %s'
         % (vm.spec_dir, FLAGS.runspec_config, num_cpus, iterations,
            defines, FLAGS.benchmark_subset))
  vm.RobustRemoteCommand(cmd)
  logging.info('SpecCPU2006 Results:')
  return ParseOutput(vm)


def Cleanup(benchmark_spec):
  """Cleanup SpecCPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  vm.RemoteCommand('rm -rf %s' % vm.spec_dir)
  vm.RemoteCommand('rm -f %s' % vm.tar_file_path)
