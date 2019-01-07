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
"""Module containing IOR installation and cleanup functions."""

import csv
import io
import posixpath
import re

from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import INSTALL_DIR

IOR_DIR = '%s/ior' % INSTALL_DIR
GIT_REPO = 'https://github.com/hpc/ior'
GIT_TAG = '945fba2aa2d571e8babc4f5f01e78e9f5e6e193e'
_METADATA_KEYS = [
    'Operation', '#Tasks', 'segcnt', 'blksiz', 'xsize', 'aggsize', 'API', 'fPP',
]
_MDTEST_RESULT_REGEX = (r'\s*(.*?)\s*:\s*(\d+\.\d+)\s*(\d+\.\d+)'
                        r'\s*(\d+\.\d+)\s*(\d+\.\d+)')
_MDTEST_SUMMARY_REGEX = r'(\d+) tasks, (\d+) files[^\n]*\n\s*\n(.*?)\n\s*\n'


def Install(vm):
  """Installs IOR on the VM."""
  vm.Install('openmpi')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, IOR_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(IOR_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && ./bootstrap && ./configure && make && '
                   'sudo make install'.format(IOR_DIR))


def Uninstall(vm):
  """Uninstalls IOR on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(IOR_DIR))


def RunIOR(master_vm, num_tasks, script_path):
  """Runs IOR against the master VM."""
  directory = master_vm.scratch_disks[0].mount_point
  ior_cmd = (
      'cd {directory} && '
      'mpiexec -oversubscribe -machinefile ~/MACHINEFILE -n {num_tasks} '
      'ior -f {script_path}'
  ).format(directory=directory, num_tasks=num_tasks, script_path=script_path)

  stdout, _ = master_vm.RobustRemoteCommand(ior_cmd)
  return ParseIORResults(stdout)


def ParseIORResults(test_output):
  """"Parses the test output and returns samples."""
  random_offsets = (ordering == 'random offsets' for ordering in
                    re.findall('ordering in a file = (.*)', test_output))
  match = re.search(
      'Summary of all tests:\n(.*?)Finished', test_output, re.DOTALL)
  fp = io.StringIO(re.sub(' +', ' ', match.group(1)))
  result_dicts = csv.DictReader(fp, delimiter=' ')
  results = []
  for result_dict in result_dicts:
    metadata = {'random_offsets': next(random_offsets)}
    for key in _METADATA_KEYS:
      metadata[key] = result_dict[key]
    bandwidth = float(result_dict['Mean(MiB)'])
    iops = float(result_dict['Mean(OPs)'])
    results.extend([
        sample.Sample('Bandwidth', bandwidth, 'MiB/s', metadata),
        sample.Sample('IOPS', iops, 'OPs/s', metadata)
    ])
  return results


def RunMdtest(master_vm, num_tasks, mdtest_args):
  """Run mdtest against the master vm."""
  directory = posixpath.join(master_vm.scratch_disks[0].mount_point, 'mdtest')
  mdtest_cmd = (
      'mpiexec -oversubscribe -machinefile MACHINEFILE -n {num_tasks} '
      'mdtest -d {directory} {additional_args}'
  ).format(
      directory=directory, num_tasks=num_tasks, additional_args=mdtest_args
  )

  stdout, _ = master_vm.RobustRemoteCommand(mdtest_cmd)
  return ParseMdtestResults(stdout)


def ParseMdtestResults(test_output):
  """Parses the test output and returns samples."""
  results = []
  match = re.search('Command line used: (.*)', test_output)
  command_line = match.group(1).strip()
  dir_per_task = '-u' in command_line
  summaries = re.findall(_MDTEST_SUMMARY_REGEX, test_output, re.DOTALL)
  for num_tasks, num_files, summary in summaries:
    metadata = {
        'command_line': command_line, 'num_tasks': num_tasks,
        'num_files': num_files, 'dir_per_task': dir_per_task
    }
    result_lines = re.findall(_MDTEST_RESULT_REGEX, summary)
    for result_line in result_lines:
      op_type, max_ops, min_ops, mean_ops, std_dev = result_line
      if not float(mean_ops):
        continue
      results.append(sample.Sample(
          op_type + ' (Mean)', float(mean_ops), 'OPs/s', metadata))
      if float(std_dev):
        results.extend([
            sample.Sample(
                op_type + ' (Max)', float(max_ops), 'OPs/s', metadata),
            sample.Sample(
                op_type + ' (Min)', float(min_ops), 'OPs/s', metadata),
            sample.Sample(
                op_type + ' (Std Dev)', float(std_dev), 'OPs/s', metadata)
        ])
  return results
