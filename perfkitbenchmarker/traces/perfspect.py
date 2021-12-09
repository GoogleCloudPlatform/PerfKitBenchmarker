# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""
PerfSpect is a system performance profiling and processing tool based on linux perf.

Usage:
  Required flags:
  --perfspect

  Example:
  ./pkb.py --cloud=AWS --benchmarks=your_wl --machine_type=m5.2xlarge --os_type=ubuntu2004 --perfspect

  Refer to ./perfkitbenchmarker/data/perfspect/README.md for more details on flags and usage
"""

import logging
import os
import posixpath

from absl import flags
from six.moves.urllib.parse import urlparse

from perfkitbenchmarker import events
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import data

FLAGS = flags.FLAGS

flags.DEFINE_boolean('perfspect', False,
                     'Install and run perfspect on the target system.')
flags.DEFINE_string('perfspect_tarball', None,
                    'Local path to perfspect tarball.')
flags.DEFINE_string('perfspect_url', None,
                    'URL for downloading perfspect tarball.')

PERFSPECT_ARCHIVE_URL = 'https://github.com/intel/PerfSpect/releases/download/v1.1.0/perfspect_1.1.0.tgz'
PREREQ_UBUNTU = ['linux-tools-common',
                 'linux-tools-generic',
                 'linux-tools-`uname -r`']
PREREQ_CENTOS = ['perf']
PREREQ_PKGS = ['python3-pip']


class PerfspectCollector(object):
  """ Manages running telemetry during a test, and fetching the results folder. """

  telemetry_dir = '/opt/perf_telemetry'

  def __init__(self):
    self.pid = None
    self.perf_dir = None

  def _InstallOSReqs(self, vm):
    """ Installs prereqs depending on the OS """
    if 'ubuntu' in vm.OS_TYPE:
      vm.InstallPackages(' '.join(PREREQ_UBUNTU))
    elif 'centos' in vm.OS_TYPE:
      vm.InstallPackages(' '.join(PREREQ_CENTOS))
    else:
      raise errors.VirtualMachine.VirtualMachineError('OS not supported')

  def _InstallTelemetry(self, vm):
    """ Installs PerfSpect telemetry on the VM. """
    logging.info('Installing PerfSpect on VM')
    self._InstallOSReqs(vm)
    vm.InstallPackages(' '.join(PREREQ_PKGS))
    vm.RemoteCommand(f'sudo rm -rf {self.telemetry_dir}')
    vm.RemoteCommand(f'sudo mkdir -p {self.telemetry_dir}')
    vm.PushFile(self.perf_dir)
    vm.RemoteCommand(f'sudo cp -r ./perfspect {self.telemetry_dir}/')

  def _StartTelemetry(self, vm):
    """ Starts PerfSpect telemetry on the VM. """
    try:
      vm.RemoteCommand('perf list')
    except errors.VirtualMachine.RemoteCommandError as ex:
      logging.exception('Failed executing perf. Is it installed?')
      raise ex
    perf_collect_file = posixpath.join(self.telemetry_dir, 'perfspect', 'perf-collect.sh')
    vm.RemoteCommand(f'sudo chmod +x {perf_collect_file}')
    perf_dir = posixpath.join(self.telemetry_dir, 'perfspect')
    stdout, _ = vm.RemoteCommand(f'cd {perf_dir} && sudo ./perf-collect.sh', should_log=True)
    self.pid = stdout.strip()
    logging.debug(f'pid of PerfSpect collector process: {self.pid}')

  def _StopTelemetry(self, vm):
    """ Stops PerfSpect telemetry on the VM. """
    logging.info('Stopping PerfSpect telemetry')
    vm.RemoteCommand('sudo pkill -9 -x perf')
    vm.RemoteCommand(f'tail --pid={self.pid} -f /dev/null')
    logging.info('Post processing PerfSpect raw metrics')
    perf_dir = posixpath.join(self.telemetry_dir, 'perfspect')
    stdout, _ = vm.RemoteCommand(f'cd {perf_dir} && sudo ./perf-postprocess -r results/perfstat.csv')

  def _FetchResults(self, vm):
    """ Fetches PerfSpect telemetry results. """
    logging.info('Fetching PerfSpect telemetry results')
    perfspect_dir = '~/' + vm.name + '-perfspect'
    vm.RemoteCommand(f'mkdir {perfspect_dir}')
    results = posixpath.join(self.telemetry_dir, 'perfspect', 'results', '*')
    vm.RemoteCommand(f'sudo cp -r {results} {perfspect_dir}')
    vm.RemoteCopy(vm_util.GetTempDir(), perfspect_dir, False)
    logging.info('PerfSpect results copied')

  def _CleanupTelemetry(self, vm):
    """ PerfSpect cleanup routines """
    logging.info('Removing PerfSpect leftover files')
    vm_util.IssueCommand(['rm', '-rf', self.perf_dir, self.perfspect_archive])
    vm.RemoteCommand('sudo rm -rf ~/*perfspect*')
    logging.info('Removing PerfSpect from VM')
    vm.RemoteCommand(f'sudo rm -rf {self.telemetry_dir}')

  def _GetLocalArchive(self):
    """ Gets the local path of the PerfSpect archive. """
    if FLAGS.perfspect_tarball:
      logging.info(f'perfspect_tarball specified: {FLAGS.perfspect_tarball}')
      local_archive_path = FLAGS.perfspect_tarball
    else:
      url = FLAGS.perfspect_url or PERFSPECT_ARCHIVE_URL
      logging.info(f'downloading PerfSpect from: {url}')
      filename = os.path.basename(urlparse(url).path)
      local_archive_path = posixpath.join(vm_util.GetTempDir(), filename)
      vm_util.IssueCommand(['curl', '-k', '-L', '-o', local_archive_path, url], timeout=None)
    return local_archive_path

  def Before(self, unused_sender, benchmark_spec):
    """ Installs PerfSpect Telemetry.

    Args:
      benchmark_spec: benchmark_spec.BenchmarkSpec. The benchmark currently
          running.
    """
    logging.info('Installing PerfSpect telemetry')
    vms = benchmark_spec.vms

    self.perf_dir = posixpath.join(vm_util.GetTempDir(), 'perfspect')
    self.perfspect_archive = self._GetLocalArchive()
    vm_util.IssueCommand(['tar', '-C', vm_util.GetTempDir(), '-xf', self.perfspect_archive])
    vm_util.IssueCommand(['cp', data.ResourcePath(posixpath.join('perfspect', 'perf-collect.sh')),
                          self.perf_dir + "/"])
    vm_util.RunThreaded(self._InstallTelemetry, vms)

    logging.info('Starting PerfSpect telemetry')
    vm_util.RunThreaded(self._StartTelemetry, vms)

  def After(self, unused_sender, benchmark_spec):
    """ Stops PerfSpect telemetry, fetch results from VM(s).

    Args:
      benchmark_spec: benchmark_spec.BenchmarkSpec. The benchmark that stopped
          running.
    """
    vms = benchmark_spec.vms
    vm_util.RunThreaded(self._StopTelemetry, vms)
    vm_util.RunThreaded(self._FetchResults, vms)
    vm_util.RunThreaded(self._CleanupTelemetry, vms)


def Register(parsed_flags):
  """ Registers the PerfSpect collector if FLAGS.perfspect is set. """
  if not parsed_flags.perfspect:
    return
  logging.info('Registering PerfSpect telemetry collector')
  telemetry_collector = PerfspectCollector()
  events.before_phase.connect(telemetry_collector.Before, events.RUN_PHASE, weak=False)
  events.after_phase.connect(telemetry_collector.After, events.RUN_PHASE, weak=False)


def IsEnabled():
  return FLAGS.perfspect
