# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Records system performance counters during benchmark runs using collectd.

http://collectd.org
"""

import logging
import os
import posixpath

from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import collectd

flags.DEFINE_boolean('collectd', False,
                     'Install and run collectd on the guest.')
flags.DEFINE_string('collectd_output', None, 'Path to store collectd results.')


class _CollectdCollector(object):
  """Manages running collectd during a test, and fetching the CSV results."""

  def __init__(self, target_dir):
    self.target_dir = target_dir

  def _FetchResults(self, vm):
    """Stops collectd on the VM, fetches CSV results."""
    logging.info('Fetching collectd results')
    local_dir = os.path.join(self.target_dir, vm.name + '-collectd')
    # On the remote host, CSV files are in:
    # self.csv_dir/<fqdn>/<category>.
    # Since AWS VMs have a FQDN different from the VM name, we rename locally.
    vm.PullFile(local_dir, posixpath.join(collectd.CSV_DIR, '*', ''))

  def Before(self, unused_sender, benchmark_spec):
    """Install collectd.

    Args:
      benchmark_spec: benchmark_spec.BenchmarkSpec. The benchmark currently
          running.
    """
    logging.info('Installing collectd')
    vms = benchmark_spec.vms
    vm_util.RunThreaded(lambda vm: vm.Install('collectd'), vms)

  def After(self, unused_sender, benchmark_spec):
    """Stop / delete collectd, fetch results from VMs.

    Args:
      benchmark_spec: benchmark_spec.BenchmarkSpec. The benchmark that stopped
          running.
    """
    logging.info('Stopping collectd')
    vms = benchmark_spec.vms
    vm_util.RunThreaded(self._FetchResults, vms)


def Register(parsed_flags):
  """Register the collector if FLAGS.collectd is set."""
  if not parsed_flags.collectd:
    return

  logging.info('Registering collectd collector')

  output_directory = parsed_flags.collectd_output or vm_util.GetTempDir()
  if not os.path.isdir(output_directory):
    raise IOError('collectd output directory does not exist: {0}'.format(
        output_directory))
  collector = _CollectdCollector(output_directory)
  events.before_phase.connect(collector.Before, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.After, events.RUN_PHASE, weak=False)
