# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs tcpdump on VMs.

Tcpdump is run in the background before the Run phase, the pcap file is copied
to the current run's temp directory with a name like
pkb-<machine_name>-<benchmark>-<UUID>-tcpdump.stdout that can be read in with
"tcpdump -r <filename>"
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker.traces import base_collector

flags.DEFINE_boolean(
    'tcpdump', False, 'Run tcpdump on each VM to collect network packets in '
    'each benchmark run.')
flags.DEFINE_list('tcpdump_ignore_ports', [22],
                  'Ports to ignore when running tcpdump')
flags.DEFINE_list(
    'tcpdump_include_ports', [], 'Ports to include when running tcpdump.  By '
    'default collects all ports except those in --tcpdump_ignore_ports')
flags.DEFINE_integer('tcpdump_snaplen', 96,
                     'Tcpdump snaplen, see man tcpdump "-s"')
flags.DEFINE_integer(
    'tcpdump_packet_count', None, 'Number of packets to collect. Default '
    'is to collect all packets in the run phase')

FLAGS = flags.FLAGS


def _PortFilter(ports):
  """Returns the port filter suitable for tcpdump.

  Example: _PortFilter([22, 53]) => ['port', '(22 or 53)']

  Args:
    ports: List of ports to filter on.

  Returns:
    Two element list to append to tcpdump command.
  """
  return ['port', r'\({}\)'.format(' or '.join([str(port) for port in ports]))]


class _TcpdumpCollector(base_collector.BaseCollector):
  """tcpdump collector.

  Installs tcpdump and runs it on the VMs.
  """

  def __init__(self,
               ignore_ports=None,
               include_ports=None,
               snaplen=None,
               packet_count=None):
    super(_TcpdumpCollector, self).__init__(None, None)
    self.snaplen = snaplen
    self.packet_count = packet_count
    if include_ports:
      self.filter = _PortFilter(include_ports)
    elif ignore_ports:
      self.filter = ['not'] + _PortFilter(ignore_ports)
    else:
      self.filter = []

  def _CollectorName(self):
    """See base class."""
    return 'tcpdump'

  def _KillCommand(self, pid):
    """See base class.

    Args:
      pid: The pid of the process to kill

    Different from base class:
    1. Needs to run as sudo as tcpdump launched as root
    2. Sends a SIGINT signal so that tcpdump can flush its cache
    3. Sleep for 3 seconds to allow the flush to happen

    Returns:
      String command to run to kill of tcpdump.
    """
    return 'sudo kill -s INT {}; sleep 3'.format(pid)

  def _InstallCollector(self, vm):
    """See base class."""
    vm.InstallPackages('tcpdump')

  def _CollectorRunCommand(self, vm, collector_file):
    """See base class."""
    cmd = ['sudo', 'tcpdump', '-n', '-w', collector_file]
    if self.snaplen:
      cmd.extend(['-s', str(self.snaplen)])
    if self.packet_count:
      cmd.extend(['-c', str(self.packet_count)])
    cmd.extend(self.filter)
    # ignore stdout, stderr, put in background and echo the pid
    cmd.extend(['>', '/dev/null', '2>&1', '&', 'echo $!'])
    return ' '.join(cmd)


def _CreateCollector(parsed_flags):
  """Creates a _TcpdumpCollector from flags."""
  return _TcpdumpCollector(
      ignore_ports=parsed_flags.tcpdump_ignore_ports,
      include_ports=parsed_flags.tcpdump_include_ports,
      snaplen=parsed_flags.tcpdump_snaplen,
      packet_count=parsed_flags.tcpdump_packet_count)


def Register(parsed_flags):
  """Registers the tcpdump collector if FLAGS.tcpdump is set."""
  if not parsed_flags.tcpdump:
    return
  collector = _CreateCollector(parsed_flags)
  events.before_phase.connect(collector.Start, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.Stop, events.RUN_PHASE, weak=False)
