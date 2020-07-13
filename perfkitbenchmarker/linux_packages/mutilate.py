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


"""Module containing mutilate installation and cleanup functions."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import INSTALL_DIR

GIT_REPO = 'https://github.com/leverich/mutilate'
MUTILATE_DIR = '%s/mutilate_benchmark' % INSTALL_DIR
MUTILATE_BIN = '%s/mutilate' % MUTILATE_DIR
APT_PACKAGES = 'scons libevent-dev gengetopt libzmq-dev'


FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'mutilate_protocol',
    'binary', ['binary', 'ascii'],
    'Protocol to use. Supported protocols are binary and ascii.')
flags.DEFINE_list(
    'mutilate_qps', [],
    'Target aggregate QPS. If not set, target for peak qps.')
flags.DEFINE_integer(
    'mutilate_time', 300,
    'Maximum time to run (seconds).')
flags.DEFINE_string(
    'mutilate_keysize', '16',
    'Length of memcached keys (distribution).')
flags.DEFINE_string(
    'mutilate_valuesize', '128',
    'Length of memcached values (distribution).')
flags.DEFINE_integer(
    'mutilate_records', 10000,
    'Number of memcached records to use.')
flags.DEFINE_float(
    'mutilate_ratio', 0.0,
    'Ratio of set:get. By default, read only.')
flags.DEFINE_list(
    'mutilate_options', [],
    'Additional mutilate long-form options (--) in comma separated form. e.g.'
    '--mutilate_options=blocking,search=99:1000.'
    'See https://github.com/leverich/mutilate for all available options.')

# If more than one value provided for threads, connections, depths, we will
# enumerate all test configurations. e.g.
# threads=1,2; connections=3,4; depths=5,6
# We will test following threads:connections:depths:
#   1,3,5; 1,3,6; 1,4,5; 1,4,6; 2,3,5; 2,3,6; 2,4,5; 2,4,6;
flags.DEFINE_list(
    'mutilate_threads', [1],
    'Number of total client threads to spawn per client VM.')
flags.DEFINE_list(
    'mutilate_connections', [1],
    'Number of connections to establish per client thread.')
flags.DEFINE_list(
    'mutilate_depths', [1],
    'Maximum depth to pipeline requests.')

# Agent mode options.
flags.DEFINE_integer(
    'mutilate_measure_connections', None,
    'Master client connections.')
flags.DEFINE_integer(
    'mutilate_measure_threads', None,
    'Master client thread count.')
flags.DEFINE_integer(
    'mutilate_measure_qps', None,
    'Master client QPS.')
flags.DEFINE_integer(
    'mutilate_measure_depth', None,
    'Master client connection depth.')
# To use remote agent mode, we need at least 2 VMs.
AGENT_MODE_MIN_CLIENT_VMS = 2


def CheckPrerequisites():
  """Verify flags are correctly specified.

  Raises:
    errors.Setup.InvalidFlagConfigurationError: On invalid flag configurations.
  """
  agent_mode_flags = [FLAGS['mutilate_measure_connections'].present,
                      FLAGS['mutilate_measure_threads'].present,
                      FLAGS['mutilate_measure_qps'].present,
                      FLAGS['mutilate_measure_depth'].present]

  error_message = (
      'To enable agent mode, set '
      'memcached_mutilate_num_client_vms > 1.')
  if any(agent_mode_flags) and (
      FLAGS.memcached_mutilate_num_client_vms < AGENT_MODE_MIN_CLIENT_VMS):
    raise errors.Setup.InvalidFlagConfigurationError(error_message)


def YumInstall(vm):
  """Installs the mutilate package on the VM."""
  raise NotImplementedError


def AptInstall(vm):
  """Installs the mutilate package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(APT_PACKAGES)
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, MUTILATE_DIR))
  vm.RemoteCommand('cd {0} && sudo scons'.format(MUTILATE_DIR))


def GetMetadata():
  """Returns mutilate metadata."""
  metadata = {
      'protocol': FLAGS.mutilate_protocol,
      'qps': FLAGS.mutilate_qps or 'peak',
      'time': FLAGS.mutilate_time,
      'keysize': FLAGS.mutilate_keysize,
      'valuesize': FLAGS.mutilate_valuesize,
      'records': FLAGS.mutilate_records,
      'ratio': FLAGS.mutilate_ratio
  }
  if FLAGS.mutilate_options:
    metadata['options'] = FLAGS.mutilate_options

  return metadata


def BuildCmd(server_ip, server_port, options):
  """Build base mutilate command in a list."""
  cmd = [MUTILATE_BIN,
         '--server=%s:%s' % (server_ip, server_port),
         '--keysize=%s' % FLAGS.mutilate_keysize,
         '--valuesize=%s' % FLAGS.mutilate_valuesize,
         '--records=%s' % FLAGS.mutilate_records] + options
  if FLAGS.mutilate_protocol == 'binary':
    cmd.append('--binary')
  return cmd


def RestartAgent(vm, threads):
  logging.info('Restarting mutilate remote agent on %s', vm.internal_ip)
  # Kill existing mutilate agent threads
  vm.RemoteCommand('pkill -9 mutilate', ignore_failure=True)
  vm.RemoteCommand(' '.join(
      ['nohup',
       MUTILATE_BIN,
       '--threads=%s' % threads,
       '--agentmode',
       '1>/dev/null',
       '2>/dev/null',
       '&']))


def Load(client_vm, server_ip, server_port):
  """Preload the server with data."""
  logging.info('Loading memcached server.')
  cmd = BuildCmd(
      server_ip, server_port,
      ['--loadonly'])
  client_vm.RemoteCommand(' '.join(cmd))


def Run(vms, server_ip, server_port):
  """Runs the mutilate benchmark on the vm."""
  samples = []
  master = vms[0]
  runtime_options = {}
  samples = []
  measure_flags = []
  additional_flags = ['--%s' % option for option in FLAGS.mutilate_options]

  if FLAGS.mutilate_measure_connections:
    runtime_options['measure_connections'] = FLAGS.mutilate_measure_connections
    measure_flags.append(
        '--measure_connections=%s' % FLAGS.mutilate_measure_connections)
  if FLAGS.mutilate_measure_threads:
    runtime_options['measure_threads'] = FLAGS.mutilate_measure_threads
  if FLAGS.mutilate_measure_qps:
    runtime_options['measure_qps'] = FLAGS.mutilate_measure_qps
    measure_flags.append(
        '--measure_qps=%s' % FLAGS.mutilate_measure_qps)
  if FLAGS.mutilate_measure_depth:
    runtime_options['measure_depth'] = FLAGS.mutilate_measure_depth
    measure_flags.append(
        '--measure_depth=%s' % FLAGS.mutilate_measure_depth)

  for thread_count in FLAGS.mutilate_threads:
    runtime_options['threads'] = thread_count
    for vm in vms[1:]:
      RestartAgent(vm, thread_count)
    for connection_count in FLAGS.mutilate_connections:
      runtime_options['connections'] = connection_count
      for depth in FLAGS.mutilate_depths:
        runtime_options['depth'] = depth
        for qps in FLAGS.mutilate_qps or [0]:  # 0 indicates peak target QPS.
          runtime_options['qps'] = int(qps) or 'peak'
          remote_agents = ['--agent=%s' % vm.internal_ip for vm in vms[1:]]
          cmd = BuildCmd(
              server_ip, server_port,
              [
                  '--noload',
                  '--qps=%s' % qps,
                  '--time=%s' % FLAGS.mutilate_time,
                  '--update=%s' % FLAGS.mutilate_ratio,
                  '--threads=%s' % (
                      FLAGS.mutilate_measure_threads or thread_count),
                  '--connections=%s' % connection_count,
                  '--depth=%s' % depth,
              ] + remote_agents + measure_flags + additional_flags)

          stdout, _ = master.RemoteCommand(' '.join(cmd))
          metadata = GetMetadata()
          metadata.update(runtime_options)
          samples.extend(ParseResults(stdout, metadata))

  return samples


LATENCY_HEADER_REGEX = r'#type([\s\w\d]*)\n'
LATENCY_REGEX = r'([\s\d\.]*)'
QPS_REGEX = r'Total QPS = ([\d\.]*)'
MISS_REGEX = r'Misses = \d+ \(([\d\.]*)%\)'
BANDWIDTH_REGEX = r'[\s\d]*bytes :\s*([\d\.]*) MB/s'


def ParseResults(result, metadata):
  """Parse mutilate result into samples.

  Sample Output:
  #type       avg     min     1st     5th    10th    90th    95th    99th
  read       52.4    41.0    43.1    45.2    48.1    55.8    56.6    71.5
  update      0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0
  op_q        1.5     1.0     1.0     1.1     1.1     1.9     2.0     2.0

  Total QPS = 18416.6 (92083 / 5.0s)

  Misses = 0 (0.0%)

  RX   22744501 bytes :    4.3 MB/s
  TX    3315024 bytes :    0.6 MB/s

  Args:
    result: Text output of running mutilate benchmark.
    metadata: metadata associated with the results.

  Returns:
    List of sample.Sample objects.
  """
  samples = []
  misses = regex_util.ExtractGroup(MISS_REGEX, result)
  metadata['miss_rate'] = float(misses)

  latency_stats = regex_util.ExtractGroup(LATENCY_HEADER_REGEX, result).split()
  # parse latency
  for metric in ('read', 'update', 'op_q'):
    latency_regex = metric + LATENCY_REGEX
    latency_values = regex_util.ExtractGroup(latency_regex, result).split()
    for idx, stat in enumerate(latency_stats):
      if idx == len(latency_values):
        logging.warning(
            'Mutilate does not report %s latency for %s.', stat, metric)
        break
      samples.append(
          sample.Sample(metric + '_' + stat,
                        float(latency_values[idx]),
                        'usec', metadata))
  # parse bandwidth
  for metric in ('TX', 'RX'):
    bw_regex = metric + BANDWIDTH_REGEX
    bw = regex_util.ExtractGroup(bw_regex, result)
    samples.append(
        sample.Sample(metric, float(bw), 'MB/s', metadata))

  qps = regex_util.ExtractFloat(QPS_REGEX, result)
  samples.append(sample.Sample('qps', qps, 'ops/s', metadata))
  return samples
