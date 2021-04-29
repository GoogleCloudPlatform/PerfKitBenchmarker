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


import logging
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

GIT_REPO = 'https://github.com/leverich/mutilate'
MUTILATE_DIR = '%s/mutilate_benchmark' % linux_packages.INSTALL_DIR
MUTILATE_BIN = '%s/mutilate' % MUTILATE_DIR
APT_PACKAGES = 'scons libevent-dev gengetopt libzmq3-dev'


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
    'mutilate_options', ['iadist=exponential:0.0'],
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
_INCREMENTAL_LOAD = flags.DEFINE_float(
    'mutilate_incremental_load', None, 'Increments target qps until hits peak.')
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
  if _INCREMENTAL_LOAD.value and (len(FLAGS.mutilate_qps) != 1 or
                                  int(FLAGS.mutilate_qps[0]) == 0):
    raise errors.Setup.InvalidFlagConfigurationError(
        'To use dynamic load, set inital target qps with --mutilate_qps '
        'and incremental with --mutilate_incremental_load.')


def YumInstall(vm):
  """Installs the mutilate package on the VM."""
  raise NotImplementedError


def AptInstall(vm):
  """Installs the mutilate package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(APT_PACKAGES)
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, MUTILATE_DIR))
  vm.RemoteCommand(
      f'sed -i "s|int total|long total|g" {MUTILATE_DIR}/mutilate.cc')
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


def BuildCmd(server_ip, server_port, num_instances, options):
  """Build base mutilate command in a list."""
  server_ips = []
  for idx in range(num_instances):
    server_ips.append(f'--server={server_ip}:{server_port + idx}')
  cmd = [
      'ulimit -n 32768; ', MUTILATE_BIN,
      '--keysize=%s' % FLAGS.mutilate_keysize,
      '--valuesize=%s' % FLAGS.mutilate_valuesize,
      '--records=%s' % FLAGS.mutilate_records,
      '--roundrobin' if len(server_ips) > 1 else ''
  ] + server_ips + options
  if FLAGS.mutilate_protocol == 'binary':
    cmd.append('--binary')
  return cmd


def Load(client_vm, server_ip, server_port):
  """Preload the server with data."""
  logging.info('Loading memcached server.')
  cmd = BuildCmd(
      server_ip, server_port, 1,
      ['--loadonly'])
  client_vm.RemoteCommand(' '.join(cmd))


def RestartAgent(vm, threads):
  logging.info('Restarting mutilate remote agent on %s', vm.internal_ip)
  # Kill existing mutilate agent threads
  vm.RemoteCommand('pkill -9 mutilate', ignore_failure=True)
  # Make sure have enough file descriptor for the agent process.
  vm.RemoteCommand(' '.join([
      'ulimit -n 32768; '
      'nohup', MUTILATE_BIN,
      '--threads=%s' % threads, '--agentmode', '&> log', '&'
  ]))


def Run(vms, server_ip, server_port, num_instances):
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

        target_qps_list = FLAGS.mutilate_qps[:] or [0]
        while True:
          target_qps = int(target_qps_list[0])
          runtime_options['qps'] = target_qps or 'peak'
          remote_agents = ['--agent=%s' % vm.internal_ip for vm in vms[1:]]
          cmd = BuildCmd(server_ip, server_port, num_instances, [
              '--noload',
              '--qps=%s' % target_qps,
              '--time=%s' % FLAGS.mutilate_time,
              '--update=%s' % FLAGS.mutilate_ratio,
              '--threads=%s' % (FLAGS.mutilate_measure_threads or thread_count),
              '--connections=%s' % connection_count,
              '--depth=%s' % depth,
          ] + remote_agents + measure_flags + additional_flags)

          try:
            stdout, _, retcode = master.RemoteHostCommandWithReturnCode(
                ' '.join(cmd), timeout=FLAGS.mutilate_time * 2,
                ignore_failure=True)
          except errors.VmUtil.IssueCommandTimeoutError:
            break
          if retcode:
            break
          metadata = GetMetadata()
          metadata.update(runtime_options)
          run_samples, actual_qps = ParseResults(stdout, metadata)
          samples.extend(run_samples)

          if _INCREMENTAL_LOAD.value and (actual_qps / target_qps >
                                          (1 - _INCREMENTAL_LOAD.value * 2)):
            target_qps_list.append(
                int(target_qps) * (1 + _INCREMENTAL_LOAD.value))
          target_qps_list.pop(0)
          if not target_qps_list:
            break
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
    List of sample.Sample objects and actual qps.
  """
  samples = []
  if FLAGS.mutilate_ratio < 1.0:
    # N/A for write only workloads.
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
  return samples, qps
