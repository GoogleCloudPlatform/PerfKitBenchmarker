"""Runs a cloudharmony iperf benchmark.

See https://github.com/cloudharmony/iperf for more info.
"""

import posixpath

from absl import flags
from perfkitbenchmarker import cloud_harmony_util
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_GIT_URL = 'https://github.com/cloudharmony/iperf.git'
BENCHMARK_NAME = 'cloudharmony_iperf'
BENCHMARK_CONFIG = """
cloudharmony_iperf:
  description: Runs cloudharmony iperf tests.
  vm_groups:
    client:
      os_type: ubuntu2004
      vm_spec: *default_single_core
    server:
      os_type: ubuntu2004
      vm_spec: *default_single_core
"""

FLAGS = flags.FLAGS
OUTPUT = 'iperf.csv'
TCP = 'TCP'
UDP = 'UDP'
_PORT = 8080
_BW_SCRATCH_FILE = '/tmp/tcpbw'
# List of cloudharmony metric names
_CLOUDHARMONY_IPERF_METRICS = [
    'bandwidth_max', 'bandwidth_mean', 'bandwidth_median', 'bandwidth_min',
    'bandwidth_p10', 'bandwidth_p25', 'bandwidth_p75', 'bandwidth_p90',
    'bandwidth_stdev',
    'jitter_max', 'jitter_mean', 'jitter_median', 'jitter_min',
    'jitter_p10', 'jitter_p25', 'jitter_p75', 'jitter_p90', 'jitter_stdev',
    'loss_max', 'loss_mean', 'loss_median', 'loss_min', 'loss_p10',
    'loss_p25', 'loss_p75', 'loss_p90', 'loss_stdev',
]


_IPERF_BANDWIDTH = flags.DEFINE_string(
    'ch_iperf_bandwidth', default=None,
    help='Set target bandwidth to n bits/sec.')
_IPERF_PARALLEL = flags.DEFINE_string(
    'ch_iperf_parallel', default='1', help='The number of simultaneous '
    'connections to make to the server. May contain [cpus] which will be '
    'automatically replaced with the number of CPU cores. May also contain an '
    'equation which will be automatically evaluated '
    '(e.g. --ch_iperf_parallel "[cpus]*2").')
_IPERF_CONCURRENCY = flags.DEFINE_integer(
    'ch_iperf_concurrency', default=1,
    help='Number of concurrent iperf server processes.')
_IPERF_TIME = flags.DEFINE_integer(
    'ch_iperf_time', default=10, help='The time in seconds to transmit for')
_IPERF_TEST = flags.DEFINE_enum(
    'ch_iperf_test', default=TCP, enum_values=[TCP, UDP],
    help='Run TCP or UDP.')
_IPERF_WARMUP = flags.DEFINE_integer(
    'ch_iperf_warmup', default=0, help='Number of initial seconds of testing to'
    ' ignore for result calculations.')
_IPERF_ZEROCOPY = flags.DEFINE_boolean(
    'ch_iperf_zerocopy', default=False, help='Use a "zero copy" method of '
    'sending data, such as sendfile, instead of the usual write.')
_IPERF_SERVER_VMS = flags.DEFINE_integer(
    'ch_iperf_server_vms', default=1, help='Number of servers for this test. ')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['server']['vm_count'] = _IPERF_SERVER_VMS.value
  return config


def _StartServer(vm):
  """Starts the server.

  Args:
    vm: the server virtual machine.
  """
  for port in list(range(_PORT, _PORT + _IPERF_CONCURRENCY.value)):
    vm.RemoteCommand(f'nohup iperf3 --server -p {port} &> /dev/null & echo $!')


def _PrepareVm(vm):
  """Prepares the server and the client.

  Args:
    vm: the server or client virtual machine.
  """
  vm.AuthenticateVm()
  vm.Install('iperf3')
  # Dependencies for cloudharmony wrapper
  vm.Install('php')
  vm.InstallPackages('gnuplot wkhtmltopdf xvfb zip')
  vm.RemoteCommand(f'git clone --recurse-submodules {BENCHMARK_GIT_URL}')


def Prepare(benchmark_spec):
  """Prepares the cloudharmony iperf benchmark."""
  vm_groups = benchmark_spec.vm_groups
  vm_util.RunThreaded(_PrepareVm, vm_groups['client'] + vm_groups['server'])
  vm_util.RunThreaded(_StartServer, vm_groups['server'])


def _Run(benchmark_spec):
  """Runs cloudharmony iperf from cloudharmony harness."""
  vm_groups = benchmark_spec.vm_groups
  client = vm_groups['client'][0]
  servers = vm_groups['server']
  ports = list(range(_PORT, _PORT + _IPERF_CONCURRENCY.value))
  ports_str = ':'.join([str(port) for port in ports])
  server_ports = [f'{server.internal_ip}:{ports_str}' for server in servers]
  endpoints = ''
  for endpoint in server_ports:
    endpoints += f' --iperf_server {endpoint}'

  metadata = {
      'iperf_server_instance_id': servers[0].machine_type,
      'iperf_server_region': FLAGS.zone[0],
      'meta_instance_id': client.machine_type,
      'iperf_time': _IPERF_TIME.value,
      'iperf_warmup': _IPERF_WARMUP.value,
      'iperf_test': _IPERF_TEST.value,
  }

  if _IPERF_PARALLEL.value:
    metadata['iperf_parallel'] = _IPERF_PARALLEL.value
  if _IPERF_BANDWIDTH.value:
    metadata['iperf_bandwidth'] = _IPERF_BANDWIDTH.value
  if _IPERF_ZEROCOPY.value:
    metadata['iperf_zerocopy'] = True
  metadata['tcp_bw_file'] = _BW_SCRATCH_FILE

  ch_warm_up_metadata = cloud_harmony_util.GetCommonMetadata(metadata)
  cmd_path = posixpath.join('iperf', 'run.sh')

  # Do a warmup tcp run (even if test is udp)
  warm_up_cmd = f'{cmd_path} {endpoints} {ch_warm_up_metadata} --wkhtml_xvfb'
  client.RobustRemoteCommand(warm_up_cmd)

  if _IPERF_TEST.value == UDP:
    metadata['iperf_reverse'] = True
    metadata['iperf_udp'] = True
    metadata['skip_bandwidth_graphs'] = True

  ch_metadata = cloud_harmony_util.GetCommonMetadata(metadata)
  cmd = (f'{cmd_path} {endpoints} {ch_metadata} --wkhtml_xvfb --verbose')

  client.RobustRemoteCommand(cmd)

  cmd_path = posixpath.join('iperf', 'save.sh')
  save_command = (f'sudo {cmd_path} ')

  if FLAGS.ch_store_results:
    save_command += cloud_harmony_util.GetSaveCommand()

  client.RemoteCommand(f'{save_command}')

  return cloud_harmony_util.ParseCsvResultsIntoMetadata(client, OUTPUT)


def Run(benchmark_spec):
  """Runs cloudharmony iperf and reports the results."""
  cloud_harmony_metadata = _Run(benchmark_spec)
  samples = cloud_harmony_util.GetMetadataSamples(cloud_harmony_metadata)

  # format pkb-style samples
  for result_row in cloud_harmony_metadata:
    for metric in _CLOUDHARMONY_IPERF_METRICS:
      samples.append(
          sample.Sample(metric, result_row[metric], _GetMetricUnit(metric),
                        result_row))

  return samples


def _GetMetricUnit(metric_name):
  """Returns the appropriate units for the given metric."""
  metric_type = metric_name.split('_')[0]
  if metric_type == 'bandwidth':
    return 'Mbits/sec'
  if metric_type == 'jitter':
    return 'ms'
  if metric_type == 'loss':
    return '%'


def Cleanup(unused_benchmark_spec):
  pass
