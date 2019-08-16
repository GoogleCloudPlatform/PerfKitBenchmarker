# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs HTTP load generators against an Nginx server."""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import wrk2

FLAGS = flags.FLAGS

_FLAG_FORMAT_DESCRIPTION = (
    'The format is "target_request_rate:duration:threads:connections", with '
    'each value being per client (so running with 2 clients would double the '
    'target rate, threads, and connections (but not duration since they are '
    'run concurrently)). The target request rate is measured in requests per '
    'second and the duration is measured in seconds.')

flags.DEFINE_string('nginx_conf', None,
                    'The path to an Nginx config file that should be applied '
                    'to the server instead of the default one.')
flags.DEFINE_integer('nginx_content_size', 10000,
                     'The size of the content Nginx will serve in bytes.')
flags.DEFINE_list('nginx_load_configs', ['100:60:1:1'],
                  'For each load spec in the list, wrk2 will be run once '
                  'against Nginx with those parameters. ' +
                  _FLAG_FORMAT_DESCRIPTION)


def _ValidateLoadConfigs(load_configs):
  """Validate that each load config has all required values."""
  if not load_configs:
    return False
  for config in load_configs:
    config_values = config.split(':')
    if len(config_values) != 4:
      return False
    for value in config_values:
      if not (value.isdigit() and int(value) > 0):
        return False
  return True


flags.register_validator(
    'nginx_load_configs', _ValidateLoadConfigs,
    'Malformed load config. ' + _FLAG_FORMAT_DESCRIPTION)

BENCHMARK_NAME = 'nginx'
BENCHMARK_CONFIG = """
nginx:
  description: Benchmarks Nginx server performance.
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: null
    server:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _ConfigureNginx(server):
  content_path = '/var/www/html/random_content'
  server.RemoteCommand('sudo mkdir -p /var/www/html')  # create folder if needed
  server.RemoteCommand('sudo dd  bs=1 count=%s if=/dev/urandom of=%s' %
                       (FLAGS.nginx_content_size, content_path))
  if FLAGS.nginx_conf:
    server.PushDataFile(FLAGS.nginx_conf)
    server.RemoteCommand('sudo cp %s /etc/nginx/nginx.conf' % FLAGS.nginx_conf)
    server.RemoteCommand('sudo service nginx restart')


def Prepare(benchmark_spec):
  """Install Nginx on the server and a load generator on the clients.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  clients = benchmark_spec.vm_groups['clients']
  server = benchmark_spec.vm_groups['server'][0]
  server.Install('nginx')
  _ConfigureNginx(server)
  vm_util.RunThreaded(lambda vm: vm.Install('wrk2'), clients)


def _RunMultiClient(clients, target, rate, connections, duration, threads):
  """Run multiple instances of wrk2 against a single target."""
  results = []
  num_clients = len(clients)

  def _RunSingleClient(client, client_number):
    """Run wrk2 from a single client."""
    client_results = list(wrk2.Run(
        client, target, rate, connections=connections,
        duration=duration, threads=threads))
    for result in client_results:
      result.metadata.update({'client_number': client_number})
    results.extend(client_results)

  args = [((client, i), {}) for i, client in enumerate(clients)]
  vm_util.RunThreaded(_RunSingleClient, args)

  requests = 0
  errors = 0
  max_latency = 0.0
  # TODO(ehankland): Since wrk2 keeps an HDR histogram of latencies, we should
  # be able to merge them and compute aggregate percentiles.

  for result in results:
    if result.metric == 'requests':
      requests += result.value
    elif result.metric == 'errors':
      errors += result.value
    elif result.metric == 'p100 latency':
      max_latency = max(max_latency, result.value)

  error_rate = errors / requests
  metadata = {
      'connections': connections * num_clients,
      'threads': threads * num_clients,
      'duration': duration,
      'rate': rate * num_clients
  }
  results += [
      sample.Sample('aggregate requests', requests, '', metadata),
      sample.Sample('aggregate errors', errors, '', metadata),
      sample.Sample('aggregate error_rate', error_rate, '', metadata),
      sample.Sample('aggregate p100 latency', max_latency, '', metadata)
  ]
  return results


def Run(benchmark_spec):
  """Run a benchmark against the Nginx server.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  clients = benchmark_spec.vm_groups['clients']
  server = benchmark_spec.vm_groups['server'][0]
  results = []

  for config in FLAGS.nginx_load_configs:
    rate, duration, threads, connections = list(map(int, config.split(':')))
    target = 'http://%s/random_content' % server.internal_ip
    results += _RunMultiClient(clients, target, rate,
                               connections, duration, threads)
  return results


def Cleanup(benchmark_spec):
  """Cleanup Nginx and load generators.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  del benchmark_spec
