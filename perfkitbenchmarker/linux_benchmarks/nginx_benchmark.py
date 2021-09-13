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

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import wrk2

FLAGS = flags.FLAGS

_FLAG_FORMAT_DESCRIPTION = (
    'The format is "target_request_rate:duration:threads:connections", with '
    'each value being per client (so running with 2 clients would double the '
    'target rate, threads, and connections (but not duration since they are '
    'run concurrently)). The target request rate is measured in requests per '
    'second and the duration is measured in seconds. Increasing the duration '
    'or connections does not impact the aggregate target rate for the client.')

flags.DEFINE_string('nginx_conf', None,
                    'The path to an Nginx config file that should be applied '
                    'to the server instead of the default one.')
flags.DEFINE_integer('nginx_content_size', 1024,
                     'The size of the content Nginx will serve in bytes. '
                     'Larger files stress the network over the VMs.')
flags.DEFINE_list('nginx_load_configs', ['100:60:1:1'],
                  'For each load spec in the list, wrk2 will be run once '
                  'against Nginx with those parameters. ' +
                  _FLAG_FORMAT_DESCRIPTION)
flags.DEFINE_boolean('nginx_throttle', False,
                     'If True, skip running the nginx_load_configs and run '
                     'wrk2 once aiming to throttle the nginx server.')
flags.DEFINE_string('nginx_client_machine_type', None,
                    'Machine type to use for the wrk2 client if different '
                    'from nginx server machine type.')
flags.DEFINE_string('nginx_server_machine_type', None,
                    'Machine type to use for the nginx server if different '
                    'from wrk2 client machine type.')
flags.DEFINE_boolean('nginx_use_ssl', False,
                     'Use HTTPs when connecting to nginx.')
flags.DEFINE_integer('nginx_worker_connections', 1024,
                     'The maximum number of simultaneous connections that can '
                     'be opened by a worker process.')


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
      vm_spec: *default_dual_core
"""


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.nginx_client_machine_type:
    vm_spec = config['vm_groups']['clients']['vm_spec']
    vm_spec[FLAGS.cloud]['machine_type'] = FLAGS.nginx_client_machine_type
  if FLAGS.nginx_server_machine_type:
    vm_spec = config['vm_groups']['server']['vm_spec']
    vm_spec[FLAGS.cloud]['machine_type'] = FLAGS.nginx_server_machine_type
  return config


def _ConfigureNginxForSsl(server):
  """Configures an nginx server for SSL/TLS."""
  server.RemoteCommand('sudo mkdir -p /etc/nginx/ssl')
  # Enable TLS/SSL with:
  # - ECDHE for key exchange
  # - ECDSA for authentication
  # - AES256-GCM for bulk encryption
  # - SHA384 for message authentication
  server.RemoteCommand('sudo openssl req -x509 -nodes -days 365 -newkey ec '
                       '-subj "/CN=localhost" '
                       '-pkeyopt ec_paramgen_curve:secp384r1 '
                       '-keyout /etc/nginx/ssl/ecdsa.key '
                       '-out /etc/nginx/ssl/ecdsa.crt')
  server.RemoteCommand(
      r"sudo sed -i 's|# \(listen 443 ssl .*\)|\1|g' "
      r"/etc/nginx/sites-enabled/default")
  server.RemoteCommand(
      r"sudo sed -i 's|# \(listen \[::\]:443 ssl .*\)|\1|g' "
      r"/etc/nginx/sites-enabled/default")
  server.RemoteCommand(
      r"sudo sed -i 's|\(\s*\)\(listen \[::\]:443 ssl .*;\)|"
      r"\1\2\n"
      r"\1ssl_certificate /etc/nginx/ssl/ecdsa.crt;\n"
      r"\1ssl_certificate_key /etc/nginx/ssl/ecdsa.key;\n"
      r"\1ssl_ciphers ECDHE-ECDSA-AES256-GCM-SHA384;|g' "
      r"/etc/nginx/sites-enabled/default")


def _ConfigureNginx(server):
  """Configures nginx server."""
  content_path = '/var/www/html/random_content'
  server.RemoteCommand('sudo mkdir -p /var/www/html')  # create folder if needed
  server.RemoteCommand('sudo dd  bs=1 count=%s if=/dev/urandom of=%s' %
                       (FLAGS.nginx_content_size, content_path))
  if FLAGS.nginx_conf:
    server.PushDataFile(FLAGS.nginx_conf)
    server.RemoteCommand('sudo cp %s /etc/nginx/nginx.conf' % FLAGS.nginx_conf)
  else:
    # disable logging, nginx logs every request by default.
    server.RemoteCommand(
        r"sudo sed -i 's|access_log .*|access_log /dev/null;|g' "
        r"/etc/nginx/nginx.conf")
    # Add some optimizations to the Nginx stack to improve throughput.
    # This is based off https://www.nginx.com/blog/tuning-nginx/
    # Increase worker_connections from to 1024 (default 768)
    server.RemoteCommand(
        r"sudo sed -i 's|worker_connections .*|worker_connections %s;|g' "
        r"/etc/nginx/nginx.conf" % FLAGS.nginx_worker_connections)
    # Increase keepalive_requests to a large value (default 1000)
    server.RemoteCommand(
        r"sudo sed -i 's|\(\s*\)keepalive_timeout .*|"
        r"\1keepalive_timeout 75;\n\1keepalive_requests 1000000000;|g' "
        r"/etc/nginx/nginx.conf")
    # Enable caching
    server.RemoteCommand("sudo sed -i 's|# server_tokens off;|"
                         'open_file_cache max=200000 inactive=20s;'
                         'open_file_cache_valid 30s;'
                         'open_file_cache_min_uses 2;'
                         'open_file_cache_errors on;'
                         "# server_tokens off;|g' "
                         '/etc/nginx/nginx.conf')

  if FLAGS.nginx_use_ssl:
    _ConfigureNginxForSsl(server)

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

  benchmark_spec.nginx_endpoint_ip = (
      benchmark_spec.vm_groups['server'][0].internal_ip)


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
      'target_rate': rate * num_clients,
      'nginx_throttle': FLAGS.nginx_throttle,
      'nginx_worker_connections': FLAGS.nginx_worker_connections,
      'nginx_use_ssl': FLAGS.nginx_use_ssl
  }
  if not FLAGS.nginx_conf:
    metadata['caching'] = True
  results += [
      sample.Sample('achieved_rate', requests / duration, '', metadata),
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
  results = []
  scheme = 'https' if FLAGS.nginx_use_ssl else 'http'
  target = f'{scheme}://{benchmark_spec.nginx_endpoint_ip}/random_content'

  if FLAGS.nginx_throttle:
    return _RunMultiClient(
        clients,
        target,
        rate=1000000,  # 1M aggregate requests/sec should max out requests.
        connections=clients[0].NumCpusForBenchmark() * 10,
        duration=60,
        threads=clients[0].NumCpusForBenchmark())

  for config in FLAGS.nginx_load_configs:
    rate, duration, threads, connections = list(map(int, config.split(':')))
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
