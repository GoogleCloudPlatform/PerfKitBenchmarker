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
"""Runs wrk2 clients against a 3-tier Nginx setup on Kubernetes.

Architecture: Client VMs -> Nginx Proxy Pods (LoadBalancer) -> Upstream Pods

The benchmark deploys:
  - Nginx proxy pods that reverse-proxy requests to upstream backends
  - Nginx upstream pods that serve static content
  - wrk2 load generators on external client VMs

This mirrors the GCE nginx_benchmark's 3-tier architecture but runs
the proxy and upstream tiers as GKE pods.
"""

import functools
import ipaddress
import logging
import os
import tempfile
import time

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import nginx_benchmark
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'kubernetes_nginx_runtime_class_name',
    None,
    'A custom runtimeClassName to apply to the nginx pods.',
)

BENCHMARK_NAME = 'kubernetes_nginx'
BENCHMARK_CONFIG = """
kubernetes_nginx:
  description: >
    Benchmarks Nginx server performance in a 3-tier architecture
    (Client -> Nginx Proxy -> Upstream Backend) on Kubernetes.
  container_specs:
    kubernetes_nginx:
      image: k8s_nginx
  container_registry: {}
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
    nodepools:
      nginx:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
          AWS:
            machine_type: m6i.xlarge
          Azure:
            machine_type: Standard_D4s_v5
      upstream:
        vm_count: 2
        vm_spec:
          GCP:
            machine_type: n2-standard-4
          AWS:
            machine_type: m6i.xlarge
          Azure:
            machine_type: Standard_D4s_v5
      clients:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
          AWS:
            machine_type: m6i.xlarge
          Azure:
            machine_type: Standard_D4s_v5
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
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
    vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.nginx_client_machine_type
  if FLAGS.nginx_server_machine_type:
    vm_spec = config['container_cluster']['nodepools']['nginx']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.nginx_server_machine_type
  if FLAGS.nginx_upstream_server_machine_type:
    vm_spec = config['container_cluster']['nodepools']['upstream']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.nginx_upstream_server_machine_type

  return config


def _MergeNginxConfigs(global_conf_path, server_conf_path, force_http=False):
  """Merges global and server nginx configs into a single file.

  The GCE benchmark uses separate VMs for proxy and upstream, each with their
  own nginx.conf. In K8s, we merge the global config with the server-specific
  config so each pod gets a single complete nginx.conf via ConfigMap.

  Args:
    global_conf_path: Path to the global nginx config (nginx/global.conf).
    server_conf_path: Path to the server-specific config
      (nginx/rp_apigw.conf or nginx/file_server.conf).
    force_http: If True, convert HTTPS listeners to HTTP.

  Returns:
    The merged nginx configuration as a string.
  """
  with open(global_conf_path) as f:
    global_conf = f.read()
  with open(server_conf_path) as f:
    server_conf = f.read()

  # Replace placeholder with K8s upstream service DNS
  upstream_dns = 'nginx-upstream.default.svc.cluster.local'
  upstream_port = 80
  server_conf = server_conf.replace(
      '# server <fileserver_1_ip_or_dns>:443;',
      f'server {upstream_dns}:{upstream_port};',
  )

  if (not FLAGS.nginx_use_ssl) or force_http:
    server_conf = server_conf.replace('ssl on;', '# ssl on;')
    server_conf = server_conf.replace('ssl_certificate', '# ssl_certificate')
    server_conf = server_conf.replace('ssl_ciphers', '# ssl_ciphers')
    server_conf = server_conf.replace('listen 443 ssl', 'listen 80')

  # Force proxy->upstream to always use HTTP
  server_conf = server_conf.replace('proxy_pass https://', 'proxy_pass http://')

  merged_conf = global_conf.replace(
      'include /etc/nginx/conf.d/*.conf;', server_conf
  )
  return merged_conf


def _CreateNginxConfigMapDir():
  """Returns a TemporaryDirectory containing merged nginx configs.

  Creates two config files:
    - nginx-proxy.conf: merged global + reverse proxy config
    - nginx-upstream.conf: merged global + file server config (HTTP only)
  """
  temp_dir = tempfile.TemporaryDirectory()

  global_conf_path = data.ResourcePath('nginx/global.conf')
  proxy_conf_path = data.ResourcePath('nginx/rp_apigw.conf')
  upstream_conf_path = data.ResourcePath('nginx/file_server.conf')

  proxy_conf_content = _MergeNginxConfigs(global_conf_path, proxy_conf_path)
  with open(os.path.join(temp_dir.name, 'nginx-proxy.conf'), 'w') as f:
    f.write(proxy_conf_content)

  # Upstream always uses HTTP regardless of SSL setting
  upstream_conf_content = _MergeNginxConfigs(
      global_conf_path, upstream_conf_path, force_http=True
  )
  with open(os.path.join(temp_dir.name, 'nginx-upstream.conf'), 'w') as f:
    f.write(upstream_conf_content)

  return temp_dir


def _WaitForConnectivity(benchmark_spec):
  """Waits for the Nginx proxy to be reachable from the client VM."""
  lb_ip = benchmark_spec.nginx_endpoint_ip
  scheme = 'https' if FLAGS.nginx_use_ssl else 'http'
  url = f'{scheme}://{lb_ip}/{nginx_benchmark._CONTENT_FILENAME}'
  cmd = f'curl -s -o /dev/null -w "%{{http_code}}" -k {url}'

  logging.info('Waiting for connectivity to %s...', url)
  start_time = time.time()
  while time.time() - start_time < 300:
    try:
      stdout, _ = benchmark_spec.vm_groups['clients'][0].RemoteCommand(cmd)
      if stdout.strip() in ('200', '301', '302'):
        logging.info('Connectivity established to %s', url)
        return
    except errors.VirtualMachine.RemoteCommandError:
      pass
    logging.info('Still waiting for connectivity...')
    time.sleep(10)

  raise errors.Benchmarks.PrepareException(
      f'Timed out waiting for connectivity to {url}'
  )


def _PrepareCluster(benchmark_spec):
  """Prepares the GKE cluster with proxy and upstream nginx deployments."""
  # 1. Create ConfigMap with merged proxy + upstream configs
  with _CreateNginxConfigMapDir() as nginx_config_map_dirname:
    kubernetes_commands.CreateConfigMap(
        'nginx-configs', nginx_config_map_dirname
    )

  container_image = benchmark_spec.container_specs['kubernetes_nginx'].image
  proxy_port = 443 if FLAGS.nginx_use_ssl else 80
  upstream_port = 80

  # 2. Deploy Upstream (file-server backend)
  upstream_replicas = benchmark_spec.container_cluster.nodepools[
      'upstream'
  ].num_nodes
  kubernetes_commands.ApplyManifest(
      'container/kubernetes_nginx/nginx_upstream.yaml.j2',
      nginx_image=container_image,
      nginx_upstream_replicas=upstream_replicas,
      nginx_content_size=FLAGS.nginx_content_size,
      nginx_port=upstream_port,
      runtime_class_name=FLAGS.kubernetes_nginx_runtime_class_name,
  )

  # 3. Deploy Proxy (reverse proxy in front of upstream)
  proxy_replicas = benchmark_spec.container_cluster.nodepools[
      'nginx'
  ].num_nodes
  kubernetes_commands.ApplyManifest(
      'container/kubernetes_nginx/nginx_proxy.yaml.j2',
      nginx_image=container_image,
      nginx_proxy_replicas=proxy_replicas,
      nginx_port=proxy_port,
      runtime_class_name=FLAGS.kubernetes_nginx_runtime_class_name,
  )

  # 4. Wait for both deployments
  kubernetes_commands.WaitForResource(
      'deploy/nginx-upstream-deployment', 'available'
  )
  kubernetes_commands.WaitForResource(
      'deploy/nginx-proxy-deployment', 'available'
  )

  # 5. Get LoadBalancer IP for the proxy service
  benchmark_spec.nginx_endpoint_ip = (
      kubernetes_commands.GetLoadBalancerIP('nginx-cluster')
  )

  # 6. Verify end-to-end connectivity
  _WaitForConnectivity(benchmark_spec)


def Prepare(benchmark_spec):
  """Install Nginx on the K8s Cluster and a load generator on the clients.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  clients = benchmark_spec.vm_groups['clients']

  prepare_fns = [functools.partial(_PrepareCluster, benchmark_spec)] + [
      functools.partial(vm.Install, 'wrk2') for vm in clients
  ]

  background_tasks.RunThreaded(lambda f: f(), prepare_fns)


def Run(benchmark_spec):
  """Run a benchmark against the Nginx proxy via the LoadBalancer.

  This cannot delegate to nginx_benchmark.Run() because that function
  expects vm_groups['server'] (a GCE VM), which doesn't exist in the
  K8s benchmark. Instead, we construct targets from the LoadBalancer IP
  and call _RunMultiClient directly.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  clients = benchmark_spec.vm_groups['clients']

  # Re-fetch LB IP if not persisted (e.g., separate run_stage=run)
  if not hasattr(benchmark_spec, 'nginx_endpoint_ip'):
    benchmark_spec.nginx_endpoint_ip = (
        kubernetes_commands.GetLoadBalancerIP('nginx-cluster')
    )

  scheme = 'https' if FLAGS.nginx_use_ssl else 'http'
  hostip = benchmark_spec.nginx_endpoint_ip
  hoststr = (
      f'[{hostip}]'
      if isinstance(ipaddress.ip_address(hostip), ipaddress.IPv6Address)
      else f'{hostip}'
  )
  portstr = f':{FLAGS.nginx_server_port}' if FLAGS.nginx_server_port else ''

  if FLAGS.nginx_scenario == 'reverse_proxy':
    target = f'{scheme}://{hoststr}{portstr}/{nginx_benchmark._CONTENT_FILENAME}'
  else:
    target = (
        f'{scheme}://{hoststr}{portstr}/'
        f'{nginx_benchmark._API_GATEWAY_PATH}/{nginx_benchmark._CONTENT_FILENAME}'
    )
  targets = [target]

  if FLAGS.nginx_throttle:
    return nginx_benchmark._RunMultiClient(
        clients,
        targets,
        rate=100000000,
        connections=clients[0].NumCpusForBenchmark() * 10,
        duration=60,
        threads=clients[0].NumCpusForBenchmark(),
    )

  # Binary search for highest RPS under the p99 latency threshold.
  if nginx_benchmark._P99_LATENCY_THRESHOLD.value:
    lower_bound = nginx_benchmark._TARGET_RATE_LOWER_BOUND
    upper_bound = nginx_benchmark._TARGET_RATE_UPPER_BOUND
    target_rate = upper_bound
    valid_results = []
    while (upper_bound - lower_bound) > nginx_benchmark._RPS_RANGE_THRESHOLD:
      results = nginx_benchmark._RunMultiClient(
          clients,
          targets,
          rate=target_rate,
          connections=clients[0].NumCpusForBenchmark() * 10,
          duration=60,
          threads=clients[0].NumCpusForBenchmark(),
      )
      for result in results:
        if result.metric == 'p99 latency':
          p99_latency = result.value
          if p99_latency > nginx_benchmark._P99_LATENCY_THRESHOLD.value:
            upper_bound = target_rate
          else:
            lower_bound = target_rate
            valid_results = results
          target_rate = (lower_bound + upper_bound) // 2
          break

    return valid_results

  results = []
  for config in FLAGS.nginx_load_configs:
    rate, duration, threads, connections = list(map(int, config.split(':')))
    results += nginx_benchmark._RunMultiClient(
        clients, targets, rate, connections, duration, threads
    )
  return results


def Cleanup(benchmark_spec):
  """Cleanup Nginx and load generators."""
  del benchmark_spec
