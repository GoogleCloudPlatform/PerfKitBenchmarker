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

"""Runs wrk2 clients against replicated nginx servers behind a load balancer."""

import functools
import os
import shutil
import tempfile

from absl import flags
import jinja2
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import nginx_benchmark

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_nginx'
BENCHMARK_CONFIG = """
kubernetes_nginx:
  description: Benchmarks Nginx server performance.
  container_specs:
    kubernetes_nginx:
      image: k8s_nginx
  container_registry: {}
  container_cluster:
    vm_count: 3
    vm_spec:
      AWS:
        zone: us-east-1a
        machine_type: c5.xlarge
      Azure:
        zone: westus
        machine_type: Standard_D3_v2
      GCP:
        machine_type: n2-standard-8
        zone: us-central1-a
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: null
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
    vm_spec = config['container_cluster']['vm_spec']
    vm_spec[FLAGS.cloud]['machine_type'] = FLAGS.nginx_server_machine_type
  return config


def _CreateRenderedManifestFile(filename, config):
  """Returns a file containing a rendered Jinja manifest (.j2) template."""
  manifest_filename = data.ResourcePath(filename)
  environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
  with open(manifest_filename) as manifest_file:
    manifest_template = environment.from_string(manifest_file.read())
  rendered_yaml = tempfile.NamedTemporaryFile(mode='w')
  rendered_yaml.write(manifest_template.render(config))
  rendered_yaml.flush()
  return rendered_yaml


def _CreateNginxConfigMapDir():
  """Returns a TemporaryDirectory containing files in the Nginx ConfigMap."""
  if FLAGS.nginx_conf:
    nginx_conf_filename = FLAGS.nginx_conf
  else:
    nginx_conf_filename = (
        data.ResourcePath('container/kubernetes_nginx/http.conf'))

  temp_dir = tempfile.TemporaryDirectory()
  config_map_filename = os.path.join(temp_dir.name, 'default')
  shutil.copyfile(nginx_conf_filename, config_map_filename)
  return temp_dir


def _PrepareCluster(benchmark_spec):
  """Prepares a cluster to run the Nginx benchmark."""
  with _CreateNginxConfigMapDir() as nginx_config_map_dirname:
    benchmark_spec.container_cluster.CreateConfigMap(
        'default-config', nginx_config_map_dirname)
  container_image = benchmark_spec.container_specs['kubernetes_nginx'].image
  replicas = benchmark_spec.container_cluster.num_nodes

  with _CreateRenderedManifestFile(
      'container/kubernetes_nginx/kubernetes_nginx.yaml.j2', {
          'nginx_image': container_image,
          'nginx_replicas': replicas,
          'nginx_content_size': FLAGS.nginx_content_size,
      }) as rendered_manifest:
    benchmark_spec.container_cluster.ApplyManifest(rendered_manifest.name)

  benchmark_spec.container_cluster.WaitForResource(
      'deploy/nginx-deployment', 'available')


def Prepare(benchmark_spec):
  """Install Nginx on the K8s Cluster and a load generator on the clients.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  clients = benchmark_spec.vm_groups['clients']

  prepare_fns = ([functools.partial(_PrepareCluster, benchmark_spec)] +
                 [functools.partial(vm.Install, 'wrk2') for vm in clients])

  vm_util.RunThreaded(lambda f: f(), prepare_fns)

  benchmark_spec.nginx_endpoint_ip = (
      benchmark_spec.container_cluster.GetLoadBalancerIP('nginx-cluster'))


def Run(benchmark_spec):
  """Run a benchmark against the Nginx server."""
  return nginx_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  """Cleanup Nginx and load generators."""
  nginx_benchmark.Cleanup(benchmark_spec)
