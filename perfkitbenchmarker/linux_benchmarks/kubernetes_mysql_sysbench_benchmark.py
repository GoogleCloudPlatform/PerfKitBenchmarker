# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run sysbench against MySQL on a K8s cluster."""

import base64
import os
import subprocess
import tempfile
from typing import Any

from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

# Import sysbench_benchmark to reuse its flags
from perfkitbenchmarker.linux_benchmarks import sysbench_benchmark  # pylint: disable=unused-import
from perfkitbenchmarker.linux_packages import sysbench
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS


_FOUR_HOURS_TIMEOUT = 14400

flags.DEFINE_string(
    'kubernetes_mysql_sysbench_image',
    'sysbench',
    'The sysbench image to use for the client job.',
)

flags.DEFINE_string(
    'kubernetes_mysql_version',
    '8.0',
    'The MySQL version (tag) to use for the server.',
)

flags.DEFINE_string(
    'kubernetes_mysql_sysbench_namespace',
    None,
    'The namespace to use for the MySQL resources.',
)


flags.DEFINE_string(
    'kubernetes_mysql_sysbench_template_path',
    'container/kubernetes_mysql_sysbench/kubernetes_mysql_sysbench.yaml.j2',
    'Path to the MySQL Kubernetes manifest template file.',
)


_CLOUD_TO_PROVISIONER = {
    'GCP': 'pd.csi.storage.gke.io',
    'AWS': 'ebs.csi.aws.com',
    'Azure': 'kubernetes.io/azure-disk',
}

BENCHMARK_NAME = 'kubernetes_mysql_sysbench'
BENCHMARK_CONFIG = """
kubernetes_mysql_sysbench:
  description: >
      Run sysbench against MySQL on Kubernetes.
  container_registry: {}
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-medium
      AWS:
        machine_type: m6i.large
      Azure:
        machine_type: Standard_D2s_v5
    nodepools:
      servers:
        vm_spec:
          GCP:
            machine_type: c4-highmem-16
            boot_disk_type: hyperdisk-balanced
          AWS:
            machine_type: c6i.4xlarge
            boot_disk_type: gp3
          Azure:
            machine_type: Standard_D16s_v5
            boot_disk_type: Premium_LRS
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-32
          AWS:
            machine_type: c6i.8xlarge
          Azure:
            machine_type: Standard_D32s_v5
        vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def _GetPassword() -> str:
  """Generates a password for the benchmark run."""
  return (FLAGS.run_uri or 'test_uri') + '_P3rfk1tbenchm4rker#'


def CheckPrerequisites(_):
  """Verifies that benchmark setup is correct."""
  pass


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Load and return benchmark config spec."""
  if (
      FLAGS.kubernetes_mysql_sysbench_image == 'sysbench'
      and not FLAGS['static_container_image'].present
  ):
    FLAGS.static_container_image = False
    FLAGS['static_container_image'].present = True
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GenerateCertificates() -> tuple[str, str, str, str, str]:
  """Generates CA, server, and client certificates/keys encoded in base64."""
  with tempfile.TemporaryDirectory() as tmpdir:
    ca_key = os.path.join(tmpdir, 'ca-key.pem')
    ca_cert = os.path.join(tmpdir, 'ca.pem')
    server_key = os.path.join(tmpdir, 'server-key.pem')
    server_req = os.path.join(tmpdir, 'server-req.pem')
    server_cert = os.path.join(tmpdir, 'server-cert.pem')
    client_key = os.path.join(tmpdir, 'client-key.pem')
    client_req = os.path.join(tmpdir, 'client-req.pem')
    client_cert = os.path.join(tmpdir, 'client-cert.pem')

    cnf_path = os.path.join(tmpdir, 'openssl.cnf')
    ns = FLAGS.kubernetes_mysql_sysbench_namespace or 'default'
    with open(cnf_path, 'w') as f:
      f.write(
          '[req]\n'
          'distinguished_name=req_distinguished_name\n'
          '[req_distinguished_name]\n'
          '[v3_req]\n'
          'subjectAltName=@alt_names\n'
          '[alt_names]\n'
          'DNS.1 = mysql-0.mysql-svc.default.svc.cluster.local\n'
          f'DNS.2 = mysql-0.mysql-svc.{ns}.svc.cluster.local\n'
          '[ca_ext]\n'
          'basicConstraints = critical,CA:TRUE\n'
          'keyUsage = critical,keyCertSign,cRLSign\n'
      )

    # Generate CA key & cert
    subprocess.run(['openssl', 'genrsa', '-out', ca_key, '2048'], check=True)
    subprocess.run(
        [
            'openssl',
            'req',
            '-x509',
            '-new',
            '-nodes',
            '-key',
            ca_key,
            '-sha256',
            '-days',
            '3650',
            '-out',
            ca_cert,
            '-config',
            cnf_path,
            '-extensions',
            'ca_ext',
            '-subj',
            '/CN=sysbench-ca',
        ],
        check=True,
    )

    # Generate Server key & cert
    subprocess.run(
        ['openssl', 'genrsa', '-out', server_key, '2048'], check=True
    )
    subprocess.run(
        [
            'openssl',
            'req',
            '-new',
            '-key',
            server_key,
            '-out',
            server_req,
            '-config',
            cnf_path,
            '-reqexts',
            'v3_req',
            '-subj',
            f'/CN=mysql-0.mysql-svc.{ns}.svc.cluster.local',
        ],
        check=True,
    )
    subprocess.run(
        [
            'openssl',
            'x509',
            '-req',
            '-in',
            server_req,
            '-CA',
            ca_cert,
            '-CAkey',
            ca_key,
            '-CAcreateserial',
            '-days',
            '365',
            '-sha256',
            '-out',
            server_cert,
            '-extfile',
            cnf_path,
            '-extensions',
            'v3_req',
        ],
        check=True,
    )

    # Generate Client key & cert
    subprocess.run(
        ['openssl', 'genrsa', '-out', client_key, '2048'], check=True
    )
    subprocess.run(
        [
            'openssl',
            'req',
            '-new',
            '-key',
            client_key,
            '-out',
            client_req,
            '-config',
            cnf_path,
            '-subj',
            '/CN=sysbench-client',
        ],
        check=True,
    )
    subprocess.run(
        [
            'openssl',
            'x509',
            '-req',
            '-in',
            client_req,
            '-CA',
            ca_cert,
            '-CAkey',
            ca_key,
            '-CAcreateserial',
            '-days',
            '365',
            '-sha256',
            '-out',
            client_cert,
        ],
        check=True,
    )

    # Read files & base64 encode
    def _ReadB64(path: str) -> str:
      with open(path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

    return (
        _ReadB64(ca_cert),
        _ReadB64(client_cert),
        _ReadB64(client_key),
        _ReadB64(server_cert),
        _ReadB64(server_key),
    )


def _LogDebugInfo(
    cluster,
    ns: str,
    label_selector: str,
    resource_name: str | None = None,
    log_tail: int = 100,
) -> None:
  """Logs debug information for a Kubernetes failure."""
  if resource_name:
    describe_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'describe',
        resource_name,
        '-n',
        ns,
    ]
    stdout, _, _ = vm_util.IssueCommand(describe_cmd, raise_on_failure=False)
    logging.error('%s description:\n%s', resource_name, stdout)

  pod_describe_cmd = [
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'describe',
      'pod',
      '-n',
      ns,
      '-l',
      label_selector,
  ]
  stdout, _, _ = vm_util.IssueCommand(pod_describe_cmd, raise_on_failure=False)
  logging.error('Pod description:\n%s', stdout)

  logs_cmd = [
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'logs',
      '-n',
      ns,
      '-l',
      label_selector,
      f'--tail={log_tail}',
  ]
  stdout, _, _ = vm_util.IssueCommand(logs_cmd, raise_on_failure=False)
  logging.error('Pod logs:\n%s', stdout)

  if getattr(cluster, 'event_poller', None):
    cluster.event_poller.GetAndLogFailureEvents()
  else:
    events_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'get',
        'events',
        '-n',
        ns,
        '--sort-by=.lastTimestamp',
    ]
    stdout, _, _ = vm_util.IssueCommand(events_cmd, raise_on_failure=False)
    logging.error('Recent events:\n%s', stdout)


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Prepares a cluster to run the MySQL benchmark."""
  if bm_spec.container_registry and not FLAGS.static_container_image:
    bm_spec.container_registry.GetOrBuild(FLAGS.kubernetes_mysql_sysbench_image)
  # Apply MySQL manifests
  password = _GetPassword()
  ca_pem, client_cert_pem, client_key_pem, server_cert_pem, server_key_pem = (
      _GenerateCertificates()
  )

  # Determine cloud-specific storage class parameters
  cloud = bm_spec.container_cluster.CLOUD
  if cloud not in _CLOUD_TO_PROVISIONER:
    raise errors.Benchmarks.RunError(f'Unsupported cloud provider: {cloud}')

  cluster = bm_spec.container_cluster
  nodepool_name = 'servers' if 'servers' in cluster.nodepools else 'default'
  nodepool = cluster.nodepools.get(nodepool_name, cluster.default_nodepool)

  storage_provisioner = _CLOUD_TO_PROVISIONER[cloud]
  storage_type = nodepool.disk_type
  if not storage_type:
    raise errors.Benchmarks.RunError(
        f'Storage type (boot_disk_type) must be configured for cloud: {cloud}'
    )

  kubernetes_commands.ApplyManifest(
      FLAGS.kubernetes_mysql_sysbench_template_path,
      ca_pem=ca_pem,
      client_cert_pem=client_cert_pem,
      client_key_pem=client_key_pem,
      server_cert_pem=server_cert_pem,
      server_key_pem=server_key_pem,
      k8s_namespace=FLAGS.kubernetes_mysql_sysbench_namespace,
      mysql_version=FLAGS.kubernetes_mysql_version,
      create_storage_class=True,
      storage_class='fast-storageclass',
      storage_provisioner=storage_provisioner,
      storage_type=storage_type,
      storage_size='500Gi',
      mysql_db='sbtest',
      mysql_user='bench_user',
      mysql_password=password,
      tcp_fin_timeout=5,
  )
  try:
    kubernetes_commands.WaitForRollout(
        'statefulset/mysql',
        namespace=FLAGS.kubernetes_mysql_sysbench_namespace,
        timeout=1200,
    )
  except Exception as e:
    logging.error('MySQL pod failed to become ready: %s', e)
    ns = FLAGS.kubernetes_mysql_sysbench_namespace or 'default'
    _LogDebugInfo(
        cluster=bm_spec.container_cluster,
        ns=ns,
        label_selector='app=mysql-app',
        log_tail=100,
    )
    raise


def Run(bm_spec: _BenchmarkSpec) -> list[sample.Sample]:
  """Run the benchmark."""
  if bm_spec.container_registry and not FLAGS.static_container_image:
    full_image = bm_spec.container_registry.GetFullRegistryTag(
        FLAGS.kubernetes_mysql_sysbench_image
    )
  else:
    full_image = FLAGS.kubernetes_mysql_sysbench_image

  # Check if the specified sysbench version is legacy (1.0.x)
  use_legacy_ssl = True

  # Apply Sysbench Job manifest
  created_resources = kubernetes_commands.ApplyManifest(
      'container/kubernetes_mysql_sysbench/kubernetes_sysbench_job.yaml.j2',
      k8s_namespace=FLAGS.kubernetes_mysql_sysbench_namespace,
      image=full_image,
      tables=FLAGS.sysbench_tables,
      table_size=FLAGS.sysbench_table_size,
      threads=FLAGS.sysbench_run_threads[0],
      prepare_threads=min(FLAGS.sysbench_tables, 128),
      time=FLAGS.sysbench_run_seconds,
      mysql_db='sbtest',
      mysql_user='bench_user',
      mysql_password=_GetPassword(),
      use_legacy_ssl=use_legacy_ssl,
  )

  job_name = None
  for resource in created_resources:
    if 'job' in resource.split('/')[0]:
      job_name = resource.split('/')[1]
      break
  if not job_name:
    raise errors.Benchmarks.RunError(
        f'Could not find sysbench Job resource in: {created_resources}'
    )

  logging.info('Waiting for sysbench Job %s to complete...', job_name)
  try:
    condition = kubernetes_commands.WaitForResourceForMultiConditions(
        f'jobs/{job_name}',
        ['condition=Complete', 'condition=Failed'],
        namespace=FLAGS.kubernetes_mysql_sysbench_namespace,
        timeout=FLAGS.sysbench_run_seconds + _FOUR_HOURS_TIMEOUT,
    )
  except Exception as e:
    logging.error('Sysbench job wait timed out or failed: %s', e)
    ns = FLAGS.kubernetes_mysql_sysbench_namespace or 'default'
    _LogDebugInfo(
        cluster=bm_spec.container_cluster,
        ns=ns,
        label_selector='app=sysbench',
        resource_name=f'job/{job_name}',
        log_tail=500,
    )
    raise

  kubectl_args = ['logs', f'jobs/{job_name}']
  if FLAGS.kubernetes_mysql_sysbench_namespace:
    kubectl_args.append(
        f'--namespace={FLAGS.kubernetes_mysql_sysbench_namespace}'
    )
  logs, _, _ = kubectl.RunKubectlCommand(kubectl_args, raise_on_failure=False)
  if condition == 'condition=Failed':
    # Output full logs at debug level to avoid overwhelming standard logs
    logging.debug('Sysbench job failed full logs: %s', logs)
    # Output a truncated tail for quick error inspection
    logging.error('Sysbench job failed. Tail: %s', logs[-2000:])
    raise errors.Benchmarks.RunError(f"Sysbench job '{job_name}' failed.")

  logging.info('Sysbench job %s completed successfully.', job_name)

  # Parse results
  metadata = sysbench_benchmark.CreateMetadataFromFlags()
  metadata.update({
      'sysbench_threads': FLAGS.sysbench_run_threads[0],
      'kubernetes_mysql_sysbench_image': FLAGS.kubernetes_mysql_sysbench_image,
      'kubernetes_mysql_version': FLAGS.kubernetes_mysql_version,
      'kubernetes_mysql_sysbench_namespace': (
          FLAGS.kubernetes_mysql_sysbench_namespace
      ),
      'kubernetes_mysql_sysbench_template_path': (
          FLAGS.kubernetes_mysql_sysbench_template_path
      ),
  })

  samples = []
  samples.extend(sysbench.ParseSysbenchTransactions(logs, metadata))
  samples.extend(sysbench.ParseSysbenchLatency([logs], metadata))

  return samples


def Cleanup(_: _BenchmarkSpec) -> None:
  pass
