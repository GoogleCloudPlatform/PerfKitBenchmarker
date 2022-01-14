"""Wraps a cloudharmony network benchmark.

Cloud Harmony is Gartner's benchmarking team.

The network benchmark works in two service types: compute and storage. For
compute service type, the benchmark tests networking performance between a VM
server and VM client, where as for storage service type, the benchmark tests
networking performance between a cloud storage store (e.g. GCS) server and a VM
client. The test files are installed on the server, and the client measures the
latency, throughput, round trip time, time to first byte etc. of accessing files
from the server.

See https://github.com/cloudharmony/network for more info.
"""

import logging
import posixpath

from absl import flags
from perfkitbenchmarker import cloud_harmony_util
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cloud_harmony_network
from perfkitbenchmarker.providers.gcp import gcs


BENCHMARK_NAME = 'cloudharmony_network'
BENCHMARK_CONFIG = """
cloudharmony_network:
  description: Runs cloudharmony network tests.
  vm_groups:
    client:
      vm_spec:
        GCP:
          machine_type: n1-standard-2
          boot_disk_type: pd-ssd
    server:
      disk_spec: *default_50_gb
      vm_spec:
        GCP:
          machine_type: n1-standard-2
"""
# network test service types
COMPUTE = 'compute'
STORAGE = 'storage'
# network compute server type
NGINX = 'nginx'
# tests accepted by the --test flag of the gartner network benchmark
# throughput & tcp tests are not defined as our harness simply supports
# running the subtests for each. In order to run either, specify the supported
# tests as defined below
# throughput: downlink, uplink
# tcp: rtt, ssl, ttfb
NETWORK_TESTS = ['latency', 'downlink', 'uplink', 'dns', 'rtt', 'ssl', 'ttfb']

flags.DEFINE_enum('ch_network_test_service_type', COMPUTE, [COMPUTE, STORAGE],
                  'The service type to host the website.')
flags.DEFINE_enum('ch_network_test', 'latency', NETWORK_TESTS,
                  'Test supported by CloudHarmony network benchmark as defined'
                  'in NETWORK_TESTS. If none is specified, latency test is run'
                  'by default.')
flags.DEFINE_boolean('ch_network_throughput_https', False,
                     'The protocol for throughput tests will default to https, '
                     'otherwise it defaults to http')
flags.DEFINE_float('ch_network_throughput_size', 5,
                   'Default size for throughput tests in megabytes.')
flags.DEFINE_boolean('ch_network_throughput_small_file', False,
                     '--ch_network_throughput_size is ignored and throughput '
                     'tests are constrained to test files smaller than 128KB. '
                     'Each thread of each request will randomly select one such'
                     ' file. When used, the throughput_size result value will '
                     'be the average file size')
flags.DEFINE_string('ch_network_throughput_threads', '2',
                    'The number of concurrent threads for throughput tests.')
flags.DEFINE_boolean('ch_network_throughput_time', False,
                     'throughput metrics will be average request times (ms) '
                     'instead of rate (Mb/s).')
flags.DEFINE_integer('ch_network_throughput_samples', 5,
                     'The number of test samples for throughput tests.')
flags.DEFINE_boolean('ch_network_throughput_slowest_thread', False, 'If set, '
                     'throughput metrics will be based on the speed of the '
                     'slowest thread instead of average speed X number of '
                     'threads.')
flags.DEFINE_integer('ch_network_tcp_samples', 10, 'The number of test samples '
                     'for TCP tests (rtt, ssl or ttfb).')
CLIENT_ZONE = flags.DEFINE_string(
    'ch_client_zone', None,
    'zone to launch the network or storage test client in. ')
ENDPOINT_ZONE = flags.DEFINE_string(
    'ch_endpoint_zone', None,
    'zone to launch the network server or storage test bucket in. ')

FLAGS = flags.FLAGS
HTTP_DIR = '/var/www/html/web-probe'
OUTPUT = 'network.csv'
TRUE = 'True'
FALSE = 'False'


def CheckPrerequisites(_):
  """Verifies that the required resources are present.

  Raises NotImplementedError.
  """
  # currently only support GCS object storage
  # TODO(user): add AWS & Azure support for object storage
  if FLAGS.ch_network_test_service_type == STORAGE and FLAGS.cloud != 'GCP':
    raise NotImplementedError('Benchmark only supports GCS object storage.')


def GetConfig(user_config):
  """Update client zone and server zone in configuration.

  Args:
    user_config: The user configuration.

  Returns:
    Updated configuration.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  config['vm_groups']['client']['vm_spec'][FLAGS.cloud]['zone'] = (
      CLIENT_ZONE.value)

  config['vm_groups']['server']['vm_spec'][FLAGS.cloud]['zone'] = (
      ENDPOINT_ZONE.value)

  return config


def _PrepareServer(vm):
  """Prepares the web servers.

  Args:
    vm: the server virtual machine.
  """
  vm.Install(NGINX)

  # Required to make uplink test function properly
  vm.RemoteCommand(
      r'sudo sed -i "/server_name _;/a error_page  405 =200 \$uri;" /etc/nginx/sites-enabled/default'
  )
  vm.RemoteCommand(
      'sudo sed -i "/server_name _;/a client_max_body_size 100M;" /etc/nginx/sites-enabled/default'
  )
  vm.RemoteCommand('sudo systemctl restart nginx')

  web_probe_file = posixpath.join(vm.GetScratchDir(), 'probe.tgz')
  vm.InstallPreprovisionedPackageData(cloud_harmony_network.PACKAGE_NAME,
                                      [cloud_harmony_network.WEB_PROBE_TAR],
                                      vm.GetScratchDir())
  vm.RemoteCommand(f'tar zxf {web_probe_file} -C {vm.GetScratchDir()}')
  vm.RemoteCommand(f'rm -f {web_probe_file}')
  # Commands to generated larger test files (since they aren't included in the
  # repository due to size limitations). Commands obtained from
  # https://github.com/cloudharmony/web-probe
  web_probe_dir = posixpath.join(vm.GetScratchDir(), 'probe')
  vm.RemoteCommand(f'dd if=/dev/urandom of={web_probe_dir}/test10gb.bin '
                   'bs=10240 count=1048576')
  vm.RemoteCommand(f'sudo ln -s {web_probe_dir} {HTTP_DIR}')


def _PrepareBucket(benchmark_spec):
  """Prepares the GCS bucket for object storage test.

  First creates a bucket in the specified bucket region. Then populates the test
  bucket with contents of https://github.com/cloudharmony/web-probe as well as
  larger generated test files. The new bucket is saved to later be set as the
  test endpoint for the object storage test.

  Args:
    benchmark_spec: the benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  # set up Google Cloud Service
  service = gcs.GoogleCloudStorageService()
  location = cloud_harmony_util.GetRegionFromZone(ENDPOINT_ZONE.value)
  service.PrepareService(location)
  # create bucket in specified bucket region
  bucket = f'ch-{location}-{FLAGS.run_uri}'
  bucket_uri = f'gs://{bucket}'
  service.MakeBucket(bucket)
  # set default permissions to allow cloudharmony test file access
  perm_cmd = ['gsutil', 'defacl', 'set', 'public-read', bucket_uri]
  vm_util.IssueCommand(perm_cmd)
  # set bucket lifecyle to ensure bucket deletion after 30 days
  lifecyle_config_file = data.ResourcePath(
      'cloudharmony_network_gcp_lifecycle.json')
  lc_cmd = ['gsutil', 'lifecycle', 'set', lifecyle_config_file, bucket_uri]
  vm_util.IssueCommand(lc_cmd)
  # prepare preprovisioned test data
  tmp_dir = vm_util.GetTempDir()
  tmp_probe_file = posixpath.join(tmp_dir, cloud_harmony_network.WEB_PROBE_TAR)
  wget_install_cmd = ['sudo', 'apt-get', 'install', 'wget']
  vm_util.IssueCommand(wget_install_cmd)
  wget_cmd = ['wget', '-O', tmp_probe_file, cloud_harmony_network.WEB_PROBE]
  vm_util.IssueCommand(wget_cmd)
  tar_cmd = ['tar', 'zxf', tmp_probe_file, '-C', tmp_dir]
  vm_util.IssueCommand(tar_cmd)
  remote_probe_dir = posixpath.join(tmp_dir, 'probe')
  dd_cmd = [
      'dd', 'if=/dev/urandom', f'of={remote_probe_dir}/test10gb.bin',
      'bs=10240', 'count=1048576'
  ]
  vm_util.IssueCommand(dd_cmd)
  # copy preprovisioned test data to test bucket
  src_path = posixpath.join(remote_probe_dir, '*')
  dst_url = f'{bucket_uri}/probe'
  cp_cmd = ['gsutil', 'cp', '-r', src_path, dst_url]
  vm_util.IssueCommand(cp_cmd, raise_on_timeout=False)
  # save the service and the bucket name
  benchmark_spec.service = service
  benchmark_spec.bucket = bucket


def Prepare(benchmark_spec):
  """Prepares the cloudharmony network benchmark."""
  # Force cleanup because no standalone VMs are created to trigger normally.
  benchmark_spec.always_call_cleanup = True

  vm_groups = benchmark_spec.vm_groups
  client = vm_groups['client'][0]
  client.Install('cloud_harmony_network')
  if FLAGS.ch_network_test_service_type == COMPUTE:
    vm_util.RunThreaded(_PrepareServer, vm_groups['server'])
  elif FLAGS.ch_network_test_service_type == STORAGE:
    _PrepareBucket(benchmark_spec)
  else:
    raise NotImplementedError()


def ParseOutput(cloud_harmony_metadata):
  """Parse CSV output from CloudHarmony network benchmark.

  Args:
    cloud_harmony_metadata: cloud harmony network metadata.

  Returns:
    A list of sample.Sample object.

  Raises:
    Benchmarks.RunError: If correct operation is not validated.
  """
  # If one run in iteration of runs fails, fail the benchmark entirely.
  for result in cloud_harmony_metadata:
    if result['status'] == 'failed':
      raise errors.Benchmarks.RunError('Cloudharmony network test failed')

    if FLAGS.ch_network_test_service_type == COMPUTE:
      result['server_type'] = NGINX

  return cloud_harmony_util.GetMetadataSamples(cloud_harmony_metadata)


def _AddComputeMetadata(client, server, metadata):
  """Updates the metadata with network compute metadata."""
  compute_metadata = {
      # test_instance_id, test_region and meta_os_info are informational and
      # used in conjunction with saving results.
      'test_instance_id': server.machine_type,
      'test_region':
          cloud_harmony_util.GetRegionFromZone(server.zone),
      'meta_instance_id': client.machine_type,
      'meta_region':
          cloud_harmony_util.GetRegionFromZone(client.zone),
      'meta_zone': client.zone,
  }
  metadata.update(compute_metadata)


def _AddStorageMetadata(vm, metadata):
  """Updates the metadata with object storage metadata."""
  storage_metadata = {
      'test_instance_id': '',
      'test_region': cloud_harmony_util.GetRegionFromZone(ENDPOINT_ZONE.value),
      'meta_instance_id': vm.machine_type,
      'meta_region': cloud_harmony_util.GetRegionFromZone(vm.zone),
      'meta_zone': vm.zone,
  }
  metadata.update(storage_metadata)


def _Run(benchmark_spec, test):
  """Runs cloudharmony network and reports the results."""
  vm_groups = benchmark_spec.vm_groups
  metadata = {
      'test_service_type': FLAGS.ch_network_test_service_type,
      'test': test,
      'throughput_size': FLAGS.ch_network_throughput_size,
      'throughput_threads': FLAGS.ch_network_throughput_threads,
      'throughput_samples': FLAGS.ch_network_throughput_samples,
      'tcp_samples': FLAGS.ch_network_tcp_samples,
  }
  client = vm_groups['client'][0]
  endpoints = ''

  if FLAGS.ch_network_test_service_type == COMPUTE:
    vms = vm_groups['server']
    endpoints = ' '.join([f'--test_endpoint={vm.internal_ip}' for vm in vms])
    _AddComputeMetadata(client, vms[0], metadata)
  elif FLAGS.ch_network_test_service_type == STORAGE:
    http_url = f'http://{benchmark_spec.bucket}.storage.googleapis.com/probe'
    endpoints = f'--test_endpoint={http_url}'
    _AddStorageMetadata(client, metadata)

  if FLAGS.ch_network_throughput_https:
    metadata['throughput_https'] = True
  if FLAGS.ch_network_throughput_small_file:
    metadata['throughput_small_file'] = True
  if FLAGS.ch_network_throughput_time:
    metadata['throughput_time'] = True
  if FLAGS.ch_network_throughput_slowest_thread:
    metadata['throughput_slowest_thread'] = True

  metadata = cloud_harmony_util.GetCommonMetadata(metadata)
  cmd_path = posixpath.join(cloud_harmony_network.INSTALL_PATH, 'run.sh')
  outdir = vm_util.VM_TMP_DIR
  cmd = f'sudo {cmd_path} {endpoints} {metadata} --output={outdir} --verbose'
  client.RobustRemoteCommand(cmd)
  save_command = posixpath.join(cloud_harmony_network.INSTALL_PATH, 'save.sh')
  client.RemoteCommand(f'{save_command} {outdir}')
  cloud_harmony_metadata = cloud_harmony_util.ParseCsvResultsIntoMetadata(
      client, OUTPUT)
  return ParseOutput(cloud_harmony_metadata)


def Run(benchmark_spec):
  """Runs cloudharmony network and reports the results."""
  samples = _Run(benchmark_spec, FLAGS.ch_network_test)
  return samples


def Cleanup(benchmark_spec):
  """Cleanup any artifacts left by the benchmark."""
  if FLAGS.ch_network_test_service_type != STORAGE or not hasattr(
      benchmark_spec, 'service'):
    logging.info(
        'Skipping cleanup as not needed or storage prepare method failed.')
    return
  service = benchmark_spec.service
  bucket = benchmark_spec.bucket
  # delete the bucket
  service.DeleteBucket(bucket)
  service.CleanupService()
