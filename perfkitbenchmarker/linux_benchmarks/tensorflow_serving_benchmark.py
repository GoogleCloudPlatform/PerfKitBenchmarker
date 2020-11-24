# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs an online prediction Tensorflow Serving benchmark.

This benchmark is composed of the following:
  * this file, tensorflow_serving_benchmark.py
  * the package which builds tf serving, linux_packages/tensorflow_serving.py
  * a client workload generator, tensorflow_serving_client_workload.py
  * preprovisioned data consisting of the imagenet 2012 validation images
    and their labels

The benchmark uses two VMs: a server, and a client.
Tensorflow Serving is built from source (in a docker image), which takes
a significant amount of time (45 minutes on an n1-standard-8).
Note that both client and server VMs build the code. This is necessary to build
an optimized binary for the CPU it will be running on.

Once the code is built, the server prepares an ResNet model
for serving. It prepares a pre-trained ResNet model using a publicly
available SavedModel. The server then starts a platform-optimized
tensorflow_model_server binary using the prepared model.

The client VM downloads the imagenet 2012 validation images from cloud storage
and begins running a client-side load generator script which does the
following:
  * launches a specified number of threads
  * each thread chooses a random image from the dataset and sends a prediction
    request to the server, notes the latency, and repeats with a new random
    image
  * once the specified time period is up, the client script prints results
    to stdout, which this benchmark reads and uses to create samples.

When the benchmark is finished, all resources are torn down.
"""

import logging
import posixpath
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import tensorflow_serving

FLAGS = flags.FLAGS
CLIENT_SCRIPT = 'tensorflow_serving_client_workload.py'
RESNET_NHWC_SAVEDMODEL_TGZ = 'resnet_v2_fp32_savedmodel_NHWC_jpg.tar.gz'
ILSVRC_VALIDATION_IMAGES_TAR = 'ILSVRC2012_img_val.tar'
SERVER_PORT = 8500
TF_SERVING_BASE_DIRECTORY = tensorflow_serving.TF_SERVING_BASE_DIRECTORY

BENCHMARK_DATA = {
    # This ResNet SavedModel (ResNet-50 v2, fp32, Accuracy 76.47%) is from the
    # official TF models repo. It takes in JPG as input and is channels-last
    # (NHWC), which is generally better for CPU. It is available here:
    # http://download.tensorflow.org/models/official/20181001_resnet/savedmodels/resnet_v2_fp32_savedmodel_NHWC_jpg.tar.gz
    RESNET_NHWC_SAVEDMODEL_TGZ:
        '545965f0f85c87386e51076abc7ef4f9f1decaf641e8a90906f98c6774547e3f',

    # Collection of 50,000 imagenet 2012 validation images.
    # Available here:
    # http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_img_val.tar
    ILSVRC_VALIDATION_IMAGES_TAR:
        'c7e06a6c0baccf06d8dbeb6577d71efff84673a5dbdd50633ab44f8ea0456ae0',
}

BENCHMARK_NAME = 'tensorflow_serving'
BENCHMARK_CONFIG = """
tensorflow_serving:
  description: Runs a Tensorflow Serving benchmark.
  vm_groups:
    clients:
      vm_spec:
        GCP:
          boot_disk_size: 200
          machine_type: n1-standard-8
          zone: us-central1-a
        Azure:
          machine_type: Standard_F8s_v2
          zone: eastus2
        AWS:
          boot_disk_size: 200
          machine_type: m5.2xlarge
          zone: us-east-1f
      os_type: ubuntu1604
    servers:
      vm_spec:
        GCP:
          boot_disk_size: 200
          machine_type: n1-standard-8
          zone: us-central1-a
          min_cpu_platform: skylake
        Azure:
          machine_type: Standard_F8s_v2
          zone: eastus2
        AWS:
          boot_disk_size: 200
          machine_type: m5.2xlarge
          zone: us-east-1f
      os_type: ubuntu1604
"""

flags.DEFINE_integer(
    'tf_serving_runtime', 60, 'benchmark runtime in seconds', lower_bound=1)
flag_util.DEFINE_integerlist(
    'tf_serving_client_thread_counts', [16, 32],
    'number of client worker threads',
    module_name=__name__)


class ClientWorkloadScriptExecutionError(Exception):
  pass


def GetConfig(user_config):
  """Loads and returns benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  pass


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  del benchmark_spec


def _PrepareClient(vm):
  """Installs Tensorflow Serving on a single client vm.

  Args:
    vm: client vm to operate on
  """
  logging.info('Installing Tensorflow Serving on client %s', vm)
  vm.Install('tensorflow_serving')
  vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME,
                                        [ILSVRC_VALIDATION_IMAGES_TAR],
                                        linux_packages.INSTALL_DIR)

  # The image tarball does not contain a subfolder, so create one
  # using the filename of the tarball, minus the extension and extract
  # it there.
  extract_dir = posixpath.join(
      linux_packages.INSTALL_DIR,
      posixpath.splitext(ILSVRC_VALIDATION_IMAGES_TAR)[0])
  vm.RemoteCommand('mkdir {0}'.format(extract_dir))

  vm.RemoteCommand('cd {0} && tar xvf {1} --directory {2}'.format(
      linux_packages.INSTALL_DIR, ILSVRC_VALIDATION_IMAGES_TAR, extract_dir))


def _PrepareServer(vm):
  """Installs Tensorflow Serving on a single server vm.

  Args:
    vm: server vm to operate on
  """
  logging.info('Installing Tensorflow Serving on server %s', vm)
  vm.Install('tensorflow_serving')
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, [RESNET_NHWC_SAVEDMODEL_TGZ], TF_SERVING_BASE_DIRECTORY)

  extract_dir = posixpath.join(
      TF_SERVING_BASE_DIRECTORY, "resnet")
  vm.RemoteCommand('mkdir {0}'.format(extract_dir))

  vm.RemoteCommand('cd {0} && tar --strip-components=2 --directory {1} -xvzf '
                   '{2}'.format(TF_SERVING_BASE_DIRECTORY, extract_dir,
                                RESNET_NHWC_SAVEDMODEL_TGZ))


def Prepare(benchmark_spec):
  """Installs and prepares Tensorflow Serving on the target vms.

  Clients and servers are prepared in parallel using RunThreaded.

  Args:
    benchmark_spec: The benchmark specification
  """
  servers = benchmark_spec.vm_groups['servers']
  clients = benchmark_spec.vm_groups['clients']
  vms = []
  # Create tuples of (function_to_run, vm) in order to dispatch
  # to the appropriate prepare function in parallel.
  for s in servers:
    vms.append(((_PrepareServer, s), {}))
  for c in clients:
    vms.append(((_PrepareClient, c), {}))

  vm_util.RunThreaded(lambda prepare_function, vm: prepare_function(vm), vms)


def _CreateMetadataDict(benchmark_spec, client_thread_count):
  """Creates a metadata dict to be added to run results samples.

  Args:
    benchmark_spec: The benchmark specification.
    client_thread_count: The client thread count used for this particular run.

  Returns:
    A dict of metadata to be added to samples.
  """
  del benchmark_spec
  metadata = dict()
  metadata['scheduled_runtime'] = FLAGS.tf_serving_runtime
  metadata['client_thread_count'] = client_thread_count
  return metadata


def _StartServer(vm):
  """Starts the tensorflow_model_server binary.

  Args:
    vm: The server VM.
  """
  model_download_directory = posixpath.join(TF_SERVING_BASE_DIRECTORY, 'resnet')

  # Use the docker development image to build the inception model
  vm.RemoteCommand(
      'sudo docker run -d --rm --name tfserving-server --network host '
      '--mount type=bind,source={0},target=/models/resnet '
      '-e MODEL_NAME=resnet '
      '-t benchmarks/tensorflow-serving --port={1}'.format(
          model_download_directory, SERVER_PORT),
      should_log=True)


def _StartClient(vm, server_ip, client_thread_count):
  """Pushes and starts the client workload script.

  Args:
    vm: The client VM.
    server_ip: The server's ip address.
    client_thread_count: The client thread count used for this particular run.

  Returns:
    Stdout from CLIENT_SCRIPT

  Raises:
    ClientWorkloadScriptExecutionError: if an error occurred during execution
      of CLIENT_SCRIPT (detected by looking at stderr).
  """
  stdout, stderr = vm.RemoteCommand(
      'python {0} --server={1}:{2} --image_directory={3} '
      '--runtime={4} --num_threads={5}'.format(
          posixpath.join(linux_packages.INSTALL_DIR,
                         CLIENT_SCRIPT), server_ip, SERVER_PORT,
          posixpath.join(linux_packages.INSTALL_DIR,
                         posixpath.splitext(ILSVRC_VALIDATION_IMAGES_TAR)[0]),
          FLAGS.tf_serving_runtime, client_thread_count),
      should_log=True)

  # Ensure that stderr from the client script is empty.
  # If it is, stderr from the remote command should contain a single line:
  # Warning: Permanently added {ip} (ECDSA) to the list of known hosts.
  if len(stderr.splitlines()) > 1:
    raise ClientWorkloadScriptExecutionError(
        'Exception occurred during execution of client script: {0}'.format(
            stderr))

  return stdout


def _CreateSingleSample(sample_name, sample_units, metadata, client_stdout):
  """Creates a sample from the tensorflow_serving_client_workload stdout.

  client_stdout is expected to contain output in the following format:
    key1: int_or_float_value_1
    key2: int_or_float_value_2

  Args:
    sample_name: Name of the sample. Used to create a regex to extract the value
      from client_stdout. Also used as the returned sample's name.
    sample_units: Units to be specified in the returned sample
    metadata: Metadata to be added to the returned sample
    client_stdout: Stdout from tensorflow_serving_client_workload.py

  Returns:
    A single floating point sample.

  Raises:
    regex_util.NoMatchError: when no line beginning with sample_name: is found
      in client_stdout
  """
  regex = sample_name + r'\:\s*(\w+\.?\w*)'
  value = regex_util.ExtractFloat(regex, client_stdout)
  return sample.Sample(sample_name, value, sample_units, metadata)


def _CreateLatenciesSample(metadata, client_stdout):
  """Extracts latency samples from client_stdout.

  Assumes latency samples start one line after 'Latencies:'
  and continue until the end of the file, and that each latency sample
  is on its own line.

  Args:
    metadata: Metadata to be added to the returned sample
    client_stdout: Stdout from tensorflow_serving_client_workload.py

  Returns:
    A single sample containing an array of latencies.
  """
  updated_metadata = metadata.copy()
  lines = client_stdout.splitlines()
  latency_start = lines.index('Latency:') + 1
  latencies = [float(line) for line in lines[latency_start:]]
  updated_metadata.update({'latency_array': latencies})
  return sample.Sample('Latency', -1, 'seconds', updated_metadata)


def _MakeSamplesFromClientOutput(benchmark_spec, client_stdout,
                                 client_thread_count):
  """Returns an array of samples extracted from client_stdout.

  Args:
    benchmark_spec: The benchmark specification.
    client_stdout: Stdout from tensorflow_serving_client_workload.py.
    client_thread_count: The client thread count used for this particular run.

  Returns:
    A list of samples extracted from client_stdout.
  """
  metadata = _CreateMetadataDict(benchmark_spec, client_thread_count)
  samples = []

  metrics_to_extract = [
      # (sample_name, units)
      ('Completed requests', 'requests'),
      ('Failed requests', 'requests'),
      ('Throughput', 'images_per_second'),
      ('Runtime', 'seconds'),
  ]

  for metric in metrics_to_extract:
    samples.append(
        _CreateSingleSample(metric[0], metric[1], metadata, client_stdout))

  samples.append(_CreateLatenciesSample(metadata, client_stdout))
  return samples


def Run(benchmark_spec):
  """Runs Tensorflow Serving benchmark.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  server = benchmark_spec.vm_groups['servers'][0]
  client = benchmark_spec.vm_groups['clients'][0]

  _StartServer(server)
  client.PushDataFile(
      CLIENT_SCRIPT, remote_path=linux_packages.INSTALL_DIR)

  samples = []
  for thread_count in FLAGS.tf_serving_client_thread_counts:
    client_stdout = _StartClient(client, server.internal_ip, thread_count)
    samples.extend(
        _MakeSamplesFromClientOutput(benchmark_spec, client_stdout,
                                     thread_count))

  return samples


def Cleanup(benchmark_spec):
  """Cleans up Tensorflow Serving.

  Args:
    benchmark_spec: The benchmark specification.
  """
  servers = benchmark_spec.vm_groups['servers']
  clients = benchmark_spec.vm_groups['clients']

  def _CleanupServer(vm):
    vm.RemoteCommand('sudo docker stop tfserving-server || true')
    vm.Uninstall('tensorflow_serving')

  def _CleanupClient(vm):
    vm.Uninstall('tensorflow_serving')

  vms = []
  # Create tuples of (function_to_run, vm) in order to dispatch
  # to the appropriate prepare function in parallel.
  for s in servers:
    vms.append(((_CleanupServer, s), {}))
  for c in clients:
    vms.append(((_CleanupClient, c), {}))

  vm_util.RunThreaded(lambda cleanup_function, vm: cleanup_function(vm), vms)

  del benchmark_spec
