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
"""Run MLPerf benchmarks on multiple workers."""

from __future__ import print_function
import posixpath
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import mlperf_benchmark
from perfkitbenchmarker.linux_packages import nvidia_driver
from perfkitbenchmarker.linux_packages import tensorflow
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
HOSTFILE = 'HOSTFILE'
PORT = '4242'
DGXSYSTEM = 'DGXA100_multinode'
CONFIG = f'config_{DGXSYSTEM}.sh'
AWS_EFA_NCCL_BASEAMI_PIPELINE_URL = 'https://github.com/aws-samples/aws-efa-nccl-baseami-pipeline.git'
NVIDIA_EFA_DOCKERFILE = 'aws-efa-nccl-baseami-pipeline/nvidia-efa-docker_base/Dockerfile.base'


BENCHMARK_NAME = 'mlperf_multiworkers'
BENCHMARK_CONFIG = """
mlperf_multiworkers:
  description: Runs MLPerf Benchmark on multiple workers.
  vm_groups:
    default:
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: a2-highgpu-8g
          zone: us-central1-b
          boot_disk_size: 200
        AWS:
          machine_type: p4d.24xlarge
          zone: us-west-2a
          boot_disk_size: 200
          image: ami-0ccc71d716eb5d6a4
        Azure:
          machine_type: Standard_ND96asr_v4
          zone: westus2
          boot_disk_size: 200
          image: microsoft-dsvm:ubuntu-hpc:1804:latest
      vm_count: null
"""

flags.DEFINE_boolean('mlperf_keep_nccl_log', False,
                     'whether to keep NCCL debug information')


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if 'tpu_groups' in config:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Invalid configuration. '
        'The multiworker mlperf benchmark cannot run with TPUs'
    )

  return config


def CheckPrerequisites(benchmark_config):
  """Verify that the required prerequisites are met.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.errors.Setup.InvalidFlagConfigurationError:
      On invalid flag configuration.
  """
  if not FLAGS.openmpi_enable_shared:
    raise errors.Setup.InvalidFlagConfigurationError(
        'The flag openmpi_enable_shared must be True '
        'in order to run with multiple workers.'
    )


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  gpus_per_vm = nvidia_driver.QueryNumberOfGpus(benchmark_spec.vms[0])
  num_vms = len(benchmark_spec.vms)
  total_num_gpus = gpus_per_vm * num_vms

  benchmark_spec.gpus_per_vm = gpus_per_vm
  benchmark_spec.num_vms = num_vms
  benchmark_spec.total_num_gpus = total_num_gpus
  benchmark_spec.zones = FLAGS.zones

  # pylint: disable=protected-access
  mlperf_benchmark._UpdateBenchmarkSpecWithFlags(benchmark_spec)
  # pylint: enable=protected-access

  storage_service = gcs.GoogleCloudStorageService()
  benchmark_spec.storage_service = storage_service
  if FLAGS.mlperf_bucket:
    benchmark_spec.bucket = FLAGS.mlperf_bucket
    benchmark_spec.model_dir = 'gs://{bucket}/pkb-{uri}'.format(
        bucket=FLAGS.mlperf_bucket, uri=FLAGS.run_uri)
  else:
    benchmark_spec.bucket = None
    benchmark_spec.model_dir = None


def _PrepareWorker(vm):
  """Install and set up cuda + openmpi on the target vm.

  Args:
    vm: The target vm
  """
  vm.Install('cuda_toolkit')
  vm.Install('openmpi')

  vm.AuthenticateVm()


def _PrepareMLPerfBenchmark(benchmark_spec, node_rank):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    node_rank: int, The rank of the node for multi-node distributed training
  """
  vm = benchmark_spec.vms[node_rank]
  mlperf_benchmark.PrepareBenchmark(benchmark_spec, vm)


def _PrepareMLPerfRunner(benchmark_spec, node_rank):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    node_rank: int, The rank of the node for multi-node distributed training
  """
  vm = benchmark_spec.vms[node_rank]
  mlperf_benchmark.PrepareRunner(benchmark_spec, vm)
  vm.RemoteCommand('sudo usermod -aG docker $USER')


def _SedPairsToString(pairs):
  """Convert a list of sed pairs to a string for the sed command.

  Args:
    pairs: a list of pairs, indicating the replacement requests

  Returns:
    a string to supply to the sed command
  """
  sed_str = '; '.join(['s/%s/%s/g' % pair for pair in pairs])
  if pairs:
    sed_str += ';'
  return sed_str


def _DictToString(dictionary):
  """Convert a dictionary to a space separated 'key=value' string.

  Args:
    dictionary: the key-value dictionary to be convert

  Returns:
    a string representing the dictionary
  """
  dict_str = ' '.join(' {key}={value}'.format(key=key, value=value)
                      for key, value in sorted(dictionary.items()))
  return dict_str


def _GetNcclParam():
  for extra_param in FLAGS.nccl_extra_params:
    param_key, param_value = extra_param.split('=', 1)
    param_value = param_value.replace('/', '\\/').replace('$', '\\$')
    yield fr'export {param_key}={param_value}'


def _GetNcclParams():
  return r'\n'.join(_GetNcclParam())


def _GetChangesForTransformer(benchmark_spec, vm, script_path, nvprof_flags,
                              config_sed_input, run_sed_input,
                              run_and_time_sed_input):
  """Get changes to config and run scripts for Transformer.

  Also updates run_training.sh on the vm.

  Args:
    benchmark_spec: The benchmark specification.
    vm: The target vm.
    script_path: The location of scripts on vm.
    nvprof_flags: The flags for nvprof.
    config_sed_input: Input list of sed pairs for config_DGXA100_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input
  per_gpu_batch_size = min(8192,
                           614400 / benchmark_spec.total_num_gpus)

  run_training_sed = []
  config_sed += [(r'MAX_TOKENS=.*', r'MAX_TOKENS={per_gpu_batch_size}'
                  .format(per_gpu_batch_size=per_gpu_batch_size))]

  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_training_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_training_sed += [(r'--max-epoch.*',
                          r'--max-update {profile_steps} \\\\'
                          .format(profile_steps=FLAGS.mlperf_profile_steps))]

  vm.RemoteCommand(
      fr'cd {script_path} && '
      fr'sed "{mlperf_benchmark.SedPairsToString(run_training_sed)}" '
      r'run_training.sh > run_training1.sh && chmod 755 run_training1.sh')

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/translation\/ \&\& sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForSSD(benchmark_spec, nvprof_flags, config_sed_input,
                      run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for SSD.

  Args:
    benchmark_spec: The benchmark specification.
    nvprof_flags: The flags for nvprof.
    config_sed_input: Input list of sed pairs for config_DGXA100_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input
  per_gpu_train_batch_size = min(24, 1680 / benchmark_spec.total_num_gpus)
  per_gpu_eval_batch_size = 40

  config_sed += [(r'--batch-size .*',
                  r'--batch-size \"{per_gpu_train_batch_size}\"'
                  .format(per_gpu_train_batch_size=per_gpu_train_batch_size))]
  config_sed += [(r'--eval-batch-size .*',
                  r'--eval-batch-size \"{per_gpu_eval_batch_size}\"'
                  .format(per_gpu_eval_batch_size=per_gpu_eval_batch_size))]

  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_and_time_sed += [(r'--epochs .*', r'--epochs 1 \\\\')]

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/single_stage_detector\/ \&\& '
               r'sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForGNMT(benchmark_spec, nvprof_flags, config_sed_input,
                       run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for GNMT.

  Args:
    benchmark_spec: The benchmark specification.
    nvprof_flags: The flags for nvprof.
    config_sed_input: Input list of sed pairs for config_DGXA100_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input
  per_gpu_train_batch_size = 32
  per_gpu_eval_batch_size = min(16, 3072 / benchmark_spec.total_num_gpus)

  config_sed += [(r'TRAIN_BATCH_SIZE=.*',
                  r'TRAIN_BATCH_SIZE={per_gpu_train_batch_size}'
                  .format(per_gpu_train_batch_size=per_gpu_train_batch_size))]
  config_sed += [(r'TEST_BATCH_SIZE=.*',
                  r'TEST_BATCH_SIZE={per_gpu_eval_batch_size}'
                  .format(per_gpu_eval_batch_size=per_gpu_eval_batch_size))]

  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_and_time_sed += [(r'--epochs .*', r'--epochs \"1\" \\\\')]

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/rnn_translator\/ \&\& sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForMask(benchmark_spec, node_rank, script_path, nvprof_flags,
                       config_sed_input, run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for MaskRCNN.

  Also update train_mlperf.py if nvprof is used.

  Args:
    benchmark_spec: The benchmark specification.
    node_rank: int, The rank of the node for multi-node distributed training
    script_path: The location of scripts on vm.
    nvprof_flags: The flags for nvprof.
    config_sed_input: Input list of sed pairs for config_DGXA100_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  vm = benchmark_spec.vms[node_rank]
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input

  config_sed += [('SOLVER_MAX_ITER=.*',
                  f'SOLVER_MAX_ITER={mlperf_benchmark.MASK_ITERATION.value}')]
  config_sed += [(r'WALLTIME_MINUTES=30',
                  r'WALLTIME_MINUTES=30\n'
                  r'export CONT=mlperf-nvidia:object_detection\n'
                  r'export DATADIR=\/data\n'
                  r'export PKLDIR=\/data\/coco2017\/pkl_coco\n'
                  r'export NEXP=1')]

  run_and_time_sed += [(r"'bind_launch'",
                        r"'bind_launch' "
                        f"'--nnodes={benchmark_spec.num_vms}' "
                        f"'--node_rank={node_rank}' "
                        f"'--master_addr={benchmark_spec.vms[0].internal_ip}' "
                        f"'--master_port={PORT}'")]
  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    vm.RemoteCommand(
        r'cd {script_path} && '
        r'cp tools/train_mlperf.py tools/train_mlperf0.py && '
        r'sed "s/min_bbox_map=.*/min_bbox_map=0.01,/g; '
        r'     s/min_segm_map=.*/min_segm_map=0.01)/g;" '
        r'  tools/train_mlperf0.py > tools/train_mlperf.py'
        .format(script_path=script_path))

  run_sed += [(r'SYSLOGGING=1', r'SYSLOGGING=0')]
  run_sed += [(r'_cont_mounts=(',
               r'_cont_mounts=(\"--volume=\${PKLDIR}:\/pkl_coco\" ')]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForResnet(benchmark_spec, node_rank, nvprof_flags,
                         config_sed_input, run_sed_input,
                         run_and_time_sed_input):
  """Get changes to config and run scripts for Resnet.

  Args:
    benchmark_spec: The benchmark specification.
    node_rank: int, The rank of the node for multi-node distributed training
    nvprof_flags: The flags for nvprof.
    config_sed_input: Input list of sed pairs for
    config_DGXA100_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for
    config_DGXA100_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input

  config_sed += [(r'NUMEPOCHS=.*',
                  fr'NUMEPOCHS={mlperf_benchmark.RESNET_EPOCHS.value}\n'
                  r'export CONT=mlperf-nvidia:image_classification\n'
                  r'export DATADIR=\/data\/imagenet')]

  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_and_time_sed += [(r'num-epochs.*',
                          r'num-epochs  \"1\"\n'
                          r'  --epoch-size  \"{profile_steps}\"'
                          .format(profile_steps=FLAGS.mlperf_profile_steps))]

  run_and_time_sed += [(r'BIND=.*', r'BIND=\"\"')]
  run_and_time_sed += [('NUMEPOCHS=.*',
                        f'NUMEPOCHS={mlperf_benchmark.RESNET_EPOCHS.value}')]

  hosts = ','.join(f'{vm.internal_ip}:{benchmark_spec.gpus_per_vm}'
                   for vm in benchmark_spec.vms)
  np = benchmark_spec.gpus_per_vm * benchmark_spec.num_vms
  run_and_time_sed += [(r'mpirun.*',
                        fr'horovodrun -H {hosts} -p {PORT} -np {np}\"')]

  run_sed += [(r'_cont_mounts=(',
               r'_cont_mounts=(\"--volume=\$HOME\/.ssh:\/tmp\/.ssh\" ')]

  if node_rank == 0:
    run_sed += [(r'sleep infinity',
                 r'bash -c \"cp -r \/tmp\/.ssh \/root\/.ssh;sleep infinity\"')]
  else:
    run_sed += [(r'sleep infinity',
                 r'bash -c \"cp -r \/tmp\/.ssh \/root\/.ssh;'
                 r'apt update;'
                 r'apt-get install -y openssh-server;'
                 r'systemctl enable ssh;'
                 r'mkdir -p \/run\/sshd;'
                 fr'\/usr\/sbin\/sshd -p {PORT};'
                 r'sleep infinity\"')]
    run_sed += [(r'.*run_and_time.*', r'')]
    run_sed += [(r'trap.*', r'')]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForBert(benchmark_spec, node_rank, nvprof_flags,
                       config_sed_input, run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for BERT.

  Also update train_mlperf.py if nvprof is used.

  Args:
    benchmark_spec: The benchmark specification.
    node_rank: int, The rank of the node for multi-node distributed training
    nvprof_flags: The flags for nvprof.
    config_sed_input: Input list of sed pairs for config_DGXA100_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input

  config_sed += [(r'source .*',
                  r'export CONT=mlperf-nvidia:language_model\n'
                  r'export NEXP=1\n'
                  fr'export MASTER_ADDR={benchmark_spec.vms[0].internal_ip}\n'
                  fr'export MASTER_PORT={PORT}')]
  config_sed += [(r'DATADIR=.*',
                  r'DATADIR=\/data\/bert_data\/2048_shards_uncompressed')]
  config_sed += [(r'MAX_STEPS=.*',
                  f'MAX_STEPS={mlperf_benchmark.BERT_STEPS.value}')]
  config_sed += [(r'DATADIR_PHASE2=.*',
                  r'DATADIR_PHASE2=\/data\/bert_data\/'
                  r'2048_shards_uncompressed')]
  config_sed += [(r'EVALDIR=.*',
                  r'EVALDIR=\/data\/bert_data\/eval_set_uncompressed')]
  config_sed += [(r'CHECKPOINTDIR=.*',
                  r'CHECKPOINTDIR=\/data\/bert_data\/tf1_ckpt')]
  config_sed += [(r'CHECKPOINTDIR_PHASE1=.*',
                  r'CHECKPOINTDIR_PHASE1=\/data\/bert_data\/tf1_ckpt')]
  config_sed += [(r'BATCHSIZE=.*',
                  fr'BATCHSIZE={mlperf_benchmark.BERT_BATCH_SIZE.value}')]

  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', fr'nvprof {nvprof_flags} python')]

  run_sed += [(r"'bind_pyt'",
               r"'bind_pyt' "
               fr"'--nnodes={benchmark_spec.num_vms}' "
               fr"'--node_rank={node_rank}' "
               fr"'--master_addr={benchmark_spec.vms[0].internal_ip}' "
               fr"'--master_port={PORT}'")]

  return config_sed, run_sed, run_and_time_sed


def _UpdateScripts(benchmark_spec, node_rank):
  """Update the running scripts on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
    node_rank: int, The rank of the node for multi-node distributed training
  """
  vm = benchmark_spec.vms[node_rank]
  benchmark = benchmark_spec.benchmark

  # TODO(tohaowu) Change config and script using a patch file.
  # request pairs to the sed command
  # each pair('str_A', 'str_B') indicates a request "replace anything
  # matching str_A to str_B" for a specific file
  config_sed = []
  config_sed += [(r'DGXSYSTEM=.*', fr'DGXSYSTEM=\"{DGXSYSTEM}\"')]
  config_sed += [(r'DGXNNODES=.*', r'DGXNNODES={num_vms}'
                  .format(num_vms=benchmark_spec.num_vms))]
  config_sed += [(r'DGXNGPU=.*', r'DGXNGPU={gpus_per_vm}'
                  .format(gpus_per_vm=benchmark_spec.gpus_per_vm))]
  config_sed += [(r'DGXNSOCKET=.*', r'DGXNSOCKET={nsockets}'
                  .format(nsockets=vm.CheckLsCpu().socket_count))]
  config_sed += [(r'DGXSOCKETCORES=.*', r'DGXSOCKETCORES={ncores}'
                  .format(ncores=vm.CheckLsCpu().cores_per_socket))]

  run_and_time_sed = []
  run_and_time_sed += [(r'run_training.sh', r'run_training1.sh')]
  run_and_time_sed += [(r'DGXSYSTEM=.*', fr'DGXSYSTEM=\"{DGXSYSTEM}\"')]

  if FLAGS.mlperf_keep_nccl_log:
    run_and_time_sed += [(r'#\!\/bin\/bash',
                          r'#\!\/bin\/bash\n'
                          r'export NCCL_DEBUG=INFO\n'
                          r'export NCCL_DEBUG_SUBSYS=ALL\n'
                          r'export NCCL_DEBUG_FILE=\/results\/%h.%p.nccl')]

  nccl_exports = _GetNcclParams() if FLAGS.nccl_extra_params else r''
  run_and_time_sed += [(r'#!\/bin\/bash',
                        r'#!\/bin\/bash\n'
                        fr'{nccl_exports}')]

  run_sed = []
  run_sed += [(r'SYSLOGGING=1', r'SYSLOGGING=0')]
  run_sed += [(r'env [|] grep SLURM', r'export SLURM_NNODES={num_vms}'
               .format(num_vms=benchmark_spec.num_vms))]
  run_sed += [(r'data -v \$LOGDIR',
               r'data -v \$(pwd):\/workspace\/{model}1 -v \$LOGDIR'
               .format(model=benchmark))]
  run_sed += [(r'scontrol show hostname',
               r'mpirun -hostfile \$HOME\/{hostfile} -N 1 hostname -I '
               r'\| awk \'{{print \$1}}\' '
               .format(hostfile=HOSTFILE))]
  run_sed += [(r'srun --mem=0 -N 1 -n 1 -w \$hostn',
               r'mpirun -N 1 -n 1 -H \$hostn')]
  run_sed += [(r'sleep 30', r'sleep 60')]
  run_sed += [(r'docker exec -it', r'docker exec -t')]
  run_sed += [(r'run_and_time.sh', r'run_and_time1.sh')]

  run_sed += [(r'nvidia-docker', r'sudo nvidia-docker')]
  run_sed += [(r'docker exec', r'sudo docker exec')]
  run_sed += [(r'docker container', r'sudo docker container')]

  if FLAGS.aws_efa or FLAGS.azure_infiniband:
    stdout, _ = vm.RemoteCommand('ls -d /dev/infiniband/*')
    devices = [device.replace('/', '\\/') for device in stdout.split()]
    device_args = ' '.join(f'--device={device}' for device in devices)
    run_sed += [(r'nvidia-docker run', fr'nvidia-docker run {device_args}')]

  if FLAGS.azure_infiniband:
    run_sed += [
        (r'_cont_mounts=(',
         r'_cont_mounts=(\"--volume=\/opt\/microsoft:\/opt\/microsoft\" ')]

  nvprof_flags = r'-f -o \/results\/%h.%p.nvprof --profile-child-processes'

  script_path = (
      r'$HOME/training_results_{version}/NVIDIA/benchmarks/{model}'
      r'/implementations/{framework}'
      .format(version=mlperf_benchmark.MLPERF_VERSION,
              model='maskrcnn' if mlperf_benchmark.MASK in benchmark
              else benchmark,
              framework='mxnet' if mlperf_benchmark.RESNET in benchmark
              else 'pytorch'))

  config_files = [CONFIG]
  if mlperf_benchmark.TRANSFORMER in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForTransformer(
        benchmark_spec, vm, script_path, nvprof_flags, config_sed, run_sed,
        run_and_time_sed)

  elif mlperf_benchmark.SSD in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForSSD(
        benchmark_spec, nvprof_flags, config_sed, run_sed, run_and_time_sed)

  elif mlperf_benchmark.GNMT in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForGNMT(
        benchmark_spec, nvprof_flags, config_sed, run_sed, run_and_time_sed)

  elif mlperf_benchmark.MASK in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForMask(
        benchmark_spec, node_rank, script_path, nvprof_flags, config_sed,
        run_sed, run_and_time_sed)

    config_files = ['config_DGXA100_multi_4x8x4.sh']

  elif mlperf_benchmark.RESNET in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForResnet(
        benchmark_spec, node_rank, nvprof_flags, config_sed, run_sed,
        run_and_time_sed)

    config_files = ['config_DGXA100_common.sh',
                    'config_DGXA100_multi_8x8x204.sh']

  elif mlperf_benchmark.BERT in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForBert(
        benchmark_spec, node_rank, nvprof_flags, config_sed, run_sed,
        run_and_time_sed)

    config_files = ['config_DGXA100_common.sh',
                    'config_DGXA100_8x8x48x1.sh']

  vm.RemoteCommand(
      f'cd {script_path} && '
      f'sed "{mlperf_benchmark.SedPairsToString(config_sed)}" '
      f'{" ".join(config_files)} > {CONFIG} && '
      f'chmod 755 {CONFIG} ')

  vm.RemoteCommand(
      f'cd {script_path} && '
      f'sed "{mlperf_benchmark.SedPairsToString(run_and_time_sed)}" '
      f'run_and_time.sh | sed "2 i source {CONFIG}" > run_and_time1.sh && '
      'chmod 755 run_and_time1.sh ')

  vm.RemoteCommand(
      f'cd {script_path} && '
      f'sed "{mlperf_benchmark.SedPairsToString(run_sed)}" run_with_docker.sh '
      f'| sed "2 i source {CONFIG}" > run_with_docker1.sh && '
      'chmod 755 run_with_docker1.sh')

  docker_file = posixpath.join(script_path, 'Dockerfile')
  if FLAGS.nccl_net_plugin:
    vm_util.ReplaceText(
        vm,
        'RUN apt-get update',
        r'RUN echo \"deb https:\/\/packages.cloud.google.com\/apt '
        r'google-fast-socket main\" | '
        r'tee \/etc\/apt\/sources.list.d\/google-fast-socket.list\n'
        r'RUN curl -s -L '
        r'https:\/\/packages.cloud.google.com\/apt\/doc\/apt-key.gpg | '
        r'apt-key add -\n'
        r'RUN rm -f \/opt\/hpcx\/nccl_rdma_sharp_plugin\/lib\/libnccl-net.so\n'
        r'RUN apt-get update',
        docker_file
    )
    vm_util.ReplaceText(
        vm,
        'apt-get install -y --no-install-recommends',
        'apt-get install -y --no-install-recommends google-fast-socket',
        docker_file
    )

  if FLAGS.aws_efa:
    vm.RemoteCommand(f'git clone {AWS_EFA_NCCL_BASEAMI_PIPELINE_URL}')
    vm.RemoteCommand(f'cat {NVIDIA_EFA_DOCKERFILE} >> {docker_file}')
    vm_util.ReplaceText(vm, 'FROM nvcr.*', '', docker_file)
    vm_util.ReplaceText(vm, 'yum-utils.*', '', docker_file)
    vm_util.ReplaceText(vm, 'python3-distutils.*', 'python3-distutils',
                        docker_file)
    vm_util.ReplaceText(vm, 'cmake', '', docker_file)


def _PrepareBucket(benchmark_spec):
  """Prepare storage bucket for profiling results, if needed.

  Args:
    benchmark_spec: The benchmark specification
  """
  if (mlperf_benchmark.NONE in FLAGS.mlperf_profiler and
      not FLAGS.mlperf_keep_nccl_log):
    return

  if FLAGS.cloud != 'GCP':
    return

  location = benchmark_spec.zones[0]
  bucket = benchmark_spec.bucket
  storage_service = benchmark_spec.storage_service
  storage_service.PrepareService(util.GetRegionFromZone(location))
  storage_service.MakeBucket(bucket, raise_on_failure=False)
  storage_service.AclBucket(benchmark_spec.gcp_service_account, gcs.WRITER,
                            bucket)


def _ClearTmpDirectory(benchmark_spec, node_rank):
  vm = benchmark_spec.vms[node_rank]
  vm.RemoteCommand(
      r'sudo rm -rf {dir}'
      .format(dir=posixpath.join(vm_util.VM_TMP_DIR, benchmark_spec.benchmark)))


def Prepare(benchmark_spec):
  """Install and set up MLPerf on multiple vms.

  Args:
    benchmark_spec: The benchmark specification
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_PrepareWorker, vms)

  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  list_params = [((benchmark_spec, node_rank), {})
                 for node_rank in range(len(vms))]
  _PrepareBucket(benchmark_spec)

  vm_util.RunThreaded(_ClearTmpDirectory, list_params)

  vm_util.RunThreaded(_PrepareMLPerfBenchmark, list_params)

  vm_util.RunThreaded(_UpdateScripts, list_params)

  vm_util.RunThreaded(_PrepareMLPerfRunner, list_params)

  hpc_util.CreateMachineFile(vms, lambda _: benchmark_spec.gpus_per_vm,
                             HOSTFILE)
  vms[0].RemoteCommand('sleep 30')


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    metadata dict
  """
  metadata = {
      'use_tpu': bool(benchmark_spec.tpus),
      'model_dir': benchmark_spec.model_dir,
      'model': benchmark_spec.benchmark,
      'version': mlperf_benchmark.MLPERF_VERSION,
  }
  return metadata


def MakeSamplesFromOutput(metadata, output, model=mlperf_benchmark.RESNET):
  """Create samples containing metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
    model: string, model name
  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_benchmark_test.py

  Returns:
    Samples containing training metrics.
  """
  return mlperf_benchmark.MakeSamplesFromOutput(
      metadata, output, use_tpu=False, model=model)


def Run(benchmark_spec):
  """Run MLPerf on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  master_vm = vms[0]
  benchmark = benchmark_spec.benchmark

  env_params = {}
  env_params['SLURM_JOB_ID'] = r'{uri}'.format(uri=FLAGS.run_uri)
  env_params['PULL'] = 0
  env_params['DGXSYSTEM'] = DGXSYSTEM
  env_params['NEXP'] = 1
  env_params['LOGDIR'] = posixpath.join(vm_util.VM_TMP_DIR, benchmark)

  script_path = (
      '$HOME/training_results_{version}/NVIDIA/benchmarks/{model}'
      r'/implementations/{framework}'
      .format(version=mlperf_benchmark.MLPERF_VERSION,
              model='maskrcnn' if mlperf_benchmark.MASK in benchmark
              else benchmark,
              framework='mxnet' if mlperf_benchmark.RESNET in benchmark
              else 'pytorch'))

  benchmark_env_params = {
      mlperf_benchmark.TRANSFORMER: {
          'CONT': r'"mlperf-nvidia:translation"',
          'DATADIR': r'/data/wmt/utf8'},
      mlperf_benchmark.SSD: {
          'CONT': r'"mlperf-nvidia:single_stage_detector"',
          'DATADIR': '/data'},
      mlperf_benchmark.GNMT: {
          'CONT': r'"mlperf-nvidia:rnn_translator"',
          'DATADIR': r'/data/gnmt'},
      mlperf_benchmark.MASK: {},
      mlperf_benchmark.RESNET: {},
      mlperf_benchmark.BERT: {},
  }
  env_params.update(benchmark_env_params.get(benchmark, {}))
  if mlperf_benchmark.RESNET in benchmark:
    env_params['SLURM_JOB_NUM_NODES'] = benchmark_spec.num_vms

  env = r''
  if nvidia_driver.CheckNvidiaGpuExists(master_vm):
    env = tensorflow.GetEnvironmentVars(master_vm)

  cmd = (f'cd {script_path} && '
         f'{env} {_DictToString(env_params)} '
         f'{FLAGS.nccl_mpi} '
         '--allow-run-as-root '
         '-hostfile $HOME/HOSTFILE '
         '--mca pml ^cm '
         '--mca btl tcp,self '
         '--mca btl_tcp_if_exclude docker0,lo '
         '--bind-to none '
         '-N 1 '
         './run_with_docker1.sh')
  if (mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler or
      FLAGS.mlperf_keep_nccl_log):
    cmd += (r' && cp /tmp/pkb/cmd* {logdir}'
            .format(logdir=posixpath.join(vm_util.VM_TMP_DIR, benchmark)))

  samples = []
  metadata = _CreateMetadataDict(benchmark_spec)
  stdout, _ = master_vm.RobustRemoteCommand(cmd, should_log=True)
  if mlperf_benchmark.NONE in FLAGS.mlperf_profiler:
    samples.extend(
        MakeSamplesFromOutput(metadata, stdout, model=benchmark))

  if (mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler or
      FLAGS.mlperf_keep_nccl_log):
    master_vm.RemoteCommand(
        r'mkdir -p /data/aggregated/{model}'.format(model=benchmark))
    master_vm.RemoteCommand(
        r'mpirun -hostfile $HOME/{hostfile} -N 1 scp -r {logdir} '
        r'{master_ip}:/data/aggregated/'
        .format(hostfile=HOSTFILE,
                logdir=posixpath.join(vm_util.VM_TMP_DIR, benchmark),
                master_ip=master_vm.internal_ip))

  return samples


def Cleanup(benchmark_spec):
  """Cleanup MLPerf on the cluster.

  Args:
    benchmark_spec: The benchmark specification.
      Contains all data that is required to run the benchmark.
  """
  del benchmark_spec  # Unused.
  pass
