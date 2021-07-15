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

BENCHMARK_NAME = 'mlperf_multiworkers'
BENCHMARK_CONFIG = """
mlperf_multiworkers:
  description: Runs MLPerf Benchmark on multiple workers.
  vm_groups:
    default:
      os_type: ubuntu1804
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: n1-highmem-96
          zone: us-west1-b
          boot_disk_size: 110
          gpu_type: v100
          gpu_count: 8
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-west-2a
          boot_disk_size: 110
          image: ami-08c6f8e3871c56139
        Azure:
          machine_type: Standard_ND40rs_v2
          zone: westus2
          boot_disk_size: 110
          image: microsoft-dsvm:ubuntu-hpc:1804:latest
      vm_count: null
"""

flags.DEFINE_boolean('mlperf_keep_nccl_log', False,
                     'whether to keep NCCL debug information')

flags.DEFINE_boolean('mlperf_use_optimized_nccl_config', True,
                     'whether to use optimized NCCL environmental '
                     'configuration for GCP')


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


def _PrepareMLPerf(benchmark_spec, vm):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    vm: The target vm
  """
  mlperf_benchmark.Prepare(benchmark_spec, vm)
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


def _GetChangesForTransformer(benchmark_spec, vm, script_path,
                              nccl_log_exports, nvprof_flags, cuda_exports,
                              config_sed_input, run_sed_input,
                              run_and_time_sed_input):
  """Get changes to config and run scripts for Transformer.

  Also updates run_training.sh on the vm.

  Args:
    benchmark_spec: The benchmark specification.
    vm: The target vm.
    script_path: The location of scripts on vm.
    nccl_log_exports: The exports to enable NCCL logging.
    nvprof_flags: The flags for nvprof.
    cuda_exports: The exports for CUDA, e.g. CUDA_VISIBLE_DEVICES.
    config_sed_input: Input list of sed pairs for config_DGX1_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGX1_multi.sh.
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

  if FLAGS.mlperf_keep_nccl_log:
    run_training_sed += [(r'export MLPERF_HOST_OS',
                          r'export MLPERF_HOST_OS\n{nccl_log_exports}'
                          .format(nccl_log_exports=nccl_log_exports))]
  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_training_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_training_sed += [(r'--max-epoch.*',
                          r'--max-update {profile_steps} \\\\'
                          .format(profile_steps=FLAGS.mlperf_profile_steps))]
  if FLAGS.mlperf_use_optimized_nccl_config:
    nccl_exports = (r'export NCCL_SOCKET_NTHREADS=2\n'
                    r'export NCCL_NSOCKS_PERTHREAD=2\n'
                    r'export NCCL_MIN_NRINGS=2\n'
                    r'export NCCL_MAX_NRINGS=2\n')
  else:
    nccl_exports = r''

  run_training_sed += [(r'export DGXSYSTEM',
                        r'export DGXSYSTEM\n'
                        r'{nccl_exports}'
                        r'{cuda_exports}'
                        .format(nccl_exports=nccl_exports,
                                cuda_exports=cuda_exports))]

  vm.RemoteCommand(
      r'cd {script_path} && '
      r'sed "{run_training_sed}" run_training.sh > run_training1.sh && '
      r'chmod 755 run_training1.sh '
      .format(script_path=script_path,
              run_training_sed=_SedPairsToString(run_training_sed)))

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/translation\/ \&\& sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForSSD(benchmark_spec,
                      nccl_log_exports, nvprof_flags, cuda_exports,
                      config_sed_input, run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for SSD.

  Args:
    benchmark_spec: The benchmark specification.
    nccl_log_exports: The exports to enable NCCL logging.
    nvprof_flags: The flags for nvprof.
    cuda_exports: The exports for CUDA, e.g. CUDA_VISIBLE_DEVICES.
    config_sed_input: Input list of sed pairs for config_DGX1_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGX1_multi.sh.
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

  if FLAGS.mlperf_keep_nccl_log:
    run_and_time_sed += [(r'run training', r'run  training\n{nccl_log_exports}'
                          .format(nccl_log_exports=nccl_log_exports))]
  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_and_time_sed += [(r'--epochs .*', r'--epochs 1 \\\\')]
  if FLAGS.mlperf_use_optimized_nccl_config:
    nccl_exports = (r'export NCCL_SOCKET_NTHREADS=12\n'
                    r'export NCCL_NSOCKS_PERTHREAD=1\n'
                    r'export NCCL_MIN_NRINGS=1\n')
  else:
    nccl_exports = ''
  run_and_time_sed += [(r'run benchmark',
                        r'run  benchmark\n'
                        r'{nccl_exports}'
                        r'{cuda_exports}'
                        .format(nccl_exports=nccl_exports,
                                cuda_exports=cuda_exports))]

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/single_stage_detector\/ \&\& '
               r'sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForGNMT(benchmark_spec,
                       nccl_log_exports, nvprof_flags, cuda_exports,
                       config_sed_input, run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for GNMT.

  Args:
    benchmark_spec: The benchmark specification.
    nccl_log_exports: The exports to enable NCCL logging.
    nvprof_flags: The flags for nvprof.
    cuda_exports: The exports for CUDA, e.g. CUDA_VISIBLE_DEVICES.
    config_sed_input: Input list of sed pairs for config_DGX1_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGX1_multi.sh.
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

  if FLAGS.mlperf_keep_nccl_log:
    run_and_time_sed += [(r'run training', r'run  training\n{nccl_log_exports}'
                          .format(nccl_log_exports=nccl_log_exports))]
  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_and_time_sed += [(r'--epochs .*', r'--epochs \"1\" \\\\')]
  if FLAGS.mlperf_use_optimized_nccl_config:
    nccl_exports = (r'export NCCL_SOCKET_NTHREADS=3\n'
                    r'export NCCL_NSOCKS_PERTHREAD=2\n'
                    r'export NCCL_MIN_NRINGS=2\n')
  else:
    nccl_exports = r''
  run_and_time_sed += [(r'running benchmark\"',
                        r'running  benchmark\"\n'
                        r'{nccl_exports}'
                        r'{cuda_exports}\n'
                        .format(nccl_exports=nccl_exports,
                                cuda_exports=cuda_exports))]

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/rnn_translator\/ \&\& sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForMask(benchmark_spec, vm, script_path,
                       nccl_log_exports, nvprof_flags, cuda_exports,
                       config_sed_input, run_sed_input, run_and_time_sed_input):
  """Get changes to config and run scripts for MaskRCNN.

  Also update train_mlperf.py if nvprof is used.

  Args:
    benchmark_spec: The benchmark specification.
    vm: The target vm.
    script_path: The location of scripts on vm.
    nccl_log_exports: The exports to enable NCCL logging.
    nvprof_flags: The flags for nvprof.
    cuda_exports: The exports for CUDA, e.g. CUDA_VISIBLE_DEVICES.
    config_sed_input: Input list of sed pairs for config_DGX1_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGX1_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input
  per_gpu_train_batch_size = 2 * benchmark_spec.total_num_gpus
  per_gpu_eval_batch_size = benchmark_spec.total_num_gpus

  # pylint: disable=line-too-long
  # The BASE_LR and WARMUP_ITERS are from https://raw.githubusercontent.com/mlperf/training_results_v0.6/master/NVIDIA/results/dgx2_ngc19.05_pytorch/maskrcnn/result_0.txt
  config_sed += [(r'BASE_LR.*', r'BASE_LR\"  \"0.12\"')]
  config_sed += [(r'WARMUP_ITERS.*', r'WARMUP_ITERS\"  \"625\"')]
  # The STEPS numbers are from https://raw.githubusercontent.com/mlperf/training_results_v0.6/master/NVIDIA/results/dgx2h_n12_ngc19.05_pytorch/maskrcnn/result_0.txt
  # pylint: enable=line-too-long
  # Using the step numbers provided by the smaller scale
  # results didn't run or converge. That's why the
  # hyperparameters are mixed.
  config_sed += [(r'STEPS.*', r'STEPS\"  \"(7000, 9333)\"')]
  config_sed += [(r'SOLVER.IMS_PER_BATCH.*',
                  r'SOLVER.IMS_PER_BATCH\"  \"{per_gpu_train_batch_size}\"'
                  .format(per_gpu_train_batch_size=per_gpu_train_batch_size))]
  config_sed += [(r'TEST.IMS_PER_BATCH.*',
                  r'TEST.IMS_PER_BATCH\" \"{per_gpu_eval_batch_size}\"'
                  .format(per_gpu_eval_batch_size=per_gpu_eval_batch_size))]
  config_sed += [(r'TOP_N_TRAIN.*', r'TOP_N_TRAIN\" \"1000\"')]

  if FLAGS.mlperf_keep_nccl_log:
    run_and_time_sed += [(r'run benchmark',
                          r'run  benchmark\n{nccl_log_exports}'
                          .format(nccl_log_exports=nccl_log_exports))]
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
  if FLAGS.mlperf_use_optimized_nccl_config:
    nccl_exports = (r'export NCCL_SOCKET_NTHREADS=3\n'
                    r'export NCCL_NSOCKS_PERTHREAD=2\n'
                    r'export NCCL_MIN_NRINGS=2\n')
  else:
    nccl_exports = r''
  run_and_time_sed += [(r'set -x',
                        r'{nccl_exports}\n'
                        r'{cuda_exports}\n'
                        r'set  -x'
                        .format(nccl_exports=nccl_exports,
                                cuda_exports=cuda_exports))]

  run_sed += [(r'sleep infinity',
               r' bash -c \"\x27 cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/object_detection\/ \&\& sleep  infinity\x27 \"'
               .format(model=benchmark_spec.benchmark))]

  return config_sed, run_sed, run_and_time_sed


def _GetChangesForResnet(benchmark_spec,
                         nccl_log_exports, nvprof_flags, cuda_exports,
                         config_sed_input, run_sed_input,
                         run_and_time_sed_input):
  """Get changes to config and run scripts for Resnet.

  Args:
    benchmark_spec: The benchmark specification.
    nccl_log_exports: The exports to enable NCCL logging.
    nvprof_flags: The flags for nvprof.
    cuda_exports: The exports for CUDA, e.g. CUDA_VISIBLE_DEVICES.
    config_sed_input: Input list of sed pairs for config_DGX1_multi.sh.
    run_sed_input: Input list of sed pairs for run.sub.
    run_and_time_sed_input: Input list of sed pairs for run_and_time.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGX1_multi.sh.
    run_sed_output: Output list of sed pairs for run.sub.
    run_and_time_sed_output: Output list of sed pairs for run_and_time.sh.
  """
  config_sed = config_sed_input
  run_sed = run_sed_input
  run_and_time_sed = run_and_time_sed_input
  per_gpu_batch_size = min(65, 33280 / benchmark_spec.total_num_gpus)

  config_sed += [(r'BATCHSIZE=.*', r'BATCHSIZE=\"{per_gpu_batch_size}\"'
                  .format(per_gpu_batch_size=per_gpu_batch_size))]

  if FLAGS.mlperf_keep_nccl_log:
    run_and_time_sed += [(r'run benchmark',
                          r'run  benchmark\n{nccl_log_exports}'
                          .format(nccl_log_exports=nccl_log_exports))]
  if mlperf_benchmark.NVPROF in FLAGS.mlperf_profiler:
    run_and_time_sed += [(r'python', r'nvprof {nvprof_flags} python'
                          .format(nvprof_flags=nvprof_flags))]
    run_and_time_sed += [(r'num-epochs.*',
                          r'num-epochs  \"1\"\n'
                          r'  --epoch-size  \"{profile_steps}\"'
                          .format(profile_steps=FLAGS.mlperf_profile_steps))]
  if FLAGS.mlperf_use_optimized_nccl_config:
    nccl_exports = (r'export NCCL_SOCKET_NTHREADS=3\n'
                    r'export NCCL_NSOCKS_PERTHREAD=1\n'
                    r'export NCCL_MIN_NRINGS=2\n'
                    r'export NCCL_MAX_NRINGS=2\n')
  else:
    nccl_exports = r''
  run_and_time_sed += [(r'SLURM_NTASKS_PER_NODE=',
                        r'{nccl_exports}'
                        r'{cuda_exports}\n'
                        r'SLURM_NTASKS_PER_NODE='
                        .format(
                            nccl_exports=nccl_exports,
                            cuda_exports=cuda_exports))]

  run_and_time_sed += [(r'BIND=.*', r'BIND=\"\"')]
  run_and_time_sed += [('NUMEPOCHS=.*',
                        f'NUMEPOCHS={mlperf_benchmark.RESNET_EPOCHS.value}')]

  run_sed += [(r'srun --mem=0 -n \$SLURM_JOB_NUM_NODES --ntasks-per-node=1',
               r'mpirun -mca btl_tcp_if_exclude docker0,lo -N 1 '
               r'-n \$SLURM_JOB_NUM_NODES -hostfile \$HOME\/{hostfile}'
               .format(hostfile=HOSTFILE))]
  run_sed += [(r'SRUNl=\"\$SRUN -l\"', r'SRUNl=\"\$SRUN \"')]
  run_sed += [(r'root --bind-to none',
               r'root -mca btl_tcp_if_exclude docker0,lo '
               r'-x CONTNAME=\$CONTNAME --bind-to none')]
  run_sed += [(r'mkdir -p .*',
               r'\$SRUNl -x SLURM_JOB_ID=\$SLURM_JOB_ID bash -c '
               r'\x27 mkdir -p \/dev\/shm\/mpi\/\${SLURM_JOB_ID} \&\& '
               r'chmod 700 \/dev\/shm\/mpi\/\${SLURM_JOB_ID}\x27')]
  run_sed += [(r'cat \/dev\/shm\/mpi.*',
               r'    for hostn in \${hosts[@]}\; do\n'
               r'      scp \/dev\/shm\/mpi\/\${SLURM_JOB_ID}.tgz '
               r'\$hostn:\/dev\/shm\/mpi\;\n'
               r'    done\;\n'
               r'  \$SRUNl -x SLURM_JOB_ID=\$SLURM_JOB_ID tar zxPf '
               r'\/dev\/shm\/mpi\/\${SLURM_JOB_ID}.tgz ')]
  run_sed += [(r'\$SRUNl cp -pr ',
               r'\$SRUNl -x SLURM_JOB_ID=\${SLURM_JOB_ID} cp -pr ')]
  run_sed += [(r'sleep infinity',
               r'cp \/workspace\/{model}1\/*.sh '
               r'\/workspace\/image_classification\/ \&\& sleep  infinity'
               .format(model=benchmark_spec.benchmark))]
  run_sed += [(r'SRUNl docker exec',
               r'SRUNl -x SLURM_JOB_ID -x CONTAINER_UID '
               r'-x CONTAINER_GID -x VARS -x CONTNAME docker exec')]
  run_sed += [(r'\$SLURM_JOB_NUM_NODES -eq 1',
               r'\$SLURM_JOB_NUM_NODES -eq 0')]

  return config_sed, run_sed, run_and_time_sed


def _UpdateScripts(benchmark_spec, vm):
  """Update the running scripts on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
    vm: The target vm.
  """
  benchmark = benchmark_spec.benchmark

  # request pairs to the sed command
  # each pair('str_A', 'str_B') indicates a request "replace anything
  # matching str_A to str_B" for a specific file
  config_sed = []
  config_sed += [(r'DGXIBDEVICES=.*', r'DGXIBDEVICES=\"\"')]
  config_sed += [(r'DGXSYSTEM=.*', r'DGXSYSTEM=\"DGX1_multi\"')]
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
  run_and_time_sed += [(r'DGXSYSTEM=.*', r'DGXSYSTEM=\"DGX1_multi\"')]
  run_and_time_sed += [(r'config_\${DGXSYSTEM}.sh',
                        r'config_\${DGXSYSTEM}1.sh')]

  run_sed = []
  run_sed += [(r'SYSLOGGING=1', r'SYSLOGGING=0')]
  run_sed += [(r'config_DGX1_multi', r'config_DGX1_multi1')]
  run_sed += [(r'config_\${DGXSYSTEM}.sh', r'config_\${DGXSYSTEM}1.sh')]
  run_sed += [(r'run_and_time.sh', r'run_and_time1.sh')]
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

  nccl_log_exports = (
      r'export NCCL_DEBUG=INFO \n'
      r'export NCCL_DEBUG_SUBSYS=ALL \n'
      r'export NCCL_DEBUG_FILE=\"\/results\/%h.%p.nccl\" \n')
  nvprof_flags = r'-f -o \/results\/%h.%p.nvprof --profile-child-processes'
  if (FLAGS.cloud == 'GCP' and benchmark_spec.gpus_per_vm == 8):
    cuda_exports = r'export CUDA_VISIBLE_DEVICES=0,1,3,2,7,6,4,5'
  else:
    cuda_exports = r''

  script_path = (
      r'$HOME/training_results_v0.6/NVIDIA/benchmarks/{model}'
      r'/implementations/{framework}'
      .format(model='maskrcnn' if mlperf_benchmark.MASK in benchmark
              else benchmark,
              framework='mxnet' if mlperf_benchmark.RESNET in benchmark
              else 'pytorch'))

  if mlperf_benchmark.TRANSFORMER in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForTransformer(
        benchmark_spec, vm, script_path,
        nccl_log_exports, nvprof_flags, cuda_exports,
        config_sed, run_sed, run_and_time_sed)

  elif mlperf_benchmark.SSD in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForSSD(
        benchmark_spec,
        nccl_log_exports, nvprof_flags, cuda_exports,
        config_sed, run_sed, run_and_time_sed)

  elif mlperf_benchmark.GNMT in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForGNMT(
        benchmark_spec,
        nccl_log_exports, nvprof_flags, cuda_exports,
        config_sed, run_sed, run_and_time_sed)

  elif mlperf_benchmark.MASK in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForMask(
        benchmark_spec, vm, script_path,
        nccl_log_exports, nvprof_flags, cuda_exports,
        config_sed, run_sed, run_and_time_sed)

  elif mlperf_benchmark.RESNET in benchmark:
    config_sed, run_sed, run_and_time_sed = _GetChangesForResnet(
        benchmark_spec,
        nccl_log_exports, nvprof_flags, cuda_exports,
        config_sed, run_sed, run_and_time_sed)

  vm.RemoteCommand(
      r'cd {script_path} && '
      r'sed "{config_sed}" config_DGX1_multi.sh > config_DGX1_multi1.sh && '
      r'chmod 755 config_DGX1_multi1.sh '
      .format(script_path=script_path,
              config_sed=_SedPairsToString(config_sed)))

  vm.RemoteCommand(
      r'cd {script_path} && '
      r'sed "{run_and_time_sed}" run_and_time.sh > run_and_time1.sh && '
      r'chmod 755 run_and_time1.sh '
      .format(script_path=script_path,
              run_and_time_sed=_SedPairsToString(run_and_time_sed)))

  vm.RemoteCommand(
      r'cd {script_path} && '
      r'sed "{run_sed}" run.sub > run1.sub && '
      r'chmod 755 run1.sub '
      .format(script_path=script_path,
              run_sed=_SedPairsToString(run_sed)))


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


def _ClearTmpDirectory(benchmark_spec, vm):
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
  list_params = [((benchmark_spec, vms[i]), {}) for i in range(len(vms))]
  _PrepareBucket(benchmark_spec)

  vm_util.RunThreaded(_ClearTmpDirectory, list_params)

  vm_util.RunThreaded(_PrepareMLPerf, list_params)

  vm_util.RunThreaded(_UpdateScripts, list_params)

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
  env_params['DGXSYSTEM'] = r'DGX1_multi'
  env_params['NEXP'] = 1
  env_params['LOGDIR'] = posixpath.join(vm_util.VM_TMP_DIR, benchmark)

  script_path = (
      r'$HOME/training_results_v0.6/NVIDIA/benchmarks/{model}'
      r'/implementations/{framework}'
      .format(model='maskrcnn' if mlperf_benchmark.MASK in benchmark
              else benchmark,
              framework='mxnet' if mlperf_benchmark.RESNET in benchmark
              else 'pytorch'))

  benchmark_env_params = {
      mlperf_benchmark.TRANSFORMER: {
          'CONT': r'"mlperf-nvidia:translation"',
          'DATADIR': r'/data/wmt/utf8'},
      mlperf_benchmark.SSD: {
          'CONT': r'"mlperf-nvidia:single_stage_detector"',
          'DATADIR': r'/data'},
      mlperf_benchmark.GNMT: {
          'CONT': r'"mlperf-nvidia:rnn_translator"',
          'DATADIR': r'/data/gnmt'},
      mlperf_benchmark.MASK: {
          'CONT': r'"mlperf-nvidia:object_detection"',
          'DATADIR': r'/data'},
      mlperf_benchmark.RESNET: {
          'CONT': r'"mlperf-nvidia:image_classification"',
          'DATADIR': r'/data/imagenet'}
  }
  env_params.update(benchmark_env_params.get(benchmark, {}))
  if mlperf_benchmark.RESNET in benchmark:
    env_params['SLURM_JOB_NUM_NODES'] = benchmark_spec.num_vms

  env = r''
  if nvidia_driver.CheckNvidiaGpuExists(master_vm):
    env = tensorflow.GetEnvironmentVars(master_vm)

  cmd = (
      r'cd {script_path} && {env} {params} ./run1.sub'
      .format(
          script_path=script_path,
          env=env,
          params=_DictToString(env_params)))
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
