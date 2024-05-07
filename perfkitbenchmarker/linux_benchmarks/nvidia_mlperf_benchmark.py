# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run MLPerf-Nvidia benchmarks.

Source:
https://github.com/mlcommons/training_results_v3.1/tree/main/NVIDIA/benchmarks
"""

import json
import logging
import os
import time
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import mlperf_benchmark as mlperf
from perfkitbenchmarker.linux_packages import nvidia_driver
from perfkitbenchmarker.linux_packages import slurm


FLAGS = flags.FLAGS


BENCHMARK_NAME = 'nvidia_mlperf'
BENCHMARK_CONFIG = """
nvidia_mlperf:
  description: Runs Mlperf-nvidia benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: a3-highgpu-8g
          gpu_count: 8
          gpu_type: h100
          zone: us-east1-d
          boot_disk_size: 1000
        AWS:
          machine_type: p5.48xlarge
          zone: us-east-1
          boot_disk_size: 1000
      disk_spec: *default_500_gb
      vm_count: null
  flags:
    placement_group_style: closest_supported
    scratch_dir: /mnt/localssd
    data_disk_type: local
    preprovision_ignore_checksum: True
"""
SLURM_BATCH_REGEX = r'Submitted batch job (%d*)'
SUPPORTED_BENCHMARKS = (mlperf.GPT3,)
BENCHMARK_NAME = 'nvidia_mlperf'
BENCHMARK_DATA = {
    'sentencepiece.model': (
        'c7322204df14896c3bfecf35ddaf3e55a81944ea78b47b5f64427273330c0219'
    ),
}

STEPS = flags.DEFINE_integer(
    'mlperf_max_steps', 20, 'Number of steps for training.'
)
FUSE_BUCKET = flags.DEFINE_string(
    'mlperf_fuse_path',
    '',
    'Object storage path that contains data. e.g. gs://abc/def',
)
_MLPERF_ENV = flags.DEFINE_string(
    'mlperf_env',
    '',
    'Environment variables to use during training, can be used be used for '
    'overriding default params. e.g. '
    '--mlperf_env="MAX_STEPS=20;RUN_ONLY_NCCL=1"',
)
_MLPERF_ITERATIVE_ENV = flags.DEFINE_list(
    'mlperf_iterative_env',
    [],
    'Environment variables to use during training. Used for parameter sweep. '
    'e.g. --mlperf_iterative_env="NCCL_MAX_STEPS=4;NCCL_MIN_STEPS=4","NCCL_MAX_STEPS=12;NCCL_MIN_STEPS=12"',
)
_MLPERF_METRICS = {
    'throughput': 'samples/sec',
    'train_step_timing': 'sec',
}


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GetVer():
  return 'training_results_v3.1'


def _GetDir():
  return f'{_GetVer()}/NVIDIA/benchmarks/gpt3/implementations/pytorch'


def _UpdateRunScript(vm):
  """Update run.sub script and build the container."""
  # TODO(yuyanting) Write a patch file instead and apply here.
  # remove last 5 lines related to hang_monitor
  vm.RemoteCommand(
      f'cd {_GetDir()}; '
      'head -n -5 run.sub > run.sub.temp; mv run.sub.temp run.sub'
  )
  # Use constant container name to skip image rebuild
  vm.RemoteCommand(
      'sed -i \'s|readonly _cont_name="${MODEL_NAME}_${SLURM_JOB_ID}"|'
      'readonly _cont_name="${MODEL_NAME}"|g\' '
      + os.path.join(_GetDir(), 'run.sub')
  )
  # Do not delete container afterwards, just print out
  vm.RemoteCommand(
      'sed -i "s|enroot remove -f|echo|g" ' + os.path.join(_GetDir(), 'run.sub')
  )
  # Build in run stage once.
  vm.RemoteCommand(
      'sed -i \'s|srun --ntasks="$((SLURM_JOB_NUM_NODES))" '
      '--container-image="${CONT_FILE}" '
      '--container-name="${_cont_name}" true||g\' '
      + os.path.join(_GetDir(), 'run.sub')
  )


def _PrepareNvidiaMlperf(vm):
  """Install packages and configure VM."""
  vm.Install('nvidia_hpc')
  nvidia_driver.EnablePersistenceMode(vm)
  vm.RemoteCommand('sudo mount -o remount,size=75% /run')
  vm.RemoteCommand(f'git clone https://github.com/mlcommons/{_GetVer()}.git')
  vm.UpdateDockerfile(os.path.join(_GetDir(), 'Dockerfile'))
  _UpdateRunScript(vm)
  # GPT3 specific setup
  vm.RobustRemoteCommand(
      f'cd {_GetDir()}; docker build -t mlperf-nvidia:gpt3 .'
  )


def _PrepareData(vm):
  """Download training dataset."""
  bucket = FUSE_BUCKET.value.split('//')[-1].split('/', 1)[0]
  path = FUSE_BUCKET.value.split('//')[-1].split('/', 1)[-1]
  provider = FUSE_BUCKET.value.split('://')[0]
  # Mount object storage
  vm.RemoteCommand('sudo mkdir -p /data/ && sudo chmod a+w /data')
  vm.RemoteCommand('sudo umount /data', ignore_failure=True)
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, ['sentencepiece.model'], vm.GetScratchDir()
  )
  if provider == 'gs':
    vm.RemoteCommand(
        'sudo mount -t gcsfuse -o '
        'allow_other,dir_mode=755,file_mode=755,implicit_dirs '
        f'{bucket} /data'
    )
  elif provider == 's3':
    vm.RemoteCommand(f'mount-s3 {bucket} /data')
  else:
    raise ValueError(f'Unsupported provider: {provider}')
  local_path = os.path.join(vm.GetScratchDir(), path)
  vm.RemoteCommand(f'mkdir -p {local_path}')
  vm.RemoteCommand(
      f"parallel-cp -a {os.path.join('/data', path, '*')} {local_path}"
  )
  vm.RemoteCommand(f'mkdir {vm.GetScratchDir()}/checkpoint')
  vm.RemoteCommand(f'mkdir {vm.GetScratchDir()}/output')
  vm.RemoteCommand('mkdir /tmp/npy')
  vm.RemoteCommand('mkdir /tmp/numba')
  vm.RemoteCommand('mkdir /tmp/mplconfigdir')


def Prepare(benchmark_spec):
  """Install and setup Nvidia Mlperf benchmark.

  Args:
    benchmark_spec: The benchmark spec.
  """
  # The flags are also used in previous mlperf benchmark
  if FLAGS.mlperf_benchmark not in SUPPORTED_BENCHMARKS:
    raise ValueError('Unsupported mlperf benchmark')
  # Update when 4.0 released
  if FLAGS.mlperf_training_version != 'v3.1':
    raise ValueError(
        f'Unsupported mlperf training version: {FLAGS.mlperf_training_version}'
    )
  if not FUSE_BUCKET.value:
    raise ValueError('mlperf_fuse_path must be specified')
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(_PrepareData, vms)
  background_tasks.RunThreaded(_PrepareNvidiaMlperf, vms)
  slurm.ConfigureSlurm(vms)


def _GetMetadata(params):
  """Get metadata."""
  metadata = {
      'MAX_STEPS': STEPS.value,
      'NUM_LAYERS': 24,
      'HIDDEN_SIZE': 4096,
      'NUM_ATTENTION_HEADS': 32,
      'LOAD_CHECKPOINT': False,
      'TENSOR_MODEL_PARALLEL': 2,
      'PIPELINE_MODEL_PARALLEL': 1,
      'MICRO_BATCH_SIZE': 4,
      'MINIBS': 256,
  }
  for env in _MLPERF_ENV.value.split(';') + params.split(';'):
    if not env:
      continue
    k, v = env.split('=', 1)
    metadata[k] = int(v) if v.isnumeric() else v
  return metadata


def _Run(benchmark_spec, additional_params=''):
  """Run mlperf training with additional parameters to override defaults."""
  controller = slurm.GetController(benchmark_spec.vms)
  gpus_per_node = nvidia_driver.QueryNumberOfGpus(controller)
  num_vms = len(benchmark_spec.vms)
  stdout, _ = controller.RemoteCommand(
      f'cd {_GetDir()}; rm slurm*out; '
      'source config_common.sh; source config_fp8.sh; '
      # Debugging Only
      # 'RUN_ONLY_NCCL=1 '
      'NCCL_DEBUG_SUBSYS=INIT,ENV '
      'NCCL_DEBUG=INFO '
      # PKB params
      'DGXSYSTEM=pkb '
      'NEXP=1  '
      'SEED=1 '
      'SLURM_MPI_TYPE=pmi2 '
      'NCCL_LLM_TEST=0 '
      'HANG_MONITOR_TIMEOUT=0 '
      f'DGXNGPU={gpus_per_node} '
      f'DGXNNODES={num_vms} '
      f'WORLD_SIZE={gpus_per_node * num_vms} '
      'TP_COMM_OVERLAP=True '
      'CONT="dockerd://mlperf-nvidia:gpt3" '
      f'LOGDIR={controller.GetScratchDir()}/output '
      f'PREPROC_DATA={controller.GetScratchDir()}/mlperf-llm-public2/c4/preprocessed_c4_spm/ '
      f'SPM={controller.GetScratchDir()}/sentencepiece.model  '
      'NUMBA_CACHE_DIR=/tmp/numba '
      'NPY_INDEX_DIR=/tmp/npy '
      'MPLCONFIGDIR=/tmp/mplconfigdir '
      'TRANSFORMERS_CACHE=/tmp/transformers_cache '
      # Checkpoint flags, set to empty folder
      # Since we are not running original 175B model, not using checkpoints.
      'INIT_GLOBAL_STEP=1 '
      'LOAD_CHECKPOINT= '
      f'LOAD_CHECKPOINTS_PATH={controller.GetScratchDir()}/checkpoint/ '
      # Tuning params: for 5B (2x8 GPUs)
      'TENSOR_MODEL_PARALLEL=2 '
      'PIPELINE_MODEL_PARALLEL=1 '
      'MICRO_BATCH_SIZE=4 '
      'MINIBS=256 '  # Should be dynamic
      f'MAX_STEPS={STEPS.value} '
      # Default Model parameters: 5B
      'NUM_LAYERS=24 '
      'HIDDEN_SIZE=4096 '
      'NUM_ATTENTION_HEADS=32 '
      # Other params
      'INTERLEAVED_PIPELINE=null '
      'SEQ_PARALLEL=False '
      'BUCKET_CAP_MB=200 '
      f'VAL_CHECK_INTERVAL={STEPS.value} '
      'LIMIT_VAL_BATCHES=0.0 '
      f'LIMIT_TRAIN_BATCHES={STEPS.value} '
      'CHECK_COMPLIANCE=0 '
      # TODO(yuyanting) Set timeout based on steps, model parameters.
      # Difficult to estimate how long does it take at runtime, set to 60 mins
      # for now.
      f'{_MLPERF_ENV.value.replace(";", " ")} '
      f'{additional_params.replace(";", " ")} '
      f'sbatch -N {num_vms} -t 60 run.sub'
  )
  job_id = regex_util.ExtractInt(r'Submitted batch job (\d+)', stdout)
  output_file = f'{_GetDir()}/slurm-{job_id}.out'
  results = []
  while True:
    # Check status and backup output every minute.
    time.sleep(60)
    controller.PullFile(vm_util.GetTempDir(), output_file)
    vm_util.IssueCommand([
        'mv',
        os.path.join(vm_util.GetTempDir(), f'slurm-{job_id}.out'),
        os.path.join(vm_util.GetTempDir(), f'slurm-{job_id}.log'),
    ])
    if not slurm.Running(controller):
      break
  metadata = {
      'gpus_per_node': gpus_per_node,
      'num_nodes': num_vms,
      'total_gpus': gpus_per_node * num_vms,
  }
  metadata.update(_GetMetadata(additional_params))
  for metric in _MLPERF_METRICS:
    try:
      lines, _ = controller.RemoteCommand(
          f'cat {output_file} | grep MLLOG | grep {metric}'
      )
      values = [
          float(json.loads(line.split('MLLOG')[-1])['value'][metric])
          for line in lines.strip().splitlines()
      ]
      results.append(
          sample.Sample(
              metric,
              sum(values) / len(values),
              _MLPERF_METRICS[metric],
              metadata,
          )
      )
    except errors.VirtualMachine.RemoteCommandError:
      logging.error(
          'Failed to parse %s, find slurm-%s.log for more info.', metric, job_id
      )
    logging.info(results)
    # Some expected to fail during parameter sweep, certain configurations
    # do not fit in GPU memory.
  return results


def Run(benchmark_spec):
  """Runs nvidia mlperf training benchmark."""
  results = []
  controller = slurm.GetController(benchmark_spec.vms)
  num_vms = len(benchmark_spec.vms)
  model_name, _ = controller.RemoteCommand(
      f'cd {_GetDir()}; cat run.sub  | grep "export MODEL_NAME" | '
      "awk -F\\\" '{print $2}'"
  )
  controller.RemoteCommand(
      f'cd {_GetDir()}; srun -N {num_vms} '
      '--container-image="dockerd://mlperf-nvidia:gpt3" '
      f'--container-name="{model_name.strip()}" true'
  )

  params_list = _MLPERF_ITERATIVE_ENV.value or ['']  # use default
  for params in params_list:
    logging.info('Parameters: %s %s', _MLPERF_ENV.value, params)
    results.extend(_Run(benchmark_spec, additional_params=params))
  return results


def Cleanup(benchmark_spec):
  del benchmark_spec
