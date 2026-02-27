"""Runs Nvidia DGXC benchmark.

Source: https://github.com/NVIDIA/dgxc-benchmarking/tree/main
"""
import posixpath
import time
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import nvidia_driver
from perfkitbenchmarker.linux_packages import slurm
BENCHMARK_NAME = 'dgxc'
BENCHMARK_CONFIG = """
dgxc:
  description: Runs nvidia dgxc benchmark.
  cluster:
    headnode:
      vm_spec:
        GCP:
          machine_type: c3-standard-88
          zone: us-central1-a
          boot_disk_size: 2000
        AWS:
          machine_type: m7i.16xlarge
          zone: us-east-2b
          boot_disk_size: 2000
      vm_count: 1
    workers:
      vm_spec:
        GCP:
          machine_type: a4-highgpu-8g
          gpu_count: 8
          gpu_type: b200
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
    preprovision_ignore_checksum: True
"""
BENCHMARK_DATA = {
    'a4.sqsh':
        '',
    'p6.sqsh':
        ''
}
FLAGS = flags.FLAGS
""" For b200
- finetune_llama4-maverick
- pretrain_deepseek-v3
- pretrain_grok1
- pretrain_llama3.1
- pretrain_llama4-maverick
- pretrain_nemotron-h
- pretrain_nemotron4
"""
_WORKLOADS = flags.DEFINE_list(
    'dgxc_workloads', ['pretrain_nemotron4:15b:fp8'],
    'DGXC workloads to install and run. '
    'e.g. workload_name:size:precision')
_HF_TOKEN = flags.DEFINE_string(
    'hf_token', 'hf_fake', 'HuggingFace Token for DGXC workloads.')


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)
  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and setup Nvidia HPL benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vm = benchmark_spec.cluster.headnode_vm
  benchmark_spec.cluster.InstallSquashImage(
      BENCHMARK_NAME,
      f'{benchmark_spec.cluster.TYPE}.sqsh',
      benchmark_spec.cluster.nfs_path,
      posixpath.join(BENCHMARK_NAME,
                     f'{benchmark_spec.cluster.TYPE}.dockerfile')
  )
  vm.RemoteCommand(
      f'cd {benchmark_spec.cluster.nfs_path}; mkdir -p images; '
      f'mv {benchmark_spec.cluster.TYPE}.sqsh '
      'images/nvidia+nemo+25.07.01.sqsh')
  vm.RemoteCommand(
      'git clone --branch v25.08.01 '
      'https://github.com/NVIDIA/dgxc-benchmarking.git')
  vm.InstallPackages('git-lfs')
  workload_list = list(set(w.split(':')[0] for w in _WORKLOADS.value))
  vm.RenderTemplate(
      data.ResourcePath('dgxc/dgxc.conf.j2'),
      'pkb-dgxc.conf',
      {
          'workloads': workload_list,
          'hf_token': _HF_TOKEN.value,
      })
  vm.RemoteCommand(
      'cd dgxc-benchmarking; echo "y\\n" | ./install.sh --play ~/pkb-dgxc.conf',
      login_shell=True)


def Run(benchmark_spec):
  """Runs Nvidia DGXC benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  headnode = benchmark_spec.cluster.headnode_vm
  ngpus = nvidia_driver.QueryNumberOfGpus(benchmark_spec.cluster.worker_vms[0])
  world_size = ngpus * len(benchmark_spec.cluster.worker_vms)
  for w in _WORKLOADS.value:
    workload_name, size, precision = w.split(':')
    log_filter = w.replace(':', '_')
    headnode.RemoteCommand(
        f'cd /opt/pkb/; export NCCL_DEBUG=INFO; '
        f'./llmb-run single -w {workload_name} -s {size} --dtype {precision} '
        f'--scale {world_size}', login_shell=True
    )
    while slurm.Running(headnode):
      time.sleep(60)
    result_file = headnode.RemoteCommand(
        f'find /opt/pkb/workloads/{workload_name}/experiments/{log_filter}'
        '*/*/*/log*out | tail -n 1'
    )[0][:-1]
    headnode.PullFile(vm_util.GetTempDir(), result_file)
    lines, _ = headnode.RemoteCommand(
        f'cat {result_file} | grep "Training epoch"'
    )
    for l in lines.splitlines():
      metadata = regex_util.ExtractAllFloatMetrics(
          l,
          r'global_step|train_step_timing in'
          r' s|TFLOPS_per_GPU|reduced_train_loss',
          delimiter_regex=r': ',
      )
      metadata['workload'] = w
      samples.append(
          sample.Sample(
              'train_step_time',
              metadata['train_step_timing in s'],
              'seconds',
              metadata,
          )
      )
  print(samples)
  return []


def Cleanup(_):
  """Cleanup Nvidia HPL."""
  pass
