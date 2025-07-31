# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs the vbench transcoding benchmark.

Paper: https://dl.acm.org/doi/pdf/10.1145/3296957.3173207
Vbench suite download link: http://arcade.cs.columbia.edu/vbench/
"""

import itertools
import logging
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample

ENCODER_LIBX264 = 'libx264'
ENCODER_LIBX265 = 'libx265'
ENCODER_VP9 = 'libvpx-vp9'
ENCODER_AV1 = 'libaom-av1'
ENCODER_H264_NVENC = 'h264_nvenc'
ENCODER_HEVC_NVENC = 'hevc_nvenc'
DEFAULT_VP9_THREADS_LIST = [1]

_VALID_ENCODERS = [
    ENCODER_LIBX264,
    ENCODER_LIBX265,
    ENCODER_VP9,
    ENCODER_AV1,
    ENCODER_H264_NVENC,
    ENCODER_HEVC_NVENC,
]
_FFMPEG_ENCODERS = flags.DEFINE_multi_enum(
    'ffmpeg_encoders',
    [ENCODER_LIBX264, ENCODER_LIBX265, ENCODER_AV1],
    _VALID_ENCODERS,
    'List of the encoders to use for the transcoding benchmark. '
    'Default is libx264.',
)
_FFMPEG_THREADS_LIST = flag_util.DEFINE_integerlist(
    'ffmpeg_threads_list',
    None,
    'List of threads to give to each ffmpeg job. Defaults to '
    '[4, 8] for h.264 and [1] for vp9.',
)
_FFMPEG_PARALLELISM_LIST = flag_util.DEFINE_integerlist(
    'ffmpeg_parallelism_list',
    None,
    'List of ffmpeg-jobs to run in parallel. Defaults to '
    '[number of logical CPUs].',
)
_FFMPEG_PRESET = flags.DEFINE_string(
    'ffmpeg_preset',
    'faster',
    'Preset option that provides a certain encoding speed to compression ratio.'
    ' A slower preset will provide better compression. Defaults to medium.',
)
_FFMPEG_CRF = flags.DEFINE_integer(
    'ffmpeg_crf',
    23,
    'Constant Rate Factor (CRF) controls output quality. The range of the CRF'
    ' scale is 0–51, where 0 is lossless, 23 is the default, and 51 is worst'
    ' quality possible. Consider 17 or 18 to be visually lossless or'
    ' nearly so.',
)
_FFMPEG_MAX_RATE = flags.DEFINE_integer(
    'ffmpeg_max_rate',
    8,
    'Maximum bitrate (in Mbps) used for encoding. Useful for online streaming.'
    ' Default value is 2.',
)
_FFMPEG_BUF_SIZE_MULTIPLIER = flags.DEFINE_integer(
    'ffmpeg_buf_size_multiplier',
    16,
    'Multiplier of ffmpeg_max_rate to get the rate control buffer which checks'
    ' to make sure the average bitrate is on target. Default value is 2.',
)
_FFMPEG_COPIES = flags.DEFINE_integer(
    'ffmpeg_copies',
    9,
    'Number of duplicate copies to create for each source video before'
    ' transcoding.',
)

_FFMPEG_DIR = flags.DEFINE_string(
    'ffmpeg_dir', '', 'Directory (ending in "/") where ffmpeg and ffprobe are '
    'located.'
)
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'vbench_transcoding'
BENCHMARK_CONFIG = """
vbench_transcoding:
  description: Runs a video transcoding benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: c4-standard-8
          boot_disk_size: 200
        AWS:
          machine_type: m7i.2xlarge
          boot_disk_size: 200
        Azure:
          machine_type: Standard_D8s_v6
          boot_disk_size: 200
      disk_spec:
        # Standardize with 500 MB/s bandwidth.
        # The largest video file is ~300 MB; we want to minimize I/O impact.
        GCP:
          disk_size: 500
          disk_type: hyperdisk-balanced
          provisioned_iops: 3000
          provisioned_throughput: 500
          mount_point: /scratch
        AWS:
          disk_size: 500
          disk_type: gp3
          provisioned_iops: 3000
          provisioned_throughput: 500
          mount_point: /scratch
        Azure:
          disk_size: 500
          disk_type: PremiumV2_LRS
          provisioned_iops: 3000
          provisioned_throughput: 500
          mount_point: /scratch
"""


BENCHMARK_DATA = {
    # Download from http://arcade.cs.columbia.edu/vbench/
    'vbench.zip': (
        'c34b873a18b151322483ca460fcf9ed6a5dbbc2bb74934c57927b88ee1de3472'
    )
}


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install FFmpeg and download sample videos on the VM.

  Args:
    spec: The benchmark specification. Contains all data that is required to run
      the benchmark.
  """
  vm = spec.vm_groups['default'][0]
  home_dir = vm.RemoteCommand('echo $HOME')[0].strip()
  vm.DownloadPreprovisionedData(home_dir, 'vbench', 'vbench.zip')
  vm.InstallPackages('unzip')
  vm.RemoteCommand('unzip -o vbench.zip')
  vm.RemoteCommand('cp -r ~/vbench /scratch')
  vm.Install('ffmpeg')
  vm.InstallPackages('parallel')
  vm.InstallPackages('time')


def Run(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  return RunParallel(spec)


def RunParallel(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs multiple Ffmpeg processes concurrently using GNU parallel.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of samples including total transcode time, aggregate bitrate, and
    average PSNR.
  """
  vm = spec.vm_groups['default'][0]
  samples = []
  input_videos_dir = '/scratch/vbench/videos/crf0'

  for encoder in _FFMPEG_ENCODERS.value:
    cuda_args = ''
    jobs_list = (
        _FFMPEG_PARALLELISM_LIST.value
        if _FFMPEG_PARALLELISM_LIST
        else [vm.NumCpusForBenchmark(report_only_physical_cpus=True)]
    )
    if encoder in [ENCODER_LIBX264, ENCODER_LIBX265]:
      ffmpeg_args = (
          f'-c:v {encoder} -preset {_FFMPEG_PRESET.value} -crf'
          f' {_FFMPEG_CRF.value} -maxrate {_FFMPEG_MAX_RATE.value}M -bufsize'
          f' {_FFMPEG_BUF_SIZE_MULTIPLIER.value * _FFMPEG_MAX_RATE.value}M'
      )
      threads_list = (
          _FFMPEG_THREADS_LIST.value
          if _FFMPEG_THREADS_LIST
          else [vm.NumCpusForBenchmark(report_only_physical_cpus=True)]
      )
    elif encoder == ENCODER_VP9:
      # A single VP9 ffmpeg thread almost saturates a CPU core. Increasing the
      # parallelism is counterproductive on all machines benchmarked so far.
      ffmpeg_args = (
          f'-c:v {encoder} -crf {_FFMPEG_CRF.value} -b:v 0 -quality good'
      )
      threads_list = (
          _FFMPEG_THREADS_LIST.value
          if _FFMPEG_THREADS_LIST
          else DEFAULT_VP9_THREADS_LIST
      )
    elif encoder in [ENCODER_H264_NVENC, ENCODER_HEVC_NVENC]:
      jobs_list = (
          _FFMPEG_PARALLELISM_LIST.value if _FFMPEG_PARALLELISM_LIST else [8]
      )
      cuda_args = '-hwaccel cuda -hwaccel_output_format cuda'
      ffmpeg_args = (
          f'-c:v {encoder} -preset medium -rc:v vbr -cq:v'
          f' {_FFMPEG_CRF.value} -qmin {_FFMPEG_CRF.value} -qmax'
          f' {_FFMPEG_CRF.value}'
      )
      threads_list = (
          _FFMPEG_THREADS_LIST.value if _FFMPEG_THREADS_LIST else [4, 8]
      )

    for jobs, threads in itertools.product(jobs_list, threads_list):
      if _FFMPEG_COPIES.value > 0:
        duplication_cmd = (
            "for f in $(ls *.mkv | grep -v '_copy'); do "
            '  filename="${f%.*}";'
            '  extension="${f##*.}";'
            f" for i in $(seq 1 {_FFMPEG_COPIES.value}); do"
            '    cp "$f" "${filename}_copy${i}.${extension}";'
            '  done;'
            'done'
        )
        vm.RemoteCommand(f'cd {input_videos_dir} && {duplication_cmd}')

      jobs_arg = f'-j{jobs}' if jobs else ''
      threads_arg = f'-threads {threads} '
      # Sort video largest to smallest to mitigate load imbalance.
      parallel_cmd = (
          f'ls -S *.mkv | parallel {jobs_arg} \'{_FFMPEG_DIR.value}ffmpeg '
          f'{threads_arg} -y {cuda_args} -i {{}} {ffmpeg_args} '
          '{.}.out.mkv </dev/null >&/dev/null\''
      )

      time_file = '~/parallel.time'
      run_cmd = (
          f'cd {input_videos_dir} && PATH="$HOME/bin:$PATH" /usr/bin/time -f'
          f' "%e" -o {time_file} bash -c "{parallel_cmd}"'
      )
      vm.RemoteCommand(run_cmd)
      total_runtime, _ = vm.RemoteCommand(
          f"awk '{{sum+=$1;}} END {{print sum}}' {time_file}"
      )
      logging.info(
          'Total runtime with %s jobs and %s threads: %s',
          jobs,
          threads,
          total_runtime,
      )

      list_files_cmd = (
          f'cd {input_videos_dir} && ls -1 -- *.out.mkv 2>/dev/null'
      )
      num_files_str, _ = vm.RemoteCommand(f'{list_files_cmd} | wc -l')
      num_files = int(num_files_str.strip())
      if num_files == 0:
        raise ValueError('Number of files is zero.')

      vm.RemoteCommand(f'cd {input_videos_dir} && rm -rf *.out.mkv')

      metadata = {
          'test': 'upload',
          'encoder': encoder,
          'num_files': num_files,
          'parallelism': jobs,
          'threads': threads,
          'ffmpeg_compiled_from_source': FLAGS.build_ffmpeg_from_source,
          'video_copies': _FFMPEG_COPIES.value,
          'ffmpeg_preset': _FFMPEG_PRESET.value,
          'ffmpeg_crf': _FFMPEG_CRF.value,
          'ffmpeg_max_rate': _FFMPEG_MAX_RATE.value,
      }
      samples.append(
          sample.Sample(
              'Total Transcode Time', total_runtime, 'seconds', metadata
          ))
  return samples


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  del spec  # Unused
