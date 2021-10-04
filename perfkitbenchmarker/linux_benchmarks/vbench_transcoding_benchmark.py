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

"""Runs the vbench transcoding benchmark with h.264 and vp9.

Paper: http://www.cs.columbia.edu/~lottarini/test/data/vbench.pdf
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

CODEC_H264 = 'h264'
CODEC_VP9 = 'vp9'
DEFAULT_H264_THREADS_LIST = [4, 8]
DEFAULT_VP9_THREADS_LIST = [1]

_FFMPEG_CODECS = flags.DEFINE_list(
    'ffmpeg_codecs', [CODEC_H264],
    'List of the codecs to use for the transcoding benchmark. '
    'For now, this is some combination of h264 and vp9.')
_FFMPEG_THREADS_LIST = flag_util.DEFINE_integerlist(
    'ffmpeg_threads_list', None,
    'List of threads to give to each ffmpeg job. Defaults to '
    '[4, 8] for h.264 and [1] for vp9.')
_FFMPEG_PARALLELISM_LIST = flag_util.DEFINE_integerlist(
    'ffmpeg_parallelism_list', None,
    'List of ffmpeg-jobs to run in parallel. Defaults to '
    '[number of logical CPUs].')
_FFMPEG_DIR = flags.DEFINE_string(
    'ffmpeg_dir', '/usr/bin', 'Directory where ffmpeg and ffprobe are located.')

_VALID_CODECS = [CODEC_H264, CODEC_VP9]
flags.register_validator(
    'ffmpeg_codecs', lambda codecs: all([c in _VALID_CODECS for c in codecs]))

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'vbench_transcoding'
BENCHMARK_CONFIG = """
vbench_transcoding:
  description: Runs a video transcoding benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n2d-highcpu-8
          zone: us-central1-f
        AWS:
          machine_type: c6g.2xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_F8s
          zone: westus2
      disk_spec:
        # Standardize with 250 MB/s bandwidth.
        # The largest video file is ~300 MB; we want to minimize I/O impact.
        GCP:
          disk_size: 521
          disk_type: pd-ssd
          mount_point: /scratch
        AWS:
          disk_size: 521
          disk_type: gp2
          mount_point: /scratch
        Azure:
          disk_size: 2048
          disk_type: Premium_LRS
          mount_point: /scratch
      os_type: ubuntu2004
"""


BENCHMARK_DATA = {
    # Download from http://arcade.cs.columbia.edu/vbench/
    'vbench.zip':
        'c34b873a18b151322483ca460fcf9ed6a5dbbc2bb74934c57927b88ee1de3472'
}


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install FFmpeg and download sample videos on the VM.

  Args:
    spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vm = spec.vms[0]
  home_dir = vm.RemoteCommand('echo $HOME')[0].strip()
  # vm.InstallPreprovisionedBenchmarkData('vbench', ['vbench.zip'], home_dir)
  vm.DownloadPreprovisionedData(home_dir, 'vbench', 'vbench.zip')
  vm.InstallPackages('unzip')
  vm.RemoteCommand('unzip -o vbench.zip')
  vm.RemoteCommand('cp -r ~/vbench /scratch')
  vm.Install('ffmpeg')
  vm.InstallPackages('parallel')


def Run(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  return RunParallel(spec)


def RunParallel(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs multiple Ffmpeg processes concurrently using GNU parallel.

  Args:
    spec: The benchmark specification.

  Returns:
    samples: A list of samples including total time to transcode.
  """
  vm = spec.vms[0]
  samples = []
  input_videos_dir = '/scratch/vbench/videos/crf0'

  for codec in _FFMPEG_CODECS.value:
    jobs_list = (
        _FFMPEG_PARALLELISM_LIST.value
        if _FFMPEG_PARALLELISM_LIST else [vm.NumCpusForBenchmark()])
    if codec == CODEC_H264:
      ffmpeg_args = '-c:v libx264 -preset medium -crf 18'
      threads_list = (
          _FFMPEG_THREADS_LIST.value
          if _FFMPEG_THREADS_LIST else DEFAULT_H264_THREADS_LIST)
    elif codec == CODEC_VP9:
      # A single VP9 ffmpeg thread almost saturates a CPU core. Increasing the
      # parallelism is counterproductive on all machines benchmarked so far.
      ffmpeg_args = '-c:v libvpx-vp9 -crf 10 -b:v 0 -quality good'
      threads_list = (
          _FFMPEG_THREADS_LIST.value
          if _FFMPEG_THREADS_LIST else DEFAULT_VP9_THREADS_LIST)

    for jobs, threads in itertools.product(jobs_list, threads_list):
      jobs_arg = f'-j{jobs}'
      threads_arg = f'-threads {threads} '
      parallel_cmd = (f'parallel {jobs_arg} {_FFMPEG_DIR.value}/ffmpeg '
                      f'{threads_arg} -y -i {{}} {ffmpeg_args} '
                      f'{{.}}.out.mkv </dev/null >&/dev/null ::: *.mkv')

      time_file = '~/parallel.time'
      run_cmd = (
          f'cd {input_videos_dir} && /usr/bin/time -f "%e" -o {time_file} '
          f'{parallel_cmd}')
      vm.RemoteCommand(run_cmd)
      total_runtime, _ = vm.RemoteCommand(
          ('awk \'{sum+=$1;} END {print sum}\' '
           f'{time_file}'))
      logging.info('Total runtime with %s jobs and %s threads: %s', jobs,
                   threads, total_runtime)
      vm.RemoteCommand(f'cd {input_videos_dir} && rm -rf *.out.mkv')

      samples.extend([
          sample.Sample(
              'Total Transcode Time',
              total_runtime,
              'seconds',
              metadata={
                  'test': 'upload',
                  'codec': codec,
                  'num_files': 15,  # TODO(spencerkim): Count *.out* files.
                  'parallelism': jobs,
                  'threads': threads,
                  'ffmpeg_compiled_from_source':
                      FLAGS.build_ffmpeg_from_source,
                  'video_copies': 1,
              })
      ])
  return samples


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  del spec  # Unused
