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
"""Tests for nccl_benchmark."""

import os
import unittest
import mock
from perfkitbenchmarker import cluster
from perfkitbenchmarker import errors
from perfkitbenchmarker import test_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import nccl_benchmark
from perfkitbenchmarker.linux_benchmarks import nccl_cluster_benchmark
from perfkitbenchmarker.sample import Sample


class NcclBenchmarkTest(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super().setUp()
    nccl_benchmark.FLAGS.mark_as_parsed()
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'nccl_output.txt'
    )
    with open(path) as fp:
      self.contents = fp.read()

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testNcclResults(self):
    samples, bandwidth = nccl_benchmark.MakeSamplesFromOutput({}, self.contents)
    metadata = {
        'size': '1048576',
        'count': '262144',
        'nccl_type': 'float',
        'out_of_place_time': '2410.0',
        'out_of_place_algbw': '0.44',
        'out_of_place_busbw': '0.44',
        'out_of_place_error': 'N/A',
        'in_place_time': '2709.7',
        'in_place_algbw': '0.39',
        'in_place_busbw': '0.39',
        'in_place_error': 'N/A',
        'nThread': '1',
        'nGpus': '1',
        'minBytes': '1048576',
        'maxBytes': '1073741824',
        'step': '2(factor)',
        'warmup_iters': '5',
        'iters': '1',
        'agg_iters': '1',
        'validation': '0',
        'graph': '0',
        'Rank  0': 'Pid   9192 on pkb-2b6a393d-2 device  0 [0x00] Tesla K80',
        'Rank  1': 'Pid   9178 on pkb-2b6a393d-3 device  0 [0x00] Tesla K80',
    }
    golden = Sample(
        metric='In place algorithm bandwidth',
        value=0.39,
        unit='GB/s',
        metadata=metadata,
    )
    self.assertIn(golden, samples)
    self.assertAlmostEqual(bandwidth, 0.63)

  def testNcclClusterCheckPrerequisites(self):
    class _GpuCluster(cluster.SlurmCluster):
      gpus_per_node = 8

    benchmark_config = mock.MagicMock()
    benchmark_config.cluster.cloud = 'GCP'
    with mock.patch.object(
        cluster, 'GetClusterClass', return_value=_GpuCluster
    ):
      nccl_cluster_benchmark.CheckPrerequisites(benchmark_config)
    with mock.patch.object(
        cluster, 'GetClusterClass', return_value=cluster.SlurmCluster
    ):
      with self.assertRaises(errors.Config.InvalidValue):
        nccl_cluster_benchmark.CheckPrerequisites(benchmark_config)

  def testNcclClusterPrepare(self):
    benchmark_spec = mock.MagicMock()
    nccl_cluster_benchmark.Prepare(benchmark_spec)
    self.assertTrue(benchmark_spec.always_call_cleanup)
    benchmark_spec.cluster.AuthenticateVM.assert_called_once_with()
    benchmark_spec.cluster.DeployContainerImage.assert_called_once_with(
        'nccl_cluster'
    )

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testNcclClusterRun(self):
    benchmark_spec = mock.MagicMock()
    benchmark_spec.cluster.gpus_per_node = 8
    benchmark_spec.cluster.RunJob.return_value = (self.contents, '')
    with (
        mock.patch.object(
            nccl_benchmark, 'CreateMetadataDict', return_value={}
        ),
        mock.patch.object(nccl_cluster_benchmark, 'FLAGS') as mock_flags,
    ):
      mock_flags.nccl_extra_params = ['NCCL_DEBUG=INFO']
      mock_flags.nccl_extra_params_spaceseplist = []
      mock_flags.nccl_operations = ['all_reduce']
      mock_flags.nccl_minbytes = '8'
      mock_flags.nccl_maxbytes = '256M'
      mock_flags.nccl_stepfactor = 2
      mock_flags.nccl_ngpus = 1
      mock_flags.nccl_check = False
      mock_flags.nccl_nthreads = 1
      mock_flags.nccl_iters = 20

      samples = nccl_cluster_benchmark.Run(benchmark_spec)

      benchmark_spec.cluster.RunJob.assert_called_once_with(
          'nccl_cluster',
          command=(
              './all_reduce_perf --minbytes 8 --maxbytes 256M --stepfactor 2 '
              '--ngpus 1 --check False --nthreads 1 --iters 20'
          ),
          env={'NCCL_DEBUG': 'INFO'},
          gpus_per_node=8,
      )
      self.assertTrue(samples)

  def testSlurmJobTemplateRender(self):
    rendered = vm_util.ReadAndRenderJinja2Template(
        'cluster/slurm_job.sbatch.j2',
        job_name='nccl_cluster',
        num_workers=2,
        output_path='/opt/apps/slurm-%j.out',
        env={'FI_PROVIDER': 'efa', 'NCCL_DEBUG': 'INFO'},
        image='/opt/apps/a4.sqsh',
        command=(
            './all_reduce_perf --minbytes 8 --maxbytes 256M --stepfactor 2 '
            '--ngpus 1 --check False --nthreads 1 --iters 20'
        ),
        gpus_per_node=8,
    )
    self.assertEqual(
        rendered,
        '#!/bin/bash\n'
        '#SBATCH --job-name=nccl_cluster\n'
        '#SBATCH --nodes=2\n'
        '#SBATCH --gpus-per-node=8\n'
        '#SBATCH --ntasks-per-node=8\n'
        '#SBATCH --output=/opt/apps/slurm-%j.out\n\n'
        'set -e\n'
        'export FI_PROVIDER="efa"\n'
        'export NCCL_DEBUG="INFO"\n\n'
        'srun \\\n'
        '  --mpi=pmix \\\n'
        '  --cpu-bind=none \\\n'
        '  --gpus-per-node=8 \\\n'
        '  --ntasks-per-node=8 \\\n'
        '  --container-image=/opt/apps/a4.sqsh \\\n'
        '  --container-writable \\\n'
        '  --wait=60 \\\n'
        '  --kill-on-bad-exit=1 \\\n'
        '  ./all_reduce_perf --minbytes 8 --maxbytes 256M --stepfactor 2 '
        '--ngpus 1 --check False --nthreads 1 --iters 20',
    )

  def testSlurmJobTemplateRenderOptionalVars(self):
    rendered = vm_util.ReadAndRenderJinja2Template(
        'cluster/slurm_job.sbatch.j2',
        job_name='custom_job',
        num_workers=4,
        output_path='/opt/apps/slurm-%j.out',
        env={},
        command='./run.sh',
        ntasks_per_node=2,
        cpus_per_task=12,
    )
    self.assertEqual(
        rendered,
        '#!/bin/bash\n'
        '#SBATCH --job-name=custom_job\n'
        '#SBATCH --nodes=4\n'
        '#SBATCH --ntasks-per-node=2\n'
        '#SBATCH --cpus-per-task=12\n'
        '#SBATCH --output=/opt/apps/slurm-%j.out\n\n'
        'set -e\n\n'
        'srun \\\n'
        '  --mpi=pmix \\\n'
        '  --cpu-bind=none \\\n'
        '  --ntasks-per-node=2 \\\n'
        '  --cpus-per-task=12 \\\n'
        '  --wait=60 \\\n'
        '  --kill-on-bad-exit=1 \\\n'
        '  ./run.sh',
    )


if __name__ == '__main__':
  unittest.main()
