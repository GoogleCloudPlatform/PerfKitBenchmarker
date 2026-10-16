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
"""Executes the TPCx-HS benchmark on a DPB (Hadoop MapReduce) cluster.

TPCx-HS (TPC Express Benchmark HS, https://www.tpc.org/tpcx-hs/) is an
industry standard Hadoop sort benchmark derived from TeraGen, TeraSort and
TeraValidate. A run consists of three stages:

* HSGen: generate the dataset (100 byte records).
* HSSort: sort the dataset.
* HSValidate: validate that the output is sorted.

The specification requires two consecutive runs; the run with the lower
performance is the reported one. The primary metric is HSph@SF (HS per hour at
scale factor), where the scale factor is the dataset size in TB.

This benchmark drives the kit's HSGen / HSSort / HSValidate programs directly
through the DPB service's job submission API (the same way
dpb_terasort_benchmark does with the hadoop examples jar). It issues the same
`hadoop jar TPCx-HS-master_MR2.jar <Stage> -Dmapreduce.job.maps=N
-Dmapreduce.job.reduces=M <args>` commands (HSValidate without -D) as the
kit's TPCx-HS-master.sh driver, but does not run the driver itself, HSDataCheck
or the cluster audit, so results are "TPCx-HS-like" and not publishable.
Samples carry a
`tpcx_hs_compliant` metadata key which only states whether the run parameters
(scale factor, number of iterations, HDFS storage) match the specification.

Backends: tested on unmanaged YARN clusters (unmanaged_dpb_svc_yarn_cluster).
Managed services (Dataproc, EMR) are supported through the same SubmitJob API
but have not been exercised; on them the worker vCPU count is unknown to PKB so
no mapreduce.job.maps / reduces sizing is applied unless
--dpb_tpcx_hs_num_maps / --dpb_tpcx_hs_num_reducers are passed.

Sizing: the kit's Benchmark_Parameters.sh ships NUM_MAPS=NUM_REDUCERS=768,
tuned for a ~50 node cluster. By default this benchmark sizes them from the
cluster instead (--dpb_tpcx_hs_maps_per_vcore * total worker vCPUs and
--dpb_tpcx_hs_reducers_per_vcore * total worker vCPUs), which keeps HSGen task
launch overhead and reducer spill reasonable from 3 to 100+ nodes.

Storage: with the default `--dpb_tpcx_hs_storage_type=ephemeral` the dataset
lives on the cluster HDFS with the Hadoop default replication of 3 (which the
specification requires). Every run needs HDFS capacity for the input and the
sorted output, i.e. 2 * SF TB * 3 replicas = 6 TB per SF, plus local disk for
the HSSort shuffle spill (~1 TB per SF). The working directory is deleted
before each iteration so only one copy is on disk at a time. Size the worker
disks so that worker_count * disk_size >= ~7 TB * SF with headroom; the
default config (3 workers x 3000 GB) fits SF 1.

The TPCx-HS kit must be obtained manually; see linux_packages/tpcx_hs.py.
"""

import logging
import os
import posixpath
import zipfile

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker.linux_packages import tpcx_hs

BENCHMARK_NAME = 'dpb_tpcx_hs_benchmark'

# 3 workers is the minimum for HDFS replication 3, which the TPCx-HS
# specification requires for the dataset. 3000 GB per worker holds the 6 TB
# (input + output, 3 replicas) that one SF 1 run needs on HDFS, plus shuffle
# spill; see the module docstring for the capacity formula.
BENCHMARK_CONFIG = """
dpb_tpcx_hs_benchmark:
  description: Run TPCx-HS on a Hadoop MapReduce cluster.
  dpb_service:
    service_type: unmanaged_dpb_svc_yarn_cluster
    worker_group:
      vm_spec:
        GCP:
          machine_type: n2-standard-8
        AWS:
          machine_type: m5.2xlarge
        Azure:
          machine_type: Standard_D8s_v5
      disk_spec:
        GCP:
          disk_size: 3000
          disk_type: pd-balanced
          mount_point: /scratch_tpcx_hs
        AWS:
          disk_size: 3000
          disk_type: gp3
          mount_point: /scratch_tpcx_hs
        Azure:
          disk_size: 3000
          disk_type: Premium_LRS
          mount_point: /scratch_tpcx_hs
    worker_count: 3
"""

_FS_TYPE_EPHEMERAL = 'ephemeral'
_FS_TYPE_PERSISTENT = 'persistent'

_SCALE_FACTOR = flags.DEFINE_integer(
    'dpb_tpcx_hs_scale_factor',
    1,
    'TPCx-HS scale factor, i.e. dataset size in TB. Must be one of '
    f'{tpcx_hs.VALID_SCALE_FACTORS} for a compliant run.',
)
flags.register_validator(
    'dpb_tpcx_hs_scale_factor',
    lambda sf: sf in tpcx_hs.VALID_SCALE_FACTORS,
    message=f'Must be one of {tpcx_hs.VALID_SCALE_FACTORS}.',
)
_NUM_RECORDS = flags.DEFINE_integer(
    'dpb_tpcx_hs_num_records',
    None,
    'Development override: number of 100-byte records to generate instead of '
    'deriving it from --dpb_tpcx_hs_scale_factor. Runs using this flag are NOT '
    'compliant with the TPCx-HS specification; the HSph@SF samples they '
    'produce are only meaningful for relative comparisons.',
    lower_bound=1,
)
_ITERATIONS = flags.DEFINE_integer(
    'dpb_tpcx_hs_iterations',
    2,
    'Number of consecutive TPCx-HS runs. The specification requires 2; the '
    'lower performing run is the reported one.',
    lower_bound=1,
)
_STORAGE_TYPE = flags.DEFINE_enum(
    'dpb_tpcx_hs_storage_type',
    _FS_TYPE_EPHEMERAL,
    [_FS_TYPE_EPHEMERAL, _FS_TYPE_PERSISTENT],
    'Filesystem to sort on: "ephemeral" uses the cluster HDFS (what the TPCx-HS'
    ' specification measures); "persistent" uses the DPB service object storage'
    ' base dir (GCS / S3) and is not compliant.',
)
_MAPS_PER_VCORE = flags.DEFINE_integer(
    'dpb_tpcx_hs_maps_per_vcore',
    8,
    'mapreduce.job.maps passed to HSGen and HSSort = this value * total worker '
    'vCPUs. 0 leaves the Hadoop default. Ignored if --dpb_tpcx_hs_num_maps is '
    'set or the worker vCPU count is unknown (managed services).',
    lower_bound=0,
)
_REDUCERS_PER_VCORE = flags.DEFINE_integer(
    'dpb_tpcx_hs_reducers_per_vcore',
    2,
    'mapreduce.job.reduces passed to HSGen and HSSort = this value * total '
    'worker vCPUs. 0 leaves the Hadoop default. Ignored if '
    '--dpb_tpcx_hs_num_reducers is set or the worker vCPU count is unknown '
    '(managed services).',
    lower_bound=0,
)
_NUM_MAPS = flags.DEFINE_integer(
    'dpb_tpcx_hs_num_maps',
    None,
    'Explicit mapreduce.job.maps for HSGen and HSSort; overrides '
    '--dpb_tpcx_hs_maps_per_vcore.',
    lower_bound=1,
)
_NUM_REDUCERS = flags.DEFINE_integer(
    'dpb_tpcx_hs_num_reducers',
    None,
    'Explicit mapreduce.job.reduces for HSGen and HSSort; overrides '
    '--dpb_tpcx_hs_reducers_per_vcore.',
    lower_bound=1,
)
_JAR_URI = flags.DEFINE_string(
    'dpb_tpcx_hs_jar_uri',
    None,
    'Object storage URI (gs:// or s3://) of an already staged TPCx-HS '
    'MapReduce kit jar. Only used with managed DPB services. If unset, the jar '
    'is extracted from the kit zip found in a local PKB data directory and '
    'uploaded to the DPB service base dir.',
)

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = (
    dpb_constants.DATAPROC,
    dpb_constants.EMR,
    dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
)

# Directory names used by the kit's driver under its working directory.
_INPUT_DIR = 'HSsort-input'
_OUTPUT_DIR = 'HSsort-output'
_REPORT_DIR = 'HSValidate'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources and flags are present.

  Args:
    benchmark_config: Config needed to run the TPCx-HS benchmark.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        f'Invalid backend {dpb_service_type} for TPCx-HS. Not in: '
        f'{SUPPORTED_DPB_BACKENDS}'
    )
  unmanaged = dpb_service_type == dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER
  if _JAR_URI.value and unmanaged:
    raise errors.Config.InvalidValue(
        '--dpb_tpcx_hs_jar_uri is only supported with managed DPB services. '
        'Unmanaged clusters install the kit from --tpcx_hs_kit_file.'
    )
  if not _JAR_URI.value and not unmanaged:
    # Managed services need the kit zip locally to extract and stage the jar.
    if not data.ResourceExists(tpcx_hs.KIT_FILE.value):
      raise errors.Config.InvalidValue(
          f'TPCx-HS kit {tpcx_hs.KIT_FILE.value!r} not found in any PKB data '
          'directory. Download it from https://www.tpc.org/tpcx-hs/ and place '
          'it in a --data_search_paths directory, or pass '
          '--dpb_tpcx_hs_jar_uri.'
      )


def GetNumRecords() -> int:
  """Number of 100-byte records to generate, honoring the dev override."""
  if _NUM_RECORDS.value is not None:
    return _NUM_RECORDS.value
  return tpcx_hs.ScaleFactorToRecords(_SCALE_FACTOR.value)


def GetScaleFactor() -> float:
  """Effective scale factor (TB) used for HSph@SF, honoring the dev override."""
  if _NUM_RECORDS.value is not None:
    return tpcx_hs.RecordsToScaleFactor(_NUM_RECORDS.value)
  return _SCALE_FACTOR.value


def IsCompliantRun() -> bool:
  """Whether flags describe a run matching the TPCx-HS specification.

  Returns:
    True if the dataset is a valid scale factor, exactly two iterations are
    run and the dataset lives on the cluster HDFS. This is a statement about
    the run parameters only; results are still not audited or publishable.
  """
  return (
      _NUM_RECORDS.value is None
      and _SCALE_FACTOR.value in tpcx_hs.VALID_SCALE_FACTORS
      and _ITERATIONS.value == 2
      and _STORAGE_TYPE.value == _FS_TYPE_EPHEMERAL
  )


def Prepare(benchmark_spec):
  """Installs or stages the TPCx-HS kit jar for the DPB service.

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    errors.Benchmarks.PrepareException: If the kit cannot be staged.
  """
  service = benchmark_spec.dpb_service
  if isinstance(service, dpb_service.UnmanagedDpbService):
    leader = service.leader
    if leader is None:
      raise errors.Benchmarks.PrepareException(
          'Cannot install the TPCx-HS kit: leader VM is not initialized.'
      )
    leader.Install(tpcx_hs.PACKAGE_NAME)
    benchmark_spec.tpcx_hs_jar = tpcx_hs.GetJarPath(leader)
  elif _JAR_URI.value:
    benchmark_spec.tpcx_hs_jar = _JAR_URI.value
  else:
    benchmark_spec.tpcx_hs_jar = _StageJarFromLocalKit(service)
  logging.info('Using TPCx-HS jar %s', benchmark_spec.tpcx_hs_jar)


def _StageJarFromLocalKit(service: dpb_service.BaseDpbService) -> str:
  """Extracts the MapReduce jar from the local kit zip and uploads it.

  Args:
    service: DPB service whose storage_service and base_dir to upload to.

  Returns:
    Object storage URI of the uploaded jar.

  Raises:
    errors.Benchmarks.PrepareException: If the service has no object storage
      or no matching jar is in the kit.
  """
  if service.storage_service is None:
    raise errors.Benchmarks.PrepareException(
        'DPB service has no object storage service to stage the kit jar to.'
    )
  kit_path = data.ResourcePath(tpcx_hs.KIT_FILE.value)
  prefix, suffix = tpcx_hs.JAR_PATTERN.split('*', 1)
  extract_dir = os.path.join(temp_dir.GetRunDirPath(), 'tpcx_hs')
  with zipfile.ZipFile(kit_path) as kit:
    candidates = sorted(
        name
        for name in kit.namelist()
        if os.path.basename(name).startswith(prefix) and name.endswith(suffix)
    )
    if not candidates:
      raise errors.Benchmarks.PrepareException(
          f'No jar matching {tpcx_hs.JAR_PATTERN!r} found in {kit_path}.'
      )
    local_jar = kit.extract(candidates[0], extract_dir)
  staged_jar = posixpath.join(service.base_dir, os.path.basename(local_jar))
  service.storage_service.Copy(local_jar, staged_jar)
  return staged_jar


def _GetBaseDir(service: dpb_service.BaseDpbService) -> str:
  if _STORAGE_TYPE.value == _FS_TYPE_PERSISTENT:
    return posixpath.join(service.base_dir, 'tpcx_hs')
  return f'/tpcx_hs-{FLAGS.run_uri}'


def _GetTotalWorkerVcpus(service: dpb_service.BaseDpbService) -> int | None:
  """Returns the total vCPUs of the worker VMs, or None if unknown.

  Only unmanaged DPB services expose their worker VMs; managed services
  (Dataproc, EMR) return None.

  Args:
    service: The DPB service.
  """
  if not isinstance(service, dpb_service.UnmanagedDpbService):
    return None
  workers = service.vms.get('worker_group') or []
  if not workers:
    return None
  return sum(vm.NumCpusForBenchmark() for vm in workers)


def GetJobProperties(total_vcpus: int | None) -> dict[str, str]:
  """Returns the mapreduce.job.maps / reduces properties for HSGen and HSSort.

  Mirrors the kit driver, which passes NUM_MAPS / NUM_REDUCERS from
  Benchmark_Parameters.sh to HSGen and HSSort (HSValidate is run without
  them).

  Args:
    total_vcpus: Total worker vCPUs used for the per-vCPU sizing, or None if
      unknown.

  Returns:
    Dict of Hadoop job properties; empty if nothing should be overridden.
  """
  properties = {}
  num_maps = _NUM_MAPS.value
  if num_maps is None and total_vcpus and _MAPS_PER_VCORE.value:
    num_maps = _MAPS_PER_VCORE.value * total_vcpus
  num_reducers = _NUM_REDUCERS.value
  if num_reducers is None and total_vcpus and _REDUCERS_PER_VCORE.value:
    num_reducers = _REDUCERS_PER_VCORE.value * total_vcpus
  if num_maps:
    properties['mapreduce.job.maps'] = str(num_maps)
  if num_reducers:
    properties['mapreduce.job.reduces'] = str(num_reducers)
  if not total_vcpus and (num_maps is None or num_reducers is None):
    logging.warning(
        'Worker vCPU count is unknown for this DPB service; HSGen / HSSort '
        'will run with the cluster default mapreduce.job.maps / reduces. Pass '
        '--dpb_tpcx_hs_num_maps and --dpb_tpcx_hs_num_reducers to size them '
        'explicitly.'
    )
  return properties


def _GetMetadata(
    service: dpb_service.BaseDpbService,
    total_vcpus: int | None,
    job_properties: dict[str, str],
) -> dict[str, object]:
  """Returns the metadata attached to every sample of a run."""
  num_records = GetNumRecords()
  metadata = dict(service.GetResourceMetadata())
  metadata.update({
      'tpcx_hs_scale_factor': GetScaleFactor(),
      'tpcx_hs_num_records': num_records,
      'tpcx_hs_dataset_size_gb': (
          num_records * tpcx_hs.RECORD_SIZE_BYTES // 10**9
      ),
      'tpcx_hs_iterations': _ITERATIONS.value,
      'tpcx_hs_storage_type': _STORAGE_TYPE.value,
      'tpcx_hs_compliant': IsCompliantRun(),
      'tpcx_hs_num_maps': job_properties.get('mapreduce.job.maps', 'default'),
      'tpcx_hs_num_reducers': job_properties.get(
          'mapreduce.job.reduces', 'default'
      ),
      'tpcx_hs_total_worker_vcpus': total_vcpus,
      'base_dir': _GetBaseDir(service),
  })
  return metadata


def _DeleteDir(service: dpb_service.BaseDpbService, path: str) -> None:
  """Deletes an HCFS directory (HDFS, GCS or S3) through the cluster.

  Uses a classname-only Hadoop job like BaseDpbService.DistributedCopy does,
  which every Hadoop backend supports (`hadoop <class>` on unmanaged clusters
  and EMR, `--class` on Dataproc).

  Args:
    service: The DPB service.
    path: HCFS directory to delete; missing directories are not an error.
  """
  service.SubmitJob(
      classname='org.apache.hadoop.fs.FsShell',
      job_arguments=['-rm', '-r', '-f', '-skipTrash', path],
      job_type=dpb_constants.HADOOP_JOB_TYPE,
  )


def Run(benchmark_spec) -> list[sample.Sample]:
  """Runs the TPCx-HS stages for the configured number of iterations.

  Like the kit driver, every iteration reuses the same working directory,
  which is deleted beforehand so that only one dataset copy is on disk.

  Args:
    benchmark_spec: Spec needed to run the benchmark.

  Returns:
    A list of samples with per stage, per iteration and overall results.

  Raises:
    JobSubmissionError if any job fails.
  """
  service = benchmark_spec.dpb_service
  total_vcpus = _GetTotalWorkerVcpus(service)
  job_properties = GetJobProperties(total_vcpus)
  metadata = _GetMetadata(service, total_vcpus, job_properties)
  service.metadata.update(metadata)
  base_dir = _GetBaseDir(service)
  num_records = GetNumRecords()
  scale_factor = GetScaleFactor()

  results = []
  if service.resource_ready_time and service.create_start_time:
    create_time = service.GetClusterCreateTime()
    if create_time is not None:
      results.append(
          sample.Sample('Time to Create', create_time, 'seconds', metadata)
      )

  input_dir = posixpath.join(base_dir, _INPUT_DIR)
  output_dir = posixpath.join(base_dir, _OUTPUT_DIR)
  report_dir = posixpath.join(base_dir, _REPORT_DIR)
  # The kit driver passes the sizing to HSGen and HSSort only; HSValidate is
  # invoked without -D options.
  stages = [
      (tpcx_hs.HSGEN, [str(num_records), input_dir], job_properties),
      (tpcx_hs.HSSORT, [input_dir, output_dir], job_properties),
      (tpcx_hs.HSVALIDATE, [output_dir, report_dir], {}),
  ]
  hsph_per_iteration = []
  for iteration in range(1, _ITERATIONS.value + 1):
    _DeleteDir(service, base_dir)
    iter_metadata = dict(metadata, iteration=iteration)
    total_run_time = 0.0
    total_wall_time = 0.0
    for stage, args, properties in stages:
      result = RunStage(benchmark_spec, stage, args, properties)
      logging.info(
          'TPCx-HS run %d %s: run_time=%s wall_time=%s',
          iteration,
          stage,
          result.run_time,
          result.wall_time,
      )
      stage_name = stage.lower()
      results.append(
          sample.Sample(
              f'{stage_name}_run_time',
              result.run_time,
              'seconds',
              iter_metadata,
          )
      )
      results.append(
          sample.Sample(
              f'{stage_name}_wall_time',
              result.wall_time,
              'seconds',
              iter_metadata,
          )
      )
      total_run_time += result.run_time
      total_wall_time += result.wall_time
    # The specification's T is the elapsed time of the whole run, so HSph is
    # computed from wall time (run time plus any job pending time on managed
    # services); the run time total is reported alongside for reference.
    hsph = tpcx_hs.ComputeHsph(scale_factor, total_wall_time)
    hsph_per_iteration.append(hsph)
    results.append(
        sample.Sample(
            'tpcx_hs_total_run_time', total_run_time, 'seconds', iter_metadata
        )
    )
    results.append(
        sample.Sample(
            'tpcx_hs_total_wall_time',
            total_wall_time,
            'seconds',
            iter_metadata,
        )
    )
    results.append(
        sample.Sample('tpcx_hs_hsph', hsph, 'HSph@SF', iter_metadata)
    )

  # Per the specification the reported result is the lower performing run.
  results.append(
      sample.Sample(
          'tpcx_hs_hsph_reported', min(hsph_per_iteration), 'HSph@SF', metadata
      )
  )
  return results


def RunStage(
    benchmark_spec,
    stage: str,
    stage_args: list[str],
    properties: dict[str, str],
) -> dpb_service.JobResult:
  """Runs one TPCx-HS stage (HSGen, HSSort or HSValidate).

  Args:
    benchmark_spec: The benchmark spec; must have tpcx_hs_jar set by Prepare.
    stage: One of tpcx_hs.STAGES.
    stage_args: Arguments for the stage program.
    properties: Hadoop job properties (see GetJobProperties) passed as -D
      options to the stage program.

  Returns:
    JobResult of the submitted job.

  Raises:
    JobSubmissionError if the job fails.
  """
  service = benchmark_spec.dpb_service
  # The jar has a program driver: the stage name must come first, then the -D
  # properties, which the stage's ToolRunner parses. SubmitJob's `properties`
  # argument cannot be used because unmanaged YARN clusters and EMR emit it
  # before job_arguments, where the driver would take it as the program name
  # (Dataproc maps it to --properties instead). For the same reason
  # --dpb_job_properties cannot be combined with this benchmark on unmanaged
  # clusters (as with dpb_terasort_benchmark).
  return service.SubmitJob(
      jarfile=benchmark_spec.tpcx_hs_jar,
      job_arguments=(
          [stage] + [f'-D{k}={v}' for k, v in properties.items()] + stage_args
      ),
      job_type=dpb_constants.HADOOP_JOB_TYPE,
  )


def Cleanup(benchmark_spec):
  """Cleans up the TPCx-HS benchmark.

  The working directory lives on the cluster HDFS or under the DPB service's
  base dir, both of which are torn down with the service.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec  # unused
