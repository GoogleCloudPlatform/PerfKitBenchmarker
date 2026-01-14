# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Flags for fio benchmark."""

from absl import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import units
from perfkitbenchmarker.linux_benchmarks.fio import constants


FLAGS = flags.FLAGS


FIO_JOBFILE = flags.DEFINE_string(
    'fio_jobfile',
    None,
    'Job file that fio will use. If not given, use a job file '
    'bundled with PKB. Cannot use with '
    '--fio_generate_scenarios.',
)
FIO_GENERATE_SCENARIOS = flags.DEFINE_list(
    'fio_generate_scenarios',
    [],
    'Generate a job file with the given scenarios. Special '
    "scenario 'all' generates all scenarios. Available "
    'scenarios are sequential_write, sequential_read, '
    'random_write, and random_read. Cannot use with '
    '--fio_jobfile.   You can also specify a scenario in the '
    'format accesspattern_blocksize_operation_workingset '
    'for a custom workload.',
)
FIO_USE_DEFAULT_SCENARIOS = flags.DEFINE_bool(
    'fio_use_default_scenarios',
    True,
    'Use the legacy scenario tables defined in fio_benchmark.py '
    'to resolve the scenario name in generate scenarios',
)
FIO_TARGET_MODE = flags.DEFINE_enum(
    'fio_target_mode',
    constants.AGAINST_FILE_WITHOUT_FILL_MODE,
    [
        constants.AGAINST_DEVICE_WITH_FILL_MODE,
        constants.AGAINST_DEVICE_WITHOUT_FILL_MODE,
        constants.AGAINST_FILE_WITH_FILL_MODE,
        constants.AGAINST_FILE_WITHOUT_FILL_MODE,
    ],
    'Whether to run against a raw device or a file, and whether to prefill.',
)
FIO_FILL_SIZE = flags.DEFINE_string(
    'fio_fill_size',
    '100%',
    'The amount of device to fill in prepare stage. '
    'The valid value can either be an integer, which '
    'represents the number of bytes to fill or a '
    'percentage, which represents the percentage '
    'of the device. A filesystem will be unmounted before '
    'filling and remounted afterwards. Only valid when '
    '--fio_target_mode is against_device_with_fill or '
    'against_file_with_fill.',
)
FIO_FILL_BLOCK_SIZE = flags.DEFINE_string(
    'fio_fill_block_size',
    '512k',
    'The block size of the IO request to fill in prepare stage. '
    'A filesystem will be unmounted before '
    'filling and remounted afterwards. Only valid when '
    '--fio_target_mode is against_device_with_fill or '
    'against_file_with_fill.',
)
FIO_NR_FILES = flags.DEFINE_integer(
    'fio_nr_files',
    1,
    'The number of files to use for FIO.',
)
FIO_IO_DEPTHS = flag_util.DEFINE_integerlist(
    'fio_io_depths',
    flag_util.IntegerList([1]),
    'IO queue depths to run on. Can specify a single '
    'number, like --fio_io_depths=1, a range, like '
    '--fio_io_depths=1-4, or a list, like '
    '--fio_io_depths=1-4,6-8',
    on_nonincreasing=flag_util.IntegerListParser.WARN,
    module_name=__name__,
)
FIO_NUM_JOBS = flag_util.DEFINE_integerlist(
    'fio_num_jobs',
    flag_util.IntegerList([1]),
    'Number of concurrent fio jobs to run.',
    on_nonincreasing=flag_util.IntegerListParser.WARN,
    module_name=__name__,
)
FIO_WORKING_SET_SIZE = flags.DEFINE_integer(
    'fio_working_set_size',
    None,
    'The size of the working set, in GB. If not given, use '
    'the full size of the device. If using '
    '--fio_generate_scenarios and not running against a raw '
    'device, you must pass --fio_working_set_size.',
    lower_bound=0,
)
FIO_BLOCKSIZE = flag_util.DEFINE_units(
    'fio_blocksize',
    None,
    'The block size for fio operations. Default is given by '
    'the scenario when using --fio_generate_scenarios. This '
    'flag does not apply when using --fio_jobfile.',
    convertible_to=units.byte,
)
FIO_RUNTIME = flags.DEFINE_integer(
    'fio_runtime',
    600,
    'The number of seconds to run each fio job for.',
    lower_bound=0,
)
FIO_RAMPTIME = flags.DEFINE_integer(
    'fio_ramptime',
    120,
    'The number of seconds to run the specified workload '
    'before logging any performance numbers',
    lower_bound=0,
)
FIO_PARAMETERS = flags.DEFINE_list(
    'fio_parameters',
    ['randrepeat=0'],
    'Parameters to apply to all PKB generated fio jobs. Each '
    'member of the list should be of the form "param=value".',
)
FIO_LAT_LOG = flags.DEFINE_boolean(
    'fio_lat_log', False, 'Whether to collect a latency log of the fio jobs.'
)
FIO_BW_LOG = flags.DEFINE_boolean(
    'fio_bw_log', False, 'Whether to collect a bandwidth log of the fio jobs.'
)
FIO_IOPS_LOG = flags.DEFINE_boolean(
    'fio_iops_log', False, 'Whether to collect an IOPS log of the fio jobs.'
)
FIO_LOG_AVG_MSEC = flags.DEFINE_integer(
    'fio_log_avg_msec',
    1000,
    'By default, this will average each log entry in the '
    'fio latency, bandwidth, and iops logs over the specified '
    'period of time in milliseconds. If set to 0, fio will '
    'log an entry for every IO that completes, this can grow '
    'very quickly in size and can cause performance overhead.',
    lower_bound=0,
)
FIO_HIST_LOG = flags.DEFINE_boolean(
    'fio_hist_log', False, 'Whether to collect clat histogram.'
)
FIO_LOG_HIST_MSEC = flags.DEFINE_integer(
    'fio_log_hist_msec',
    1000,
    'Same as fio_log_avg_msec, but logs entries for '
    'completion latency histograms. If set to 0, histogram '
    'logging is disabled.',
)
FIO_COMMAND_TIMEOUT_SEC = flags.DEFINE_integer(
    'fio_command_timeout_sec', None, 'Timeout for fio commands in seconds.'
)
FIO_RNG = flags.DEFINE_enum(
    'fio_rng',
    'tausworthe64',
    ['tausworthe', 'lfsr', 'tausworthe64'],
    'Which RNG to use for 4k Random IOPS.',
)
FIO_IOENGINE = flags.DEFINE_enum(
    'fio_ioengine',
    'libaio',
    ['libaio', 'windowsaio', 'sync'],
    'Defines how the job issues I/O to the file',
)
FIO_RATE_BANDWIDTH_LIMIT = flags.DEFINE_string(
    'fio_rate_bandwidth_limit',
    None,
    'The bandwidth cap in bytes/sec. For example, using '
    'rate=1m caps bandwidth to 1MiB/sec.',
)
FIO_INCLUDE_LATENCY_PERCENTILES = flags.DEFINE_boolean(
    'fio_include_latency_percentiles',
    True,
    'Whether to include FIO latency stats.',
)
DIRECT_IO = flags.DEFINE_boolean(
    'fio_direct',
    True,
    'Whether to use O_DIRECT to bypass OS cache. This is strongly '
    'recommended, but not supported by all files.',
)
FLAGS_IGNORED_FOR_CUSTOM_JOBFILE = frozenset({
    'fio_generate_scenarios',
    'fio_io_depths',
    'fio_runtime',
    'fio_ramptime',
    'fio_blocksize',
    'fio_num_jobs',
    'fio_parameters',
})

FIO_LATENCY_TARGET = flags.DEFINE_string(
    'fio_latency_target',
    '1ms',
    'Fio latency target, allowed units are ms, us and s. It is taken in'
    ' microseconds if no unit is specified.',
)
FIO_LATENCY_PERCENTILE = flags.DEFINE_float(
    'fio_latency_percentile',
    99,
    'Fio latency percentile.',
)

FIO_SEPARATE_JOBS_FOR_DISKS = flags.DEFINE_bool(
    'fio_separate_jobs_for_disks', False, 'Run separate fio jobs for each disk.'
)

FIO_RUN_PARALLEL_JOBS_ON_DISKS = flags.DEFINE_bool(
    'fio_run_parallel_jobs_on_disks',
    False,
    'When fio_separate_jobs_for_disks is true, fio_run_parallel_jobs_on_disks'
    ' will control if we want to run fio scenario on the disks parallelly or'
    ' sequentially.',
)

FIO_TEST_DISK_COUNT = flags.DEFINE_integer(
    'fio_disk_count',
    None,
    'How many disks to run the fio on, used only when'
    ' --fio_separate_jobs_for_disks is true.',
)

FIO_OPERATION_TYPE = flags.DEFINE_enum(
    'fio_operation_type',
    constants.OPERATION_READ,
    [
        constants.OPERATION_READ,
        constants.OPERATION_WRITE,
        constants.OPERATION_READWRITE,
    ],
    'Fio operation/workload type.',
)

FIO_RW_MIX_READ = flags.DEFINE_integer(
    'fio_rwmixread',
    70,
    'Percentage of a mixed workload that should be reads.',
)

FIO_TEST_COUNT = flags.DEFINE_integer(
    'fio_test_count',
    1,
    'Number of fio tests to run.',
)

FIO_PREFILL_TYPE = flags.DEFINE_enum(
    'fio_prefill_type',
    'only_sequential',
    ['only_sequential', 'sequential_and_random'],
    'Type of prefill to use.',
)
