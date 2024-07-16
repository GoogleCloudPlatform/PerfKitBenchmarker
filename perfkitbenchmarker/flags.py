# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""File used for the management of PKB flags."""

import getpass
from absl import flags

MAX_RUN_URI_LENGTH = 12
FLAGS = flags.FLAGS


def GetCurrentUser():
  """Get the current user name.

  On some systems the current user information may be unavailable. In these
  cases we just need a string to tag the created resources with. It should
  not be a fatal error.

  Returns:
    User name OR default string if user name not available.
  """
  try:
    return getpass.getuser()
  except KeyError:
    return 'user_unknown'


flags.DEFINE_boolean(
    'accept_licenses',
    False,
    'Acknowledge that PKB may install software thereby accepting license'
    ' agreements on the users behalf.',
)
flags.DEFINE_list('ssh_options', [], 'Additional options to pass to ssh.')
flags.DEFINE_boolean('use_ipv6', False, 'Whether to use ipv6 for ssh/scp.')
flags.DEFINE_list(
    'benchmarks',
    ['cluster_boot'],
    'Benchmarks and/or benchmark sets that should be run. The '
    'default is cluster_boot. For more information about '
    'benchmarks and benchmark sets, see the README and '
    'benchmark_sets.py.',
)
flags.DEFINE_boolean(
    'multi_os_benchmark',
    False,
    'Whether is benchmark will involve multiple os types.',
)
flags.DEFINE_string(
    'archive_bucket',
    None,
    'Archive results to the given S3/GCS bucket.'
    'Must include gs:// or s3:// prefix.',
)
flags.DEFINE_string(
    'project', None, 'GCP project ID under which to create the virtual machines'
)
flags.DEFINE_multi_string(
    'zone',
    [],
    'Similar to the --zones flag, but allows the flag to be specified '
    'multiple times on the commandline. For example, --zone=a --zone=b is '
    'equivalent to --zones=a,b. Furthermore, any values specified by --zone '
    'will be appended to those specified by --zones.',
)
flags.DEFINE_list(
    'zones',
    [],
    'A list of zones within which to run PerfKitBenchmarker. '
    'This is specific to the cloud provider you are running on. '
    'If multiple zones are given, PerfKitBenchmarker will create 1 VM in '
    'zone, until enough VMs are created as specified in each '
    'benchmark. The order in which this flag is applied to VMs is '
    'undefined.',
)
flags.DEFINE_list(
    'extra_zones',
    [],
    'Zones that will be appended to the "zones" list. This is functionally '
    'the same, but allows flag matrices to have two zone axes.',
)
# TODO(user): note that this is currently very GCE specific. Need to create a
#    module which can translate from some generic types to provider specific
#    nomenclature.
flags.DEFINE_string(
    'machine_type',
    None,
    'Machine '
    "types that will be created for benchmarks that don't "
    'require a particular type.',
)
flags.DEFINE_integer(
    'num_vms',
    1,
    'For benchmarks which can make use of a '
    'variable number of machines, the number of VMs to use.',
)
flags.DEFINE_string(
    'image', None, 'Default image that will be linked to the VM'
)
flags.DEFINE_string(
    'run_uri',
    None,
    'Name of the Run. If provided, this '
    'should be alphanumeric and less than or equal to %d '
    'characters in length.' % MAX_RUN_URI_LENGTH,
)
flags.DEFINE_boolean(
    'use_pkb_logging',
    True,
    'Whether to use PKB-specific '
    'logging handlers. Disabling this will use the standard '
    'ABSL logging directly.',
)
flags.DEFINE_boolean(
    'log_dmesg',
    False,
    'Whether to log dmesg from '
    'each VM to the PKB log file before the VM is deleted.',
)
flags.DEFINE_boolean(
    'always_teardown_on_exception',
    False,
    'Whether to tear '
    'down VMs when there is exception during the PKB run. If'
    'enabled, VMs will be torn down even if FLAGS.run_stage '
    'does not specify teardown.',
)
RESTORE_PATH = flags.DEFINE_string(
    'restore', None, 'Path to restore resources from.'
)
FREEZE_PATH = flags.DEFINE_string(
    'freeze', None, 'Path to freeze resources to.'
)
COLLECT_MEMINFO = flags.DEFINE_bool(
    'collect_meminfo', False, 'Whether to collect /proc/meminfo stats.'
)
OWNER = flags.DEFINE_string(
    'owner',
    GetCurrentUser(),
    'Owner name. Used to tag created resources and performance records.',
)
flags.DEFINE_integer(
    'duration_in_seconds',
    None,
    'duration of benchmarks. (only valid for mesh_benchmark)',
)
flags.DEFINE_string(
    'static_vm_file',
    None,
    'The file path for the Static Machine file. See '
    'static_virtual_machine.py for a description of this file.',
)
flags.DEFINE_boolean(
    'version', False, 'Display the version and exit.', allow_override_cpp=True
)
flags.DEFINE_boolean('time_commands', False, 'Times each command issued.')
flags.DEFINE_string(
    'data_disk_type',
    None,
    'Type for all data disks. If a provider keeps the operating system and '
    'user data on separate disks, this only affects the user data disk(s).'
    'If the provider has OS and user data on the same disk, this flag affects'
    'that disk.',
)
flags.DEFINE_integer('data_disk_size', None, 'Size, in gb, for all data disks.')
flags.DEFINE_integer(
    'num_striped_disks',
    None,
    'The number of data disks for PKB to create and stripe together into a '
    'single RAID 0 device. This defaults to 1, which means no striping. If '
    'data disks are local this is simply the number of existing disks you '
    'would like to combine. If data disks are provisioned remote disks, PKB '
    'will create this many disks and stripe them together. You do not need to '
    'set vm_group.disk_count. If you do set both vm_group.disk_count=m and '
    'num_striped_disks=n, you will end up with m logical disks striped out of '
    'm x n remote disks, which is probably not what you want.',
    lower_bound=1,
)
flags.DEFINE_integer(
    'num_partitions',
    None,
    'The number of partitions to created from each disk. So far it is only '
    'supported on Linux.',
    lower_bound=1,
)
flags.DEFINE_string(
    'partition_size',
    None,
    'The size of the partitions to create from each disk, e.g. 100M, 500G. '
    'Note that this is only supported on Linux so far.',
)
flags.DEFINE_bool(
    'install_packages',
    None,
    'Override for determining whether packages should be '
    'installed. If this is false, no packages will be installed '
    'on any VMs. This option should probably only ever be used '
    'if you have already created an image with all relevant '
    'packages installed.',
)
flags.DEFINE_bool(
    'stop_after_benchmark_failure',
    False,
    'Determines response when running multiple benchmarks serially and a '
    'benchmark run fails. When True, no further benchmarks are scheduled, and '
    'execution ends. When False, benchmarks continue to be scheduled. Does not '
    'apply to keyboard interrupts, which will always prevent further '
    'benchmarks from being scheduled.',
)
flags.DEFINE_boolean(
    'ignore_package_requirements',
    False,
    'Disables Python package requirement runtime checks.',
)
flags.DEFINE_boolean(
    'publish_after_run',
    False,
    'If true, PKB will publish all samples available immediately after running '
    'each benchmark. This may be useful in scenarios where the PKB run time '
    'for all benchmarks is much greater than a single benchmark.',
)
flags.DEFINE_integer(
    'publish_period',
    None,
    'The period in seconds to publish samples from repeated run stages. '
    'This will only publish samples if publish_after_run is True.',
)
flags.DEFINE_integer(
    'run_stage_time',
    0,
    'PKB will run/re-run the run stage of each benchmark until it has spent '
    'at least this many seconds. It defaults to 0, so benchmarks will only '
    'be run once unless some other value is specified. This flag and '
    'run_stage_iterations are mutually exclusive.',
)
flags.DEFINE_integer(
    'run_stage_iterations',
    1,
    'PKB will run/re-run the run stage of each benchmark this many times. '
    'It defaults to 1, so benchmarks will only be run once unless some other '
    'value is specified. This flag and run_stage_time are mutually exclusive.',
)
flags.DEFINE_integer(
    'run_stage_retries',
    0,
    'The number of allowable consecutive failures during the run stage. After '
    'this number of failures any exceptions will cause benchmark termination. '
    'If run_stage_time is exceeded, the run stage will not be retried even if '
    'the number of failures is less than the value of this flag.',
)
MAX_RETRIES = flags.DEFINE_integer(
    'retries',
    0,
    'The amount of times PKB should retry each benchmark.'
    'Use with --retry_substatuses to specify which failure substatuses to '
    'retry on. Defaults to all valid substatuses.',
)
RETRY_DELAY_SECONDS = flags.DEFINE_integer(
    'retry_delay_seconds', 0, 'The time to wait in between retries.'
)
# Retries could also allow for a dict of failed_substatus: 'zone'|'region'
# retry method which would make the retry functionality more customizable.
SMART_QUOTA_RETRY = flags.DEFINE_bool(
    'smart_quota_retry',
    False,
    'If True, causes the benchmark to rerun in a zone in a different region '
    'in the same geo on a quota exception. Currently only works for benchmarks '
    'that specify a single zone (via --zone or --zones). The zone is selected '
    'at random and overrides the --zones flag or the --zone flag, depending on '
    'which is provided. QUOTA_EXCEEDED must be in the list of retry '
    'substatuses for this to work.',
)
SMART_CAPACITY_RETRY = flags.DEFINE_bool(
    'smart_capacity_retry',
    False,
    'If True, causes the benchmark to rerun in a different zone in the same '
    'region on a capacity/config exception. Currently only works for '
    'benchmarks that specify a single zone (via --zone or --zones). The zone '
    'is selected at random and overrides the --zones flag or the --zone flag, '
    'depending on which is provided. INSUFFICIENT_CAPACITY and UNSUPPORTED '
    'must be in the list of retry substatuses for this to work.',
)
flags.DEFINE_boolean(
    'boot_samples', False, 'Whether to publish boot time samples for all tests.'
)
MEASURE_DELETE = flags.DEFINE_boolean(
    'collect_delete_samples',
    False,
    'Whether to publish delete time samples for all tests.',
)
flags.DEFINE_boolean(
    'gpu_samples',
    False,
    'Whether to publish GPU memcpy bandwidth samples for GPU tests.',
)
flags.DEFINE_integer(
    'run_processes',
    None,
    'The number of parallel processes to use to run benchmarks.',
    lower_bound=1,
)
flags.DEFINE_float(
    'run_processes_delay',
    None,
    "The delay in seconds between parallel processes' invocation. "
    'Increasing this value may reduce provider throttling issues.',
    lower_bound=0,
)
flags.DEFINE_string(
    'completion_status_file',
    None,
    'If specified, this file will contain the completion status of each '
    'benchmark that ran (SUCCEEDED, FAILED, or SKIPPED). The file has one json '
    'object per line, each with the following format:\n'
    '{ "name": <benchmark name>, "flags": <flags dictionary>, '
    '"status": <completion status> }',
)
flags.DEFINE_string(
    'helpmatch',
    '',
    'Shows only flags defined in a module whose name matches the given regex.',
    allow_override_cpp=True,
)
flags.DEFINE_string(
    'helpmatchmd',
    '',
    'helpmatch query with markdown friendly output. '
    'Shows only flags defined in a module whose name matches the given regex.',
    allow_override_cpp=True,
)
flags.DEFINE_boolean(
    'create_failed_run_samples',
    False,
    'If true, PKB will create a sample specifying that a run stage failed. '
    'This sample will include metadata specifying the run stage that '
    'failed, the exception that occurred, as well as all the flags that '
    'were provided to PKB on the command line.',
)
CREATE_STARTED_RUN_SAMPLE = flags.DEFINE_boolean(
    'create_started_run_sample',
    False,
    'Whether PKB will create a sample at the start of the provision phase of '
    'the benchmark run.',
)
CREATE_STARTED_STAGE_SAMPLES = flags.DEFINE_boolean(
    'create_started_stage_samples',
    False,
    'Whether PKB will create a sample at the start of the each stage of '
    'the benchmark run.',
)
flags.DEFINE_integer(
    'failed_run_samples_error_length',
    10240,
    'If create_failed_run_samples is true, PKB will truncate any error '
    'messages at failed_run_samples_error_length.',
)
flags.DEFINE_boolean(
    'dry_run',
    False,
    'If true, PKB will print the flags configurations to be run and exit. '
    'The configurations are generated from the command line flags, the '
    'flag_matrix, and flag_zip.',
)
flags.DEFINE_string(
    'skip_pending_runs_file',
    None,
    'If file exists, any pending runs will be not be executed.',
)
flags.DEFINE_boolean('use_vpn', False, 'Creates VPN tunnels between vm_groups')
flags.DEFINE_integer(
    'after_prepare_sleep_time',
    0,
    'The time in seconds to sleep after the prepare phase. This can be useful '
    'for letting burst tokens accumulate.',
)
flags.DEFINE_integer(
    'after_run_sleep_time',
    0,
    'The time in seconds to sleep after the run phase. This can be useful '
    'for letting the VM sit idle after the bechmarking phase is complete.',
)
flags.DEFINE_bool(
    'before_run_pause',
    False,
    'If true, wait for command line input before executing the run phase. '
    'This is useful for debugging benchmarks during development.',
)
flags.DEFINE_bool(
    'before_cleanup_pause',
    False,
    'If true, wait for command line input before executing the cleanup phase. '
    'This is useful for debugging benchmarks during development.',
)
flags.DEFINE_integer(
    'timeout_minutes',
    240,
    'An upper bound on the time in minutes that the benchmark is expected to '
    'run. This time is annotated or tagged on the resources of cloud '
    'providers. Note that for retries, this applies to each individual retry.',
)
flags.DEFINE_integer(
    'persistent_timeout_minutes',
    240,
    'An upper bound on the time in minutes that resources left behind by the '
    'benchmark. Some benchmarks purposefully create resources for other '
    'benchmarks to use. Persistent timeout specifies how long these shared '
    'resources should live.',
)
flags.DEFINE_bool(
    'disable_interrupt_moderation',
    False,
    'Turn off the interrupt moderation networking feature',
)
flags.DEFINE_bool(
    'disable_rss',
    False,
    'Whether or not to disable the Receive Side Scaling feature.',
)
flags.DEFINE_boolean(
    'record_lscpu', True, 'Whether to record the lscpu output in a sample'
)
RECORD_PROCCPU = flags.DEFINE_boolean(
    'record_proccpu',
    True,
    'Whether to record the /proc/cpuinfo output in a sample',
)
flags.DEFINE_boolean(
    'record_cpu_vuln',
    True,
    'Whether to record the CPU vulnerabilities on linux VMs',
)
flags.DEFINE_boolean(
    'record_gcc', True, 'Whether to record the gcc version in a sample'
)
flags.DEFINE_boolean(
    'record_glibc', True, 'Whether to record the glibc version in a sample'
)
# Support for using a proxy in the cloud environment.
flags.DEFINE_string(
    'http_proxy',
    '',
    'Specify a proxy for HTTP in the form [user:passwd@]proxy.server:port.',
)
flags.DEFINE_string(
    'https_proxy',
    '',
    'Specify a proxy for HTTPS in the form [user:passwd@]proxy.server:port.',
)
flags.DEFINE_string(
    'ftp_proxy',
    '',
    'Specify a proxy for FTP in the form [user:passwd@]proxy.server:port.',
)
flags.DEFINE_string(
    'no_proxy',
    '',
    'Specify host(s) to exclude from proxy, e.g. '
    '--no_proxy=localhost,.example.com,192.168.0.1',
)
flags.DEFINE_bool(
    'randomize_run_order',
    False,
    'When running with more than one benchmarks, '
    'randomize order of the benchmarks.',
)
ALWAYS_CALL_CLEANUP = flags.DEFINE_boolean(
    'always_call_cleanup',
    False,
    'Indicates that this benchmark run should always run the Cleanup phase.'
)
SKIP_TEARDOWN_CONDITIONS = flags.DEFINE_list(
    'skip_teardown_conditions',
    [],
    'A list of conditions that warrant skipping teardown. This is useful for '
    'investigating resources with interesting performance characteristics.\n'
    'Each item contains three tokens: '
    '\tmetric: the metric to check\n'
    '\tdirection: the direction to check against ("<" or ">")\n'
    '\tthreshold: the threshold to check against (in seconds)\n'
    'For example: "kernel_start>50"\n'
    'Additional conditions should be separated by commas. '
    'Adjust the --timeout_minutes flag to annotate the affected resources with '
    'your desired keep up time. The PKB run will complete, so users of this '
    'flag must have a spearate teardown procedure in place for resources with '
    'extended uptimes.',
)
SKIP_TEARDOWN_ZONAL_VM_LIMIT = flags.DEFINE_integer(
    'skip_teardown_zonal_vm_limit',
    None,
    'The maximum number of VMs in the zone (within a project) that can be left '
    'behind via the --skip_teardown_conditions flag. If skipping teardown will '
    'cause the number of VMs in the project to exceed this limit, teardown '
    'will be performed regardless of the --skip_teardown_conditions flag.',
)
SKIP_TEARDOWN_KEEP_UP_MINUTES = flags.DEFINE_integer(
    'skip_teardown_keep_up_minutes',
    1440,  # 24 hours
    'The time in minutes that VMs left behind by the benchmark should live. '
    'This is used to annotate the "timeout_utc" tag for resources that are '
    'kept alive through the --skip_teardown_conditions flag.\n'
    'Only implemented for GCE VMs.',
)
