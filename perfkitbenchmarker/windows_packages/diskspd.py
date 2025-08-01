# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing DiskSpd installation and cleanup functions.

DiskSpd is a tool made for benchmarking Windows disk performance.
"""

import collections
import logging
import ntpath
import re
import xml.etree.ElementTree

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

LATENCY_PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999, 99.9999, 99.99999]
SampleData = collections.namedtuple('SampleData', ['metric', 'value', 'unit'])

flags.DEFINE_boolean(
    'diskspd_prefill',
    False,
    'If we want to prefill the file with random data before running the test.',
)

flags.DEFINE_integer(
    'diskspd_prefill_duration',
    None,
    'In seconds. Diskspd needs a duration to run. For prefilling, use a'
    ' duration that is large enough to allow diskspd to write the file to the'
    ' required size.',
)

flags.DEFINE_integer(
    'diskspd_duration',
    20,
    'The number of seconds to run diskspd test.Defaults to 30s. Unit: seconds.',
)

flags.DEFINE_integer(
    'diskspd_warmup',
    5,
    'The warm up time for diskspd, the time needed to enter'
    'steady state of I/O operation. '
    'Defaults to 5s. Unit: seconds.',
)

flags.DEFINE_integer(
    'diskspd_cooldown',
    5,
    'The cool down time for diskspd, the time to ensure that'
    'each instance of diskspd is active during each'
    'measurement period of each instance. '
    'Defaults: 5s. Unit: seconds',
)

flags.DEFINE_integer(
    'diskspd_thread_number_per_file',
    1,
    'The thread number created per file toperform read and write. Defaults: 1.',
)

flags.DEFINE_enum(
    'diskspd_access_pattern',
    's',
    ['s', 'r'],
    'the access patten of the read and write'
    'the performance will be downgrade a little bit if use'
    'different hints'
    'available option: r|s, '
    'r: random access'
    's: sequential access. '
    'Defaults: s.',
)

flags.DEFINE_integer(
    'diskspd_write_read_ratio',
    0,
    'The ratio of write workload to read workload.'
    'Example: 50 means 50%, and write and read each takes'
    '50% of the total I/O data.'
    'To test read speed, set this value to 0. '
    'To test write speed, set this value to 100. '
    'Defaults: 0. Unit: percent.',
)

flags.DEFINE_integer(
    'diskspd_block_size',
    64,
    'The block size used when reading and writing data. '
    'Defaults: 64K. Unit: KB, '
    'can be set via --diskspd_block_unit',
)

flags.DEFINE_enum(
    'diskspd_block_unit',
    'K',
    ['K', 'M', 'G'],
    'The unit of the block size, available option: K|M|G. '
    'Will be used as the unit for --diskspd_block_size '
    'Defaults: K.',
)

flags.DEFINE_integer(
    'diskspd_stride_or_alignment',
    64,
    'If the access pattern is sequential, then this value'
    'means the stride for the access'
    'If the access pattern is random, then this value means'
    'the specified number of bytes that random I/O aligns to.'
    'Defaults: 64K. Unit: KB, can be set',
)

flags.DEFINE_enum(
    'diskspd_stride_or_alignment_unit',
    'K',
    ['K', 'M', 'G', 'b'],
    'The unit of the stride_or_alignment,available option: K|M|G|bDefaults: K.',
)

flags.DEFINE_bool(
    'diskspd_large_page',
    False,
    'Whether use large page for IO buffers. Defaults: False',
)

flags.DEFINE_bool(
    'diskspd_latency_stats',
    False,
    'Whether measure the latency statisticsDefaults: False',
)

flags.DEFINE_bool(
    'diskspd_disable_affinity',
    False,
    'Whether to diable the group affinity,'
    'group affinity is to round robin tasks. '
    'across processor group. '
    'Defaults: False',
)

flags.DEFINE_bool(
    'diskspd_write_through',
    True,
    'Whether to enable write through IO. Defaults: True',
)

flags.DEFINE_bool(
    'diskspd_software_cache',
    True,
    'Whether to disable software cachingDefaults: True',
)

flags.DEFINE_integer(
    'diskspd_outstanding_io',
    '2',
    'The number of outstanding I/O per thread per target.Defaults: 2.',
)

flags.DEFINE_integer(
    'diskspd_throughput_per_ms',
    None,
    'The throughput per thread per target. Defaults: None. Unit: bytes per ms.',
)

flags.DEFINE_integer(
    'diskspd_file_size',
    819200,
    'The file size DiskSpd will create when testing. '
    'Defaults: 819200. Unit: KB.',
)

flags.DEFINE_list(
    'diskspd_config_list',
    None,
    'comma separated list of configs to run with diskspd. The '
    'format for a single config is RANDOM_ACCESS:IS_READ:BLOCK_SIZE, '
    'for example FALSE:TRUE:64. '
    'Default Behavior: diskspd benchmark test will try to combine'
    '--diskspd_access_pattern, --diskspd_write_read_ratio, '
    '--diskspd_block_size together and form a set a config to run.',
)

DISKSPD_RETRIES = 10
DISKSPD_ZIP = 'DiskSpd.zip'
DISKSPD_URL = (
    'https://github.com/microsoft/diskspd/releases/download/v2.2/DiskSpd.ZIP'
)

DISKSPD_TMPFILE = 'testfile.dat'
DISKSPD_XMLFILE = 'result.xml'
DISKSPD_TIMEOUT_MULTIPLIER = 3

TRUE_VALS = ['True', 'true', 't', 'TRUE']
FALSE_VALS = ['False', 'false', 'f', 'FALSE']

# When adding new configs to diskspd_config_list, increase this value
_NUM_PARAMS_IN_CONFIG = 3

# named tuple used in passing configs around
DiskspdConf = collections.namedtuple(
    'DiskspdConf', ['access_pattern', 'write_ratio', 'block_size']
)


def DiskspdConfigListValidator(value):
  """Returns whether or not the config list flag is valid."""
  if not value:
    return True
  for config in value:
    config_vals = config.split(':')
    if len(config_vals) < _NUM_PARAMS_IN_CONFIG:
      return False
    try:
      is_random_access = config_vals[0]
      is_read = config_vals[1]
      block_size = int(config_vals[2])
    except ValueError:
      return False

    if is_random_access not in TRUE_VALS + FALSE_VALS:
      return False

    if is_read not in TRUE_VALS + FALSE_VALS:
      return False

    if block_size <= 0:
      return False
  return True


flags.register_validator(
    'diskspd_config_list', DiskspdConfigListValidator, 'malformed config list'
)


def ParseConfigList():
  """Get the list of configs for the test from the flags."""
  conf_list = []

  if FLAGS.diskspd_config_list is None:
    return [
        DiskspdConf(
            access_pattern=FLAGS.diskspd_access_pattern,
            write_ratio=FLAGS.diskspd_write_read_ratio,
            block_size=FLAGS.diskspd_block_size,
        )
    ]

  for config in FLAGS.diskspd_config_list:
    confs = config.split(':')

    conf_list.append(
        DiskspdConf(
            access_pattern='r' if (confs[0] in TRUE_VALS) else 's',
            write_ratio=0 if (confs[1] in TRUE_VALS) else 100,
            block_size=int(confs[2]),
        )
    )
  return conf_list


def Install(vm):
  """Installs the DiskSpd package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, DISKSPD_ZIP)
  vm.DownloadFile(DISKSPD_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def _RunDiskSpdWithOptions(vm, options):
  total_runtime = (
      FLAGS.diskspd_warmup + FLAGS.diskspd_cooldown + FLAGS.diskspd_duration
  )
  timeout_duration = total_runtime * DISKSPD_TIMEOUT_MULTIPLIER

  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  command = 'cd {diskspd_exe_dir}; .\\diskspd.exe {diskspd_options}'.format(
      diskspd_exe_dir=diskspd_exe_dir, diskspd_options=options
  )
  vm.RobustRemoteCommand(command, timeout=timeout_duration)


def _RemoveXml(vm):
  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  rm_command = 'cd {diskspd_exe_dir}; rm xml.txt'.format(
      diskspd_exe_dir=diskspd_exe_dir
  )
  vm.RemoteCommand(rm_command, ignore_failure=True)


def _CatXml(vm):
  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  cat_command = 'cd {diskspd_exe_dir}; cat {result_xml}'.format(
      diskspd_exe_dir=diskspd_exe_dir, result_xml=DISKSPD_XMLFILE
  )
  diskspd_xml, _ = vm.RemoteCommand(cat_command)
  return diskspd_xml


def _RemoveTempFile(vm):
  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  rm_command = 'cd {diskspd_exe_dir}; rm .\\{tmp_file_name}'.format(
      diskspd_exe_dir=diskspd_exe_dir, tmp_file_name=DISKSPD_TMPFILE
  )
  vm.RemoteCommand(rm_command, ignore_failure=True)


def _RunDiskSpd(
    running_vm, access_pattern, diskspd_write_read_ratio, block_size, metadata
):
  """Run single iteration of Diskspd test."""
  sending_options = _GenerateOption(
      access_pattern, diskspd_write_read_ratio, block_size
  )
  process_args = [(_RunDiskSpdWithOptions, (running_vm, sending_options), {})]
  background_tasks.RunParallelProcesses(process_args, 200)
  result_xml = _CatXml(running_vm)
  _RemoveTempFile(running_vm)
  _RemoveXml(running_vm)

  return ParseDiskSpdResults(result_xml, metadata)


def _GenerateOption(access_pattern, diskspd_write_read_ratio, block_size):
  """Generate running options from the given flags.

  Args:
    access_pattern: the access pattern of diskspd, 's' or 'r'
    diskspd_write_read_ratio: the ratio of writing compared to reading.
    block_size: the block size of read/ write.

  Returns:
    list of samples from the results of the diskspd tests.
  """

  large_page_string = '-l' if FLAGS.diskspd_large_page else ''
  latency_stats_string = '-L' if FLAGS.diskspd_latency_stats else ''
  disable_affinity_string = '-n' if FLAGS.diskspd_disable_affinity else ''
  software_cache_string = '-Su' if FLAGS.diskspd_software_cache else ''
  write_through_string = '-Sw' if FLAGS.diskspd_write_through else ''
  block_size_string = str(block_size) + str(FLAGS.diskspd_block_unit)
  access_pattern_string = (
      str(access_pattern)
      + str(FLAGS.diskspd_stride_or_alignment)
      + str(FLAGS.diskspd_stride_or_alignment_unit)
  )
  throughput_per_ms_string = ''
  if FLAGS.diskspd_throughput_per_ms:
    throughput_per_ms_string = '-g' + str(FLAGS.diskspd_throughput_per_ms)

  sending_options = (
      '-c{filesize}K -d{duration} -t{threadcount} '
      '-W{warmup} -C{cooldown} -Rxml -w{ratio} '
      '{large_page} {latency_stats} {disable_affinity} '
      '{software_cache} {write_through} {throughput}'
      '-b{block_size} -f{hint_string} -{access_pattern} '
      '-o{outstanding_io} -L '
      'C:\\scratch\\{tempfile} > {xmlfile}'
  ).format(
      filesize=FLAGS.diskspd_file_size,
      duration=FLAGS.diskspd_duration,
      threadcount=FLAGS.diskspd_thread_number_per_file,
      warmup=FLAGS.diskspd_warmup,
      cooldown=FLAGS.diskspd_cooldown,
      ratio=diskspd_write_read_ratio,
      tempfile=DISKSPD_TMPFILE,
      xmlfile=DISKSPD_XMLFILE,
      large_page=large_page_string,
      latency_stats=latency_stats_string,
      disable_affinity=disable_affinity_string,
      software_cache=software_cache_string,
      write_through=write_through_string,
      access_pattern=access_pattern_string,
      block_size=block_size_string,
      hint_string=access_pattern,
      throughput=throughput_per_ms_string,
      outstanding_io=FLAGS.diskspd_outstanding_io,
  )
  return sending_options


@vm_util.Retry(max_retries=DISKSPD_RETRIES)
def RunDiskSpd(running_vm):
  """Run Diskspd and return the samples collected from the run."""

  metadata = {}
  for k, v in running_vm.GetResourceMetadata().items():
    metadata['{}'.format(k)] = v

  # add the flag information to the metadata
  # some of the flags information has been included in the xml file
  metadata['diskspd_block_size_unit'] = FLAGS.diskspd_block_unit
  metadata['diskspd_stride_or_alignment'] = FLAGS.diskspd_stride_or_alignment
  metadata['diskspd_stride_or_alignment_unit'] = (
      FLAGS.diskspd_stride_or_alignment_unit
  )
  metadata['diskspd_large_page'] = FLAGS.diskspd_large_page
  metadata['diskspd_latency_stats'] = FLAGS.diskspd_latency_stats
  metadata['diskspd_disable_affinity'] = FLAGS.diskspd_disable_affinity
  metadata['diskspd_write_through'] = FLAGS.diskspd_write_through
  metadata['diskspd_software_cache'] = FLAGS.diskspd_software_cache
  metadata['diskspd_outstanding_io'] = FLAGS.diskspd_outstanding_io
  metadata['diskspd_throughput'] = FLAGS.diskspd_throughput_per_ms

  sample_list = []
  conf_list = ParseConfigList()

  # run diskspd in four different scenario, will generate a metadata list
  for conf in conf_list:
    sample_list.extend(
        _RunDiskSpd(
            running_vm,
            conf.access_pattern,
            conf.write_ratio,
            conf.block_size,
            metadata,
        )
    )

  return sample_list


def Prefill(running_vm):
  """Prefills the test file with random data using diskspd.

  Args:
    running_vm: The VM to prefill the file on.
  """
  if not FLAGS.diskspd_prefill:
    return
  prefill_duration = FLAGS.diskspd_prefill_duration
  if prefill_duration is None:
    raise errors.Setup.InvalidConfigurationError(
        '--diskspd_prefill_duration is None'
    )
  logging.info('Prefilling file with random data')
  diskspd_exe_dir = ntpath.join(running_vm.temp_dir, 'x86')
  diskspd_options = (
      f'-c{FLAGS.diskspd_file_size}K -t10 -w100 -b4k -d{prefill_duration} -r'
      f' C:\\scratch\\{DISKSPD_TMPFILE}'
  )
  command = f'cd {diskspd_exe_dir}; .\\diskspd.exe {diskspd_options}'
  running_vm.RobustRemoteCommand(command)


def ParseDiskSpdResults(result_xml, metadata):
  """Parses the xml output from DiskSpd and returns a list of samples.

  each list of sample only have one sample with read speed as value
  all the other information is stored in the meta data

  Args:
    result_xml: diskspd output
    metadata: the running info of vm

  Returns:
    list of samples from the results of the diskspd tests.
  """
  xml_root = xml.etree.ElementTree.fromstring(result_xml)
  metadata = metadata.copy()
  sample_data = []
  # Get the parameters from the sender XML output. Add all the
  # information of diskspd result to metadata
  for item in list(xml_root):
    if item.tag == 'TimeSpan':
      for subitem in list(item):
        if subitem.tag == 'Thread':
          target_item = subitem.find('Target')
          for read_write_info in list(target_item):
            if read_write_info.tag not in [
                'Path',
                'FileSize',
            ] and 'latency' not in read_write_info.tag.lower():
              # parsing latency metrics from the latency section
              if read_write_info.tag not in metadata:
                metadata[read_write_info.tag] = 0
              try:
                metadata[read_write_info.tag] += int(read_write_info.text)
              except ValueError:
                metadata[read_write_info.tag] += float(read_write_info.text)
        elif subitem.tag == 'CpuUtilization':
          target_item = subitem.find('Average')
          for cpu_info in list(target_item):
            metadata[cpu_info.tag] = cpu_info.text
        elif subitem.tag == 'Latency':
          for latency_info in list(subitem):
            if latency_info.tag.lower() == 'bucket':
              sample_data.extend(ParseLatencyBucket(latency_info))
            else:
              sample_data.append(
                  SampleData(
                      FormatLatencyMetricName(latency_info.tag),
                      latency_info.text,
                      'ms' if 'Milliseconds' in latency_info.tag else '',
                  )
              )
        else:
          metadata[subitem.tag] = subitem.text
    if item.tag == 'Profile':
      for subitem in list(item):
        if subitem.tag == 'TimeSpans':
          timespan_info = subitem.find('TimeSpan')
          for timespan_item in list(timespan_info):
            if timespan_item.tag == 'Targets':
              target_info = timespan_item.find('Target')
              for target_item in list(target_info):
                if target_item.tag == 'WriteBufferContent':
                  pattern_item = target_item.find('Pattern')
                  metadata[pattern_item.tag] = pattern_item.text
                else:
                  metadata[target_item.tag] = target_item.text
            else:
              metadata[timespan_item.tag] = timespan_item.text

  read_bytes = int(metadata['ReadBytes'])
  write_bytes = int(metadata['WriteBytes'])
  read_count = int(metadata['ReadCount'])
  write_count = int(metadata['WriteCount'])
  total_byte = int(metadata['BytesCount'])
  total_count = int(metadata['IOCount'])
  testtime = float(metadata['TestTimeSeconds'])

  # calculate the read and write speed (Byte -> MB)
  read_speed = int(read_bytes / testtime / 1024 / 1024)
  write_speed = int(write_bytes / testtime / 1024 / 1024)
  total_speed = int(total_byte / testtime / 1024 / 1024)

  # calculate the read write times per second
  read_iops = int(read_count / testtime)
  write_iops = int(write_count / testtime)
  total_iops = int(total_count / testtime)
  samples = [
      sample.Sample('total_speed', total_speed, 'MB/s', metadata),
      sample.Sample('total_iops', total_iops, '', metadata),
  ]
  if read_speed != 0:
    samples.extend([
        sample.Sample('read_speed', read_speed, 'MB/s', metadata),
        sample.Sample('read_iops', read_iops, '', metadata),
    ])
  if write_speed != 0:
    samples.extend([
        sample.Sample('write_speed', write_speed, 'MB/s', metadata),
        sample.Sample('write_iops', write_iops, '', metadata),
    ])
  samples.extend([
      sample.Sample(item.metric, item.value, item.unit, metadata)
      for item in sample_data
  ])
  return samples


def ParseLatencyBucket(latency_bucket):
  """Parse latency percentile data from bucket xml tag."""
  percentile = latency_bucket.find('Percentile')
  read_milliseconds = latency_bucket.find('ReadMilliseconds')
  write_milliseconds = latency_bucket.find('WriteMilliseconds')
  total_milliseconds = latency_bucket.find('TotalMilliseconds')
  if (
      percentile is None
      or total_milliseconds is None
  ):
    raise errors.Benchmarks.RunError(
        'Missing values in latency bucket info in diskspd xml output, please'
        f' check {xml.etree.ElementTree.tostring(latency_bucket)}'
    )
  percentile = float(percentile.text)
  if percentile not in LATENCY_PERCENTILES:
    return []
  sample_data = [
      SampleData(
          f'total_latency_p{percentile}', float(total_milliseconds.text), 'ms'
      )
  ]
  if read_milliseconds is not None:
    sample_data.append(
        SampleData(
            f'read_latency_p{percentile}', float(read_milliseconds.text), 'ms'
        )
    )
  if write_milliseconds is not None:
    sample_data.append(
        SampleData(
            f'write_latency_p{percentile}', float(write_milliseconds.text), 'ms'
        )
    )
  return sample_data


def FormatLatencyMetricName(text):
  text = text.replace('Milliseconds', '')
  # Convert camel case to snake case for latency metric name.
  text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
  if 'latency' not in text:
    return text + '_latency'
  return text
