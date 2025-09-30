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
import copy
import logging
import ntpath
import re
import time
import xml.etree.ElementTree

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

LATENCY_PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999, 99.9999, 99.99999]
SampleData = collections.namedtuple('SampleData', ['metric', 'value', 'unit'])

flags.DEFINE_integer(
    'diskspd_prefill_duration',
    None,
    'In seconds. Diskspd needs a duration to run. For prefilling, use a'
    ' duration that is large enough to allow diskspd to write the file to the'
    ' required size.',
)

flags.DEFINE_integer(
    'diskspd_duration',
    300,
    'The number of seconds to run diskspd test.Defaults to 300s. Unit:'
    ' seconds.',
)

flags.DEFINE_integer(
    'diskspd_warmup',
    120,
    'The warm up time for diskspd, the time needed to enter'
    'steady state of I/O operation. '
    'Defaults to 120s. Unit: seconds.',
)

flags.DEFINE_integer(
    'diskspd_cooldown',
    30,
    'The cool down time for diskspd, the time to ensure that'
    'each instance of diskspd is active during each'
    'measurement period of each instance. '
    'Defaults: 30s. Unit: seconds',
)

flags.DEFINE_enum(
    'diskspd_access_pattern',
    's',
    ['s', 'r', 'si'],
    'the access patten of the read and write'
    'the performance will be downgrade a little bit if use'
    'different hints'
    'available option: r|s|si, '
    'r: random access'
    's: sequential access. '
    'si: A single interlocked offset shared between all threads, prevents'
    'overlapping I/Os. '
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

DISKSPD_BLOCK_SIZE = flags.DEFINE_string(
    'diskspd_block_size',
    '4K',
    'The block size used when reading and writing data. '
    'Defaults: 4K. Please use K|M|G to specify unit.',
)

flags.register_validator(
    DISKSPD_BLOCK_SIZE.name,
    lambda x: x[-1] in ['K', 'M', 'G'],
    'Please specify Unit for --diskspd_block_size',
)

DISKSPD_STRIDE = flags.DEFINE_string(
    'diskspd_stride',
    None,
    'Stride is for sequential access, specifies the offset for an IO operation.'
    ' For example, if a 64KiB stride is chosen for a 4KB block size, the first'
    ' I/O will be at zero, the second at 64KB and so forth. Please use B|K|M|G'
    ' to specify unit. Defaults: None.',
)

flags.register_validator(
    DISKSPD_STRIDE,
    lambda x: x is None or x[-1] in ['B', 'K', 'M', 'G']
    and FLAGS.diskspd_access_pattern != 'r',
    message=(
        'Diskspd_stride is only supported for sequential access pattern. Please'
        ' specify Unit for --diskspd_stride.'
    ),
)

flags.DEFINE_bool(
    'diskspd_large_page',
    False,
    'Whether use large page for IO buffers. Defaults: False',
)

flags.DEFINE_bool(
    'diskspd_latency_stats',
    True,
    'Whether measure the latency statistics. Defaults: True',
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
    'Whether to disable software caching. Defaults: True',
)

DISKSPD_OUTSTANDING_IO = flag_util.DEFINE_integerlist(
    'diskspd_outstanding_io',
    flag_util.IntegerList([1]),
    'The number of outstanding I/O per thread per target.Defaults: 1.',
    on_nonincreasing=flag_util.IntegerListParser.WARN,
    module_name=__name__,
)

DISKSPD_THREAD_NUMBER_PER_FILE = flag_util.DEFINE_integerlist(
    'diskspd_thread_number_per_file',
    flag_util.IntegerList([1]),
    'The number of threads per file. Defaults: 1.',
    on_nonincreasing=flag_util.IntegerListParser.WARN,
    module_name=__name__,
)

flags.DEFINE_integer(
    'diskspd_throughput_per_ms',
    None,
    'The throughput per thread per target. Defaults: None. Unit: bytes per ms.',
)

DISKSPD_FILE_SIZE = flags.DEFINE_string(
    'diskspd_file_size',
    '8K',
    'The file size DiskSpd will create when testing. Defaults: 8K. Please use'
    ' K|M|G to specify unit.',
)

flags.register_validator(
    DISKSPD_FILE_SIZE.name,
    lambda x: x[-1] in ['K', 'M', 'G'],
    'Please specify Unit for --diskspd_file_size',
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

# named tuple used in passing configs around
DiskspdConf = collections.namedtuple(
    'DiskspdConf', ['access_pattern', 'write_ratio', 'block_size']
)


def Install(vm):
  """Installs the DiskSpd package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, DISKSPD_ZIP)
  vm.DownloadFile(DISKSPD_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def _RunDiskSpdWithOptions(vm, options):
  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  command = f'cd {diskspd_exe_dir}; .\\diskspd.exe {options}'
  vm.RobustRemoteCommand(command)


def _RemoveXml(vm):
  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  rm_command = f'cd {diskspd_exe_dir}; rm {DISKSPD_XMLFILE}'
  vm.RemoteCommand(rm_command, ignore_failure=True)


def _CatXml(vm):
  diskspd_exe_dir = ntpath.join(vm.temp_dir, 'x86')
  cat_command = f'cd {diskspd_exe_dir}; cat {DISKSPD_XMLFILE}'
  diskspd_xml, _ = vm.RemoteCommand(cat_command)
  return diskspd_xml


def _RemoveTempFile(vm):
  rm_command = f'rm C:\\scratch\\{DISKSPD_TMPFILE}'
  vm.RemoteCommand(rm_command, ignore_failure=True)


def EnablePrefill():
  return (
      FLAGS.diskspd_prefill_duration is not None
      and FLAGS.diskspd_prefill_duration != 0
  )


def _RunDiskSpd(running_vm, outstanding_io, threads, metadata):
  """Run single iteration of Diskspd test."""
  diskspd_config = _GenerateDiskspdConfig(outstanding_io, threads)
  try:
    _RunDiskSpdWithOptions(running_vm, diskspd_config)
    result_xml = _CatXml(running_vm)
    if not EnablePrefill():
      # Only remove the temp file if we did not prefill the file.
      _RemoveTempFile(running_vm)
    _RemoveXml(running_vm)
    return ParseDiskSpdResults(result_xml, metadata)
  except errors.VirtualMachine.RemoteCommandError as e:
    # Diskspd gets unstable for higher pending requests and doesn't return
    # results.
    if 'Got non-zero return code (1) executing' in str(e):
      logging.exception(
          'Diskspd failed to run for %s threads and %s outstanding IOs',
          threads,
          outstanding_io,
      )
      return []
    raise e


def _GenerateDiskspdConfig(outstanding_io, threads):
  """Generate running options from the given flags."""

  large_page_string = '-l' if FLAGS.diskspd_large_page else ''
  latency_stats_string = '-L' if FLAGS.diskspd_latency_stats else ''
  disable_affinity_string = '-n' if FLAGS.diskspd_disable_affinity else ''
  software_cache_string = '-Su' if FLAGS.diskspd_software_cache else ''
  write_through_string = '-Sw' if FLAGS.diskspd_write_through else ''
  access_pattern = FLAGS.diskspd_access_pattern
  diskspd_write_read_ratio = FLAGS.diskspd_write_read_ratio
  diskspd_block_size = FLAGS.diskspd_block_size
  os_hint = access_pattern
  if os_hint == 'si':
    os_hint = 's'
  if DISKSPD_STRIDE.value:
    access_pattern = (
        str(access_pattern)
        + DISKSPD_STRIDE.value
    )

  throughput_per_ms_string = ''
  if FLAGS.diskspd_throughput_per_ms:
    throughput_per_ms_string = '-g' + str(FLAGS.diskspd_throughput_per_ms)

  return (
      f'-c{FLAGS.diskspd_file_size} -d{FLAGS.diskspd_duration}'
      f' -t{threads} -o{outstanding_io} {latency_stats_string} -W{FLAGS.diskspd_warmup}'
      f' -C{FLAGS.diskspd_cooldown} -Rxml -w{diskspd_write_read_ratio}'
      f' {large_page_string} {disable_affinity_string}'
      f' {software_cache_string} {write_through_string}'
      f' {throughput_per_ms_string} -b{diskspd_block_size}'
      f' -{access_pattern} -f{os_hint}'
      f' F:\\{DISKSPD_TMPFILE} > {DISKSPD_XMLFILE}'
  )


@vm_util.Retry(max_retries=DISKSPD_RETRIES)
def RunDiskSpd(running_vm):
  """Run Diskspd and return the samples collected from the run."""

  metadata = {}
  for k, v in running_vm.GetResourceMetadata().items():
    metadata['{}'.format(k)] = v

  # add the flag information to the metadata
  # some of the flags information has been included in the xml file
  metadata['diskspd_stride'] = DISKSPD_STRIDE.value
  metadata['diskspd_large_page'] = FLAGS.diskspd_large_page
  metadata['diskspd_latency_stats'] = FLAGS.diskspd_latency_stats
  metadata['diskspd_disable_affinity'] = FLAGS.diskspd_disable_affinity
  metadata['diskspd_write_through'] = FLAGS.diskspd_write_through
  metadata['diskspd_software_cache'] = FLAGS.diskspd_software_cache
  metadata['diskspd_throughput'] = FLAGS.diskspd_throughput_per_ms
  metadata['diskspd_write_read_ratio'] = FLAGS.diskspd_write_read_ratio
  metadata['diskspd_access_pattern'] = FLAGS.diskspd_access_pattern
  metadata['diskspd_block_size'] = FLAGS.diskspd_block_size

  sample_list = []
  # We can use io array or thread array for calculate max IOPs
  # or max throughput.
  outstanding_io_list = sorted(FLAGS.diskspd_outstanding_io)
  threads_list = sorted(FLAGS.diskspd_thread_number_per_file)
  for outstanding_io in outstanding_io_list:
    for threads in threads_list:
      metadata = copy.deepcopy(metadata)
      metadata['outstanding_io'] = outstanding_io
      metadata['threads_per_file'] = threads
      try:
        sample_list.extend(
            _RunDiskSpd(
                running_vm,
                outstanding_io,
                threads,
                metadata,
            )
        )
        time.sleep(
            60
        )  # Sleep for 60 seconds after each test run for disk cooldown.
      except errors.VirtualMachine.RemoteCommandError as e:
        if 'Could not allocate a buffer for target' in str(e):
          logging.exception(
              'Diskspd is not able to allocate buffer for this configuration,'
              ' try using smaller block size or reduce outstanding io or'
              ' threads: %s',
              e,
          )
          if not sample_list:
            # No successful run till now, higher outstanding io and/or thread
            # count won't be successful.
            raise e
        else:
          raise e
  if not sample_list:
    raise errors.Benchmarks.RunError(
        'Diskspd failed to run for all thread/IO configurations. Review the'
        ' logs and try smaller requests.'
    )
  sample_list.extend(_CalculateMaxMetric(sample_list))
  return sample_list


def _CalculateMaxMetric(sample_list):
  """Calculate the max metric from the sample list."""
  iops_metric = 'total_iops'
  throughput_metric = 'total_bandwidth'
  max_iops_sample = None
  max_throughput_sample = None
  max_samples = []
  for sample_item in sample_list:
    if sample_item.metric == iops_metric and (
        max_iops_sample is None or sample_item.value > max_iops_sample.value
    ):
      max_iops_sample = sample_item
    if sample_item.metric == throughput_metric and (
        max_throughput_sample is None
        or sample_item.value > max_throughput_sample.value
    ):
      max_throughput_sample = sample_item
  if max_iops_sample is not None:
    max_samples.append(
        sample.Sample(
            'max_' + iops_metric,
            max_iops_sample.value,
            max_iops_sample.unit,
            max_iops_sample.metadata,
        )
    )
  if max_throughput_sample is not None:
    max_samples.append(
        sample.Sample(
            'max_' + throughput_metric,
            max_throughput_sample.value,
            max_throughput_sample.unit,
            max_throughput_sample.metadata,
        )
    )
  return max_samples


def Prefill(running_vm):
  """Prefills the test file with random data using diskspd.

  Args:
    running_vm: The VM to prefill the file on.
  """
  if not EnablePrefill():
    logging.info('Prefill duration is not set, skipping prefill.')
    return
  logging.info('Prefilling file with random data')
  prefill_duration = FLAGS.diskspd_prefill_duration
  diskspd_exe_dir = ntpath.join(running_vm.temp_dir, 'x86')
  diskspd_options = (
      f'-c{FLAGS.diskspd_file_size} -t16 -w100 -b4k -d{prefill_duration} -Rxml'
      f' -Sw -Su -o16 -r C:\\scratch\\{DISKSPD_TMPFILE} >'
      f' {DISKSPD_XMLFILE}'
  )
  command = f'cd {diskspd_exe_dir}; .\\diskspd.exe {diskspd_options}'
  running_vm.RobustRemoteCommand(command)
  result_xml = _CatXml(running_vm)
  _RemoveXml(running_vm)

  prefill_samples = ParseDiskSpdResults(result_xml, {})
  for prefill_sample in prefill_samples:
    if prefill_sample.metric != 'write_throughput':
      continue
    write_throughput = prefill_sample.value
    total_seconds = float(prefill_sample.metadata['TestTimeSeconds'])
    total_data_written, unit = GetDiskspdFileSizeInPrefillSizeUnit(
        write_throughput, total_seconds
    )
    logging.info('Prefill Data written: %s %s', total_data_written, unit)
    if total_data_written < float(DISKSPD_FILE_SIZE.value[0:-1]):
      logging.error(
          'Prefill data written is less than the file size, please check the'
          ' prefill duration.'
      )


def GetDiskspdFileSizeInPrefillSizeUnit(
    write_throughput: float, test_duration: float
) -> float:
  """Returns the diskspd file size in GB."""
  unit = DISKSPD_FILE_SIZE.value[-1]
  written_data = 0
  if unit == 'G':
    written_data = (write_throughput * test_duration) / 1024
  elif unit == 'M':
    written_data = write_throughput * test_duration
  elif unit == 'K':
    written_data = (write_throughput * test_duration) * 1024
  return written_data, unit


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
  samples = []
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
          samples.append(ParseCpuUtilizationAsSample(subitem, metadata.copy()))
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
  read_bandwidth = ConvertTotalBytesToMBPerSecond(read_bytes, testtime)
  write_bandwidth = ConvertTotalBytesToMBPerSecond(write_bytes, testtime)
  total_bandwidth = ConvertTotalBytesToMBPerSecond(total_byte, testtime)

  # calculate the read write times per second
  read_iops = int(read_count / testtime)
  write_iops = int(write_count / testtime)
  total_iops = int(total_count / testtime)
  samples.extend([
      sample.Sample('total_bandwidth', total_bandwidth, 'MB/s', metadata),
      sample.Sample('total_iops', total_iops, '', metadata),
  ])
  if read_bandwidth != 0:
    samples.extend([
        sample.Sample('read_bandwidth', read_bandwidth, 'MB/s', metadata),
        sample.Sample('read_iops', read_iops, '', metadata),
    ])
  if write_bandwidth != 0:
    samples.extend([
        sample.Sample('write_bandwidth', write_bandwidth, 'MB/s', metadata),
        sample.Sample('write_iops', write_iops, '', metadata),
    ])
  samples.extend([
      sample.Sample(item.metric, item.value, item.unit, metadata)
      for item in sample_data
  ])
  return samples


def ConvertTotalBytesToMBPerSecond(
    bytes_value: int, test_duration: float
) -> float:
  """Converts total bytes to MB per second."""
  return bytes_value / test_duration / 1024 / 1024


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


def ParseCpuUtilizationAsSample(xml_root, metadata):
  """Parse CPU Utilization from cpu xml tag."""
  per_cpu_usage = {}
  average_cpu_usage = {}
# Sample CPU tag:
# <CPU>
#         <Socket>0</Socket>
#         <Node>0</Node>
#         <Group>0</Group>
#         <Core>0</Core>
#         <EfficiencyClass>0</EfficiencyClass>
#         <Id>0</Id>
#         <UsagePercent>27.89</UsagePercent>
#         <UserPercent>2.68</UserPercent>
#         <KernelPercent>25.21</KernelPercent>
#         <IdlePercent>72.11</IdlePercent>
# </CPU>
# <CPU>
#         <Socket>0</Socket>
#         <Node>0</Node>
#         <Group>0</Group>
#         <Core>0</Core>
#         <EfficiencyClass>0</EfficiencyClass>
#         <Id>1</Id>
#         <UsagePercent>98.38</UsagePercent>
#         <UserPercent>0.47</UserPercent>
#         <KernelPercent>97.91</KernelPercent>
#         <IdlePercent>1.62</IdlePercent>
# </CPU>
  for item in list(xml_root):
    if item.tag == 'CPU':
      # this tag shows per cpu core usage
      cpu_socket = item.find('Socket')
      cpu_node = item.find('Node')
      cpu_group = item.find('Group')
      cpu_core = item.find('Core')
      cpu_id = item.find('Id')
      cpu_key_parts = []
      if cpu_socket is not None:
        cpu_key_parts.append(cpu_socket.text)
      if cpu_node is not None:
        cpu_key_parts.append(cpu_node.text)
      if cpu_group is not None:
        cpu_key_parts.append(cpu_group.text)
      if cpu_core is not None:
        cpu_key_parts.append(cpu_core.text)
      if cpu_id is not None:
        cpu_key_parts.append(cpu_id.text)
      cpu_key = '_'.join(cpu_key_parts)
      cpu_usage = ParseCpuUsageTags(item)
      per_cpu_usage[f'usage_cpu_{cpu_key}'] = cpu_usage
    elif item.tag == 'Average':
      average_cpu_usage = ParseCpuUsageTags(item)
  metadata['per_cpu_usage'] = per_cpu_usage
  metadata['average_cpu_usage'] = average_cpu_usage
  return sample.Sample(
      'cpu_total_utilization',
      average_cpu_usage['cpu_total_utilization'],
      '',
      metadata,
  )


def ParseCpuUsageTags(cpu_tag):
  """Parse CPU Utilization from cpu xml tag."""
  usage_percent = cpu_tag.find('UsagePercent')
  user_percent = cpu_tag.find('UserPercent')
  kernel_percent = cpu_tag.find('KernelPercent')
  idle_percent = cpu_tag.find('IdlePercent')
  if (
      usage_percent is None
      or user_percent is None
      or kernel_percent is None
      or idle_percent is None
  ):
    raise errors.Benchmarks.RunError(
        'Cpu utilization info missing in diskspd xml output'
    )
  return {
      'cpu_total_utilization': float(usage_percent.text),
      'cpu_user_percent': float(user_percent.text),
      'cpu_kernel_percent': float(kernel_percent.text),
      'cpu_idle_percent': float(idle_percent.text),
  }


def FormatLatencyMetricName(text):
  text = text.replace('Milliseconds', '')
  # Convert camel case to snake case for latency metric name.
  text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
  if 'latency' not in text:
    return text + '_latency'
  return text
