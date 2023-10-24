# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License);
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
"""Collect Aerospike server data.

Calling Aerospike Admin to get summary info for the current health of the
Aerospike cluster.
"""
import logging
import os
import re
from absl import flags
from dateutil import parser
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import events
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker.traces import base_collector
import six

flags.DEFINE_boolean(
    'enable_asadm_log', False, 'Collect Aerospike cluster info.'
)

flags.DEFINE_integer(
    'asadm_interval_secs', 1, 'Interval of the metrics to collect.'
)

FLAGS = flags.FLAGS

MEMORY_USED_METRIC = 'memory_used'
MEMORY_USED_PERCENTAGES_METRIC = 'memory_used_percentages'
DISK_USED_METRIC = 'disk_used'
DISK_USED_PERCENTAGES_METRIC = 'disk_used_percentages'

DEFAULT_RESOURCE_SIZE_UNIT = 'GB'
GB_CONVERSION_FACTOR = {
    'TB': 1000.0,
    'GB': 1.0,
    'MB': 1 / 1000.0,
    'KB': 1 / (1000.0 ** 2),
    'B': 1 / (1000.0 ** 3)}


class _AsadmSummaryCollector(base_collector.BaseCollector):
  """Ops Agent collector.

  Installs Ops Agent and runs it on the VMs.
  """

  def _CollectorName(self):
    """See base class."""
    return 'AsadmSummary'

  def _CollectorRunCommand(self, vm, collector_file):
    """See base class."""
    vm.RemoteCommand(f'sudo touch {collector_file}')
    return (
        f'sudo asadm -e "watch {self.interval} summary" -o {collector_file} >'
        ' /dev/null 2>&1 & echo $!'
    )

  def Analyze(self, unused_sender, benchmark_spec, samples):
    """Analyze asadm summary file and record samples."""

    def _Analyze(role, f):
      """Parse file and record samples."""
      with open(
          os.path.join(self.output_directory, os.path.basename(f)), 'r'
      ) as fp:
        output = fp.read()
        metadata = {
            'event': 'asadm_summary',
            'interval': self.interval,
            'role': role,
        }
        _AnalyzeAsadmSummaryResults(metadata, output, samples)

    background_tasks.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)]
    )


def Register(parsed_flags):
  """Registers the ops agent collector if FLAGS.enable_asadm_log is set."""

  if not parsed_flags.enable_asadm_log:
    return
  logging.info('Registering asadm collector.')

  collector = _AsadmSummaryCollector(interval=FLAGS.asadm_interval_secs)
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
  events.benchmark_samples_created.connect(collector.Analyze, weak=False)


def _AnalyzeAsadmSummaryResults(metadata, output, samples):
  """Parse asadm result.

  Sample data:
    [ 2023-10-18 21:35:28 'summary' sleep: 2.0s iteration: 77 ]
    ~~~~~~~~~~~~~~~~~~~~~~~~~Cluster Summary~~~~~~~~~~~~~~~~~~~~~~~~~
    Migrations                |False
    Server Version            |E-6.2.0.19
    OS Version                |Ubuntu 20.04.6 LTS (5.15.0-1042-gcp)
    Cluster Size              |1
    Devices Total             |12
    Devices Per-Node          |12
    Devices Equal Across Nodes|True
    Memory Total              |1.087 TB
    Memory Used               |53.644 GB
    Memory Used %             |4.82
    Memory Avail              |1.034 TB
    Memory Avail%             |95.18
    Device Total              |24.000 TB
    Device Used               |898.540 GB
    Device Used %             |3.66
    Device Avail              |23.040 TB
    Device Avail%             |96.0
    License Usage Latest      |865.851 GB
    Active                    |1
    Total                     |2
    Active Features           |KVS
    Number of rows: 21

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Namespace
    Summary~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Namespace|~~~~Drives~~~~|~~~~~~~Memory~~~~~~~|~~~~~~~~Device~~~~~~~|Replication|
    Master|~License~~
            |Total|Per-Node|   Total|Used|Avail%|    Total|Used|Avail%|
            Factors|  Objects|~~Usage~~~
            |     |        |        |   %|      |         |   %|      |
            |         |    Latest
    bar      |    0|       0|4.000 GB| 0.0| 100.0|       --|  --|    --|
    2|  0.000  |  0.000 B
    test     |   12|      12|1.083 TB|4.84| 95.16|24.000 TB|3.66|  96.0|
    1|900.000 M|865.851 GB
    Number of rows: 2
    0. timestamp: 1234567890

  Args:
    metadata: metadata of the sample.
    output: the output of the stress-ng benchmark.
    samples: list of samples to return.
  """
  output_lines = output.splitlines()
  timestamps_in_ms = []
  memory_used = []
  memory_used_percentages = []
  disk_used = []
  disk_used_percentages = []
  for line in output_lines:
    if not line:  # Skip if the line is empty.
      continue
    if re.search(r'\[.*\]', line):
      timestamps_in_ms.append(ParseTimestamp(line))
      continue
    line_split = line.split('|')
    if not line_split or len(line_split) != 2:
      continue
    name = line_split[0].strip()
    value_str = line_split[1].strip()
    if name == 'Memory Used':
      value, unit = ParseUsedValue(value_str)
      memory_used.append(ConvertToGB(value, unit))
    elif name == 'Memory Used %':
      memory_used_percentages.append(float(value_str))
    elif name == 'Device Used':
      value, unit = ParseUsedValue(value_str)
      disk_used.append(ConvertToGB(value, unit))
    elif name == 'Device Used %':
      disk_used_percentages.append(float(value_str))

  effective_metric_length = len(timestamps_in_ms)
  if (
      not len(timestamps_in_ms)
      == len(memory_used)
      == len(memory_used_percentages)
      == len(disk_used)
      == len(disk_used_percentages)
  ):
    logging.warning(
        'Lists are not in the same length: timestamps[%d], memory_used[%d],'
        ' memory_used_percentages[%d], disk_used[%d],'
        ' disk_used_percentages[%d]',
        len(timestamps_in_ms),
        len(memory_used),
        len(memory_used_percentages),
        len(disk_used),
        len(disk_used_percentages),
    )
    effective_metric_length = min(
        len(timestamps_in_ms),
        len(memory_used),
        len(memory_used_percentages),
        len(disk_used),
        len(disk_used_percentages),
    )
  samples.extend([
      sample.CreateTimeSeriesSample(
          values=memory_used[:effective_metric_length],
          timestamps=timestamps_in_ms[:effective_metric_length],
          metric=MEMORY_USED_METRIC,
          units=DEFAULT_RESOURCE_SIZE_UNIT,
          interval=metadata['interval'],
      ),
      sample.CreateTimeSeriesSample(
          values=memory_used_percentages[:effective_metric_length],
          timestamps=timestamps_in_ms[:effective_metric_length],
          metric=MEMORY_USED_PERCENTAGES_METRIC,
          units='%',
          interval=metadata['interval'],
      ),
      sample.CreateTimeSeriesSample(
          values=disk_used[:effective_metric_length],
          timestamps=timestamps_in_ms[:effective_metric_length],
          metric=DISK_USED_METRIC,
          units=DEFAULT_RESOURCE_SIZE_UNIT,
          interval=metadata['interval'],
      ),
      sample.CreateTimeSeriesSample(
          values=disk_used_percentages[:effective_metric_length],
          timestamps=timestamps_in_ms[:effective_metric_length],
          metric=DISK_USED_PERCENTAGES_METRIC,
          units='%',
          interval=metadata['interval'],
      ),
  ])


def ParseTimestamp(line: str) -> float:
  """Convert a timestamp string to an epoch time.

  Args:
    line: the line with a timestamp string.

  Returns:
    The epoch time in ms (float) for the given timestamp.
  """
  if not str:
    raise ValueError('Timestamp in wrong format: emptry string')
  timestamp = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)[0]
  return sample.ConvertDateTimeToUnixMs(parser.parse(timestamp))


def ParseUsedValue(value: str) -> (float, str):
  """Parse the Aerospike output with unit.

  Args:
    value: The output from Aerospike, which contains a number and its unit.

  Returns:
    A tuple of a number and its unit.
  """
  splits = value.split()
  if len(splits) != 2:
    raise ValueError('Used value in wrong format: %s' % value)
  return float(splits[0]), splits[1]


def ConvertToGB(value: str, unit: str) -> float:
  """Convert value to GB so all records uses the same unit.

  Args:
    value: The data to be converted.
    unit: The unit of the data.

  Returns:
    The value in GB.
  """
  if unit.upper() not in GB_CONVERSION_FACTOR:
    raise ValueError(f'Support GB|TB but get {unit.upper()}')
  return value * GB_CONVERSION_FACTOR[unit.upper()]
