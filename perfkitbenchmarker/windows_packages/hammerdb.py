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


"""Module containing HammerDB installation and cleanup functions.

HammerDB is a tool made for benchmarking sql performance.

More information about HammerDB may be found here:
https://www.hammerdb.com/index.html
"""

import ntpath
import os
import re

import numpy as np

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import data

FLAGS = flags.FLAGS

flags.DEFINE_list('hammerdb_tpcc_virtual_user_list', [1],
                  'The list of numbers of virtual users making '
                  'transaction at the same time. '
                  'Default: [1]')

flags.DEFINE_integer('hammerdb_tpcc_warehouse', 1,
                     'The number of warehouse used in tpcc benchmarking. '
                     'Default: 1')

flags.DEFINE_integer('hammerdb_tpcc_schema_virtual_user', 1,
                     'The number of virtual user used when '
                     'building the schema. '
                     'Default: 1')

flags.DEFINE_integer('hammerdb_tpcc_runtime', 60,
                     'The running time for tpcc benchmark test'
                     'Default: 60. Unit: second')

flags.DEFINE_bool('hammerdb_run_tpcc', True,
                  'tpcc is a sql benchmark to measure transaction speed.'
                  'Default: True')

flags.DEFINE_bool('hammerdb_run_tpch', True,
                  'tpch is a sql benchmark to calculate the query per hour'
                  'performance metrics.'
                  'Default: True')

flags.DEFINE_integer('hammerdb_tpch_scale_fact', 1,
                     'The running time for tpcc benchmark test. '
                     'Default: 60. Unit: second')

flags.DEFINE_integer('hammerdb_tpch_virtual_user', 4,
                     'The virtual user number to run tpch test. '
                     'Default: 4.')


HAMMERDB_RETRIES = 10
HAMMERDB_DIR = 'HammerDB-3.1-Win'
HAMMERDB_ZIP = HAMMERDB_DIR + '.zip'
HAMMERDB_URL = ('https://versaweb.dl.sourceforge.net/project/'
                'hammerdb/HammerDB/HammerDB-3.1/' + HAMMERDB_ZIP)
HAMMERDB_LOGFILE = 'hammerdb.log'

# the files that are dynamically generated on the vm
# to be run by the hammerdb test
HAMMERDB_SCHEMA_FILE = 'schema.tcl'
HAMMERDB_SQLRUN_FILE = 'hammsqlrun.tcl'
HAMMERDB_SCHEMA_FILE_TPCH = 'schema-tpch.tcl'
HAMMERDB_SQLRUN_FILE_TPCH_POWER = 'sqlrun-tpch-power.tcl'
HAMMERDB_SQLRUN_FILE_TPCH_THROUGHPUT = 'sqlrun-tpch-throughput.tcl'
HAMMERDB_CLI_FILE = 'hammerdbcli'
HAMMERDB_CLI_FILE_TPCH = 'hammerdbclitpch'
HAMMERDB_CLI_BAT_FILE = 'hammerdbcli.bat'
HAMMERDB_CLI_BAT_FILE_TPCH = 'hammerdbclitpch.bat'

HAMMERDB_TEST_TIMEOUT_MULTIPLIER = 2
HAMMERDB_SCHEMA_WAITTIME = 5000
HAMMERDB_SQLRUN_WAITIME_ADDON = 80
HAMMERDB_CREATE_FILE_TIMEOUT = 10
HAMMERDB_DB_CONFIG_TIMEOUT = 200

HAMMERDB_SCALE_TO_STREAMS = {
    '1': 2,
    '10': 3,
    '30': 4,
    '100': 5,
    '300': 6,
    '1000': 7,
    '3000': 8,
    '10000': 9,
    '30000': 10,
    '100000': 11
}


def _GetDataContents(file_name):
  """Cet the files in the data folder."""
  path = data.ResourcePath('hammerdb/' + filename)
  with open(path) as fp:
    contents = fp.read()
  return contents


def Install(vm):
  """Installs the HammerDB package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, HAMMERDB_DIR)
  vm.DownloadFile(HAMMERDB_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def _CreateSingleScript(vm, contents, filename):
  """Create a single file named as <filename> with <contents> as contents."""
  hammerdb_exe_dir = ntpath.join(vm.temp_dir, 'HammerDB-3.1')
  command = ('cd {hammerdb_exe_dir}; echo \"{contents}\" > .\\tmp.txt; '
             'cat tmp.txt | Out-File -FilePath {filename}'
             ' -Encoding ascii').format(
                 hammerdb_exe_dir=hammerdb_exe_dir,
                 contents=contents,
                 filename=filename)
  vm.RemoteCommand(command, timeout=HAMMERDB_CREATE_FILE_TIMEOUT)


HAMMERDB_SCHEMA_FILE = 'schema.tcl'
HAMMERDB_SQLRUN_FILE = 'sqlrun.tcl'
HAMMERDB_SCHEMA_FILE_TPCH = 'schema-tpch.tcl'
HAMMERDB_SQLRUN_FILE_TPCH_POWER = 'sqlrun-tpch-power.tcl'
HAMMERDB_SQLRUN_FILE_TPCH_THROUGHPUT = 'sqlrun-tpch-throughput.tcl'
HAMMERDB_CLI_FILE = 'hammerdbcli'
HAMMERDB_CLI_FILE_TPCH = 'hammerdbclitpch'
HAMMERDB_CLI_BAT_FILE = 'hammerdbcli.bat'
HAMMERDB_CLI_BAT_FILE_TPCH = 'hammerdbclitpch.bat'


def _CreateFiles(vm):
  """Create the file dynamically used by the hammerdb.

  This function creates the following files:
    - schema.tcl and schema-tpch.tcl, the files for hammerdb to
        build the schema of tpcc and tpch benchmarks
    - sqlrun.tcl, sqlrun-tpch-power.tcl and sqlrun-tpch-throughput.tcl, the
        benchmark test script that does the actual measurements.
    - hammerdbcli, hammerdbclitpch, hammerdbcli.bat and hammerdbclitpch.bat, the
        cli tool and batch file for starting the hammerdb and run the different
        scripts of each benchmarking stage.
  """
  # create the content for the schema building file of tpcc
  schema = _GetDataContents('hammerdb_schema_tpcc.txt').replace(
      '*ware_house_num*', str(FLAGS.hammerdb_tpcc_warehouse)).replace(
          '*virtual_user_num*', str(FLAGS.hammerdb_tpcc_schema_virtual_user))

  # create the content for the tpcc benchmark run time file.
  virtual_user_seq = ''
  for virtual_user_num in FLAGS.hammerdb_tpcc_virtual_user_list:
    virtual_user_seq += str(virtual_user_num)
    virtual_user_seq += ' '
  sqlrun = _GetDataContents('hammerdb_run_tpcc.txt').replace(
      '*virtual_user_seq*', virtual_user_seq).replace(
          '*timer*', str(FLAGS.hammerdb_tpcc_runtime + 60)).replace(
              '*duration*', str(FLAGS.hammerdb_tpcc_runtime / 60))

  whether_run_tpcc = 'true' if FLAGS.hammerdb_run_tpcc else 'false'
  whether_run_tpch = 'true' if FLAGS.hammerdb_run_tpch else 'false'

  # create the content for the tpcc cli tool for tun time.
  cli = _GetDataContents('hammerdb_cli_tpcc.txt').replace(
      '*schema_file_name*', HAMMERDB_SCHEMA_FILE).replace(
          '*sqlrun_file_name*', HAMMERDB_SQLRUN_FILE).replace(
              '*whether_run_tpcc*', whether_run_tpcc)

  # create the content for the tpch cli tool for tun time.
  cli_tpch = _GetDataContents('hammerdb_cli_tpch.txt').replace(
      '*whether_run_tpch*', whether_run_tpch)
  cli_tpch = cli_tpch.replace('*schema_file_name_tpch*',
                              HAMMERDB_SCHEMA_FILE_TPCH)
  cli_tpch = cli_tpch.replace('*sqlrun_power_file_name*',
                              HAMMERDB_SQLRUN_FILE_TPCH_POWER)
  cli_tpch = cli_tpch.replace('*sqlrun_throughput_file_name*',
                              HAMMERDB_SQLRUN_FILE_TPCH_THROUGHPUT)

  cli_bat = _GetDataContents('hammerdb_cli_bat_tpcc.txt')
  cli_bat_tpch = _GetDataContents('hammerdb_cli_bat_tpch.txt')

  schema_tpch = _GetDataContents('hammerdb_schema_tpch.txt')
  sqlrun_tpch_power = _GetDataContents('hammerdb_run_tpch.txt').replace(
      '*virtual_user*', str(1)).replace(
          '*test_sequence_complete_sentence*', '\"TPCH POWER COMPLETE\"')
  sqlrun_tpch_throughput = _GetDataContents('hammerdb_run_tpch.txt').replace(
      '*virtual_user*', str(FLAGS.hammerdb_tpch_virtual_user)).replace(
          '*test_sequence_complete_sentence*', '\"TPCH THROUGHPUT COMPLETE\"')

  schema = schema.replace('\"', '`\"')
  sqlrun = sqlrun.replace('\"', '`\"')
  schema_tpch = schema_tpch.replace('\"', '`\"')
  sqlrun_tpch_power = sqlrun_tpch_power.replace('\"', '`\"')
  sqlrun_tpch_throughput = sqlrun_tpch_throughput.replace('\"', '`\"')
  cli = cli.replace('\"', '`\"')
  cli_tpch = cli_tpch.replace('\"', '`\"')
  cli_bat = cli_bat.replace('\"', '`\"')
  cli_bat_tpch = cli_bat_tpch.replace('\"', '`\"')

  # create the necessary files of running hammerdb
  _CreateSingleScript(vm, schema, HAMMERDB_SCHEMA_FILE)
  _CreateSingleScript(vm, sqlrun, HAMMERDB_SQLRUN_FILE)
  _CreateSingleScript(vm, cli, HAMMERDB_CLI_FILE)
  _CreateSingleScript(vm, cli_bat, HAMMERDB_CLI_BAT_FILE)
  _CreateSingleScript(vm, cli_tpch, HAMMERDB_CLI_FILE_TPCH)
  _CreateSingleScript(vm, cli_bat_tpch, HAMMERDB_CLI_BAT_FILE_TPCH)
  _CreateSingleScript(vm, schema_tpch, HAMMERDB_SCHEMA_FILE_TPCH)
  _CreateSingleScript(vm, sqlrun_tpch_power, HAMMERDB_SQLRUN_FILE_TPCH_POWER)
  _CreateSingleScript(vm, sqlrun_tpch_throughput,
                      HAMMERDB_SQLRUN_FILE_TPCH_THROUGHPUT)


def _CatFile(vm, filename):
  """Cat out the content of a file."""
  hammerdb_exe_dir = ntpath.join(vm.temp_dir, 'HammerDB-3.1')
  command = 'cd {hammerdb_exe_dir}; cat {filename}'.format(
      hammerdb_exe_dir=hammerdb_exe_dir, filename=filename)
  cat_output, _ = vm.RemoteCommand(command)
  return cat_output


def _RunHammerDbTPCC(vm):
  """Run the tpcc benchmark by starting the batch script."""
  hammerdb_exe_dir = ntpath.join(vm.temp_dir, 'HammerDB-3.1')
  command = 'cd {hammerdb_exe_dir}; .\\hammerdbcli.bat'.format(
      hammerdb_exe_dir=hammerdb_exe_dir)
  total_time_out = ((HAMMERDB_SCHEMA_WAITTIME + HAMMERDB_SQLRUN_WAITIME_ADDON +
                     FLAGS.hammerdb_tpcc_runtime) *
                    HAMMERDB_TEST_TIMEOUT_MULTIPLIER)
  vm.RemoteCommand(command, timeout=total_time_out)


def _RunHammerDbTPCH(vm):
  """Run the tpch benchmark by starting the batch script."""
  hammerdb_exe_dir = ntpath.join(vm.temp_dir, 'HammerDB-3.1')
  command = 'cd {hammerdb_exe_dir}; .\\hammerdbclitpch.bat'.format(
      hammerdb_exe_dir=hammerdb_exe_dir)
  total_time_out = ((HAMMERDB_SCHEMA_WAITTIME + HAMMERDB_SQLRUN_WAITIME_ADDON +
                     FLAGS.hammerdb_tpcc_runtime) *
                    HAMMERDB_TEST_TIMEOUT_MULTIPLIER)
  vm.RemoteCommand(command, timeout=total_time_out)


@vm_util.Retry(max_retries=HAMMERDB_RETRIES)
def RunHammerDB(vm):
  """Run HammerDB and return the samples collected from the run."""

  _CreateFiles(vm)

  if FLAGS.hammerdb_run_tpcc:
    _RunHammerDbTPCC(vm)
  if FLAGS.hammerdb_run_tpch:
    _RunHammerDbTPCH(vm)

  hammer_result = _CatFile(vm, 'C://hammerdb.log')

  metadata = {}
  for k, v in vm.GetResourceMetadata().iteritems():
    metadata[k] = v

  metadata['hammerdb_tpcc_warehouse'] = FLAGS.hammerdb_tpcc_warehouse
  metadata['hammerdb_tpcc_runtime'] = FLAGS.hammerdb_tpcc_runtime
  metadata['hammerdb_run_tpcc'] = FLAGS.hammerdb_run_tpcc
  metadata['hammerdb_run_tpch'] = FLAGS.hammerdb_run_tpch

  return _ParseHammerDBResults(hammer_result, metadata,
                               FLAGS.hammerdb_tpcc_virtual_user_list)


def _ParseHammerDBResults(result, metadata, virtual_user_list):
  samples = []
  if FLAGS.hammerdb_run_tpcc:
    samples.extend(ParseHammerDBResultTPCC(result, metadata, virtual_user_list))
  if FLAGS.hammerdb_run_tpch:
    samples.extend(ParseHammerDBResultTPCH(result, metadata,
                                           FLAGS.hammerdb_tpch_scale_fact))
  return samples


def ParseHammerDBResultTPCC(result, metadata, virtual_user_list):
  """Parses the text log file from TPCC benchmark and returns a list of samples.

  each list of sample only have one sample with read speed as value
  all the other information is stored in the meta data

  Args:
    result: HammerDB output
    metadata: the running info of vm
    virtual_user_list: the list of virtual user number

  Returns:
    list of samples from the results of the HammerDB tests.
  """
  samples = []
  result_prefix = 'TEST RESULT : System achieved '
  result_suffix = ' SQL Server TPM at'
  start_list = [m.start() for m in re.finditer(result_prefix, result)]
  end_list = [m.start() for m in re.finditer(result_suffix, result)]
  for i, virtual_user_num in enumerate(virtual_user_list):
    metadata['hammerdb_tpcc_virtual_user'] = virtual_user_num
    start_pos = start_list[i] + len(result_prefix)
    end_pos = end_list[i]
    result_tpm = int(result[start_pos: end_pos])
    samples.append(
        sample.Sample('TPM', result_tpm, 'times/minutes',
                      metadata.copy()))
  return samples


def ParseHammerDBResultTPCH(result, metadata, scale_fact):
  """Parses the text log file from TPCH benchmark and returns a list of samples.

  each list of sample only have one sample with read speed as value
  all the other information is stored in the meta data, this uses the equation:
  https://www.hammerdb.com/docs/ch09s02.html

  Args:
    result: HammerDB output
    metadata: the running info of vm
    scale_fact: the scale factor of running tpch

  Returns:
    list of samples from the results of the HammerDB tests.
  """
  samples = []
  query_time_list = []
  refresh_time_list = []
  for i in range(22):
    result_prefix = 'query {0} completed in '.format(str(i+1))
    result_suffix = ' seconds'
    start_pos = result.find(result_prefix) + len(result_prefix)
    end_pos = result.find(result_suffix, start_pos)
    query_time_list.append(float(result[start_pos: end_pos]))

  result_prefix = 'New Sales refresh complete in '
  result_suffix = ' seconds'
  start_pos = result.find(result_prefix) + len(result_prefix)
  end_pos = result.find(result_suffix, start_pos)
  refresh_time_list.append(float(result[start_pos: end_pos]))

  result_prefix = 'Old Sales refresh complete in '
  result_suffix = ' seconds'
  start_pos = result.find(result_prefix) + len(result_prefix)
  end_pos = result.find(result_suffix, start_pos)
  refresh_time_list.append(float(result[start_pos: end_pos]))

  result_prefix = ' query set.s. in '
  result_suffix = ' seconds'
  throughput_time = 0
  start_list = [m.start() for m in re.finditer(result_prefix, result)]

  for index in start_list[1:]:
    start_pos = index + len(result_prefix)
    end_pos = result.find(result_suffix, start_pos)
    throughput_time = max(throughput_time, int(result[start_pos: end_pos]))

  tpch_power = _CalculateTPCHPower(query_time_list, refresh_time_list,
                                   scale_fact)

  stream_num = HAMMERDB_SCALE_TO_STREAMS[str(scale_fact)]
  tpch_throughput = stream_num * 22.0 * 3600 * scale_fact / throughput_time

  qphh = np.sqrt(tpch_power * tpch_throughput)
  samples.append(
      sample.Sample('qphh', qphh, 'N/A',
                    metadata.copy()))
  return samples


def _CalculateTPCHPower(query_time_list, refresh_time_list, scale_fact):
  """helper function for calculating tpch power test result.

  This uses the equation given by:
  https://www.hammerdb.com/docs/ch09s02.html
  """
  maxi = np.amax(query_time_list)
  mini = np.amin(query_time_list)
  if mini < maxi / 1000:
    query_time_list = [maxi / 1000 for x in query_time_list if x < maxi / 1000]
  query_time_sum = np.sum([np.log(x) for x in query_time_list])
  refresh_time_sum = np.sum([np.log(x) for x in refresh_time_list])
  norm_factor = -1 / float((len(query_time_list) + len(refresh_time_list)))
  return 3600 * np.exp(norm_factor * (query_time_sum + refresh_time_sum)) * \
      scale_fact
