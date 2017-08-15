#!/usr/bin/env python

# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Plots per second output from mysql_service_benchmark STDERR files.
Prerequisites/assumptions:
Gnuplot must be installed on the machine to use this module.
  To check if gnuplot is installed on the machine enter:
    'dpkg -l gnuplot'
  To install gnuplot:
    sudo apt-get install gnuplot-x11
mysql_service_benchmark Stderr file
  Can be generated with launch_mysql_service. If enabled, per_second_graphs flag
  will automatically call this module and generate the per second graphs.
no sysbench_warmup time
  Parsing of file assumes sysbench_warmup_seconds is set to 0.
Sysbench version
  Assumes Sysbench 0.5 (https://github.com/akopytov/sysbench) stderr output.
  If TPS value printout changes, _parse_file method will throw a PatternNotFound
  exception and need to be updated.
How to use plot_sysbench_results:
  Initialize a plotter instance.
  Add relevant STDERR files from mysql_service using add_file() method.
  When finished adding files, utilize plot() method to generate a chart.
  If using launch_mysql_service enabling 'per_second_graphs' feature flag will
  automatically add files as runs complete and plot after last thread count call
  finishes.
"""
import datetime
import plot_scatter_points
import subprocess

# Assumes Sysbench 0.5 stderr output.
DATETIME_FORMAT = '{:%m_%d_%Y_%H_%M_}'
DATA_INDICATOR_LINE = 'Threads started!'
TPS = 'tps:'
BREAK = ','
FILENAME_SUFFIX = '_TPS_gnuplot_data.txt'


class STDERRFileDoesNotExistError(Exception):
  pass


class PatternNotFoundError(Exception):
  pass


class Plotter():
  """Plotter generates a per second output graph of TPS vs Thread Values.
  Given run configurations, and run stderr filenames for any number of PKB runs,
  Plotter extracts TPS values and generates a gnuplot graph which can be
  uploaded to cloud storage.
  """

  def __init__(self, run_seconds, report_interval, run_uri):
    """Args:
      run_seconds: (integer) length of run phase.
      report_interval: (integer) seconds between TPS reports.
      run_uri: (string) run identifier.
    """
    self.run_uri = run_uri
    self.data_entries_per_file = run_seconds / report_interval
    self.filename = self._generate_filename()
    self.max_tps = 0
    self.iterations = 0

  def _generate_filename(self):
    """Generates filename for parsed data.
    Returns:
      (string): Filename for gnuplot data (tps numbers).
    """
    date_string = DATETIME_FORMAT.format(datetime.datetime.now())
    filename = date_string + self.run_uri + FILENAME_SUFFIX
    return filename

  def add_file(self, filename):
    """Given STDERR filename for ONE run with a given thread count, add data.
    Args:
      filename: (string) Name of file to be parsed.
    Raises:
      STDERRFileDoesNotExistError:
    """
    try:
      f = open(filename)
    except:
      raise STDERRFileDoesNotExistError(
          ('Unable to open file (%s). Assume this is because run failed. Will'
           ' raise exception to kill run now.' % filename))
    data = self._parse_file(f)
    f.close()
    self._add_data(data)
    self.iterations += 1

  def _parse_file(self, f):
    """Parses stderr file, f, extracts list of TPS values.
    Assumes no warmup phase and only one report per file.
    Method will need to be updated if Sysbench output format changes. Assumes
    Sysbench 0.5.
    Args:
      f: (file object) file to be parsed.
    Returns:
      (list): list of TPS values.
    Raises:
      PatternNotFoundError.
    """
    tps_values = []
    line = f.readline()
    while line:
      if line.strip() == DATA_INDICATOR_LINE:
        line = f.readline()  # blank line
        for _ in range(self.data_entries_per_file):
          line = f.readline()
          start_id = line.find(TPS) + len(TPS)
          end_id = line.find(BREAK, start_id)
          if start_id == -1 or end_id == -1:
            raise PatternNotFoundError('No thread data (OR improper run seconds'
                                       '/report interval given) found in STDERR'
                                       '. Assume run failed.')
          tps = float(line[start_id:end_id].strip())
          tps_values.append(tps)
          if tps > self.max_tps:
            self.max_tps = tps
        break
      line = f.readline()
    return tps_values

  def _add_data(self, data):
    """Given data, adds to self.filename.
    Args:
      data: list of tps values.
    """
    with open(self.filename, 'a') as f:
      for d in data:
        f.write(str(d) + '\n')

  def plot(self):
    """Generates a graph using gnuplot and data from filename.
    """
    p = plot_scatter_points.GnuplotInfo(self.filename,
                                        self.data_entries_per_file,
                                        self.run_uri,
                                        self.max_tps,
                                        self.iterations)
    output_gnuplot_file, output_chart = p.create_file()
    subprocess.Popen(['gnuplot', output_gnuplot_file])
    # TODO(samspano): Implement copy command to copy output_chart
