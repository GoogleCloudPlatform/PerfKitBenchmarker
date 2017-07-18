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
"""Plots per second output from STDERR file. Uploads to cloud storage.

How to use:
  Initialize a plotter instance. Include Google Cloud Storage bucket to upload
    charts.
  Add relevant STDERR files.
  When finished adding files, utilize Plot() method to generate a chart.
  If using launch_mysql_service enabling feature flag will automatically add
  files as runs complete and plot after last thread count call finishes.
"""
import datetime
import plot_scatter_points
import shlex
import subprocess

DATETIME_FORMAT = '{:%m_%d_%Y_%H_%M_}'
DATA_INDICATOR_LINE = 'Threads started!'
TPS = 'tps:'
BREAK = ','
FILENAME_SUFFIX = 'TPS_gnuplot_data.txt'


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

  def __init__(self, run_seconds, report_interval, filename=None,
               cloud_storage=None):
    """Args:

      run_seconds: (integer) length of run phase.
      report_interval: (integer) seconds between TPS reports.
      filename: (string, optional) name of file where parsed data writes.
      cloud_storage: (string, optional) Cloud storage bucket to upload chart.
  """
    self.entries = run_seconds / report_interval
    if filename:
      self.filename = filename
    else:
      self.filename = self._generate_filename()
    self.cloud_storage = cloud_storage

  def _generate_filename(self):
    """Generates filename for parsed data.

    Returns:
      (string): Filename for gnuplot data (tps numbers).
    """
    date_string = DATETIME_FORMAT.format(datetime.datetime.now())
    filename = date_string + FILENAME_SUFFIX
    return filename

  def add_file(self, filename):
    """Given STDERR filename for ONE run with a given thread count, add data.

    Args:
      filename: (string) Name of file to be parsed.

    Raises:
      STDERRFileDoesNotExistError:
    """
    # TODO: Open file (try/catch)
    try:
      f = open(filename)
    except:
      raise STDERRFileDoesNotExistError(
          ('Unable to open file (%s). Assume this is because run failed. Will'
           ' raise exception to kill run now.' % filename))
    data = self._parse_file(f)
    f.close()
    self._add_data(data)

  def _parse_file(self, f):
    """Parses stderr file, f, extracts list of TPS values.

    Assumes no warmup phase and only one report per file.

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
        # Data collection phase: self._entries lines of collection.
        for _ in range(self.entries):
          line = f.readline()
          start_id = line.find(TPS) + len(TPS)
          end_id = line.find(BREAK, start_id)
          if start_id == -1 or end_id == -1:
            raise PatternNotFoundError('No thread data (OR improper run seconds'
                                       '/report interval given) found in STDERR'
                                       '. Assume run failed.')
          tps = float(line[start_id:end_id].strip())
          tps_values.append(tps)
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
    """Generates a graph using gnuplot and data from added files.
    """
    [output_gnuplot_file,
     output_chart] = plot_scatter_points.gnuplot_info(self.filename)
    subprocess.Popen(['gnuplot', output_gnuplot_file])
    if self.cloud_storage:
      cloud_cmd = 'gsutil cp {} gs://{}/{}'.format(
          output_chart, self.cloud_storage, output_gnuplot_file)
      cloud_cmd_list = shlex.split(cloud_cmd)
      subprocess.Popen(cloud_cmd_list)
