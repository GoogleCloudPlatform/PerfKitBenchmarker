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
"""Generates gnuplot info file.
Given the filename of a text file with a list of TPS values,
returns a gnuplot info file. This file can create a per second chart by
calling:
  'gnuplot {filename}'.

Designed to be used with plot_sysbench_results.py.
"""

import os
import datetime


DATETIME_FORMAT = '{:%m_%d_%Y_%H_%M_}'
DATETIME_TITLE_FORMAT = '{: %m %d %Y %H %M}'
CHART_TITLE_PREFIX = 'Sysbench TPS'

X_LABEL = 'Thread Count'
Y_LABEL = 'TPS'

# This variable controls the verticle lines inside the chart.
#   0 means no vertical lines.
Y_TICS = '100'
DEFAULT_ITERATIONS = '10'


class GnuplotInfo():


  def __init__(self, gnuplot_data_filename,
               entries_per_run,
               run_uri,
               y_max,
               iterations=DEFAULT_ITERATIONS,
               title=None):
    """Initialize GnuplotInfo object.
    Args:
      gnuplot_data_filename: filename of TPS data.
      entries_per_run: Number of TPS values collected for each run.
      run_uri: (string) run identifier.
      y_max: maximum y value. Used for y-axis limit.
      title: (optional, string) Chart title.
    """
    self.gnuplot_data_filename = gnuplot_data_filename
    self._generate_filenames(run_uri)
    self.X_INTERVAL = str(entries_per_run)
    self.Y_HEIGHT = str(int(100 * round(float(y_max) / 100)))
    self.title = title
    self.iterations = str(iterations)

  def _generate_filenames(self, run_uri):
    """Sets filename (with path) of gnuplot input and chart.
    Args:
      run_uri: (string) run identifier.
    """
    date_string = DATETIME_FORMAT.format(datetime.datetime.now())
    date_title_string = DATETIME_TITLE_FORMAT.format(datetime.datetime.now())
    self.chart_title = CHART_TITLE_PREFIX + date_title_string
    identifier = date_string + run_uri + '_sysbench_run.png'
    self.output_chart = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'charts',
        identifier)
    self.output_gnuplot_file = self.output_chart + '_gnuplot_input'

  def create_file(self):
    """Generates a gnuplot info file.
    Returns:
      output_file (string): Name of gnuplot output file.
      output_chart (string): Name of output chart file.
    """
    color = '38761d'

    # Titles for the data series
    title = self.title or 'Cloud SQL Prod'

    output_file = open(self.output_gnuplot_file, 'w')
    output_file.write('set terminal pngcairo size 1500,800 '
                      'enhanced font "Verdana,12"\n')
    output_file.write('set output "' + self.output_chart + '"\n')
    output_file.write('set multiplot\n')
    output_file.write('set grid\n')
    output_file.write('set border 4095 ls 0 lc rgb \"black\"\n')
    output_file.write('set title (\"' + self.chart_title +
                      '") font \"aerial, 14\" noenhanced\n')
    output_file.write('set xlabel "' + X_LABEL + '"\n')
    output_file.write('set ylabel "' + Y_LABEL + '"\n')

    # If you want to move the legend to the top left, use this:
    output_file.write('set key left top\n')

    if self.Y_HEIGHT > 0:
      output_file.write('y=' + self.Y_HEIGHT + '\n')
      output_file.write('set yrange [0:y]\n')
      output_file.write('set ytics ' + Y_TICS + '\n')
      output_file.write('unset xtics\n')

      output_file.write('thread=1\n')
      output_file.write('x=0\n')
      output_file.write('do for [t=1:' + self.iterations + ':+1] {\n')
      output_file.write(
          '\tset label (sprintf(\"%d\", thread)) at x+20, 0  offset -2\n')
      output_file.write(
          '\tset arrow from x,0 to x,y nohead ls 0 lc rgb \"blue\"\n')
      # TODO: This code assumes thread count increases by 2 times the previous
      # number. Future implementation should take a list of thread counts and
      # properly handle that here.
      output_file.write('\tthread=thread*2\n')
      output_file.write('\tx=x+' + self.X_INTERVAL + '\n')
      output_file.write('}\n')

    # plotting data series
    output_file.write('plot\\\n')

    column = '1'
    output_file.write('\"' + self.gnuplot_data_filename + '\" using ' + column +
                      ' with points lc rgb \"#' + color + '\"  title \"' + title
                      + '\"')

    return self.output_gnuplot_file, self.output_chart
