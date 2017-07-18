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

Given the TPS data file, returns a gnuplot info file.
"""


CHART_TITLE = "Sysbench TPS"

X_LABEL = 'thread count'
Y_LABEL = 'tps'

# This variable controls the verticle lines inside the chart.
#   0 means no vertical lines.
Y_HEIGHT = '3000'
Y_TICS = '100'
X_INTERVAL = '1200'
ITERATIONS = '9'


class gnuplot_info():

  def __init__(self, gnuplot_data_filename):
    self.output_dir = "./charts"
    self.output_chart = self.output_dir + "/innodb_pages.png"
    self.output_gnuplot_file = self.output_chart + '_gnuplot_input'
    self.gnuplot_data_filename = gnuplot_data_filename
    self._create_file()


  def _create_file(self):
    """Generates a gnuplot info file.

    Returns:
      output_file (string): Name of gnuplot output file.
      output_chart (string): Name of output chart file.
    """
    # Colors for the data series
    # aws: be69138
    # another aws (gold): ffd700
    # gcp: 38761d
    # red: 'DC143C'

    color = '38761d'

    # Titles for the data series
    title = '1TB DB Instance'

    output_file = open(self.output_gnuplot_file, 'w')
    output_file.write('set terminal pngcairo size 1500,800 '
                      'enhanced font "Verdana,12"\n')
    output_file.write('set output "' + self.output_chart + '"\n')
    output_file.write('set multiplot\n')
    output_file.write('set grid\n')
    output_file.write('set border 4095 ls 0 lc rgb \"black\"\n')
    output_file.write('set title (\"' + CHART_TITLE +
                      '") font \"aerial, 14\"\n')
    output_file.write('set xlabel "' + X_LABEL + '"\n')
    output_file.write('set ylabel "' + Y_LABEL + '"\n')

    # If you want to move the legend to the top left, use this:
    output_file.write('set key left top\n')

    if Y_HEIGHT > 0:
      output_file.write('y=' + Y_HEIGHT + '\n')
      output_file.write('set yrange [0:y]\n')
      output_file.write('set ytics ' + Y_TICS + '\n')
      output_file.write('unset xtics\n')

      output_file.write('thread=1\n')
      output_file.write('x=0\n')
      output_file.write('do for [t=1:' + ITERATIONS + ':+1] {\n')
      output_file.write(
          '\tset label (sprintf(\"%d\", thread)) at x+20, 0  offset -2\n')
      output_file.write(
          '\tset arrow from x,0 to x,y nohead ls 0 lc rgb \"blue\"\n')
      output_file.write('\tthread=thread*2\n')
      output_file.write('\tx=x+' + X_INTERVAL + '\n')
      output_file.write('}\n')

    # plotting data series
    output_file.write('plot\\\n')

    column = '1'
    output_file.write('\"' + self.gnuplot_data_filename + '\" using ' + column +
                      ' with points lc rgb \"#' + color + '\"  title \"' + title
                      + '\"')

    return [self.output_gnuplot_file, self.output_chart]
