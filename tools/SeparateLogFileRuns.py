#!/usr/bin/env python

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

import os
import re
import sys

# When perfkitbenchmarker is run with multiple processes, the output
# from each thread is interleaved in the output.  This program takes
# a log file with interleaved messages, and separates them back into their
# own file.


class LogDeInterlace(object):

  def __init__(self, root_file_name):
    self.root_file_name = root_file_name
    self.map_index_to_stream = {}

  def _GetRootComponents(self, fullpath):
    path = os.path.dirname(fullpath)
    (filename, extension) = os.path.splitext(os.path.basename(fullpath))

    return (path, filename, extension)

  def _CreateStreamForIndex(self, index):
    (path, filename, extension) = self._GetRootComponents(self.root_file_name)
    filename = os.path.join(path,
                            filename + '-' + str(index) + extension)
    if os.path.exists(filename):
      print 'Warning file %s already exists.  Log will be lost' % filename
      return None

    print 'Creating %s' % filename

    file_object = open(filename, 'w')
    return file_object

  def GetStreamForIndex(self, index):
    if index not in self.map_index_to_stream:
      self.map_index_to_stream[index] = self._CreateStreamForIndex(index)
    return self.map_index_to_stream[index]

  def __enter__(self):
    return self

  def __exit__(self, types, value, traceback):
    for file_object in self.map_index_to_stream.itervalues():
      if file_object is not None:
        file_object.close()


def main(argv):
  if len(argv) != 2 or argv[1] == '--help':
    print 'usage: SeparateLogFileRuns <filename>'
    print ''
    print ('Takes a pkb.log which was created from a single invocation of '
           'perfkitbenchmarker running multiple benchmarks.  The output '
           'from each thread is written out to its own stream.')
    sys.exit(1)

  input_file = argv[1]
  # the threads are numbered starting at 1 ... so use 0 for beginning
  # and ending stream output
  sentinel_stream = 0

  with LogDeInterlace(input_file) as logs:
    with open(input_file) as f:
      current_stream = logs.GetStreamForIndex(sentinel_stream)
      for line in f:
        # matches lines like:
        # 2018-02-13 22:30:41,701 6538b6ae MainThread pgbench(1/9) ...
        stream_match = re.match(r'^\d\d\d\d-\d\d-\d\d .*?Thread'
                                r'.*?\((\d*)\/\d*\)', line)

        # matches lines like (one line):
        # 2018-02-14 17:59:57,297 6538b6ae MainThread pkb.py:856 INFO
        # Benchmark run statuses:
        end_match = re.match(r'^\d\d\d\d-\d\d-\d\d.*'
                             r'Benchmark run statuses:', line)

        if stream_match:
          stream_index = int(stream_match.group(1))
          current_stream = logs.GetStreamForIndex(stream_index)
        elif end_match:
          current_stream = logs.GetStreamForIndex(sentinel_stream)

        if current_stream is not None:
          current_stream.write(line)


if __name__ == '__main__':
  main(sys.argv)
