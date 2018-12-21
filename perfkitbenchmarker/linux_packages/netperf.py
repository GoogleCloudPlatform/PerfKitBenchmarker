# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing netperf installation and cleanup functions."""

import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker.data import ResourceNotFound
from perfkitbenchmarker.linux_packages import INSTALL_DIR

flags.DEFINE_integer(
    'netperf_histogram_buckets', 100,
    'The number of buckets per bucket array in a netperf histogram. Netperf '
    'keeps one array for latencies in the single usec range, one for the '
    '10-usec range, one for the 100-usec range, and so on until the 10-sec '
    'range. The default value that netperf uses is 100. Using more will '
    'increase the precision of the histogram samples that the netperf '
    'benchmark produces.')
FLAGS = flags.FLAGS
NETPERF_TAR = 'netperf-2.7.0.tar.gz'
NETPERF_URL = 'https://github.com/HewlettPackard/netperf/archive/%s' % (
              NETPERF_TAR)
NETPERF_DIR = '%s/netperf-netperf-2.7.0' % INSTALL_DIR

NETPERF_SRC_DIR = NETPERF_DIR + '/src'
NETSERVER_PATH = NETPERF_SRC_DIR + '/netserver'
NETPERF_PATH = NETPERF_SRC_DIR + '/netperf'
NETLIB_PATCH = NETPERF_SRC_DIR + '/netperf.patch'


def _Install(vm):
  """Installs the netperf package on the VM."""
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install absl-py')
  vm.Install('build_tools')
  _CopyTar(vm)
  vm.RemoteCommand('cd %s && tar xvzf %s' % (INSTALL_DIR, NETPERF_TAR))
  # Modify netperf to print out all buckets in its histogram rather than
  # aggregating.
  vm.PushDataFile('netperf.patch', NETLIB_PATCH)
  vm.RemoteCommand('cd %s && patch -p2 < netperf.patch' %
                   NETPERF_SRC_DIR)
  vm.RemoteCommand('cd %s && CFLAGS=-DHIST_NUM_OF_BUCKET=%s '
                   './configure --enable-histogram=yes '
                   '&& make' % (NETPERF_DIR, FLAGS.netperf_histogram_buckets))


def _CopyTar(vm):
  """Copy the tar file for installation.

  Tries local data directory first, then NET_PERF_URL
  """

  try:
    vm.PushDataFile(NETPERF_TAR, remote_path=(INSTALL_DIR + '/'))
  except ResourceNotFound:
    vm.Install('curl')
    vm.RemoteCommand('curl %s -L -o %s/%s' % (
        NETPERF_URL, INSTALL_DIR, NETPERF_TAR))


def YumInstall(vm):
  """Installs the netperf package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the netperf package on the VM."""
  _Install(vm)


def ParseHistogram(netperf_stdout):
  """Parses the histogram output from netperf.

  Args:
    netperf_output: string. The stdout from netperf containing a histogram.

  Returns:
    A dict mapping latency to sample count or None if the output did not
    contain a histogram.
  """
  # Here is an example of a netperf histogram:
  #
  # Histogram of request/response times
  # UNIT_USEC     :    0:    0:    0:    0:    0:    0:    0:    0:    0:    0
  # TEN_USEC      :    0:    0:    0:    0:    0:    0:    0:    0:    0:    0
  # HUNDRED_USEC  :    0: 433684: 9696:  872:  140:   56:   27:   28:   17:   10
  # UNIT_MSEC     :    0:   24:   57:   40:    5:    2:    0:    0:    0:    0
  # TEN_MSEC      :    0:    0:    0:    0:    0:    0:    0:    0:    0:    0
  # HUNDRED_MSEC  :    0:    0:    0:    0:    0:    0:    0:    0:    0:    0
  # UNIT_SEC      :    0:    0:    0:    0:    0:    0:    0:    0:    0:    0
  # TEN_SEC       :    0:    0:    0:    0:    0:    0:    0:    0:    0:    0
  # >100_SECS: 0
  # HIST_TOTAL:      444658
  histogram_text = regex_util.ExtractGroup(
      '(UNIT_USEC.*?)>100_SECS', netperf_stdout, flags=re.S)

  # The total number of usecs that this row of the histogram represents.
  row_size = 10.0
  hist = {}

  for l in histogram_text.splitlines():
    buckets = [int(b) for b in l.split(':')[1:]]
    bucket_size = row_size / len(buckets)
    hist.update({(i * bucket_size): count
                 for i, count in enumerate(buckets) if count})
    # Each row is 10x larger than the previous row.
    row_size *= 10

  return hist
