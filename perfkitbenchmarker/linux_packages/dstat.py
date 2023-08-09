# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing dstat installation and cleanup functions.

TODO(user): Migrate to dool, because dstat is not maintained and broken
in Python 3: https://github.com/dstat-real/dstat.
"""

import csv
import itertools
from typing import List, TextIO, Tuple

import numpy as np


def ParseCsvFile(fp: TextIO) -> Tuple[List[str], np.ndarray]:
  """Parse pcp dstat results file in csv format.

  This is an example of a pcp dstat CSV output:
      "pcp-dstat 5.0.3 CSV Output"
      "Author:","PCP team <pcp@groups.io> and Dag Wieers <dag@wieers.com>"...
      "Host:","pkb-d43f64f0-0",,,,"User:","perfkit"
      "Cmdline:","pcp-dstat --epoch ... ,,,,"Date:","04 Aug 2023 01:14:42 UTC"
      "epoch","total usage","load avg",,,"io/total",,,,"dsk/total",,"net/total",
      "epoch",,"1m","5m","15m","read","writ","dsk/total:read"...,
      1691111682,0.080,0.240,0.120,,,,,,,,,,,,,0,0,,224164,6772060...
      1691111702,0.060,0.220,0.120,0,0.200,0,2.000,0,2.000,41...

   There is always 4 lines of headers.
   Then comes a newline in some versions (not in 0.7.3).
   Then comes the high level categories (empty categories mean the same as
   previous)
   Then comes the sub-labels of the categories.
   Lastly comes the data itself.

  Args:
    fp: file. File to read

  Returns:
    A tuple of list of dstat labels and ndarray containing parsed data.

  Raises:
    ValueError on parsing issues.
  """
  reader = csv.reader(fp)
  headers = list(itertools.islice(reader, 4))
  if len(headers) < 4:
    raise ValueError(f'Expected 4 header lines got {len(headers)}\n{headers}')
  if not headers[0] or 'pcp-dstat' not in headers[0][0]:
    raise ValueError(
        f'Expected first header cell to contain "pcp-dstat"\n{headers[0]}')
  if not headers[2] or 'Host:' not in headers[2][0]:
    raise ValueError(
        f'Expected first cell in third line to be "Host:"\n{headers[2]}')

  categories = next(reader)
  if not categories:
    # Dstat 0.7.2 has a newline between headers and categories. 0.7.3 does not.
    categories = next(reader)

  if not categories or categories[0] != 'epoch':
    raise ValueError('Expected first category to "epoch". '
                     f'Categories were:\n{categories}')

  # Categories are not repeated; copy category name across columns in the
  # same category
  for i, category in enumerate(categories):
    categories[i] = category or categories[i - 1]
  labels = next(reader)

  if not labels or labels[0] != 'epoch':
    raise ValueError(f'Expected first label to "epoch". Labels were:\n{labels}')
  if len(labels) != len(categories):
    raise ValueError(
        f'Number of categories ({len(categories)}) does not match number of '
        f'labels ({len(labels)}\nCategories: {categories}\nLabels:{labels}')

  # Generate new column names
  labels = [f'{label}__{cat}' for label, cat in zip(labels, categories)]

  data = []
  for i, row in enumerate(reader):
    # Remove the trailing comma
    if len(row) == len(labels) + 1:
      if row[-1]:
        raise ValueError(f'Expected the last element of row {row} to be empty,'
                         f' found {row[-1]}')
      row = row[:-1]

    if len(labels) != len(row):
      if i == 0:
        continue
      raise ValueError(
          f'Number of labels ({len(labels)}) does not match number of '
          f'columns ({len(row)}) in row {i}:\n{row}')
    data.append(row)
  return labels, np.array(data, dtype=float)


def _Install(vm):
  """Installs the dstat package on the VM."""
  vm.InstallPackages('pcp')


def YumInstall(vm):
  """Installs the dstat package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the dstat package on the VM."""
  _Install(vm)
