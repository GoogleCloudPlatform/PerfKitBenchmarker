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
  """Parse dstat results file in csv format.

  This is an example of a Dstat CSV output:
      "Dstat 0.7.2 CSV output"
      "Author:","Dag Wieers <dag@wieers.com>",,,,"URL:","http://dag.wieers.co...
      "Host:","pc-ubu",,,,"User:","pclay"
      "Cmdline:","dstat -Tcmno bar",,,,"Date:","09 Jun 2021 18:59:00 UTC"

      "epoch","total cpu usage",,,,,,"memory usage",,,,"net/total",
      "epoch","usr","sys","idl","wai","hiq","siq","used","buff","cach","free"...
      1623265140.892,0.054,0.035,99.883,0.029,0.0,0.000,216711168.0,108556288...
      1623265141.893,0.0,0.0,100.0,0.0,0.0,0.0,216707072.0,108556288.0,467206...

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
  if not headers[0] or 'Dstat' not in headers[0][0]:
    raise ValueError(
        f'Expected first header cell to contain "Dstat"\n{headers[0]}')
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
      raise ValueError(
          f'Number of labels ({len(labels)}) does not match number of '
          f'columns ({len(row)}) in row {i}:\n{row}')
    data.append(row)
  return labels, np.array(data, dtype=float)


def _Install(vm):
  """Installs the dstat package on the VM."""
  vm.InstallPackages('dstat')


def YumInstall(vm):
  """Installs the dstat package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the dstat package on the VM."""
  _Install(vm)
