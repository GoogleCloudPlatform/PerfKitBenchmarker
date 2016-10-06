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


"""Module containing dstat installation and cleanup functions."""

import csv
import itertools
import numpy as np


def ParseCsvFile(fp):
  """Parse dstat results file in csv format.

  Args:
    file: string. Name of the file.

  Returns:
    A tuple of list of dstat labels and ndarray containing parsed data.
  """
  reader = csv.reader(fp)
  headers = list(itertools.islice(reader, 5))
  if len(headers) != 5:
    raise ValueError(
        'Expected exactly 5 header lines got {}\n{}'.format(
            len(headers), headers))
  if 'Dstat' not in headers[0][0]:
    raise ValueError(
        'Expected first header cell to contain "Dstat"\n{}'.format(
            headers[0]))
  if 'Host:' not in headers[2][0]:
    raise ValueError(('Expected first cell in third line to be '
                      '"Host:"\n{}').format(headers[2]))
  categories = next(reader)

  # Categories are not repeated; copy category name across columns in the
  # same category
  for i, category in enumerate(categories):
    if not categories[i]:
        categories[i] = categories[i - 1]
  labels = next(reader)

  if len(labels) != len(categories):
    raise ValueError((
        'Number of categories ({}) does not match number of '
        'labels ({})\nCategories: {}\nLabels:{}').format(
            len(categories), len(labels), categories, labels))

  # Generate new column names
  labels = ['%s__%s' % x for x in zip(labels, categories)]

  data = []
  for i, row in enumerate(reader):
    # Remove the trailing comma
    if len(row) == len(labels) + 1:
      if row[-1]:
        raise ValueError(('Expected the last element of row {0} to be empty,'
                          ' found {1}').format(row, row[-1]))
      row = row[:-1]

    if len(labels) != len(row):
      raise ValueError(('Number of labels ({}) does not match number of '
                        'columns ({}) in row {}:\n{}').format(
                            len(labels), len(row), i, row))
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
