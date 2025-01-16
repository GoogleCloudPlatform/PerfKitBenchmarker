"""Utilities for managing a locust benchmark on a VM."""

import csv
import enum
import logging
import re
from typing import Iterable, TYPE_CHECKING
from absl import flags

from perfkitbenchmarker import data
from perfkitbenchmarker import sample

if TYPE_CHECKING:
  from perfkitbenchmarker import linux_virtual_machine  # pylint: disable=g-import-not-at-top


FLAGS = flags.FLAGS


class Locustfile(enum.Enum):
  """Enum with paths to predefined locustfiles."""
  SIMPLE = 'locust/simple.py'
  RAMPUP = 'locust/rampup.py'

  def GetPath(self):
    return data.ResourcePath(self.value)


_LOCUST_FILE = flags.DEFINE_string(
    'locust_path',
    None,
    'Path to the locust file to use, e.g. `locust/simple.py`. Required.',
)


def Install(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine') -> None:
  """Installs locust on the given VM.

  Installs locust on the indicated VM. Does not start locust.

  Running this a second time will idempotently install locust, but will have no
  other effect. (If locust is already running at the time, it will not interrupt
  it.)

  Args:
    vm: Already running VM where locust should be installed.

  Raises:
    errors.VirtualMachine.RemoteCommandError: If an error occurred on the VM.
  """
  vm.RunCommand(['sudo', 'apt', 'update'])
  vm.RunCommand(['sudo', 'apt', 'install', 'python3-locust', '-y'])


def Prep(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine',
) -> None:
  """Prepares a locustfile to run on the given VM.

  Prepares the locustfile, but does not start (or install) locust.

  Running this a second time will idempotently replace the locustfile, but will
  have no other effect. (If locust is already running at the time, it will not
  interrupt it.)

  Args:
    vm: Already running VM where locust should be installed.

  Raises:
    errors.VirtualMachine.RemoteCommandError: If an error occurred on the VM.
  """
  if _LOCUST_FILE.value is None:
    raise ValueError('Locustfile path must be specified via flag.')
  locustfile_path = data.ResourcePath(_LOCUST_FILE.value)
  vm.RemoteCopy(locustfile_path, 'locustfile.py')


def Run(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine', target_host: str
) -> Iterable[sample.Sample]:
  """Runs locust.

  This won't return until the test is complete. (Test length is defined by the
  locustfile.) This can be called repeatedly, in which case, the test will run
  again. This should not be called repeatedly *in parallel*.

  Args:
    vm: Already running VM where locust/locustfile has already been installed.
    target_host: The SUT. Can be an ipaddr or hostname. Must include the scheme.
      e.g. 'http://192.168.0.1:8080'

  Yields:
    Samples corresponding to the locust results.

  Raises:
    errors.VirtualMachine.RemoteCommandError: If an error occurred on the VM.
      (Notably, if `prep()` was not previously called to install locust, then a
      RemoteCommandError will be raised.)
  """
  _, stderr, code = vm.RunCommand(
      [
          'locust',
          '-f',
          'locustfile.py',
          '--host',
          target_host,
          '--autostart',
          '--csv',
          'test1',
          '--autoquit',
          '5',
      ],
      ignore_failure=True,
  )
  if code != 0:
    logging.info(
        'Locust had non-zero exit code: %s and error: %s. This may just mean'
        ' there were some number of failing requests, which is acceptable.',
        code,
        stderr,
    )
  stdout, _, _ = vm.RunCommand(['cat', 'test1_stats_history.csv'])
  yield from _ConvertLocustResultsToSamples(stdout)

  # Grab the last line again and re-export those samples as the "locust_overall"
  # samples. NB:
  # 1. CSV outputs sequentially so last line is last timestamp
  # 2. Timestamps continue to aggregate, so last timestamp is "overall".
  stdout, _, _ = vm.RunCommand(
      '(head -n1 && tail -n1) < test1_stats_history.csv'
  )
  yield from _ConvertLocustResultsToSamples(
      stdout, metric_namespace='locust_overall'
  )


def _ConvertLocustResultsToSamples(
    locust_results: str,
    metric_namespace: str = 'locust',
) -> Iterable[sample.Sample]:
  """Converts each csv row from locust to a PKB sample."""
  lines = locust_results.splitlines()
  reader = csv.DictReader(lines)

  for row in reader:
    for field in reader.fieldnames:
      if field in ['Timestamp', 'Type', 'Name']:
        continue
      if row[field] == 'N/A':
        continue

      yield sample.Sample(
          metric=metric_namespace + '/' + _SanitizeFieldName(field),
          value=float(row[field]),
          unit='',
          metadata={'locustfile_path': _LOCUST_FILE.value},
          timestamp=int(row['Timestamp']),
      )


def _SanitizeFieldName(field: str) -> str:
  field = re.sub(' ', '_', field)
  field = re.sub('%', 'p', field)
  field = re.sub('/', '_per_', field)
  return field
