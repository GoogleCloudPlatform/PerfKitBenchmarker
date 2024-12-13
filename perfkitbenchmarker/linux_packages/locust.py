"""Utilities for managing a locust benchmark on a VM."""

import csv
import enum
import re
from typing import Iterable, TYPE_CHECKING

from perfkitbenchmarker import data
from perfkitbenchmarker.sample import Sample

if TYPE_CHECKING:
  from perfkitbenchmarker import linux_virtual_machine  # pylint: disable=g-import-not-at-top


class Locustfile(enum.Enum):
  SIMPLE = 'locust/simple.py'
  RAMPUP = 'locust/rampup.py'

  def GetPath(self):
    return data.ResourcePath(self.value)


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
    locustfile_path: str | Locustfile,
) -> None:
  """Prepares a locustfile to run on the given VM.

  Prepares the locustfile, but does not start (or install) locust.

  Running this a second time will idempotently replace the locustfile, but will
  have no other effect. (If locust is already running at the time, it will not
  interrupt it.)

  Args:
    vm: Already running VM where locust should be installed.
    locustfile_path: Path of the locustfile; see
      https://docs.locust.io/en/stable/writing-a-locustfile.html. NB: Two
        pre-defined locust files exist in this module that you can use:
        Locustfile.SIMPLE, Locustfile.RAMPUP.

  Raises:
    errors.VirtualMachine.RemoteCommandError: If an error occurred on the VM.
  """
  if isinstance(locustfile_path, Locustfile):
    locustfile_path = locustfile_path.GetPath()
  vm.RemoteCopy(locustfile_path, 'locustfile.py')


def Run(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine', target_host: str
) -> Iterable[Sample]:
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
  vm.RunCommand([
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
  ])
  stdout, _, _ = vm.RunCommand(['cat', 'test1_stats_history.csv'])
  yield from _ConvertLocustResultsToSamples(stdout)


def _ConvertLocustResultsToSamples(locust_results: str) -> Iterable[Sample]:
  lines = locust_results.splitlines()
  reader = csv.DictReader(lines)

  for row in reader:
    for field in reader.fieldnames:
      if field in ['Timestamp', 'Type', 'Name']:
        continue
      if row[field] == 'N/A':
        continue

      yield Sample(
          metric='locust/' + _SanitizeFieldName(field),
          value=float(row[field]),
          unit='',
          metadata={},
          timestamp=int(row['Timestamp']),
      )


def _SanitizeFieldName(field: str) -> str:
  field = re.sub(' ', '_', field)
  field = re.sub('%', 'p', field)
  field = re.sub('/', '_per_', field)
  return field
