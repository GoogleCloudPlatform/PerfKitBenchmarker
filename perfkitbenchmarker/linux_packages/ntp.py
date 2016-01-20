# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing NTP installation and cleanup functions."""

import logging
import re

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

NTP_PORT = 123  # NTP uses UDP on port 123.
NTP_PEERS_NUM_HEADER_LINES = 2  # 'ntpq -p' outputs two lines of header.
NTP_PEERS_WHEN_FIELDNO = 4  # 'when' is field 4 (starting from 0).


def YumInstall(vm):
  """Installs the NTP package on the VM."""
  vm.InstallPackages('ntp')


def AptInstall(vm):
  """Installs the NTP package on the VM."""
  vm.InstallPackages('ntp')


def IsNTPSynced(vm):
  """Determine whether a VM's clock is NTP synchronized.

  Args:
    vm: a virtual_machine.VirtualMachine object.

  Returns:
    bool. True if clock is synchronized by NTP, False if not.
  """

  # See https://www.eecis.udel.edu/~mills/ntp/html/decode.html#sys for
  # why sync_ntp is the right string to search for. Note that we test
  # that the VM's clock is synchronized specifically by NTP, so if it
  # syncs via some special-purpose clock hardware, we will return
  # False.
  out, _ = vm.RemoteCommand('ntpq -c "readvar 0"', should_log=True)

  return 'sync_ntp' in out


@vm_util.Retry()
def WaitForNTPSync(vm):
  """Wait until the given VM's clock is synced via NTP.

  Args:
    vm: a virtual_machine.VirtualMachine object.

  Raises:
    errors.Resource.RetryableCreationError if the wait time exceeds a deadline.

  Returns:
    None.
  """
  if not IsNTPSynced(vm):
    raise errors.Resource.RetryableCreationError('NTP has not yet synced.')


def IsNTPDRunning(vm):
  """Check whether ntpd is running on a given VM.

  This function does not check the status of ntpd, just that an ntpd
  process is running.

  Args:
    vm: a virtual_machine.VirtualMachine object.

  Returns:
    boolean. True if ntpd is running, False if not.
  """

  try:
    vm.RemoteCommand('pgrep --exact ntpd')
    return True
  except errors.VirtualMachine.RemoteCommandError:
    return False


@vm_util.Retry()
def WaitForNTPDaemon(vm):
  """Wait until the NTP daemon has started on the given VM."""

  vm.RemoteCommand('sudo service ntp start', should_log=True)

  # This is sort of sad, because IsNTPDRunning traps an exception and
  # turns it into a boolean, which we immediately convert back to an
  # exception. But we do convert it back into an exception with a
  # nicer message, which I think is important for usability.
  if not IsNTPDRunning(vm):
    raise errors.Resource.RetryableCreationError('NTP daemon not started.')


def StartNTP(vm):
  """Install and start NTP on the given virtual machine."""

  vm.firewall.AllowPort(vm, NTP_PORT)
  vm.Install('ntp')

  WaitForNTPDaemon(vm)

  WaitForNTPSync(vm)


def SecsSinceLastSync(vm):
  """Find the time since the last NTP sync on a VM.

  Args:
    vm: a virtual_machine.VirtualMachine object.

  Returns:
    An integer or None. The number of seconds since the last NTP sync
      with any peer, or None if there are no peers.
  """

  out, err = vm.RemoteCommand('ntpq -p', should_log=True)

  return _SecsSinceLastSyncFromNTPQP(out)


def _SecsSinceLastSyncFromNTPQP(out):
  """Find the time since the last NTP sync on a VM.

  This function is the implementation of SecsSinceLastSync(), but
  separate to make testing easier.

  Args:
    out: the stdout of 'ntpq -p'.

  Returns:
    An integer or None. The number of seconds since the last NTP sync
      with any peer, or None if there are no peers.
  """

  lines = out.splitlines()
  min_secs = None

  # Check that the 'when' field is where we expect it to be
  if lines[0].split()[NTP_PEERS_WHEN_FIELDNO] != 'when':
    raise ValueError("Unexpected 'ntpq -p' header line: %s" % lines[0])

  # the last line of output can be blank
  if not lines[-1] or lines[-1].isspace():
    lines = lines[:-1]

  lines = lines[NTP_PEERS_NUM_HEADER_LINES:]

  logging.info('Scanning %s peers for last sync', len(lines))
  if len(lines) == 0:
    raise ValueError('No NTP servers to sync with.')

  for line in lines:
    # For example, in GCP, a line might look like this:
    # noqa: *metadata.google 120.68.64.68     2 u   30   64    1    0.513    0.300   0.248
    fields = line.split()
    logging.info('Fields: %s', fields)
    when_field = fields[NTP_PEERS_WHEN_FIELDNO]
    if when_field.isdigit():
      secs_since_sync = int(when_field)
      if min_secs is None or secs_since_sync < min_secs:
        min_secs = secs_since_sync
    else:
      # when_field can be '-' if we have never synced with this peer.
      assert when_field == '-'

  return min_secs


def _ReadDecimalSystemVar(vm, var_name):
  """Read the given NTP system var, as a float.

  The variable must be represented as a decimal.

  Args:
    vm: a virtual_machine.VirtualMachine.
    var_name: string. The name of the NTP variable to read.

  Returns:
    The value of the given variable, as a float.
  """

  out, err = vm.RemoteCommand('ntpq -c "readvar 0 %s"' % var_name,
                              should_log=True)
  match = re.search('%s=(-?\d+(\.\d+)?)' % var_name, out)

  return float(match.group(1))


def GetNTPTimeOffset(vm):
  """Return the NTP estimated time offset, in milliseconds."""

  return _ReadDecimalSystemVar(vm, 'offset')


def GetNTPClockWander(vm):
  """Return the NTP estimated clock wander, in PPM."""

  return _ReadDecimalSystemVar(vm, 'clk_wander')
