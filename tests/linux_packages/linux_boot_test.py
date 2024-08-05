"""Unit tests for perfkitbenchmarker.linux_packages.linux_boot module.

Tests mostly focusing on parsing logic of extracting information from
systemd and dmesg and converting into sample.Sample objects.
Test data files stored in perfkitbenchmarker/tests/data/linux_boot.
"""

import datetime
import os
import unittest

from absl.testing import parameterized
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import linux_boot
from tests import pkb_common_test_case
import pytz


class LinuxBootTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    self.data_dir = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'linux_boot'
    )
    vm_spec = pkb_common_test_case.CreateTestVmSpec()
    self.mock_vm = pkb_common_test_case.TestLinuxVirtualMachine(vm_spec=vm_spec)

  def testDatetimeToUTCSeconds(self):
    self.assertEqual(
        linux_boot.DatetimeToUTCSeconds(
            datetime.datetime(2023, 4, 6, 18, 1, 1)
        ),
        1680804061,
    )

  def testScrapeConsoleLogLines(self):
    """Test startup script output parsing."""
    with open(os.path.join(self.data_dir, 'boot.output')) as f:
      boot_output = f.read().split('\n')
    # Comparing golden samples and parsed samples
    # Since sample.Sample will set timestamp upon object creation, use
    # assertSampleListsEqualUpToTimestamp method to ignore timestamp.
    self.assertSampleListsEqualUpToTimestamp(
        [
            sample.Sample(
                metric='startup_script_run',
                value=7.685235,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='linux_booting',
                value=3.698,
                unit='second',
                metadata={},
                timestamp=9,
            ),
        ],
        linux_boot.ScrapeConsoleLogLines(
            boot_output,
            datetime.datetime.fromtimestamp(1680741770, pytz.timezone('UTC')),
            linux_boot.CONSOLE_FIRST_START_MATCHERS,
        ),
    )

  def testUtcTimestampToDatetime(self):
    self.assertEqual(
        linux_boot.UtcTimestampToDatetime('1680741777.685234619'),
        datetime.datetime(
            2023, 4, 6, 0, 42, 57, 685235, tzinfo=datetime.timezone.utc
        ),
    )

  def testCollectKernelSamples(self):
    """Test dmesg parsing."""
    with open(os.path.join(self.data_dir, 'dmesg')) as f:
      dmesg = f.read()
    self.mock_vm.RemoteCommand = mock.Mock(return_value=(dmesg, ''))
    # Compare golden samples and parsed dmesg samples
    self.assertSampleListsEqualUpToTimestamp(
        [
            sample.Sample(
                metric='check_timer',
                value=1.163323,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='aesni_init',
                value=3.646818,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='pci_bridge_created',
                value=1.464983,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='pci_dma_setup',
                value=1.788605,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='scsi_init',
                value=1.958622,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='serial_8250_init',
                value=1.800432,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='tcp_bind_alloc',
                value=1.612092,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='rod_marked',
                value=1.873822,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='ps2_controller',
                value=1.806604,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='rng_init',
                value=1.15044,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='taskstats_reg',
                value=1.825742,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='scsi_pd',
                value=1.96243,
                unit='second',
                metadata={},
                timestamp=0,
            ),
        ],
        linux_boot.CollectKernelSamples(self.mock_vm, 1),
    )

  def testKernelStartTimestamp(self):
    self.mock_vm.RemoteCommand = mock.Mock(
        return_value=(
            """169174.74 2653910.92
1680819951.881177330""",
            '',
        )
    )
    self.assertEqual(
        linux_boot.GetKernelStartTimestamp(self.mock_vm), 1680650777.1411774
    )

  @parameterized.named_parameters(
      ('HourLong', '1h 19min 20.378s', 4760.378),
      ('MinuteLong', '1min 3.02s', 63.02),
      ('MSecLong', '203ms', 0.203),
  )
  def testParseSeconds(self, raw_str, sec):
    self.assertEqual(linux_boot._ParseSeconds(raw_str), sec)

  def testParseUserTotalTimes(self):
    """Test kernel time, total time parsing."""
    self.assertEqual(
        linux_boot.ParseUserTotalTimes(
            'Startup finished in 2.2s (kernel) + 1min 12.5s (userspace) '
            '= 1min 14.774s'
        ),
        (2.2, 74.7),
    )
    self.assertEqual(
        linux_boot.ParseUserTotalTimes(
            'Startup finished in 448ms (firmware) + 1.913s (loader) + '
            '1.183s (kernel) + 52.438s (initrd) + 30.413s (userspace) '
            '= 1min 26.398s'
        ),
        (53.621, 84.034),
    )

  def testParseSystemDCriticalChain(self):
    """Test systemd critical-chain output parsing."""
    with open(os.path.join(self.data_dir, 'systemd1.output')) as f:
      self.assertEqual(
          linux_boot.ParseSystemDCriticalChainOutput(f.read()), 2.741
      )
    with open(os.path.join(self.data_dir, 'systemd2.output')) as f:
      output = f.read()
      self.assertEqual(
          linux_boot.ParseSystemDCriticalChainOutput(output), 0.671103
      )
      self.assertEqual(
          linux_boot.ParseSystemDCriticalChainServiceTime(output), 0.000103
      )

  def testCollectVmToVmSamples(self):
    """Test vm to vm networking result parsing."""
    # Load startup script data, which ingress timestamps.
    with open(os.path.join(self.data_dir, 'boot.output')) as f:
      boot_output = f.read()
    # Load tcpdump output, which contains egress timestamps.
    self.enter_context(
        mock.patch.object(
            vm_util,
            'PrependTempDir',
            return_value=os.path.join(self.data_dir, 'tcpdump.output'),
        )
    )
    self.mock_vm.RemoteCommand = mock.Mock(return_value=(boot_output, ''))
    self.mock_vm.internal_ip = '10.128.0.11'
    # Compare golden samples with calculated egress/ingress samples.
    self.assertSampleListsEqualUpToTimestamp(
        linux_boot.CollectVmToVmSamples(
            self.mock_vm,
            ('10.128.0.2', ''),
            datetime.datetime.fromtimestamp(1680741767, pytz.timezone('UTC')),
        ),
        [
            sample.Sample(
                metric='internal_ingress',
                value=10.778103,
                unit='second',
                metadata={},
                timestamp=0,
            ),
            sample.Sample(
                metric='internal_egress',
                value=10.390749,
                unit='second',
                metadata={},
                timestamp=0,
            ),
        ],
    )


if __name__ == '__main__':
  unittest.main()
