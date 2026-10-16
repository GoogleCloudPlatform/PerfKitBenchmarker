"""Tests for the tpcx_hs linux package."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import tpcx_hs
from tests import pkb_common_test_case


class TpcxHsPackageTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()
    self.vm.RemoteCommand.return_value = ('', '')
    self.vm.ShouldDownloadPreprovisionedData.return_value = True
    self.enter_context(
        mock.patch.object(tpcx_hs.data, 'ResourceExists', return_value=False)
    )

  def testInstallDownloadsAndUnzipsStockKit(self):
    tpcx_hs.AptInstall(self.vm)
    self.vm.Install.assert_called_once_with('unzip')
    self.vm.ShouldDownloadPreprovisionedData.assert_called_once_with(
        'tpcx_hs', 'TPCX-HS-Tool.zip'
    )
    self.vm.DownloadPreprovisionedData.assert_called_once_with(
        tpcx_hs.TPCX_HS_DIR, 'tpcx_hs', 'TPCX-HS-Tool.zip'
    )
    self.vm.CheckPreprovisionedData.assert_called_once_with(
        tpcx_hs.TPCX_HS_DIR,
        'tpcx_hs',
        'TPCX-HS-Tool.zip',
        tpcx_hs.PREPROVISIONED_DATA['TPCX-HS-Tool.zip'],
    )
    commands = [c.args[0] for c in self.vm.RemoteCommand.call_args_list]
    self.assertIn(
        f'unzip -o -q {tpcx_hs.TPCX_HS_DIR}/TPCX-HS-Tool.zip -d '
        f'{tpcx_hs.TPCX_HS_DIR}',
        commands,
    )

  @flagsaver.flagsaver(
      tpcx_hs_kit_file='tpcx-hs.tar.gz',
      tpcx_hs_kit_bucket_dir='custom_dir',
      preprovision_ignore_checksum=True,
  )
  def testInstallTarballFromCustomDirWithoutChecksum(self):
    tpcx_hs.YumInstall(self.vm)
    self.vm.DownloadPreprovisionedData.assert_called_once_with(
        tpcx_hs.TPCX_HS_DIR, 'custom_dir', 'tpcx-hs.tar.gz'
    )
    self.vm.CheckPreprovisionedData.assert_not_called()
    commands = [c.args[0] for c in self.vm.RemoteCommand.call_args_list]
    self.assertIn(
        f'tar -xzf {tpcx_hs.TPCX_HS_DIR}/tpcx-hs.tar.gz -C'
        f' {tpcx_hs.TPCX_HS_DIR}',
        commands,
    )

  @flagsaver.flagsaver(tpcx_hs_kit_file='unknown.zip')
  def testInstallUnknownChecksumRaisesBeforeDownload(self):
    with self.assertRaisesRegex(
        errors.Setup.BadPreprovisionedDataError, 'No sha256 known'
    ):
      tpcx_hs.AptInstall(self.vm)
    self.vm.DownloadPreprovisionedData.assert_not_called()
    self.vm.CheckPreprovisionedData.assert_not_called()

  @flagsaver.flagsaver(preprovision_ignore_checksum=True)
  def testInstallStockKitIgnoreChecksumSkipsCheck(self):
    tpcx_hs.AptInstall(self.vm)
    self.vm.DownloadPreprovisionedData.assert_called_once()
    self.vm.CheckPreprovisionedData.assert_not_called()

  def testInstallMissingKitRaises(self):
    self.vm.ShouldDownloadPreprovisionedData.return_value = False
    with self.assertRaisesRegex(
        errors.Setup.BadPreprovisionedDataError, 'was not found'
    ):
      tpcx_hs.AptInstall(self.vm)
    self.vm.DownloadPreprovisionedData.assert_not_called()

  def testInstallPrefersLocalDataDir(self):
    self.enter_context(
        mock.patch.object(tpcx_hs.data, 'ResourceExists', return_value=True)
    )
    self.enter_context(
        mock.patch.object(
            tpcx_hs.data, 'ResourcePath', return_value='/local/TPCX-HS-Tool.zip'
        )
    )
    tpcx_hs.AptInstall(self.vm)
    self.vm.PushFile.assert_called_once_with(
        '/local/TPCX-HS-Tool.zip', tpcx_hs.TPCX_HS_DIR
    )
    self.vm.ShouldDownloadPreprovisionedData.assert_not_called()
    self.vm.DownloadPreprovisionedData.assert_not_called()

  def testGetJarPath(self):
    self.vm.RemoteCommand.return_value = (
        f'{tpcx_hs.TPCX_HS_DIR}/TPCx-HS_Kit/TPCx-HS-master_MR2.jar\n',
        '',
    )
    jar = tpcx_hs.GetJarPath(self.vm)
    self.assertEqual(
        jar, f'{tpcx_hs.TPCX_HS_DIR}/TPCx-HS_Kit/TPCx-HS-master_MR2.jar'
    )
    self.vm.RemoteCommand.assert_called_once()
    self.assertIn(
        'TPCx-HS-master_MR2*.jar', self.vm.RemoteCommand.call_args.args[0]
    )

  def testGetJarPathMissing(self):
    self.vm.RemoteCommand.return_value = ('\n', '')
    with self.assertRaises(errors.Setup.InvalidSetupError):
      tpcx_hs.GetJarPath(self.vm)

  def testScaleFactorConversions(self):
    self.assertEqual(tpcx_hs.ScaleFactorToRecords(1), 10**10)
    self.assertEqual(tpcx_hs.ScaleFactorToRecords(3), 3 * 10**10)
    self.assertAlmostEqual(tpcx_hs.RecordsToScaleFactor(10**9), 0.1)

  def testComputeHsph(self):
    # 1 TB sorted in one hour is exactly 1 HSph@1.
    self.assertAlmostEqual(tpcx_hs.ComputeHsph(1, 3600), 1.0)
    # 10 TB in half an hour is 20 HSph@10.
    self.assertAlmostEqual(tpcx_hs.ComputeHsph(10, 1800), 20.0)
    # Non-positive total_seconds reports 0.0 without crashing the benchmark.
    self.assertEqual(tpcx_hs.ComputeHsph(1, 0), 0.0)
    self.assertEqual(tpcx_hs.ComputeHsph(1, -10), 0.0)


if __name__ == '__main__':
  unittest.main()
