import copy
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import disk
from perfkitbenchmarker.linux_benchmarks.fio import utils
from tests import pkb_common_test_case


FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'
_COMPONENT = 'test_component'


class FioUtitTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.filename = '/test/filename'
    self.mock_vm = mock.MagicMock()
    self.disk_spec = disk.BaseDiskSpec(
        _COMPONENT,
        FLAGS,
        device_path='/tmp/disk/path',
        disk_number=1,
        disk_size=10,
        disk_type='hyperdisk-balanced',
        mount_point='/scratch',
        num_striped_disks=1,
    )
    self.disks = [disk.BaseDisk(self.disk_spec)]
    self.mock_vm.scratch_disks = self.disks

  @flagsaver.flagsaver(fio_target_mode='against_device_with_fill')
  def testGetFilenameForDisk(self):
    self.assertEqual(utils.GetFilename(self.disks), '/tmp/disk/path')

  @flagsaver.flagsaver(fio_target_mode='against_file_with_fill')
  def testGetFilenameForDiskWorFile(self):
    self.assertEqual(utils.GetFilename(self.disks), 'fio-temp-file')

  def testGetAllDiskPaths(self):
    test_disk = self.disks[0]
    test_disk.is_striped = True
    test_disk.disks = [copy.deepcopy(test_disk), copy.deepcopy(test_disk)]
    disk_paths = utils.GetAllDiskPaths([test_disk])
    self.assertEqual(disk_paths, ['/tmp/disk/path', '/tmp/disk/path'])

  @flagsaver.flagsaver(fio_runtime=30)
  def testJobRenderingForOneDisk(self):
    generated_job_file = utils.GenerateJobFile(
        self.disks,
        ['rand_4k_read_100%_iodepth-1_numjobs-1'],
        {},
        'fio-parent.job',
    )
    expected_job_file = '[global]\nioengine=libaio\ninvalidate=1\ndirect=1\nruntime=30\nramp_time=120\ntime_based\nfilename=fio-temp-file\ndo_verify=0\nverify_fatal=0\ngroup_reporting=1\nrandrepeat=0\n\n[rand_4k_read_100%-io-depth-1-num-jobs-1]\nstonewall\nrw=randread\nblocksize=4k\niodepth=1\nsize=100%\nnumjobs=1'
    self.assertMultiLineEqual(expected_job_file, generated_job_file)

  @flagsaver.flagsaver(fio_runtime=30)
  def testJobRenderingForMultipleDisks(self):
    test_disk = self.disks[0]
    test_disk.disks = [copy.deepcopy(test_disk), copy.deepcopy(test_disk)]
    test_disk.is_striped = True
    FLAGS['fio_separate_jobs_for_disks'].value = True
    generated_job_file = utils.GenerateJobFile(
        [test_disk],
        ['rand_4k_read_100%_iodepth-1_numjobs-1'],
        {},
        'fio-parent.job',
    )
    expected_job_file = '[global]\nioengine=libaio\ninvalidate=1\ndirect=1\nruntime=30\nramp_time=120\ntime_based\nfilename=fio-temp-file\ndo_verify=0\nverify_fatal=0\ngroup_reporting=1\nrandrepeat=0\n\n[rand_4k_read_100%-io-depth-1-num-jobs-1.0]\nstonewall\nrw=randread\nblocksize=4k\niodepth=1\nsize=100%\nnumjobs=1\nfilename=/tmp/disk/path\n\n[rand_4k_read_100%-io-depth-1-num-jobs-1.1]\nstonewall\nrw=randread\nblocksize=4k\niodepth=1\nsize=100%\nnumjobs=1\nfilename=/tmp/disk/path'
    self.assertMultiLineEqual(expected_job_file, generated_job_file)


if __name__ == '__main__':
  unittest.main()
