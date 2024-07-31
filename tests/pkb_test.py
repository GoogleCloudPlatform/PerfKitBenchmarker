# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for pkb.py."""

import json
import textwrap
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_status
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import pkb
from perfkitbenchmarker import providers
from perfkitbenchmarker import publisher
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker import test_util
from perfkitbenchmarker.providers.gcp import util as gcp_utils
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

test_machine_type = ['n1-standard-2', 'n1-standard-4']


class TestCreateFailedRunSampleFlag(unittest.TestCase):

  def PatchPkbFunction(self, function_name):
    patcher = mock.patch(pkb.__name__ + '.' + function_name)
    mock_function = patcher.start()
    self.addCleanup(patcher.stop)
    return mock_function

  def setUp(self):
    super().setUp()
    self.flags_mock = self.PatchPkbFunction('FLAGS')
    self.provision_mock = self.PatchPkbFunction('DoProvisionPhase')
    self.prepare_mock = self.PatchPkbFunction('DoPreparePhase')
    self.run_mock = self.PatchPkbFunction('DoRunPhase')
    self.cleanup_mock = self.PatchPkbFunction('DoCleanupPhase')
    self.teardown_mock = self.PatchPkbFunction('DoTeardownPhase')
    self.publish_failed_run_sample_mock = self.PatchPkbFunction(
        'PublishFailedRunSample'
    )

    self.flags_mock.skip_pending_runs_file = None
    self.flags_mock.run_stage = [
        stages.PROVISION,
        stages.PREPARE,
        stages.RUN,
        stages.CLEANUP,
        stages.TEARDOWN,
    ]

    self.spec = mock.MagicMock()
    self.collector = mock.MagicMock()

  def testCreateProvisionFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.provision_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.publish_failed_run_sample_mock.assert_called_once_with(
        self.spec, error_msg, stages.PROVISION, self.collector
    )

  def testCreatePrepareFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.prepare_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.publish_failed_run_sample_mock.assert_called_once_with(
        self.spec, error_msg, stages.PREPARE, self.collector
    )

  def testCreateRunFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.run_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.publish_failed_run_sample_mock.assert_called_once_with(
        self.spec, error_msg, stages.RUN, self.collector
    )

  def testCreateCleanupFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.cleanup_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.publish_failed_run_sample_mock.assert_called_once_with(
        self.spec, error_msg, stages.CLEANUP, self.collector
    )

  def testCreateTeardownFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.teardown_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.publish_failed_run_sample_mock.assert_called_once_with(
        self.spec, error_msg, stages.TEARDOWN, self.collector
    )

  def testDontCreateFailedRunSample(self):
    self.flags_mock.create_failed_run_samples = False
    self.run_mock.side_effect = Exception('error')

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.publish_failed_run_sample_mock.assert_not_called()


class TestPublishStageStartedSamples(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.collector = (
        mock.patch.object(publisher, 'SampleCollector', autospec=True)
        .start()
        .return_value
    )
    self.benchmark_spec = mock.create_autospec(
        'perfkitbenchmarker.benchmark_spec.BenchmarkSpec', instance=True
    )
    self.benchmark_spec.name = 'fake_benchmark'

  @mock.patch('time.time', return_value=123.45)
  def testProvisionStageDefault(self, _):
    pkb._PublishStageStartedSamples(
        stages.PROVISION, benchmark_spec=self.benchmark_spec
    )
    self.collector.PublishSamples.assert_not_called()

  @flagsaver.flagsaver(create_started_run_sample=True)
  @flagsaver.flagsaver(create_started_stage_samples=True)
  @mock.patch('time.time', return_value=123.45)
  def testProvisionStageFlags(self, _):
    pkb._PublishStageStartedSamples(
        stages.PROVISION, benchmark_spec=self.benchmark_spec
    )
    self.collector.AddSamples.assert_has_calls([
        mock.call(
            [sample.Sample('Run Started', 123.45, 'seconds', {'flags': '{}'})],
            'fake_benchmark',
            self.benchmark_spec,
        ),
        mock.call(
            [sample.Sample('Provision Stage Started', 123.45, 'seconds', {})],
            'fake_benchmark',
            self.benchmark_spec,
        ),
    ])
    self.collector.PublishSamples.assert_called()


class TestPublishFailedRunSample(unittest.TestCase):

  @mock.patch('time.time', return_value=123.45)
  @mock.patch('perfkitbenchmarker.sample.Sample')
  def testPublishFailedRunSample(self, sample_mock, _):
    error_msg = 'error'
    spec = mock.MagicMock()
    spec.vms = []
    spec.failed_substatus = None
    pkb.PublishFailedRunSample(
        spec, error_msg, stages.PROVISION, collector=mock.Mock()
    )

    sample_mock.assert_called_once_with(
        'Run Failed',
        123.45,
        'seconds',
        {
            'error_message': error_msg,
            'run_stage': stages.PROVISION,
            'flags': '{}',
        },
    )

  @mock.patch('time.time', return_value=123.45)
  @mock.patch('perfkitbenchmarker.sample.Sample')
  def testPublishFailedRunSampleWithTruncation(self, sample_mock, _):
    error_msg = 'This is a long error message that should be truncated.'
    spec = mock.MagicMock()
    spec.vms = []
    spec.failed_substatus = 'QuotaExceeded'
    pkb.FLAGS.failed_run_samples_error_length = 7

    pkb.PublishFailedRunSample(
        spec, error_msg, stages.PROVISION, collector=mock.Mock()
    )

    sample_mock.assert_called_once_with(
        'Run Failed',
        123.45,
        'seconds',
        {
            'error_message': 'This is',
            'run_stage': stages.PROVISION,
            'flags': '{}',
            'failed_substatus': 'QuotaExceeded',
        },
    )


class TestMiscFunctions(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):
  """Testing for various functions in pkb.py."""

  def _MockVm(
      self, name: str, remote_command_text: str
  ) -> linux_virtual_machine.BaseLinuxVirtualMachine:
    vm_spec = pkb_common_test_case.CreateTestVmSpec()
    vm = pkb_common_test_case.TestLinuxVirtualMachine(vm_spec=vm_spec)
    vm.OS_TYPE = 'debian9'
    vm.name = name
    vm.RemoteCommand = mock.Mock(return_value=(remote_command_text, ''))
    return vm

  def _MockVmWithVuln(
      self, name: str, cpu_vuln: linux_virtual_machine.CpuVulnerabilities
  ) -> mock.Mock:
    vm = mock.Mock(OS_TYPE='debian9')
    vm.name = name
    type(vm).cpu_vulnerabilities = mock.PropertyMock(return_value=cpu_vuln)
    return vm

  def testGatherCpuVulnerabilitiesNonLinux(self):
    # Windows VMs do not currently have code to detect CPU vulnerabilities
    vuln = linux_virtual_machine.CpuVulnerabilities()
    vuln.mitigations['a'] = 'b'
    vm = self._MockVmWithVuln('vm1', vuln)
    vm.OS_TYPE = 'windows'
    self.assertLen(pkb._CreateCpuVulnerabilitySamples([vm]), 0)

  def testGatherCpuVulnerabilitiesEmpty(self):
    # Even if CpuVulnerabilities is empty a sample is created
    vm = self._MockVmWithVuln('vm1', linux_virtual_machine.CpuVulnerabilities())
    samples = pkb._CreateCpuVulnerabilitySamples([vm])
    self.assertEqual({'vm_name': 'vm1'}, samples[0].metadata)
    self.assertLen(samples, 1)

  def testGatherCpuVulnerabilities(self):
    prefix = '/sys/devices/system/cpu/vulnerabilities'
    vm0 = self._MockVm('vm0', f"""{prefix}/itlb_multihit:KVM: Vulnerable""")
    vm1 = self._MockVm('vm1', f"""{prefix}/l1tf:Mitigation: PTE Inversion""")
    samples = pkb._CreateCpuVulnerabilitySamples([vm0, vm1])
    self.assertEqual('cpu_vuln', samples[0].metric)
    expected_metadata0 = {
        'vm_name': 'vm0',
        'vulnerabilities': 'itlb_multihit',
        'vulnerability_itlb_multihit': 'KVM',
    }
    expected_metadata1 = {
        'vm_name': 'vm1',
        'mitigations': 'l1tf',
        'mitigation_l1tf': 'PTE Inversion',
    }
    self.assertEqual(expected_metadata0, samples[0].metadata)
    self.assertEqual(expected_metadata1, samples[1].metadata)
    self.assertLen(samples, 2)

  def testParseMeminfo(self):
    meminfo_text = textwrap.dedent("""
    MemTotal:       16429552 kB
    MemFree:        13772912 kB
    HugePages_Free: 0
    BadValue1:      a
    BadValue2:      1 mB
    BadValue3:      1 kB extra""").strip()

    parsed, unparsed = pkb._ParseMeminfo(meminfo_text)

    expected_parsed = {
        'HugePages_Free': 0,
        'MemFree': 13772912,
        'MemTotal': 16429552,
    }
    expected_unparsed = [
        'BadValue1:      a',
        'BadValue2:      1 mB',
        'BadValue3:      1 kB extra',
    ]
    self.assertEqual(expected_parsed, parsed)
    self.assertEqual(expected_unparsed, unparsed)

  def testCollectMeminfoHandlerDefault(self):
    # must set --collect_meminfo to collect samples
    vm = mock.Mock()
    benchmark_spec = mock.Mock(vms=[vm])
    samples = []

    pkb._CollectMeminfoHandler(None, benchmark_spec, samples)

    self.assertEmpty(samples)
    vm.RemoteCommand.assert_not_called()

  @flagsaver.flagsaver(collect_meminfo=True)
  def testCollectMeminfoHandlerIgnoreWindows(self):
    vm = mock.Mock()
    vm.OS_TYPE = 'windows2019_desktop'
    benchmark_spec = mock.Mock(vms=[vm])
    samples = []

    pkb._CollectMeminfoHandler(None, benchmark_spec, samples)

    self.assertEmpty(samples)

  @flagsaver.flagsaver(collect_meminfo=True)
  def testCollectMeminfoHandler(self):
    vm = mock.Mock()
    vm.RemoteCommand.return_value = 'b: 100\na: 10\nbadline', ''
    vm.name = 'pkb-1234-0'
    vm.OS_TYPE = 'ubuntu2004'
    vm.machine_type = 'n1-standard-2'
    benchmark_spec = mock.Mock(vms=[vm])
    samples = []

    pkb._CollectMeminfoHandler(None, benchmark_spec, samples)

    expected_metadata = {
        'a': 10,
        'b': 100,
        'meminfo_keys': 'a,b',
        'meminfo_malformed': 'badline',
        'meminfo_machine_type': 'n1-standard-2',
        'meminfo_os_type': 'ubuntu2004',
        'meminfo_vmname': 'pkb-1234-0',
    }
    expected_sample = sample.Sample('meminfo', 0, '', expected_metadata)
    self.assertSampleListsEqualUpToTimestamp([expected_sample], samples)
    vm.RemoteCommand.assert_called_with('cat /proc/meminfo')

  def test_IsException_subclass(self):
    e = errors.Resource.CreationInternalError('internal error')
    self.assertTrue(pkb._IsException(e, errors.Resource.CreationInternalError))

  def test_IsException_substring(self):
    def RaiseCreationInternalError(unused):
      raise errors.Resource.CreationInternalError('internal error')

    with self.assertRaises(errors.VmUtil.ThreadException) as cm:
      background_tasks.RunThreaded(RaiseCreationInternalError, [None])
    self.assertTrue(
        pkb._IsException(cm.exception, errors.Resource.CreationInternalError)
    )

  def test_IsException_false(self):
    e = errors.Resource.CreationInternalError('internal error')
    self.assertFalse(pkb._IsException(e, errors.Benchmarks.QuotaFailure))


class TestRunBenchmarks(pkb_common_test_case.PkbCommonTestCase):

  def _MockLoadProviderUtils(self, utils_module):
    return self.enter_context(
        mock.patch.object(
            providers,
            'LoadProviderUtils',
            autospec=True,
            return_value=utils_module,
        )
    )

  def _MockGcpUtils(self, function_name, return_value=None, side_effect=None):
    return self.enter_context(
        mock.patch.object(
            gcp_utils,
            function_name,
            autospec=True,
            return_value=return_value,
            side_effect=side_effect,
        )
    )

  @flagsaver.flagsaver(retries=3)
  def testRunRetries(self):
    test_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    # Generate some exceptions for each run.
    self.enter_context(
        mock.patch.object(
            pkb,
            'DoProvisionPhase',
            side_effect=errors.Benchmarks.QuotaFailure(),
        )
    )

    benchmark_specs, _ = pkb.RunBenchmarkTask(spec=test_spec)

    self.assertEqual(len(benchmark_specs), 4)

  @parameterized.named_parameters(
      {
          'testcase_name': 'SuccessStatus',
          'status': benchmark_status.SUCCEEDED,
          'failed_substatus': None,
          'retry_substatuses': pkb._RETRY_SUBSTATUSES.value,
          'expected_retry': False,
      },
      {
          'testcase_name': 'UncategorizedFailure',
          'status': benchmark_status.FAILED,
          'failed_substatus': benchmark_status.FailedSubstatus.UNCATEGORIZED,
          'retry_substatuses': pkb._RETRY_SUBSTATUSES.value,
          'expected_retry': False,
      },
      {
          'testcase_name': 'FailedSubstatusNotIncluded',
          'status': benchmark_status.FAILED,
          'failed_substatus': benchmark_status.FailedSubstatus.QUOTA,
          'retry_substatuses': [benchmark_status.FailedSubstatus.INTERRUPTED],
          'expected_retry': False,
      },
      {
          'testcase_name': 'FailedSubstatusIncluded',
          'status': benchmark_status.FAILED,
          'failed_substatus': (
              benchmark_status.FailedSubstatus.INSUFFICIENT_CAPACITY
          ),
          'retry_substatuses': [
              benchmark_status.FailedSubstatus.INSUFFICIENT_CAPACITY
          ],
          'expected_retry': True,
      },
  )
  def testShouldRetry(
      self, status, failed_substatus, retry_substatuses, expected_retry
  ):
    test_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    test_spec.status = status
    test_spec.failed_substatus = failed_substatus
    with flagsaver.flagsaver(retry_substatuses=retry_substatuses):
      self.assertEqual(pkb._ShouldRetry(test_spec), expected_retry)

  @parameterized.named_parameters(
      {
          'testcase_name': 'SmartRetry',
          'quota_flag_value': True,
          'capacity_flag_value': True,
          'retry_count': 2,
          'run_results': [
              errors.Benchmarks.QuotaFailure(),
              errors.Benchmarks.InsufficientCapacityCloudFailure(),
              Exception(),
          ],
          'expected_run_count': 3,
          'expected_quota_retry_calls': 1,
          'expected_capacity_retry_calls': 1,
      },
      {
          'testcase_name': 'Default',
          'quota_flag_value': False,
          'capacity_flag_value': False,
          'retry_count': 2,
          'run_results': [
              errors.Benchmarks.QuotaFailure(),
              errors.Benchmarks.InsufficientCapacityCloudFailure(),
              Exception(),
          ],
          'expected_run_count': 3,
          'expected_quota_retry_calls': 0,
          'expected_capacity_retry_calls': 0,
      },
  )
  @flagsaver.flagsaver
  def testRunBenchmarkTask(
      self,
      quota_flag_value,
      capacity_flag_value,
      retry_count,
      run_results,
      expected_run_count,
      expected_quota_retry_calls,
      expected_capacity_retry_calls,
  ):
    FLAGS.zones = ['test_zone']
    FLAGS.cloud = 'GCP'
    FLAGS.retries = retry_count
    FLAGS.smart_quota_retry = quota_flag_value
    FLAGS.smart_capacity_retry = capacity_flag_value
    test_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    # Generate some exceptions for each run.
    self.enter_context(
        mock.patch.object(pkb, 'DoProvisionPhase', side_effect=run_results)
    )
    # Mock the retry options.
    mock_quota_retry = self.enter_context(
        mock.patch.object(pkb.ZoneRetryManager, '_AssignZoneToNewRegion')
    )
    mock_capacity_retry = self.enter_context(
        mock.patch.object(pkb.ZoneRetryManager, '_AssignZoneToSameRegion')
    )
    self._MockGcpUtils('GetZonesFromMachineType', return_value=set())

    benchmark_specs, _ = pkb.RunBenchmarkTask(spec=test_spec)

    self.assertEqual(len(benchmark_specs), expected_run_count)
    # Retry preparation functions should have the right calls.
    self.assertEqual(mock_quota_retry.call_count, expected_quota_retry_calls)
    self.assertEqual(
        mock_capacity_retry.call_count, expected_capacity_retry_calls
    )

  @parameterized.named_parameters(
      ('1', False, False, ['test_zone_1'], ['test_zone_2'], 1, True),
      ('2', True, True, ['test_zone_1'], ['test_zone_2'], 1, False),
      ('3', True, True, [], [], 1, False),
      ('4', True, True, [], ['test_zone_1'], 1, True),
      ('5', True, True, ['test_zone_2'], [], 1, True),
      ('6', True, False, ['test_zone_1', 'test_zone_2'], [], 1, False),
      ('7', True, True, ['test_zone_2'], [], 0, False),
  )
  def testValidateSmartZoneRetryFlags(
      self,
      smart_quota_retry,
      smart_capacity_retry,
      zone,
      zones,
      retries,
      is_valid,
  ):
    flags_dict = {
        'retries': retries,
        'smart_quota_retry': smart_quota_retry,
        'smart_capacity_retry': smart_capacity_retry,
        'zone': zone,
        'zones': zones,
    }
    self.assertEqual(pkb.ValidateSmartZoneRetryFlags(flags_dict), is_valid)

  @parameterized.named_parameters(
      ('NoRetrySomeStages', 0, [stages.PROVISION, stages.PREPARE], True),
      ('RetrySomeStages', 1, [stages.PROVISION, stages.PREPARE], False),
      ('NoRetryAllStages', 0, stages.STAGES, True),
      ('RetryAllStages', 1, stages.STAGES, True),
  )
  def testValidateRetriesAndBenchmarkStages(self, retries, run_stage, is_valid):
    flags_dict = {'retries': retries, 'run_stage': run_stage}
    self.assertEqual(pkb.ValidateRetriesAndRunStages(flags_dict), is_valid)

  @parameterized.named_parameters(
      ('InvalidCondition', ['invalid_condition']),
      ('InvalidDirection', ['metric1=1.5']),
      ('InvalidThreshold', ['metric1>invalid_threshold']),
  )
  def testParseSkipTeardownConditionsInvalid(self, conditions):
    self.assertRaises(
        ValueError,
        pkb.ParseSkipTeardownConditions,
        conditions,
    )

  @parameterized.named_parameters(
      (
          'ValidCondition',
          ['metric1>1.5'],
          {
              'metric1': {'lower_bound': 1.5, 'upper_bound': None}
          },
      ),
      (
          'ValidConditions',
          ['metric1>1.5', 'metric2<2.5'],
          {
              'metric1': {'lower_bound': 1.5, 'upper_bound': None},
              'metric2': {'lower_bound': None, 'upper_bound': 2.5},
          },
      ),
      (
          'UpperAndLowerBounds',
          ['metric1>1.5', 'metric1<2.5'],
          {
              'metric1': {'lower_bound': 1.5, 'upper_bound': 2.5},
          },
      ),
      (
          'LowerBoundReplaced',
          ['metric1>2.5', 'metric1>1.5'],
          {
              'metric1': {'lower_bound': 1.5, 'upper_bound': None},
          },
      ),
      (
          'UpperBoundReplaced',
          ['metric1<1.5', 'metric1<2.5'],
          {
              'metric1': {'lower_bound': None, 'upper_bound': 2.5},
          },
      ),
  )
  def testParseSkipTeardownConditionsValid(self, conditions, parsed):
    self.assertEqual(pkb.ParseSkipTeardownConditions(conditions), parsed)

  @parameterized.named_parameters(
      ('NoConditions', {'skip_teardown_conditions': []}, True),
      (
          'InvalidThreshold',
          {'skip_teardown_conditions': ['metric1>invalid_threshold']},
          False,
      ),
      (
          'ValidConditions',
          {'skip_teardown_conditions': ['metric1>1.5', 'metric2<2.5']},
          True,
      ),
  )
  def testValidateSkipTeardownConditions(self, flags_dict, is_valid):
    self.assertEqual(pkb.ValidateSkipTeardownConditions(flags_dict), is_valid)

  @flagsaver.flagsaver(zone=['zone_1'], smart_quota_retry=True, retries=1)
  def testSmartQuotaRetry(self):
    test_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    test_spec.failed_substatus = benchmark_status.FailedSubstatus.QUOTA
    # Start with zone_1 in region_1.
    self._MockLoadProviderUtils(gcp_utils)
    self._MockGcpUtils('GetZonesFromMachineType', return_value=set())
    self._MockGcpUtils('GetRegionFromZone', return_value='region_1')
    self._MockGcpUtils('GetGeoFromRegion')
    self._MockGcpUtils('GetRegionsInGeo', return_value={'region_1', 'region_2'})
    # Expect that region_1 is skipped when getting zones.
    mock_get_zones = self._MockGcpUtils(
        'GetZonesInRegion', return_value={'zone_2'}
    )

    test_retry_manager = pkb.ZoneRetryManager(test_machine_type)
    test_retry_manager.HandleSmartRetries(test_spec)

    # Function should not get zones from region_1, resulting in only 1 call.
    mock_get_zones.assert_called_once()
    # zone_1 is recorded.
    self.assertEqual(test_retry_manager._zones_tried, {'zone_1'})
    # zone_2 is the new zone picked.
    self.assertEqual(FLAGS.zone, ['zone_2'])

  @flagsaver.flagsaver(zone=['zone_1'], smart_capacity_retry=True, retries=1)
  def testSmartCapacityRetry(self):
    test_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    test_spec.failed_substatus = (
        benchmark_status.FailedSubstatus.INSUFFICIENT_CAPACITY
    )
    self._MockLoadProviderUtils(gcp_utils)
    self._MockGcpUtils('GetRegionFromZone')
    # Expect that the correct possible zones are passed to the function below.
    self._MockGcpUtils('GetZonesInRegion', return_value={'zone_1', 'zone_2'})
    self._MockGcpUtils(
        'GetZonesFromMachineType', return_value={'zone_1', 'zone_2'}
    )

    test_retry_manager = pkb.ZoneRetryManager(test_machine_type)
    test_retry_manager.HandleSmartRetries(test_spec)

    # zone_1 is recorded.
    self.assertEqual(test_retry_manager._zones_tried, {'zone_1'})
    # zone_2 is the new zone picked.
    self.assertEqual(FLAGS.zone, ['zone_2'])

  @parameterized.named_parameters(('ZonesFlag', 'zones'), ('ZoneFlag', 'zone'))
  @flagsaver.flagsaver(retries=2)
  def testChooseAndSetNewZone(self, zone_flag):
    FLAGS[zone_flag].parse(['us-west1-a'])
    FLAGS.smart_quota_retry = True
    self._MockGcpUtils('GetZonesFromMachineType', return_value=set())
    test_retry_manager = pkb.ZoneRetryManager(test_machine_type)
    possible_zones = {'us-west1-a', 'us-west1-b'}

    # us-west1-b is chosen.
    test_retry_manager._ChooseAndSetNewZone(possible_zones)
    self.assertEqual(FLAGS[zone_flag].value[0], 'us-west1-b')
    self.assertEqual(test_retry_manager._zones_tried, {'us-west1-a'})

    # All possible zones are exhausted so the original zone is used.
    test_retry_manager._ChooseAndSetNewZone(possible_zones)
    self.assertEqual(FLAGS[zone_flag].value[0], 'us-west1-a')
    self.assertEmpty(test_retry_manager._zones_tried)


class FreezeRestoreTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(freeze='mock_freeze_path')
  def testSpecPickledEvenWithException(self):
    # Make provision have an exception.
    self.enter_context(
        mock.patch.object(pkb, 'DoProvisionPhase', side_effect=Exception())
    )
    # Make benchmark_spec avoid pickling for test.
    test_benchmark_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        pkb_common_test_case.SIMPLE_CONFIG, 'cluster_boot'
    )
    self.enter_context(mock.patch.object(test_benchmark_spec, 'Pickle'))
    mock_freeze = self.enter_context(
        mock.patch.object(test_benchmark_spec, 'Freeze')
    )
    collector = publisher.SampleCollector()

    # Run the benchmark loop.
    with self.assertRaises(Exception):
      pkb.RunBenchmark(test_benchmark_spec, collector)

    # PKB should still attempt to freeze benchmark spec.
    mock_freeze.assert_called_once()

  @flagsaver.flagsaver(freeze='mock_freeze_path', restore='mock_restore_path')
  def testCompletionStatusesContainFreezeRestoreStatus(self):
    # Arrange
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    test_bm_spec.status = benchmark_status.SUCCEEDED
    mock_status_file = mock.Mock()
    test_bm_spec.config.flags = {}

    # Act
    pkb._WriteCompletionStatusFile([test_bm_spec], mock_status_file)

    # Assert
    mock_status_file.write.assert_called_once_with(
        json.dumps({
            'name': 'cluster_boot',
            'status': benchmark_status.SUCCEEDED,
            'flags': {
                'freeze': 'mock_freeze_path',
                'restore': 'mock_restore_path',
            },
        })
        + '\n'
    )

  def testRestoreRelationalDb(self):
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    test_bm_spec.restore_spec = 'test_spec'
    test_bm_spec.relational_db = mock.Mock()

    test_bm_spec.Provision()

    test_bm_spec.relational_db.Create.assert_called_with(restore=True)

  def testFreezeRelationalDb(self):
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    test_bm_spec.freeze_path = 'test_path'
    self.enter_context(mock.patch.object(test_bm_spec, 'Freeze'))
    test_bm_spec.relational_db = mock.Mock()

    test_bm_spec.Delete()

    test_bm_spec.relational_db.Delete.assert_called_with(freeze=True)


class TestConditionalSkipTeardown(parameterized.TestCase):
  SAMPLES = [
      sample.Sample(
          metric='metric1',
          value=1.0,
          unit='seconds',
          timestamp=1678147200.0,
      ).asdict(),
      sample.Sample(
          metric='metric2',
          value=2.0,
          unit='seconds',
          timestamp=1678147200.0,
      ).asdict(),
      sample.Sample(
          metric='metricminus10',
          value=-10.0,
          unit='seconds',
          timestamp=1678147200.0,
      ).asdict(),
  ]

  @parameterized.named_parameters(
      {
          'testcase_name': 'no_conditions_met',
          'conditions': {
              'metric1': {'lower_bound': 1.5, 'upper_bound': None},
              'metric2': {'lower_bound': None, 'upper_bound': 1.5},
          },
      },
      {
          'testcase_name': 'upper_greater_than_lower',
          'conditions': {
              'metric1': {'lower_bound': 1.5, 'upper_bound': 2.5},
          },
      },
      {
          'testcase_name': 'upper_less_than_lower',
          'conditions': {
              'metric1': {'lower_bound': 1.5, 'upper_bound': 0.5},
          },
      },
      {
          'testcase_name': 'neither_bound',
          'conditions': {
              'metric1': {'lower_bound': None, 'upper_bound': None},
          },
      },
      {
          'testcase_name': 'no_flag_passed',
          'conditions': None,
      },
  )
  def testTeardownAsUsual(self, conditions):
    self.assertTrue(
        pkb.ShouldTeardown(
            skip_teardown_conditions=conditions,
            samples=self.SAMPLES,
        )
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'less_test',
          'conditions': {
              'metric1': {'lower_bound': None, 'upper_bound': 1.5},
          },
      },
      {
          'testcase_name': 'greater_test',
          'conditions': {
              'metric2': {'lower_bound': 1.5, 'upper_bound': None},
          },
      },
      {
          'testcase_name': 'multiple_conditions',
          'conditions': {
              'metric1': {'lower_bound': None, 'upper_bound': 1.5},
              'metric2': {'lower_bound': 1.5, 'upper_bound': None},
          },
      },
      {
          'testcase_name': 'upper_greater_than_lower',
          'conditions': {
              'metric1': {'lower_bound': 0.5, 'upper_bound': 1.5},
          },
      },
      {
          'testcase_name': 'upper_less_than_lower',
          'conditions': {
              'metric1': {'lower_bound': 2.5, 'upper_bound': 1.5},
          },
      },
      {
          'testcase_name': 'zero_bound',
          'conditions': {
              'metricminus10': {'lower_bound': None, 'upper_bound': 0.0},
          },
      },
  )
  def testSkipTeardown(self, conditions):
    self.assertFalse(
        pkb.ShouldTeardown(
            skip_teardown_conditions=conditions,
            samples=self.SAMPLES,
        )
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'teardown_anyway',
          'num_vms': 1,
          'zonal_vm_limit': 1,
          'expected_result': True,
      },
      {
          'testcase_name': 'skip_teardown',
          'num_vms': 1,
          'zonal_vm_limit': 2,
          'expected_result': False,
      },
      {
          'testcase_name': 'teardown_anyway_multiple_vms',
          'num_vms': 2,
          'zonal_vm_limit': 2,
          'expected_result': True,
      },
  )
  def testShouldTeardownZonalVmLimit(
      self, num_vms, zonal_vm_limit, expected_result
  ):
    test_vm = mock.MagicMock()
    test_vm.GetNumTeardownSkippedVms.return_value = 1
    test_conditions = {
        'metric1': {'lower_bound': None, 'upper_bound': 1.5},
    }
    self.assertEqual(
        expected_result,
        pkb.ShouldTeardown(
            skip_teardown_conditions=test_conditions,
            samples=self.SAMPLES,
            vms=[test_vm] * num_vms,
            skip_teardown_zonal_vm_limit=zonal_vm_limit,
        )
    )


class TestParseArgs(pkb_common_test_case.PkbCommonTestCase):

  def testBasicCase(self):
    pkb._ParseFlags(['./pkb.py', '--benchmarks=foo'])
    self.assertSameElements(FLAGS.benchmarks, ['foo'])

  def testPositionalArg(self):
    with self.assertRaises(flags.Error):
      pkb._ParseFlags(['./pkb.py', '--benchmarks=foo', 'positional_arg'])

  def testUnknownFlag(self):
    with self.assertRaises(flags.Error):
      pkb._ParseFlags(['./pkb.py', '--unknown-flag=foo'])

if __name__ == '__main__':
  unittest.main()
