# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Unit tests for the kubernetes_opensearch_esrally_benchmark module."""

# pylint: disable=invalid-name,missing-function-docstring

import json
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import (
    kubernetes_opensearch_esrally_benchmark as esrally_bm,
)
from tests import pkb_common_test_case

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_RACE_JSON_ONE_OP = {
    'rally-version': '2.12.0',
    'track': 'geonames',
    'challenge': 'append-no-conflicts',
    'results': {
        'op_metrics': [
            {
                'task': 'bulk-index',
                'throughput': {'mean': 5000.0, 'unit': 'docs/s'},
                'latency': {'mean': 12.5, 'unit': 'ms', '90_0': 25.0},
            },
        ]
    },
}

_RACE_JSON_MULTI_OP = {
    'rally-version': '2.12.0',
    'track': 'geonames',
    'challenge': 'append-no-conflicts',
    'results': {
        'op_metrics': [
            {
                'task': 'bulk-index',
                'throughput': {'mean': 5000.0, 'unit': 'docs/s'},
                'latency': {'mean': 12.5, 'unit': 'ms', '90_0': 25.0},
            },
            {
                'task': 'default',
                'throughput': {'mean': 200.0, 'unit': 'ops/s'},
                'latency': {'mean': 8.1, 'unit': 'ms'},
            },
        ]
    },
}

_RACE_JSON_NO_P90 = {
    'rally-version': '2.12.0',
    'track': 'geonames',
    'challenge': 'append-no-conflicts',
    'results': {
        'op_metrics': [
            {
                'task': 'bulk-index',
                'throughput': {'mean': 3000.0, 'unit': 'docs/s'},
                'latency': {'mean': 20.0, 'unit': 'ms'},
            },
        ]
    },
}

_RACE_JSON_EMPTY_OPS = {
    'rally-version': '2.12.0',
    'track': 'geonames',
    'challenge': 'append-no-conflicts',
    'results': {'op_metrics': []},
}


def _make_logs(race_json):
  """Wrap race_json dict in PKB sentinel markers as a pod log string."""
  begin = esrally_bm._RACE_JSON_BEGIN  # pylint: disable=protected-access
  end = esrally_bm._RACE_JSON_END  # pylint: disable=protected-access
  return f'[pkb] Race complete.\n{begin}\n{json.dumps(race_json)}\n{end}\n'


def _base_nodepool_config():
  """Minimal nodepool config dict matching BENCHMARK_CONFIG structure."""
  return {
      'container_cluster': {
          'nodepools': {
              'servers': {
                  'vm_count': 1,
                  'vm_spec': {
                      'GCP': {
                          'machine_type': 'n2-standard-8',
                          'zone': 'us-central1-a',
                      },
                      'AWS': {
                          'machine_type': 'm6i.2xlarge',
                          'zone': 'us-east-1a',
                      },
                  },
              },
              'clients': {
                  'vm_count': 1,
                  'vm_spec': {
                      'GCP': {'machine_type': 'n2-standard-4'},
                      'AWS': {'machine_type': 'm6i.xlarge'},
                  },
              },
          }
      }
  }


# ---------------------------------------------------------------------------
# PrepareTest
# ---------------------------------------------------------------------------


class PrepareTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for Prepare() — OpenSearch StatefulSet deploy + rollout wait."""

  def setUp(self):
    super().setUp()
    self.mock_kube = self.enter_context(
        mock.patch.object(esrally_bm, 'kubernetes_commands')
    )

  def test_prepare_applies_opensearch_manifest(self):
    """Prepare() must apply the opensearch.yaml.j2 manifest."""
    esrally_bm.Prepare(mock.MagicMock())
    self.mock_kube.ApplyManifest.assert_called_once()
    call_args = self.mock_kube.ApplyManifest.call_args[0][0]
    self.assertIn('opensearch.yaml.j2', call_args)

  def test_prepare_waits_for_opensearch_rollout(self):
    """Prepare() must wait for statefulset/opensearch to be ready."""
    esrally_bm.Prepare(mock.MagicMock())
    self.mock_kube.WaitForRollout.assert_called_once()
    call_args = self.mock_kube.WaitForRollout.call_args[0][0]
    self.assertIn('statefulset/opensearch', call_args)

  def test_prepare_passes_opensearch_version_to_manifest(self):
    """Prepare() must forward opensearch_version flag to ApplyManifest."""
    esrally_bm.Prepare(mock.MagicMock())
    _, kwargs = self.mock_kube.ApplyManifest.call_args
    self.assertIn('opensearch_version', kwargs)


# ---------------------------------------------------------------------------
# RunTest
# ---------------------------------------------------------------------------


class RunTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for Run() — esrally Job lifecycle and result parsing."""

  def setUp(self):
    super().setUp()
    self.mock_kube = self.enter_context(
        mock.patch.object(esrally_bm, 'kubernetes_commands')
    )
    self.mock_kubectl = self.enter_context(
        mock.patch.object(esrally_bm, 'kubectl')
    )
    # Default: ApplyManifest returns one job resource path.
    self.mock_kube.ApplyManifest.return_value = {'job/esrally-pkb'}

  def _setup_successful_run(self, race_json=None):
    logs = _make_logs(race_json or _RACE_JSON_ONE_OP)
    self.mock_kube.WaitForResourceForMultiConditions.return_value = (
        'condition=Complete'
    )
    self.mock_kubectl.RunKubectlCommand.return_value = (logs, '', 0)

  def test_run_returns_samples_on_success(self):
    """Run() returns non-empty sample list when Job completes successfully."""
    self._setup_successful_run()
    samples = esrally_bm.Run(mock.MagicMock())
    self.assertNotEmpty(samples)

  def test_run_raises_run_error_on_job_failure(self):
    """Run() raises RunError when esrally Job reaches condition=Failed."""
    self.mock_kube.WaitForResourceForMultiConditions.return_value = (
        'condition=Failed'
    )
    with self.assertRaises(errors.Benchmarks.RunError):
      esrally_bm.Run(mock.MagicMock())

  def test_run_raises_run_error_on_job_timeout(self):
    """Run() raises RunError when WaitForResourceForMultiConditions times out."""
    self.mock_kube.WaitForResourceForMultiConditions.return_value = None
    with self.assertRaises(errors.Benchmarks.RunError):
      esrally_bm.Run(mock.MagicMock())

  def test_run_deletes_existing_job_before_apply(self):
    """Run() must delete any leftover Job before applying a new one."""
    self._setup_successful_run()
    esrally_bm.Run(mock.MagicMock())
    # First kubectl call must be a delete.
    first_call_args = self.mock_kubectl.RunKubectlCommand.call_args_list[0][0][
        0
    ]
    self.assertIn('delete', first_call_args)
    self.assertIn('esrally-pkb', first_call_args)


# ---------------------------------------------------------------------------
# ParseResultsTest
# ---------------------------------------------------------------------------


class KubernetesOpenSearchEsrallyBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):
  """Tests for kubernetes_opensearch_esrally_benchmark."""

  # ---- _ParseResults -------------------------------------------------------

  def test_parse_results_valid_single_op_with_p90(self):
    """Valid race.json with one op and p90 produces 3 samples."""
    logs = _make_logs(_RACE_JSON_ONE_OP)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access

    self.assertLen(results, 3)  # throughput + latency_mean + latency_p90

    metrics = {s.metric: s for s in results}
    self.assertIn('bulk_index_throughput', metrics)
    self.assertIn('bulk_index_latency_mean', metrics)
    self.assertIn('bulk_index_latency_p90', metrics)

    self.assertEqual(metrics['bulk_index_throughput'].value, 5000.0)
    self.assertEqual(metrics['bulk_index_throughput'].unit, 'docs/s')
    self.assertEqual(metrics['bulk_index_latency_mean'].value, 12.5)
    self.assertEqual(metrics['bulk_index_latency_mean'].unit, 'ms')
    self.assertEqual(metrics['bulk_index_latency_p90'].value, 25.0)

  def test_parse_results_valid_single_op_no_p90(self):
    """Op without p90 latency produces only throughput + latency_mean."""
    logs = _make_logs(_RACE_JSON_NO_P90)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access

    self.assertLen(results, 2)
    metrics = {s.metric for s in results}
    self.assertIn('bulk_index_throughput', metrics)
    self.assertIn('bulk_index_latency_mean', metrics)
    self.assertNotIn('bulk_index_latency_p90', metrics)

  def test_parse_results_multiple_ops(self):
    """Two ops: first with p90, second without, produce 5 samples total."""
    logs = _make_logs(_RACE_JSON_MULTI_OP)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access

    # bulk-index: throughput + latency_mean + latency_p90 = 3
    # default:    throughput + latency_mean (no p90) = 2
    self.assertLen(results, 5)

  def test_parse_results_empty_op_metrics_returns_empty_list(self):
    """race.json with empty op_metrics list produces no samples."""
    logs = _make_logs(_RACE_JSON_EMPTY_OPS)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access
    self.assertEmpty(results)

  def test_parse_results_missing_begin_sentinel_returns_empty_list(self):
    """Missing begin sentinel produces no samples."""
    end = esrally_bm._RACE_JSON_END  # pylint: disable=protected-access
    logs = f'{json.dumps(_RACE_JSON_ONE_OP)}\n{end}\n'
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access
    self.assertEmpty(results)

  def test_parse_results_missing_end_sentinel_returns_empty_list(self):
    """Missing end sentinel produces no samples."""
    begin = esrally_bm._RACE_JSON_BEGIN  # pylint: disable=protected-access
    logs = f'{begin}\n{json.dumps(_RACE_JSON_ONE_OP)}\n'
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access
    self.assertEmpty(results)

  def test_parse_results_no_sentinels_returns_empty_list(self):
    """Logs with no sentinel markers produce no samples."""
    results = esrally_bm._ParseResults(  # pylint: disable=protected-access
        'some random pod output\nno markers here'
    )
    self.assertEmpty(results)

  def test_parse_results_malformed_json_returns_empty_list(self):
    """Malformed JSON between sentinels produces no samples."""
    begin = esrally_bm._RACE_JSON_BEGIN  # pylint: disable=protected-access
    end = esrally_bm._RACE_JSON_END  # pylint: disable=protected-access
    logs = f'{begin}\n{{not valid json\n{end}\n'
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access
    self.assertEmpty(results)

  def test_parse_results_sample_metadata_fields(self):
    """Each sample carries the expected metadata keys."""
    logs = _make_logs(_RACE_JSON_ONE_OP)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access
    self.assertNotEmpty(results)

    meta = results[0].metadata
    self.assertIn('rally_version', meta)
    self.assertIn('rally_track', meta)
    self.assertIn('rally_challenge', meta)
    self.assertIn('opensearch_version', meta)
    self.assertIn('swap_enabled', meta)
    self.assertIn('esrally_task', meta)

    self.assertEqual(meta['rally_version'], '2.12.0')
    self.assertEqual(meta['rally_track'], 'geonames')
    self.assertEqual(meta['rally_challenge'], 'append-no-conflicts')

  def test_parse_results_task_hyphens_converted_to_underscores(self):
    """Task names with hyphens are normalised to underscores in metric names."""
    race_json = {
        'rally-version': '2.12.0',
        'track': 'geonames',
        'challenge': 'append-no-conflicts',
        'results': {
            'op_metrics': [
                {
                    'task': 'append-no-conflicts',
                    'throughput': {'mean': 1000.0, 'unit': 'docs/s'},
                    'latency': {'mean': 5.0, 'unit': 'ms'},
                },
            ]
        },
    }
    logs = _make_logs(race_json)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access

    metrics = {s.metric for s in results}
    self.assertIn('append_no_conflicts_throughput', metrics)
    self.assertIn('append_no_conflicts_latency_mean', metrics)
    self.assertNotIn('append-no-conflicts_throughput', metrics)

  def test_parse_results_returns_sample_instances(self):
    """Every returned object is a sample.Sample."""
    logs = _make_logs(_RACE_JSON_ONE_OP)
    results = esrally_bm._ParseResults(logs)  # pylint: disable=protected-access
    for s in results:
      self.assertIsInstance(s, sample.Sample)

  # ---- GetConfig -----------------------------------------------------------

  @mock.patch.object(configs, 'LoadConfig', autospec=True)
  def test_get_config_swap_disabled_returns_base_config(self, mock_load_config):
    """Default (swap disabled) config has no swap_config on servers nodepool."""
    base_config = _base_nodepool_config()
    mock_load_config.return_value = base_config

    config = esrally_bm.GetConfig({})

    mock_load_config.assert_called_once_with(
        esrally_bm.BENCHMARK_CONFIG, {}, esrally_bm.BENCHMARK_NAME
    )
    server_np = config['container_cluster']['nodepools']['servers']
    self.assertNotIn('swap_config', server_np)
    self.assertEqual(
        server_np['vm_spec']['GCP']['machine_type'], 'n2-standard-8'
    )

  @flagsaver.flagsaver(kubernetes_opensearch_esrally_swap_enabled=True)
  @mock.patch.object(configs, 'LoadConfig', autospec=True)
  def test_get_config_swap_enabled_injects_swap_config(self, mock_load_config):
    """swap_config dict is injected on servers nodepool when flag is set."""
    mock_load_config.return_value = _base_nodepool_config()

    config = esrally_bm.GetConfig({})

    server_np = config['container_cluster']['nodepools']['servers']
    self.assertIn('swap_config', server_np)

    swap_cfg = server_np['swap_config']
    self.assertTrue(swap_cfg['enabled'])
    self.assertEqual(swap_cfg['swappiness'], 100)
    self.assertEqual(swap_cfg['min_free_kbytes'], 67584)
    self.assertIn('watermark_scale_factor', swap_cfg)
    self.assertIn('boot_disk_iops', swap_cfg)
    self.assertIn('boot_disk_throughput', swap_cfg)

  @flagsaver.flagsaver(kubernetes_opensearch_esrally_swap_enabled=True)
  @mock.patch.object(configs, 'LoadConfig', autospec=True)
  def test_get_config_swap_enabled_upgrades_gcp_machine_type(
      self, mock_load_config
  ):
    """GCP servers nodepool is upgraded to n4-highmem-32 + hyperdisk."""
    mock_load_config.return_value = _base_nodepool_config()

    config = esrally_bm.GetConfig({})

    gcp_spec = config['container_cluster']['nodepools']['servers']['vm_spec'][
        'GCP'
    ]
    self.assertEqual(gcp_spec['machine_type'], 'n4-highmem-32')
    self.assertEqual(gcp_spec['boot_disk_type'], 'hyperdisk-balanced')
    self.assertEqual(gcp_spec['boot_disk_size'], 500)

  @flagsaver.flagsaver(kubernetes_opensearch_esrally_swap_enabled=True)
  @mock.patch.object(configs, 'LoadConfig', autospec=True)
  def test_get_config_swap_enabled_upgrades_aws_machine_type(
      self, mock_load_config
  ):
    """AWS servers nodepool is upgraded to r6i.8xlarge + gp3."""
    mock_load_config.return_value = _base_nodepool_config()

    config = esrally_bm.GetConfig({})

    aws_spec = config['container_cluster']['nodepools']['servers']['vm_spec'][
        'AWS'
    ]
    self.assertEqual(aws_spec['machine_type'], 'r6i.8xlarge')
    self.assertEqual(aws_spec['boot_disk_type'], 'gp3')

  @flagsaver.flagsaver(kubernetes_opensearch_esrally_swap_enabled=True)
  @mock.patch.object(configs, 'LoadConfig', autospec=True)
  def test_get_config_swap_enabled_does_not_modify_clients_nodepool(
      self, mock_load_config
  ):
    """Enabling swap must not change the clients nodepool configuration."""
    mock_load_config.return_value = _base_nodepool_config()

    config = esrally_bm.GetConfig({})

    clients_np = config['container_cluster']['nodepools']['clients']
    self.assertNotIn('swap_config', clients_np)
    self.assertEqual(
        clients_np['vm_spec']['GCP']['machine_type'], 'n2-standard-4'
    )


if __name__ == '__main__':
  unittest.main()
