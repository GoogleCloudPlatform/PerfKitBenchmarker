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
"""Unit tests for kubernetes_nginx_benchmark."""

import os
import tempfile
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks.kubernetes import kubernetes_nginx_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class KubernetesNginxBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = "1234"
    FLAGS.data_search_paths = ["cloud/performance/artemis/data"]
    self.temp_artifact_dir = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp_artifact_dir.cleanup)
    self.enter_context(
        mock.patch.object(
            vm_util,
            "PrependTempDir",
            side_effect=lambda name: os.path.join(
                self.temp_artifact_dir.name, name
            ),
        )
    )

  def testGetConfigDefault(self):
    """Tests GetConfig with default options."""
    config = kubernetes_nginx_benchmark.GetConfig({})
    self.assertEqual(
        config["container_cluster"]["nodepools"]["servers"]["vm_spec"]["GCP"][
            "machine_type"
        ],
        "n2-standard-4",
    )

  @flagsaver.flagsaver(
      nginx_client_machine_type="c4-standard-32",
      nginx_server_machine_type="c4-standard-8",
      nginx_upstream_server_machine_type="c4-standard-16",
  )
  def testGetConfigMachineTypeOverrides(self):
    """Tests GetConfig with custom machine type flags."""
    config = kubernetes_nginx_benchmark.GetConfig({})
    self.assertEqual(
        config["vm_groups"]["clients"]["vm_spec"]["GCP"]["machine_type"],
        "c4-standard-32",
    )
    self.assertEqual(
        config["container_cluster"]["nodepools"]["servers"]["vm_spec"]["GCP"][
            "machine_type"
        ],
        "c4-standard-8",
    )
    self.assertEqual(
        config["container_cluster"]["nodepools"]["upstream"]["vm_spec"]["GCP"][
            "machine_type"
        ],
        "c4-standard-16",
    )

  def testMergeNginxConfigsSSL(self):
    """Tests _MergeNginxConfigs with SSL enabled."""
    global_conf = data.ResourcePath("nginx/global.conf")
    proxy_conf = data.ResourcePath("nginx/rp_apigw.conf")
    with flagsaver.flagsaver(nginx_use_ssl=True):
      merged = kubernetes_nginx_benchmark._MergeNginxConfigs(
          global_conf, proxy_conf
      )
    self.assertIn("listen 443 ssl", merged)
    self.assertIn(
        "server nginx-upstream.default.svc.cluster.local:80;", merged
    )
    self.assertIn("proxy_pass http://", merged)

  def testMergeNginxConfigsNoSSL(self):
    """Tests _MergeNginxConfigs with SSL disabled."""
    global_conf = data.ResourcePath("nginx/global.conf")
    proxy_conf = data.ResourcePath("nginx/rp_apigw.conf")
    with flagsaver.flagsaver(nginx_use_ssl=False):
      merged = kubernetes_nginx_benchmark._MergeNginxConfigs(
          global_conf, proxy_conf
      )
    self.assertIn("listen 80", merged)
    self.assertIn("# ssl on;", merged)
    self.assertIn("# ssl_certificate", merged)

  def testCreateNginxConfigMapDirDefault(self):
    """Tests that default configs are merged when no custom flags are passed."""
    temp_dir = kubernetes_nginx_benchmark._CreateNginxConfigMapDir()
    self.addCleanup(temp_dir.cleanup)

    proxy_path = os.path.join(temp_dir.name, "nginx-proxy.conf")
    upstream_path = os.path.join(temp_dir.name, "nginx-upstream.conf")

    self.assertTrue(os.path.exists(proxy_path))
    self.assertTrue(os.path.exists(upstream_path))

    with open(proxy_path) as f:
      proxy_content = f.read()

    # Default global config has worker_connections 1024
    self.assertIn("worker_connections 1024;", proxy_content)

  def testCreateNginxConfigMapDirCustomGlobalConf(self):
    """Tests custom global and server conf flags override config."""
    custom_global = self.create_tempfile(
        content=(
            "user custom_user;\n"
            "worker_processes 42;\n"
            "include /etc/nginx/conf.d/*.conf;\n"
        )
    ).full_path
    custom_server = self.create_tempfile(
        content=(
            "upstream custom_upstream { server 1.2.3.4:80; }\n"
            "location / { proxy_pass http://custom_upstream; }\n"
        )
    ).full_path

    with flagsaver.flagsaver(
        kubernetes_nginx_global_conf=custom_global,
        kubernetes_nginx_server_conf=custom_server,
    ):
      temp_dir = kubernetes_nginx_benchmark._CreateNginxConfigMapDir()
      self.addCleanup(temp_dir.cleanup)

      proxy_path = os.path.join(temp_dir.name, "nginx-proxy.conf")
      self.assertTrue(os.path.exists(proxy_path))

      with open(proxy_path) as f:
        proxy_content = f.read()

      self.assertIn("user custom_user;", proxy_content)
      self.assertIn("worker_processes 42;", proxy_content)
      self.assertIn("custom_upstream", proxy_content)


if __name__ == "__main__":
  unittest.main()
