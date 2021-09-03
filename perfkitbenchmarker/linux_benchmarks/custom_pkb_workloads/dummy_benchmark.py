# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

import posixpath
import os

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker.linux_packages import INSTALL_DIR

INSTALLED_PACKAGES = {}
BENCHMARK_NAME = "dummy"
BENCHMARK_DATA_DIR = "dummy_benchmark"
BENCHMARK_DIR = posixpath.join(INSTALL_DIR, BENCHMARK_DATA_DIR)
BENCHMARK_CONFIG = """
dummy:
  description: Runs a sample benchmark.
  vm_groups:
    vm_group1:
      os_type: ubuntu2004
      vm_spec:
        AWS:
          machine_type: m5.xlarge
          zone: us-east-2
          boot_disk_size: 100
        GCP:
          machine_type: n1-standard-4
          zone: us-central1-a
          image: null
        Azure:
          machine_type: Standard_F2s_v2
          zone: eastus2
          image: null
"""

FLAGS = flags.FLAGS

""" Configuration flags """
flags.DEFINE_string("dummy_version", "v1.0", "Set Version of the Workload")


""" Tunable flags """
flags.DEFINE_integer("dummy_set_hugepages", 100, "Set Hugepages to desired number")



def _GetInternalResources(vm, urls):
  if urls:
    dir_name = "internal_resources_{0}".format(vm.name)
    internal_dir = vm_util.PrependTempDir(dir_name)
    vm_util.IssueCommand("mkdir -p {0}".format(internal_dir).split())
    os.chdir(os.path.abspath(internal_dir))
    for url in urls:
      vm_util.IssueCommand("curl -O {0}".format(url).split(), timeout=None)
    archive_name = "{0}.tar.gz".format(dir_name)
    archive_path = vm_util.PrependTempDir(archive_name)
    remote_archive_path = posixpath.join(BENCHMARK_DIR, archive_name)
    vm_util.IssueCommand("tar czf {0} -C {1} .".format(archive_path, internal_dir).split())
    vm.RemoteCopy(archive_path, remote_archive_path)
    vm.RemoteCommand("tar xf {0} -C {1}".format(remote_archive_path, BENCHMARK_DIR))


def _ConvertNumericValue(val):
  converted_val = val
  try:
    converted_val = int(val)
  except BaseException:
    try:
      converted_val = float(val)
    except BaseException:
      if val.lower().startswith('0x'):
        try:
          converted_val = int(val, 16)
        except BaseException:
          pass
  return converted_val


def _InitFlags(benchmark_spec):
  pass


def GetConfig(user_config):
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepares the VMs and other resources for running the benchmark.

  This is a good place to download binaries onto the VMs, create any data files
  needed for a benchmark run, etc.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  all_vms = [vm for group, vms in benchmark_spec.vm_groups.items() for vm in vms]
  source_dir = data.ResourcePath("custom_pkb_workloads/" + BENCHMARK_DATA_DIR)
  vm_util.RunThreaded(lambda vm: vm.RemoteCopy(source_dir, INSTALL_DIR), all_vms)
  vm_util.RunThreaded(lambda vm: vm.RemoteCommand("sudo chmod -R +x {0}".format(INSTALL_DIR)), all_vms)
  _InitFlags(benchmark_spec)
  vm_group_vm_group1 = benchmark_spec.vm_groups["vm_group1"]
  INSTALLED_PACKAGES["vm_group1"] = []
  vm_util.RunThreaded(lambda vm: vm.Install("{0}_{1}_deps".format(BENCHMARK_NAME, "vm_group1")), vm_group_vm_group1)


def Run(benchmark_spec):
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  vm_group_vm_group1 = benchmark_spec.vm_groups["vm_group1"]
  print(vm_group_vm_group1)
  results = []
  metadata = {
      "type": "Stats",
      "load": "Unknown",
  }
  packages_versions = GetInstalledPackageVersions(benchmark_spec.vm_groups)
  if packages_versions:
    metadata["packages"] = packages_versions
  run_cmd = "cd {0} && ".format(BENCHMARK_DIR) + "./dummy_run.sh {0}".format(FLAGS.dummy_set_hugepages)
  vm_group_vm_group1[0].RemoteCommand(run_cmd)
  metadata["goal"] = "Sustain"
  vm = vm_group_vm_group1[0]
  out, _ = vm.RemoteCommand("cat ~/dummy_results/results.txt | grep MemAvailable | awk -F ' ' '{print $2}'")
  val = out.strip()
  results.append(sample.Sample("MemAvailable", val, "KB", metadata))
  metadata["goal"] = "Sustain"
  vm = vm_group_vm_group1[0]
  out, _ = vm.RemoteCommand("cat ~/dummy_results/results.txt | grep HugePages_Total  | awk -F ' ' '{print $2}'")
  val = out.strip()
  results.append(sample.Sample("HugePages", val, "Units", metadata))
  vm_group_vm_group1[0].RemoteCopy(vm_util.GetTempDir(), "~/dummy_results", False)
  return results


def Cleanup(benchmark_spec):
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  vm_group_vm_group1 = benchmark_spec.vm_groups["vm_group1"]
  vm_util.RunThreaded(lambda vm: vm.Uninstall("{0}_{1}_deps".format(BENCHMARK_NAME, "vm_group1")), vm_group_vm_group1)
  target = vm_group_vm_group1[0]
  print(list)
  if isinstance(target, list):
    vm_util.RunThreaded(lambda vm: vm.RemoteCommand("cd ~/dummy_results && rm results.txt && cd .. && rmdir  ~/dummy_results"), target)
  else:
    target.RemoteCommand("cd ~/dummy_results && rm results.txt && cd .. && rmdir  ~/dummy_results")


def GetInstalledPackageVersions(vm_groups):
  packages = INSTALLED_PACKAGES
  packages_diff_versions = {}
  for group, pkgs in packages.items():
    local_pkgs = []
    for pkg in pkgs:
      local_pkgs.append(pkg)
    if local_pkgs:
      packages_diff_versions[group] = local_pkgs
  return packages_diff_versions
