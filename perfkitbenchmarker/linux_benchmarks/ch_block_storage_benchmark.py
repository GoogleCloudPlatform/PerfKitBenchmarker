# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a cloudharmony benchmark.

See https://github.com/cloudharmony/block-storage for more info.
"""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ch_block_storage

BENCHMARK_NAME = 'ch_block_storage'
BENCHMARK_CONFIG = """
ch_block_storage:
  description: Runs cloudharmony block storage tests.
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

flags.DEFINE_multi_enum(
    'ch_block_tests', ['iops'],
    ['iops', 'throughput', 'latency', 'wsat', 'hir'],
    'A list of tests supported by CloudHarmony block storage benchmark.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  disk_spec = config['vm_groups']['default']['disk_spec']
  # Use raw device
  # TODO(yuyanting): Support filesystem?
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = None
  return config


def Prepare(benchmark_spec):
  """Prepares the cloudharmony block storage benchmark."""
  vm = benchmark_spec.vms[0]
  vm.Install('ch_block_storage')


def _PrepareDevicePath(vm, link):
  """Find device path and grant full permission.

  Args:
    vm: VirtualMachine object.
    link: string. Represents device path.

  Returns:
    String represents the actual path to the device.
  """
  path = vm.RemoteCommand('readlink -f %s' % link)[0][:-1]
  vm.RemoteCommand('sudo chmod 777 %s' % path)
  return path


def _LocateFioJson(vm, outdir, test):
  """Locate raw fio output.

  Args:
    vm: VirtualMachine object.
    outdir: string. Output directory.
    test: string. Test name.

  Returns:
    A list of strings representing fio output filename.
  """
  fns, _ = vm.RemoteCommand('ls %s/fio-%s*.json' % (outdir, test))
  return fns.split()


def Run(benchmark_spec):
  """Runs cloudharmony block storage and reports the results."""
  vm = benchmark_spec.vms[0]
  target = ' '.join(['--target=%s' % _PrepareDevicePath(vm, dev.GetDevicePath())
                     for dev in vm.scratch_disks])
  tests = ' '.join(['--test=%s' % test for test in FLAGS.ch_block_tests])
  args = ' '.join(['--%s' % param for param in FLAGS.ch_params])
  outdir = vm_util.VM_TMP_DIR
  cmd = ('{benchmark_dir}/run.sh '
         '{target} {tests} '
         '--output={outdir} --noreport {args} --verbose').format(
             benchmark_dir=ch_block_storage.INSTALL_PATH,
             target=target, outdir=outdir,
             tests=tests, args=args)
  vm.RobustRemoteCommand('sudo %s' % cmd)
  results = []
  for test in FLAGS.ch_block_tests:
    metadata = {'ch_test': test}
    result_json, _ = vm.RemoteCommand('cat %s/%s.json' % (outdir, test))
    fns = _LocateFioJson(vm, outdir, test)
    fio_json_list = [
        vm.RemoteCommand('cat %s' % fn)[0] for fn in fns]
    tmp_results = ch_block_storage.ParseOutput(result_json, fio_json_list)
    for r in tmp_results:
      r.metadata.update(metadata)
    results += tmp_results
  return results


def Cleanup(benchmark_spec):
  vm = benchmark_spec.vms[0]
  vm.RemoteCommand('rm -rf %s' % ch_block_storage.INSTALL_PATH)
