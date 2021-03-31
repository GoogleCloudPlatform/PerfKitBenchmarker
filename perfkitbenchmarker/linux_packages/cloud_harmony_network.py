"""Contains cloudharmony network benchmark installation functions."""

import posixpath
from absl import flags
from perfkitbenchmarker import linux_packages


FLAGS = flags.FLAGS

BENCHMARK = 'network'
INSTALL_PATH = posixpath.join(linux_packages.INSTALL_DIR, BENCHMARK)
BENCHMARK_GIT_URL = f'https://github.com/cloudharmony/{BENCHMARK}.git'
WEB_PROBE_TAR = 'probe.tgz'
WEB_PROBE = posixpath.join('http://cloudfront.cloudharmony.net', WEB_PROBE_TAR)
PREPROVISIONED_DATA = {
    WEB_PROBE_TAR:
        'b6e0580a5d5bbcffc6c7fbe48485721727ad18141cde01f86fc9f96a2612eb79'
}
PACKAGE_DATA_URL = {WEB_PROBE_TAR: WEB_PROBE}
PACKAGE_NAME = 'cloud_harmony_network'


def Install(vm):
  """Install Cloud Harmory network benchmark on VM."""
  for deps in ['php', 'build_tools', 'curl']:
    vm.Install(deps)
  vm.RemoteCommand(f'git clone --recurse-submodules {BENCHMARK_GIT_URL} '
                   f'{INSTALL_PATH}')
