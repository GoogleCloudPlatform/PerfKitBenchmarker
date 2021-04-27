"""Installs the Intel oneAPI base toolkit.

"The Intel oneAPI Base Toolkit is a core set of tools and libraries for
 developing high-performance, data-centric applications across diverse
 architectures."

Intel oneAPI: https://software.intel.com/oneapi
Base toolkit: https://software.intel.com/oneapi/base-kit
"""

import logging
from typing import Dict, List
from absl import flags

# Download the offline install script (~3.5GB) and put into preprovisioned data
# https://software.intel.com/content/www/us/en/develop/tools/oneapi/base-toolkit/download.html
# Install requires 23.3GB disk space.

_HPC_VERSION = flags.DEFINE_enum('intel_oneapi_basekit_version', '2021.2.0',
                                 ['2021.2.0'],
                                 'Version of Intel OneAPI base toolkit.')

PREPROVISIONED_DATA = {
    # original file name: l_BaseKit_p_2021.2.0.2883_offline.sh
    '2021.2.0.sh':
        '7e91f63614aa50cf931ca4264c941210fb829b4dc83ca00e94ca82efc7416a27',
}

# command to run to get mpicc, etc files in PATH
SOURCE_VARS_COMMAND = '. /opt/intel/oneapi/setvars.sh'

# do not log these environment variables in _VerifyInstall
_IGNORE_VARIABLES_DEBUG = ('SSL_CLIENT', 'SSL_CONNECTION', 'XDG_SESSION_ID',
                           'MANPATH')


def Install(vm):
  """Installs Intel oneAPI."""
  preprovisioned_filename = f'{_HPC_VERSION.value}.sh'
  vm.InstallPreprovisionedPackageData('intel_oneapi_basekit',
                                      [preprovisioned_filename], '.')
  # needed for warning if disk size is too small
  vm.InstallPackages('bc')
  vm.RemoteCommand(f'chmod +x {preprovisioned_filename}; '
                   f'sudo ./{preprovisioned_filename} -a -s --eula accept; '
                   f'rm {preprovisioned_filename}')
  _VerifyInstall(vm)


def _GetVariables(vm, use_source_vars: bool = False) -> Dict[str, List[str]]:
  """Returns all the environment variables on the VM."""
  text, _ = vm.RemoteCommand(
      f'{SOURCE_VARS_COMMAND} >/dev/null; env' if use_source_vars else 'env')
  ret: Dict[str, List[str]] = {}
  for line in text.splitlines():
    variable, value = line.split('=', 1)
    if variable not in _IGNORE_VARIABLES_DEBUG:
      ret[variable] = value.split(':')
  return ret


def _LogEnvDifferences(pre_variables: Dict[str, List[str]],
                       post_variables: Dict[str, List[str]]):
  """Logs the differences between the two sets of environment variables."""
  differences = []
  for variable in sorted(set(list(pre_variables) + list(post_variables))):
    pre_value = pre_variables.get(variable, [])
    post_value = post_variables.get(variable, [])
    additions = sorted(set(post_value).difference(pre_value))
    if additions:
      differences.append(f'{variable}={":".join(additions)}')
  if differences:
    logging.info('Changes in %s environment variables:\n%s', len(differences),
                 '\n'.join(differences))
  else:
    logging.info('No change in environment variables, most likely an error')


def _VerifyInstall(vm):
  """Logs changes to environment variables and checks I_MPI_ROOT set."""
  pre_variables = _GetVariables(vm)
  post_variables = _GetVariables(vm, True)
  _LogEnvDifferences(pre_variables, post_variables)
  expected_mpi_root = f'/opt/intel/oneapi/mpi/{_HPC_VERSION.value}'
  mpi_root = post_variables.get('I_MPI_ROOT', [])
  if mpi_root != [expected_mpi_root]:
    raise ValueError(f'Expected "I_MPI_ROOT={expected_mpi_root}" '
                     f'have "{sorted(mpi_root)}"')
