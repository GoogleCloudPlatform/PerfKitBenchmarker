import posixpath
from perfkitbenchmarker import linux_packages

# Define the Geekbench version and the URL to download the tarball
GEEKBENCH_VERSION = "6.3.0"
GEEKBENCH_URL = f'https://cdn.geekbench.com/Geekbench-{GEEKBENCH_VERSION}-Linux.tar.gz'

# Set the directory where Geekbench will be installed
GEEKBENCH_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'geekbench')

# Define the path to the Geekbench executable
GEEKBENCH_EXEC = posixpath.join(GEEKBENCH_DIR, 'geekbench6')

def _Install(vm):
    """Installs the Geekbench package on the VM."""

    # Create the installation directory for Geekbench
    vm.RemoteCommand(f'mkdir -p {GEEKBENCH_DIR}')
    
    # Download and extract the Geekbench tarball directly to the installation directory
    # `--strip-components=1` removes the top-level directory from the tarball
    vm.RemoteCommand(f'wget -qO- {GEEKBENCH_URL} | tar xz -C {GEEKBENCH_DIR} --strip-components=1')

    # Make sure the Geekbench executable has the correct permissions to be run
    vm.RemoteCommand(f'chmod +x {GEEKBENCH_EXEC}')

def YumInstall(vm):
    """Installs Geekbench on the VM for systems using the yum package manager."""
    _Install(vm)

def AptInstall(vm):
    """Installs Geekbench on the VM for systems using the apt package manager."""
    _Install(vm)
