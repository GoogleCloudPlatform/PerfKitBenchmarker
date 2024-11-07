import unittest
from unittest import mock
from unittest.mock import patch
import posixpath

# Mock constants for the expected paths used in `_Install`
GEEKBENCH_VERSION = "6.3.0"
GEEKBENCH_URL = f"https://cdn.geekbench.com/Geekbench-{GEEKBENCH_VERSION}-Linux.tar.gz"
GEEKBENCH_DIR = "/opt/pkb/geekbench"  # Update to the actual install directory
GEEKBENCH_EXEC = posixpath.join(GEEKBENCH_DIR, 'geekbench6')

# Import the Install function for Linux
from perfkitbenchmarker.linux_packages import geekbench


class TestGeekbenchLinuxInstall(unittest.TestCase):
    """Unit test case for the Geekbench Install function in the Linux package.

    This test case verifies that the `_Install` function in `geekbench.py`
    performs the expected commands for:
        - Creating the installation directory on the virtual machine.
        - Downloading and extracting the Geekbench tarball.
        - Making the Geekbench executable accessible.

    The test case uses mocks to simulate file paths and command calls,
    allowing verification of the function's behavior without executing actual
    installation steps.
    """

    @patch('perfkitbenchmarker.linux_packages.geekbench.posixpath')
    def test_install_geekbench_linux(self, mock_posixpath):
        """Tests the `_Install` function for Linux installation.

        This method:
            - Mocks a virtual machine (VM) object to track `RemoteCommand` calls.
            - Sets up expectations for directory creation, file download, extraction, and permissions.
            - Verifies that the `_Install` function sends the correct commands to the VM.

        Args:
            mock_posixpath: A mock for the `posixpath` module used within the `_Install` function,
                            enabling control over path joining behavior.
        """

        # Mock VM object with RemoteCommand method
        mock_vm = mock.Mock()
        
        # Define expected paths
        expected_dir = GEEKBENCH_DIR
        expected_exec_path = GEEKBENCH_EXEC

        # Mock posixpath joins to return expected paths
        mock_posixpath.join.side_effect = lambda *args: posixpath.join(*args)

        # Call the Install function
        geekbench._Install(mock_vm)

        # Print all RemoteCommand calls to verify
        print("RemoteCommand calls made:", mock_vm.RemoteCommand.call_args_list)

        # Assertions to verify commands
        mock_vm.RemoteCommand.assert_any_call(f'mkdir -p {expected_dir}')
        mock_vm.RemoteCommand.assert_any_call(
            f'wget -qO- {GEEKBENCH_URL} | tar xz -C {expected_dir} --strip-components=1'
        )
        mock_vm.RemoteCommand.assert_any_call(f'chmod +x {expected_exec_path}')


if __name__ == '__main__':
    unittest.main()
