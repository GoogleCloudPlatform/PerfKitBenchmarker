import unittest
from unittest import mock
from unittest.mock import patch
import ntpath

# Mock constants for the expected paths used in `Install`
GEEKBENCH_VERSION = "6.3.0"
GEEKBENCH_EXE = f"Geekbench-{GEEKBENCH_VERSION}-WindowsSetup.exe"
GEEKBENCH_EXE_URL = f"https://cdn.geekbench.com/{GEEKBENCH_EXE}"
GEEKBENCH_DIR = "geekbench"

# Import the Install function for Windows
from perfkitbenchmarker.windows_packages import geekbench


class TestGeekbenchWindowsInstall(unittest.TestCase):
    """Unit test case for the Geekbench Install function in the Windows package.

    This test case verifies that the `Install` function in `geekbench.py`
    performs the expected commands for:
        - Creating a download directory on the virtual machine.
        - Downloading the Geekbench installer to the specified directory.
        - Running the installer with silent install options.

    Mocks are used to replace actual file paths and commands with controlled
    expectations, allowing the function's behavior to be verified without
    performing real installations.
    """

    @patch('perfkitbenchmarker.windows_packages.geekbench.ntpath')
    def test_install_geekbench_windows(self, mock_ntpath):
        """Tests the `Install` function for Windows installation.

        This method:
            - Mocks a virtual machine (VM) object.
            - Sets up expectations for directory creation, file download, and installer execution.
            - Verifies that the `Install` function sends the correct commands to the VM.

        Args:
            mock_ntpath: A mock for the `ntpath` module used within the `Install` function,
                         allowing control over path joining behavior.
        """

        # Mock VM object with RemoteCommand and DownloadFile methods
        mock_vm = mock.Mock()
        mock_vm.temp_dir = "C:\\temp"
        
        # Expected paths for download directory and installer
        expected_download_path = ntpath.join(mock_vm.temp_dir, GEEKBENCH_DIR, "")
        expected_exe_path = ntpath.join(expected_download_path, GEEKBENCH_EXE)

        # Mock ntpath.join to simulate path joining
        mock_ntpath.join.side_effect = lambda *args: ntpath.join(*args)

        # Call the Install function
        geekbench.Install(mock_vm)

        # Assertions to verify commands
        mock_vm.RemoteCommand.assert_any_call(f"New-Item -Path {expected_download_path} -ItemType Directory")
        mock_vm.DownloadFile.assert_called_once_with(GEEKBENCH_EXE_URL, expected_exe_path)
        mock_vm.RemoteCommand.assert_any_call(f"{expected_exe_path} /SILENT /NORESTART Dir={expected_download_path}")

        # Print all RemoteCommand and DownloadFile calls to verify
        print("RemoteCommand calls made:", mock_vm.RemoteCommand.call_args_list)
        print("DownloadFile calls made:", mock_vm.DownloadFile.call_args_list)


if __name__ == '__main__':
    unittest.main()
