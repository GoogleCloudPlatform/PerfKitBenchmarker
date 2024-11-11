import ntpath

# Define the Geekbench version and the URL to download the Windows installer
GEEKBENCH_VERSION = "6.3.0"
GEEKBENCH_EXE = f"Geekbench-{GEEKBENCH_VERSION}-WindowsSetup.exe"
GEEKBENCH_EXE_URL = f"https://cdn.geekbench.com/{GEEKBENCH_EXE}"

def Install(vm):
    """Installs the Geekbench package on the VM."""
    
    # Create a directory for downloading the installer within the VM's temporary directory
    geekbench_download_path = ntpath.join(vm.temp_dir, "geekbench", "")
    vm.RemoteCommand(f"New-Item -Path {geekbench_download_path} -ItemType Directory")
    
    # Define the full path to where the installer will be downloaded
    geekbench_exe = ntpath.join(geekbench_download_path, GEEKBENCH_EXE)
    
    # Download the Geekbench installer from the specified URL to the download path
    vm.DownloadFile(GEEKBENCH_EXE_URL, geekbench_exe)
    
    # Run the Geekbench installer with silent installation options to avoid manual intervention
    vm.RemoteCommand(f"{geekbench_exe} /SILENT /NORESTART Dir={geekbench_download_path}")
