function Get-SqlInstallSourceDirectory {
  # Depending on cloud provider SQL Server installation source directory changes
  $sql_install_source_directory = ''
  # AWS
  if (Test-Path -Path 'C:\SQLServerSetup') {
    $sql_install_source_directory = 'C:\SQLServerSetup\'
  }
  # Azure
  if (Test-Path -Path 'C:\SQLServerFull') {
      $sql_install_source_directory = 'C:\SQLServerFull\'
  }
  # GCP
  if (Test-Path -Path 'C:\sql_server_install') {
      $sql_install_source_directory = 'C:\sql_server_install\'
  }
  return $sql_install_source_directory
}
$parameters = '/Action=Uninstall', '/FEATURES=SQL,AS,IS,RS', '/INSTANCENAME=MSSQLSERVER', '/Q'

$sql_install_file = (Get-SqlInstallSourceDirectory) + 'Setup.exe'

Write-Host $sql_install_file

& $sql_install_file $parameters
