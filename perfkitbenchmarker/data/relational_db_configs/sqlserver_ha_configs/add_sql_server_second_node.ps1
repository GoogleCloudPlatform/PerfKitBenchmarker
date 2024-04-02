param (
    [string]$clusterIpAddress,
    [string]$perf_password,
    [string]$perf_domain
 )

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

try {
  $scriptsFolder = 'c:\scripts'
  $domainUser = "$perf_domain\pkbadminuser"
  $sql_service_account = "$perf_domain\sql_server"
  $sql_server_install_folder = Get-SqlInstallSourceDirectory

  $localServerName = [System.Net.Dns]::GetHostName()
  $taskScriptPath = $scriptsFolder + '\fci-cluster-task.ps1'

  if (-not (Test-Path $scriptsFolder)) {
    New-Item -Path $scriptsFolder  -ItemType directory
  }

  Write-Host 'Install SQL Server'
  # SQL Server 2022 needs /PRODUCTCOVEREDBYSA=False
  $sql2022InstallParam = '/PRODUCTCOVEREDBYSA=False'
  $sql2022InstallParam = ''
  $sqlInstallString = [string]::Format('{3}Setup.exe /Action=AddNode /UpdateEnabled=True {2} /ENU=True /CONFIRMIPDEPENDENCYCHANGE=false /SQLSVCACCOUNT=''{4}'' /SQLSVCPASSWORD=''{0}'' /AGTSVCACCOUNT=''{4}'' /AGTSVCPASSWORD=''{0}'' /INSTANCENAME=''MSSQLSERVER'' /FAILOVERCLUSTERNETWORKNAME=''sql'' /FAILOVERCLUSTERIPADDRESSES=''IPv4;{1};Cluster Network 1;255.255.240.0'' /FAILOVERCLUSTERGROUP=''SQL Server (MSSQLSERVER)'' /SQLSVCINSTANTFILEINIT=''False'' /FTSVCACCOUNT=''NT Service\MSSQLFDLauncher'' /IAcceptSQLServerLicenseTerms=1 /INDICATEPROGRESS /Q 2>&1 > c:\scripts\sqlsetuplog.log',$perf_password,$clusterIpAddress,$sql2022InstallParam,$sql_server_install_folder, $sql_service_account)

  Out-File -FilePath $taskScriptPath -InputObject $sqlInstallString

  $currentDateTime = Get-Date

  $taskName = 'Run-Cluster-Tasks'
  $taskAction = New-ScheduledTaskAction -Execute 'PowerShell.exe' -Argument $taskScriptPath
  $taskTrigger = New-ScheduledTaskTrigger -Once -At $currentDateTime.AddMinutes(120).ToString('HH:mm:sstt')
  $scheduledTask = New-ScheduledTask -Action $taskAction -Trigger $taskTrigger

  Register-ScheduledTask -TaskName $taskName -InputObject $scheduledTask -User $domainUser -Password $perf_password
  Start-ScheduledTask -TaskName $taskName


  Write-Host 'Running task'
  Write-Host (Get-ScheduledTask -TaskName $taskName).State

  while ((Get-ScheduledTask -TaskName $taskName).State  -ne 'Ready') {
      Write-Host  -Message 'Waiting on scheduled task...'
      Start-Sleep -Seconds 10
  }
  Start-Sleep -Seconds 10
  Disable-ScheduledTask -TaskName $taskName

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
