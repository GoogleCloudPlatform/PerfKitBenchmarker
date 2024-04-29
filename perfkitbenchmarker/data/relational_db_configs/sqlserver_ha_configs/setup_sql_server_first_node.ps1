param (
    [string]$cluster_ip_address,
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

  Write-Host "Sql install folder: $sql_server_install_folder"
  $localServerName = [System.Net.Dns]::GetHostName()
  $taskScriptPath = $scriptsFolder + '\fci-cluster-task.ps1'

  if (-not(Test-Path $scriptsFolder)) {
    New-Item -Path $scriptsFolder  -ItemType directory
  }

  $sqlDestFolder = 'C:\ClusterStorage\Data'
  $clusterStorageName = 'Cluster Virtual Disk (Data)'

  if (Test-Path -Path 'D:\MSSQL\fcimw.txt' -PathType Leaf) {
    $Disks = Get-CimInstance -Namespace Root\MSCluster -ClassName MSCluster_Resource -ComputerName $localServerName | ?{$_.Type -eq 'Physical Disk'}
    $diskDetails = ($Disks | %{Get-CimAssociatedInstance -InputObject $_ -ResultClassName MSCluster_DiskPartition})

    $sqlDestFolder = 'D:\MSSQL'
    $clusterStorage = Get-ClusterResource | Where-Object { $_.ResourceType -eq 'Physical Disk' }
    $clusterStorageName = $clusterStorage.Name
  }

  Write-Host 'Install SQL Server'
  # SQL Server 2022 needs /PRODUCTCOVEREDBYSA=False
  $sql2022InstallParam = '/PRODUCTCOVEREDBYSA=False'
  $sql2022InstallParam = ''
  $sqlInstallString = [string]::Format('{5}Setup.exe /Action=InstallFailoverCluster /UpdateEnabled=True {2} /ENU=True /SQLSVCACCOUNT=''{6}'' /SQLSVCPASSWORD=''{0}'' /SAPWD=''{0}'' /AGTSVCACCOUNT=''{6}'' /AGTSVCPASSWORD=''{0}'' /FEATURES=SQLENGINE,REPLICATION,FULLTEXT,DQ /INSTANCEID=''MSSQLSERVER'' /INSTANCENAME=''MSSQLSERVER'' /INSTALLSHAREDDIR=''C:\Program Files\Microsoft SQL Server'' /INSTANCEDIR=''C:\Program Files\Microsoft SQL Server'' /FAILOVERCLUSTERDISKS=''{3}'' /FAILOVERCLUSTERNETWORKNAME=''sql'' /FAILOVERCLUSTERIPADDRESSES=''IPv4;{1};Cluster Network 1;255.255.240.0'' /FAILOVERCLUSTERGROUP=''SQL Server (MSSQLSERVER)'' /SQLSVCINSTANTFILEINIT=''True'' /SQLSYSADMINACCOUNTS=''{7}\domain admins'' /IACCEPTSQLSERVERLICENSETERMS=1 /INSTALLSQLDATADIR=''{4}\MSSQL\Data'' /SQLUSERDBLOGDIR=''{4}\MSSQL\Log'' /SQLTEMPDBDIR=''{4}\MSSQL\Temp'' /FTSVCACCOUNT=''NT Service\MSSQLFDLauncher'' /INDICATEPROGRESS /SECURITYMODE=SQL /Q 2>&1 > c:\scripts\sqlsetuplog.log',$perf_password,$cluster_ip_address,$sql2022InstallParam,$clusterStorageName,$sqlDestFolder,$sql_server_install_folder,$sql_service_account,$perf_domain)

  Out-File -FilePath $taskScriptPath -InputObject $sqlInstallString
  Add-Content $taskScriptPath 'Add-ClusterResource -Name fci-dnn -ResourceType ''Distributed Network Name'' -Group ''SQL Server (MSSQLSERVER)'''
  Add-Content $taskScriptPath 'Get-ClusterResource -Name fci-dnn | Set-ClusterParameter -Name DnsName -Value fcidnn'
  Add-Content $taskScriptPath 'Start-ClusterResource -Name fci-dnn'

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
  Start-Sleep -Seconds 25
  Disable-ScheduledTask -TaskName $taskName

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
