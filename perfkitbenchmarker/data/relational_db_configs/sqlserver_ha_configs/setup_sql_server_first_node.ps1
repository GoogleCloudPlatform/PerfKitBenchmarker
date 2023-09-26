 try {
  $scriptsFolder = 'c:\scripts'
  $domainUser = 'perflab\administrator'
  $localServerName = [System.Net.Dns]::GetHostName()
  $clearPassword = $args[1]
  $taskScriptPath = $scriptsFolder + '\fci-cluster-task.ps1'
  $clusterIpAddress = $args[0]

  if (-not(Test-Path $scriptsFolder)) {
    New-Item -Path $scriptsFolder  -ItemType directory
}

  Write-Host 'Install SQL Server'
  # SQL Server 2022 needs /PRODUCTCOVEREDBYSA=False
  $sql2022InstallParam = '/PRODUCTCOVEREDBYSA=False'
  $sql2022InstallParam = ''
  $sqlInstallString = [string]::Format('c:\sql_server_install\Setup.exe /Action=InstallFailoverCluster /UpdateEnabled=True {2} /ENU=True /SQLSVCACCOUNT=''perflab\sql_server'' /SQLSVCPASSWORD=''{0}'' /SAPWD=''{0}'' /AGTSVCACCOUNT=''perflab\sql_server'' /AGTSVCPASSWORD=''{0}'' /FEATURES=SQLENGINE,REPLICATION,FULLTEXT,DQ /INSTANCEID=''MSSQLSERVER'' /INSTANCENAME=''MSSQLSERVER'' /INSTALLSHAREDDIR=''C:\Program Files\Microsoft SQL Server'' /INSTANCEDIR=''C:\Program Files\Microsoft SQL Server'' /FAILOVERCLUSTERDISKS=''Cluster Virtual Disk (Data)'' /FAILOVERCLUSTERNETWORKNAME=''sql'' /FAILOVERCLUSTERIPADDRESSES=''IPv4;{1};Cluster Network 1;255.255.240.0'' /FAILOVERCLUSTERGROUP=''SQL Server (MSSQLSERVER)'' /SQLSVCINSTANTFILEINIT=''True'' /SQLSYSADMINACCOUNTS=''perflab\domain admins'' /IACCEPTSQLSERVERLICENSETERMS=1 /INSTALLSQLDATADIR=''C:\ClusterStorage\Data\'' /SQLUSERDBLOGDIR=''C:\ClusterStorage\Data\MSSQL\Log'' /SQLTEMPDBDIR=''C:\ClusterStorage\Data\MSSQL\Temp'' /FTSVCACCOUNT=''NT Service\MSSQLFDLauncher'' /INDICATEPROGRESS /SECURITYMODE=SQL /Q 2>&1 > c:\scripts\sqlsetuplog.log',$clearPassword, $clusterIpAddress,$sql2022InstallParam)

  Out-File -FilePath $taskScriptPath -InputObject $sqlInstallString
  Add-Content $taskScriptPath 'Add-ClusterResource -Name fci-dnn -ResourceType ''Distributed Network Name'' -Group ''SQL Server (MSSQLSERVER)'''
  Add-Content $taskScriptPath 'Get-ClusterResource -Name fci-dnn | Set-ClusterParameter -Name DnsName -Value fcidnn'
  Add-Content $taskScriptPath 'Start-ClusterResource -Name fci-dnn'

  $currentDateTime = Get-Date

  $taskName = 'Run-Cluster-Tasks'
  $taskAction = New-ScheduledTaskAction -Execute 'PowerShell.exe' -Argument $taskScriptPath
  $taskTrigger = New-ScheduledTaskTrigger -Once -At $currentDateTime.AddMinutes(120).ToString('HH:mm:sstt')
  $scheduledTask = New-ScheduledTask -Action $taskAction -Trigger $taskTrigger

  Register-ScheduledTask -TaskName $taskName -InputObject $scheduledTask -User $domainUser -Password $clearPassword
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
