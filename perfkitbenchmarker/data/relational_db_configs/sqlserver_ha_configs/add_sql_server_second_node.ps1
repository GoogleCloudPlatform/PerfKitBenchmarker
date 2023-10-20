 try {
  $scriptsFolder = 'c:\scripts'
  $domainUser = 'perflab\administrator'
  $localServerName = [System.Net.Dns]::GetHostName()
  $clearPassword = $args[1]
  $taskScriptPath = $scriptsFolder + '\fci-cluster-task.ps1'
  $clusterIpAddress = $args[0]

  if (-not (Test-Path $scriptsFolder)) {
    New-Item -Path $scriptsFolder  -ItemType directory
  }

  Write-Host 'Install SQL Server'
  # SQL Server 2022 needs /PRODUCTCOVEREDBYSA=False
  $sql2022InstallParam = '/PRODUCTCOVEREDBYSA=False'
  $sql2022InstallParam = ''
  $sqlInstallString = [string]::Format('c:\sql_server_install\Setup.exe /Action=AddNode /UpdateEnabled=True {2} /ENU=True /CONFIRMIPDEPENDENCYCHANGE=false /SQLSVCACCOUNT=''perflab\sql_server'' /SQLSVCPASSWORD=''{0}'' /AGTSVCACCOUNT=''perflab\sql_server'' /AGTSVCPASSWORD=''{0}'' /INSTANCENAME=''MSSQLSERVER'' /FAILOVERCLUSTERNETWORKNAME=''sql'' /FAILOVERCLUSTERIPADDRESSES=''IPv4;{1};Cluster Network 1;255.255.240.0'' /FAILOVERCLUSTERGROUP=''SQL Server (MSSQLSERVER)'' /SQLSVCINSTANTFILEINIT=''False'' /FTSVCACCOUNT=''NT Service\MSSQLFDLauncher'' /IAcceptSQLServerLicenseTerms=1 /INDICATEPROGRESS /Q 2>&1 > c:\scripts\sqlsetuplog.log',$clearPassword,$clusterIpAddress,$sql2022InstallParam)

  Out-File -FilePath $taskScriptPath -InputObject $sqlInstallString

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
  Start-Sleep -Seconds 10
  Disable-ScheduledTask -TaskName $taskName

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
