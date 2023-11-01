try {
  $domainUser = 'perflab\administrator'
  $localServerName    = [System.Net.Dns]::GetHostName()
  $passwordSecureString = (ConvertTo-SecureString -AsPlainText $args[0] -Force)
  $domainCredential = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

  $node1 = [System.Net.Dns]::GetHostByName($localServerName).HostName
  $node2 = $node1.replace('-1.','-2.')

  # get list of domain controllers
  $ad_dc = Get-ADDomainController -Credential $domainCredential

  $ClusterDisks = Get-PhysicalDisk -CanPool $True |
      Where-Object { ($_ |
          Get-PhysicalDiskStorageNodeView |
          Select-Object -Property StorageNodeObjectId) -like ('*' + $localServerName + '*') }

  if ($ClusterDisks.count > 2) {
    $Pool = New-StoragePool -StorageSubsystemFriendlyName 'Clustered*' -FriendlyName FciPool -PhysicalDisks $ClusterDisks -ResiliencySettingNameDefault Simple -Verbose
    $Pool | New-Volume -FriendlyName 'Data' -FileSystem CSVFS_ReFS -AllocationUnitSize 65536 -ProvisioningType 'Fixed' -UseMaximumSize
  }
  else {
    $diskCount = 0
    $diskLetter = @('D','E')
    foreach($disk in $ClusterDisks) {

      Initialize-Disk -Number $ClusterDisks.DeviceId

      New-Partition -disknumber $ClusterDisks.DeviceId -usemaximumsize -DriveLetter $diskLetter[$diskCount] | Format-Volume -FileSystem ReFS -newfilesystemlabel DataDisk -AllocationUnitSize 65536
      $diskCount = $diskCount + 1
    }

    Get-ClusterAvailableDisk | Add-ClusterDisk
  }

  New-Item -ItemType Directory -Path 'd:\MSSQL'
  Get-Date | Out-File -FilePath 'd:\MSSQL\fcimw.txt'

  Write-Host 'Test Cluster'
  Invoke-Command -ComputerName  $ad_dc.HostName -Credential $domainCredential -ArgumentList $node1,$node2 -ScriptBlock {
        Test-Cluster -Node $args[0],$args[1] -Include 'Inventory', 'Network', 'System Configuration' -Confirm:0;
     }
  Write-Host 'Add SQL service account'
  Invoke-Command -ComputerName  $ad_dc.HostName -Credential $domainCredential -ArgumentList $passwordSecureString -ScriptBlock {
        New-ADUser -Name 'sql_server' -Description 'SQL Agent and SQL Admin account.' -AccountPassword $args[0] -Enabled $true -PasswordNeverExpires $true
     }
}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
