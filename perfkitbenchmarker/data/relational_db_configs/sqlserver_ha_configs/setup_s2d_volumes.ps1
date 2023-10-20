try {
  $domainUser = 'perflab\administrator'
  $localServerName    = [System.Net.Dns]::GetHostName()

  $passwordSecureString = (ConvertTo-SecureString -AsPlainText $args[0] -Force)
  $node1 = $localServerName + '.perflab.local'

  $partVmName = $localServerName.Substring(0, $localServerName.Length - 1)

  $node2 = $partVmName + '2.perflab.local'
  $dc = $partVmName + '3.perflab.local'

  $domainCredential = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

  $cacheDiskModel = 'EphemeralDisk'

  $nvme_disks = Get-PhysicalDisk | Where-Object { $_.FriendlyName -like 'nvme_card' }
  $scsi_disks = Get-PhysicalDisk | Where-Object { $_.FriendlyName -like 'EphemeralDisk' }

  if ($nvme_disks.count -gt 0) {
      $cacheDiskModel = 'nvme_card'
  }
  if ($scsi_disks.count -gt 0) {
      $cacheDiskModel = 'EphemeralDisk'
  }
  Write-Host $cacheDiskModel
  if ([string]::IsNullOrEmpty($cacheDiskModel)) {
      Enable-ClusterStorageSpacesDirect -PoolFriendlyName 's2dpool' -Confirm:0;
  }
  else {
      Invoke-Command -ComputerName  $node1 -Credential $domainCredential -ArgumentList $cacheDiskModel -ScriptBlock {
        Enable-ClusterStorageSpacesDirect -CacheDeviceModel $args[0]  -PoolFriendlyName 's2dpool' -Confirm:0;
     }
  }
  Start-Sleep -Seconds 40
  (Get-Cluster).BlockCacheSize = 2048

  New-Volume -StoragePoolFriendlyName S2D* -FriendlyName 'Data' -FileSystem CSVFS_ReFS `
    -AllocationUnitSize 65536 -ProvisioningType 'Fixed' `
    -UseMaximumSize

  Write-Host 'Test Cluster'
  Invoke-Command -ComputerName  $dc -Credential $domainCredential -ArgumentList $node1,$node2 -ScriptBlock {
        Test-Cluster -Node $args[0],$args[1] -Confirm:0;
     }
  Write-Host 'Add SQL service account'
  Invoke-Command -ComputerName  $dc -Credential $domainCredential -ArgumentList $passwordSecureString -ScriptBlock {
        New-ADUser -Name 'sql_server' -Description 'SQL Agent and SQL Admin account.' -AccountPassword $args[0] -Enabled $true -PasswordNeverExpires $true
     }
}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
