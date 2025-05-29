param (
    [string]$perf_domain_password,
    [string]$replica_vm_name,
    [string]$perf_domain
 )

try {
  $domainUser = "$perf_domain\pkbadminuser"
  $localServerName    = [System.Net.Dns]::GetHostName()

  $passwordSecureString = (ConvertTo-SecureString -AsPlainText $perf_domain_password -Force)
  $domainCredential = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

  $node1 = "$localServerName.$perf_domain.local"
  $node2 = "$replica_vm_name.$perf_domain.local"
  $ad_dc = Get-ADDomainController -Credential $domainCredential

  $cacheDiskModel = ''

  $nvme_disks = Get-PhysicalDisk | Where-Object { $_.FriendlyName -like 'nvme_card' }
  $scsi_disks = Get-PhysicalDisk | Where-Object { $_.FriendlyName -like 'EphemeralDisk' }

  if ($nvme_disks.count -gt 0) {
      $cacheDiskModel = 'nvme_card'
  }
  if ($scsi_disks.count -gt 0) {
      $cacheDiskModel = 'EphemeralDisk'
  }
  Write-Host $cacheDiskModel

  $maxRetryAttempts = 3
  for (($i = 0); $i -lt $maxRetryAttempts; $i++) {
    try {
      if ([string]::IsNullOrEmpty($cacheDiskModel)) {
        Invoke-Command -ComputerName  $node1 -Credential $domainCredential -ArgumentList $cacheDiskModel -ScriptBlock {
          Enable-ClusterStorageSpacesDirect -PoolFriendlyName 's2dpool' -Confirm:0;
        }
      }
      else {
        Invoke-Command -ComputerName  $node1 -Credential $domainCredential -ArgumentList $cacheDiskModel -ScriptBlock {
          Enable-ClusterStorageSpacesDirect -CacheDeviceModel $args[0]  -PoolFriendlyName 's2dpool' -Confirm:0;
        }
      }
      break
    }
    catch {
      Write-Host 'Exception occured while creating S2D cluster: '$_.Exception.Message
      Start-Sleep -Seconds 10
    }
  }

  Start-Sleep -Seconds 40
  (Get-Cluster).BlockCacheSize = 2048

  New-Volume -StoragePoolFriendlyName S2D* -FriendlyName 'Data' -FileSystem CSVFS_ReFS `
    -AllocationUnitSize 65536 -ProvisioningType 'Fixed' `
    -UseMaximumSize

  Invoke-Command -ComputerName $ad_dc.HostName -Credential $domainCredential -ArgumentList $ad_dc.HostName -ScriptBlock {
      $fileShareWitness = "\\$($args[0])\QWitness"
      Set-ClusterQuorum -FileShareWitness $fileShareWitness -Cluster sql-fci
  }

  Write-Host 'Verify Cluster'
  Invoke-Command -ComputerName $ad_dc.HostName -Credential $domainCredential -ArgumentList $node1,$node2 -ScriptBlock {
    Test-Cluster -Node $args[0],$args[1] -Confirm:0;
  }
  Write-Host 'Add SQL service account'
  Invoke-Command -ComputerName $ad_dc.HostName -Credential $domainCredential -ArgumentList $passwordSecureString -ScriptBlock {
        New-ADUser -Name 'sql_server' -Description 'SQL Agent and SQL Admin account.' -AccountPassword $args[0] -Enabled $true -PasswordNeverExpires $true
  }
}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
