param (
    [string]$perf_domain_password,
    [string]$replica_vm_name,
    [string]$perf_domain
 )

try {

  $domainUser = "$perf_domain\pkbadminuser"
  $localServerName    = [System.Net.Dns]::GetHostName()
  $initializedDisks = New-Object Collections.Generic.List[String]

  $passwordSecureString = (ConvertTo-SecureString -AsPlainText $perf_domain_password -Force)
  $domainCredential = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

  $node1 = "$localServerName.$perf_domain.local"
  $node2 = "$replica_vm_name.$perf_domain.local"
  $ad_dc = Get-ADDomainController -Credential $domainCredential

  $ClusterDisks = Get-PhysicalDisk -CanPool $True |
      Where-Object { ($_.FriendlyName -ne 'nvme_card' -and $_.FriendlyName -ne 'EphemeralDisk') -and ($_ |
          Get-PhysicalDiskStorageNodeView |
          Select-Object -Property StorageNodeObjectId) -like ('*' + $localServerName + '*') }

  $diskCount = 0
  [char]$initialDisk = 'D'

  foreach($disk in $ClusterDisks) {
    $diskLetter = [char]([byte]$initialDisk + $diskCount)

    Initialize-Disk -Number $disk.DeviceId
    New-Partition -DiskNumber $disk.DeviceId -UseMaximumSize  -DriveLetter $diskLetter | Format-Volume -FileSystem NTFS -newfilesystemlabel DataDisk -AllocationUnitSize 65536

    Start-Sleep -Seconds 30
    $initializedDisks.Add($diskLetter)
    $diskCount = $diskCount + 1
  }

  Write-Host 'Test Cluster'
  Invoke-Command -ComputerName  $ad_dc.HostName -Credential $domainCredential -ArgumentList $node1,$node2 -ScriptBlock {
        Test-Cluster -Node $args[0],$args[1] -Include 'Inventory', 'Network', 'System Configuration' -Confirm:0;
  }
  Write-Host 'Add SQL service account'
  Invoke-Command -ComputerName  $ad_dc.HostName -Credential $domainCredential -ArgumentList $passwordSecureString -ScriptBlock {
        New-ADUser -Name 'sql_server' -Description 'SQL Agent and SQL Admin account.' -AccountPassword $args[0] -Enabled $true -PasswordNeverExpires $true
  }

  $clDiskList = Get-ClusterAvailableDisk
  Write-Host 'Add Cluster Disks'
  if ($clDiskList.count -gt 0) {
    Get-ClusterAvailableDisk | Add-ClusterDisk
    Start-Sleep -Seconds 30

    $AddedClusterDisks = Get-ClusterResource -Name 'Cluster Disk *'
    foreach($addedDisk in $AddedClusterDisks) {
      if ($addedDisk.OwnerNode -ne $localServerName) {
        Write-Host "Moving Disk Ownership for $addedDisk.Name"
        Move-ClusterGroup -Name $addedDisk.OwnerGroup -Node $localServerName
        Start-Sleep -Seconds 15
      }
    }
    Start-Sleep -Seconds 30
    foreach($initDisk in $initializedDisks) {
      if (Get-PSDrive -Name $initDisk) {
        New-Item -ItemType Directory -Path "$($initDisk):\MSSQL"
        Get-Date | Out-File -FilePath "$($initDisk):\MSSQL\fcimw.txt"
      }
    }
  }
  else {
    Write-Host 'No cluster disks available'
  }

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
