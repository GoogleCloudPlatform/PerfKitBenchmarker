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
      Where-Object { ($_ |
          Get-PhysicalDiskStorageNodeView |
          Select-Object -Property StorageNodeObjectId) -like ('*' + $localServerName + '*') }

  if ($ClusterDisks.count > 3) {
    $Pool = New-StoragePool -StorageSubsystemFriendlyName 'Clustered*' -FriendlyName FciPool -PhysicalDisks $ClusterDisks -ResiliencySettingNameDefault Simple -Verbose
    $Pool | New-Volume -FriendlyName 'Data' -FileSystem CSVFS_ReFS -AllocationUnitSize 65536 -ProvisioningType 'Fixed' -UseMaximumSize
  }
  else {
    $diskCount = 0
    $diskLetters = 'D','E','F'

    foreach($disk in $ClusterDisks) {
      $diskLetter = $diskLetters[$diskCount]

      Initialize-Disk -Number $disk.DeviceId
      New-Partition -DiskNumber $disk.DeviceId -UseMaximumSize  -DriveLetter $diskLetter | Format-Volume -FileSystem NTFS -newfilesystemlabel DataDisk -AllocationUnitSize 65536

      Start-Sleep -Seconds 10
      $initializedDisks.Add($diskLetter)
      $diskCount = $diskCount + 1
    }
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
  if ($clDiskList.count -gt 0) {
    $clDiskCount = 1
    foreach($disk in $ClusterDisks) {
        Get-Disk -Number $disk.DeviceId | Add-ClusterDisk
        Start-Sleep -Seconds 30
        $selectedClusterDisk = Get-ClusterResource -Name "Cluster Disk $clDiskCount"
        if ($selectedClusterDisk.OwnerNode -ne $localServerName) {
          Write-Host 'Moving Disk Ownership'
          Move-ClusterGroup -Name $selectedClusterDisk.OwnerGroup -Node $localServerName
        }
        $clDiskCount++
    }

    foreach($initDisk in $initializedDisks) {
      if (Get-PSDrive -Name $initDisk) {
        Write-Host "Disk $initDisk Created"
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
