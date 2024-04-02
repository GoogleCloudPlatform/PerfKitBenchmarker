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
    $diskLetter = 'D','E','F'

    foreach($disk in $ClusterDisks) {
      Write-Host "Disk letter: $diskLetter[$diskCount]"
      Initialize-Disk -Number $disk.DeviceId
      New-Partition -disknumber $disk.DeviceId -usemaximumsize -DriveLetter $diskLetter[$diskCount] | Format-Volume -FileSystem ReFS -newfilesystemlabel DataDisk -AllocationUnitSize 65536
      New-Item -ItemType Directory -Path "$diskLetter[$diskCount]:\MSSQL"
      Get-Date | Out-File -FilePath "$diskLetter[$diskCount]:\MSSQL\fcimw.txt"

      $diskCount = $diskCount + 1
    }

    Get-ClusterAvailableDisk | Add-ClusterDisk
  }

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
