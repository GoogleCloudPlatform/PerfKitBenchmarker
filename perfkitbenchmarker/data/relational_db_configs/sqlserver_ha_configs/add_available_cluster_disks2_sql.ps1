$SqlResourceName = 'SQL Server'
$SqlGroupName = 'SQL Server (MSSQLSERVER)'
$DiskResourceGroup = 'Available Storage'

# 1. Get the list of disk resources that are available on the cluster
$NewDisks = Get-ClusterResource | Where-Object {$_.ResourceType -eq 'Physical Disk'} | Where-Object {$_.OwnerGroup -eq $DiskResourceGroup}

# Check if any disks were found
if ($NewDisks.Count -eq 0) {
    Write-Host "No availablePhysical Disk resources found in '$DiskResourceGroup' group. Exiting."
    exit 1
}

# 2. Bring the SQL Server resource offline before making  changes
Write-Host "Stopping SQL Server resource: $SqlResourceName..."
Stop-ClusterResource -Name $SqlResourceName

# 3. Loop through each disk and move it to the SQL Server group and add it as a dependency to SQL Server resource
Write-Host "Adding disk dependencies to $SqlResourceName..."
foreach ($Disk in $NewDisks) {
  Write-Host "    Processing disk: $($Disk.Name)"
  Write-Host '    Moving disk to SQL Server role...'
  Move-ClusterResource -Name $Disk.Name -Group $SqlGroupName

  # Add the current disk as an AND dependency to the SQL Server resource
  Add-ClusterResourceDependency -Resource $SqlResourceName -Provider $Disk.Name
}

# 4. Bring all cluster resources back online
Get-ClusterGroup | Start-ClusterGroup
