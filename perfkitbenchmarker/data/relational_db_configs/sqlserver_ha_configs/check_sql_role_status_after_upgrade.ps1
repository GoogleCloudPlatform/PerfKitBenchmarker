$cluster_group_name = 'SQL Server (MSSQLSERVER)'
$localServerName    = [System.Net.Dns]::GetHostName()

#$cluster_nodes = Get-ClusterNode
Start-Sleep -Seconds 60

$sql_cluster_group = Get-ClusterGroup -Name $cluster_group_name

if ($sql_cluster_group.State -eq 'Online') {
  $cluster_shared_volumes = Get-ClusterSharedVolume
  foreach ($cluster_shared_volume in $cluster_shared_volumes) {
      Move-ClusterSharedVolume -Name $cluster_shared_volume.Name
  }
  Move-ClusterGroup -Name $sql_cluster_group.Name
}

$sql_cluster_group = Get-ClusterGroup -Name $cluster_group_name
if ($sql_cluster_group.OwnerNode -ne $localServerName) {
    Move-ClusterGroup -Name $sql_cluster_group.Name -Node $localServerName
}

if ($sql_cluster_group.State -ne 'Online') {
  Write-Host 'SQL Cluster Role not online'
  if ($sql_cluster_group.OwnerNode -ne $localServerName) {
    Move-ClusterGroup -Name $sql_cluster_group.Name -Node $localServerName
  }
  Start-ClusterGroup -Name $cluster_group_name

  for ($i=0;$i -lt 3;$i++) {
    $sql_server = Get-ClusterResource | Where-Object {$_.ResourceType.Name -eq 'SQL Server'}

    if ($sql_cluster_group.State -ne 'Online') {
      Start-ClusterResource -name $sql_server.Name
    }
    else {
      Write-Host 'SQL Server role started and online'
      break
    }
  }
}
else {
  Write-Host 'SQL Server role is online'
}
