param (
    [string]$skip_cluster_check = 'False'
 )

function Get-SqlServiceStatus {
  $sqlServerService = Get-Service -Name 'MSSQLSERVER' -ErrorAction SilentlyContinue

  if ($sqlServerService) {
    if ($sqlServerService.Status -eq 'Running') {
      return $true
    }
  }
  return $false
}

function Get-ClusterStatus {
  $clusterStatus = Get-Cluster -ErrorAction SilentlyContinue
  if ($clusterStatus) {
    return $true
  }
  return $false
}


function Get-SqlServerConnectionState {
  param (
    [Parameter(Mandatory = $true)]
    [String] $hostName
  )
  try {
    $sqlConn = New-Object System.Data.SqlClient.SqlConnection
    $sqlConn.ConnectionString =  "Server=$hostName;Initial Catalog=master;Integrated Security=true;"
    $sqlConn.Open()

    $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
    $sqlcmd.Connection = $sqlConn

    $query = "SELECT serverproperty('ComputerNamePhysicalNetBIOS') as currentNode"

    $sqlcmd.CommandText = $query
    $dataReader = $sqlcmd.ExecuteReader()

    $dataReader.Read()
    $currentNode = $dataReader.GetString(0)
    return $true
  }
  catch {
    return $false
  }
}

try {
  # Check SQL Server status up to 10 times
  $localServerName = [System.Net.Dns]::GetHostName()
  $sqlState = 0

  for (($i = 0); $i -lt 24; $i++) {

    $serviceRunning = Get-SqlServiceStatus
    if ($skip_cluster_check -eq 'True') {
      $isClustered = $true
    }
    else {
      $isClustered = Get-ClusterStatus
    }
    $canConnect = Get-SqlServerConnectionState -HostName $localServerName

    if ($serviceRunning -and $isClustered -and $canConnect) {
      Write-Host 'SQL Server is healthy'
      $sqlState = 0
      return
    }
    else {
      Write-Host 'SQL Server is not healthy on this attempt. Status details:'
      Write-Host "  - SQL Service (MSSQLSERVER) Running: $serviceRunning"
      Write-Host "  - Server is part of a cluster: $isClustered"
      Write-Host "  - Can connect to SQL Server instance: $canConnect"

      $sqlState = 1
    }
    Start-Sleep 10
  }
  # Throw exception if SQL Server is not healthy after 240 seconds
  throw 'SQl Server not responding after 240 seconds'

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
