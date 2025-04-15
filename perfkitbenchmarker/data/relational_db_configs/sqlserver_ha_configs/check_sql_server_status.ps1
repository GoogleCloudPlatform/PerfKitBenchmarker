
function Get-SqlServiceStatus {
  $sqlServerService = Get-Service -Name 'MSSQLSERVER' -ErrorAction SilentlyContinue

  if ($sqlServerService) {
    if ($sqlServerService.Status -eq 'Running') {
      return $true
    }
    return $false
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
    if ((Get-SqlServiceStatus -eq $true) -and (Get-ClusterStatus -eq $true) -and ((Get-SqlServerConnectionState -HostName $localServerName) -eq $true)) {
      Write-Host 'SQL Server is healthy'
      $sqlState = 0
      break
    }
    else {
      Write-Host 'SQL Server is not healthy'
      $sqlState = 1
    }
    Start-Sleep 10
  }
  exit $sqlState

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}


