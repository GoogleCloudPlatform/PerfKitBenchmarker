param (
    [string]$dns_server_ip,
    [string]$perf_password,
    [string]$perf_domain
 )

$domainUser = "$perf_domain\pkbadminuser"
$domainName  = "$perf_domain.local"
$connectionVerified = $false

$passwordSecureString = (ConvertTo-SecureString -AsPlainText $perf_password -Force)
$nic = Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias 'E*'

Write-Host "Set DNS Server to $dns_server_ip"
Write-Host $nic.InterfaceAlias

Set-DnsClientServerAddress -InterfaceIndex $nic[0].ifIndex -ServerAddresses $dns_server_ip

$dns_server = Get-DnsClientServerAddress -InterfaceIndex  $nic[0].ifIndex -AddressFamily IPv4

if (Test-Connection -ComputerName $DomainName -Count 1 -Quiet) {
    Write-Host 'Network connectivity to $DomainName is successful.'
}
else {
    Write-Host 'Cannot reach $DomainName via ICMP. Check DNS settings and network connectivity.'
}

for ($i=0; $i -lt 10; $i++) {
  try {
    $test_connection = Test-NetConnection -ComputerName $DomainName -Port 135
    if ($test_connection.TcpTestSucceeded) {
      Write-Host 'Network connectivity to $DomainName on port 135 is successful.'
      $connectionVerified = $true
      break
    }
    else {
      Write-Host 'Cannot reach $DomainName on port 135.'
      Start-Sleep -Seconds 30
    }
  }
  catch {
    Start-Sleep -Seconds 30
  }
}

if (-not $connectionVerified) {
  throw 'Cannot reach $DomainName.'
}

if ($dns_server.ServerAddresses -eq $dns_server_ip) {
  for ($i=0; $i -lt 3; $i++) {
    try {
      Write-Host "DNS Set. Joining $domainName  Domain"
      $credObject = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

      Add-Computer -DomainName $domainName  -credential $credObject -ErrorAction Stop -PassThru
      Write-Host 'Done'
      break
    }
    catch {
      Write-Host 'Domain join test failed. Review the error details below:'
      Write-Error $_.Exception.Message
      Start-Sleep -Seconds 30
    }
  }
}
