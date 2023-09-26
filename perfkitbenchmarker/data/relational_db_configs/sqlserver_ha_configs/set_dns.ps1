$dnsServerIpAddress=$args[0]

$nic = Get-NetAdapter

Write-Host "Set DNS Server to $dnsServerIpAddress"

Set-DnsClientServerAddress -InterfaceIndex $nic[0].ifIndex -ServerAddresses $dnsServerIpAddress

$dns_server = Get-DnsClientServerAddress -InterfaceIndex  $nic[0].ifIndex -AddressFamily IPv4

if ($dns_server.ServerAddresses -eq $dnsServerIpAddress) {
  Write-Host 'DNS Set completed.'
}
