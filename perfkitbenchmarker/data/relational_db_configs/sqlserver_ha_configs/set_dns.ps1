param (
    [string]$dns_server_ip
 )

$nic = Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias 'E*'

Write-Host "Set DNS Server to $dns_server_ip"
Write-Host $nic.InterfaceAlias

Set-DnsClientServerAddress -InterfaceIndex $nic[0].ifIndex -ServerAddresses $dns_server_ip

$dns_server = Get-DnsClientServerAddress -InterfaceIndex  $nic[0].ifIndex -AddressFamily IPv4

if ($dns_server.ServerAddresses -eq $dns_server_ip) {
  Write-Host 'DNS Set completed.'
}
