$domainUser = 'perflab\administrator'
$dnsServerIpAddress=$args[0]

$passwordSecureString = (ConvertTo-SecureString -AsPlainText $args[1] -Force)
$nic = Get-NetAdapter

Write-Host "Set DNS Server to $dnsServerIpAddress"

Set-DnsClientServerAddress -InterfaceIndex $nic[0].ifIndex -ServerAddresses $dnsServerIpAddress

$dns_server = Get-DnsClientServerAddress -InterfaceIndex  $nic[0].ifIndex -AddressFamily IPv4

if ($dns_server.ServerAddresses -eq $dnsServerIpAddress) {
  Write-Host 'DNS Set. Joining perflab.local Domain'
  $credObject = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

  Add-Computer -DomainName perflab.local -credential $credObject
  Write-Host 'Done'
}
