param (
    [string]$replica_vm_name,
    [string]$perf_domain_password,
    [string]$perf_domain
 )

try {
    $domainUser = "$perf_domain\pkbadminuser"
    $localServerName    = [System.Net.Dns]::GetHostName()
    $passwordSecureString = (ConvertTo-SecureString -AsPlainText $perf_domain_password -Force)
    $domainCredential   = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

    $ad_dc = Get-ADDomainController -Credential $domainCredential
    $node1 = "$localServerName.$perf_domain.local"
    $node2 = "$replica_vm_name.$perf_domain.local"

    Write-Host 'Create failover cluster'
    Invoke-Command -ComputerName $ad_dc.HostName -Credential $domainCredential -ArgumentList $node1,$node2 -ScriptBlock {
        New-Cluster -Name sql-fci -Node  $args[0],$args[1] -NoStorage -ManagementPointNetworkType Distributed
    }
}
catch {
    Write-Host $_.Exception.Message
    throw $_.Exception.Message
}
