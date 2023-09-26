try {
    $domainUser = 'perflab\administrator'
    $localServerName    = [System.Net.Dns]::GetHostName()

    $passwordSecureString = (ConvertTo-SecureString -AsPlainText $args[2] -Force)
    $node1 = $localServerName + '.perflab.local'
    $node2 = $args[0] + '.perflab.local'
    $fulldcname = $args[1] + '.perflab.local'

    Write-Host $args[2]

    $domainCredential   = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

    Write-Host 'Create failover cluster'
    Invoke-Command -ComputerName  $fulldcname -Credential $domainCredential -ArgumentList $node1,$node2 -ScriptBlock {
        New-Cluster -Name sql-fci -Node  $args[0],$args[1] -NoStorage -ManagementPointNetworkType Distributed
    }
}
catch {
    Write-Host $_.Exception.Message
    throw $_.Exception.Message
}
