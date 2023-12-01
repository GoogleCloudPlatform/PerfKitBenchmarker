param (
   [Parameter(Mandatory=$true)][string]$Ag, # provide a AG name
   [Parameter(Mandatory=$true)][string]$Dns, # provide a DNS name
   [Parameter(Mandatory=$true)][string]$Port # provide a unique port number
)

# DNN config script
# Make sure multi subnet failover =True
Write-Host "Add a DNN listener for availability group $Ag with DNS name $Dns and port $Port"

$ErrorActionPreference = 'Stop'

# create the DNN resource with the port as the resource name
Add-ClusterResource -Name $Port -ResourceType 'Distributed Network Name' -Group $Ag

# set the DNS name of the DNN resource
Get-ClusterResource -Name $Port | Set-ClusterParameter -Name DnsName -Value $Dns

# start the DNN resource
Start-ClusterResource -Name $Port


$Dep = Get-ClusterResourceDependency -Resource $Ag
if ( $Dep.DependencyExpression -match '\s*\((.*)\)\s*' ) {
$DepStr = "$($Matches.1) or [$Port]"
}
else {
$DepStr = "[$Port]"
}

Write-Host $DepStr

# add the Dependency from availability group resource to the DNN resource
Set-ClusterResourceDependency -Resource $Ag -Dependency $DepStr


#bounce the AG resource
Stop-ClusterResource -Name $Ag
Start-ClusterResource -Name $Ag
