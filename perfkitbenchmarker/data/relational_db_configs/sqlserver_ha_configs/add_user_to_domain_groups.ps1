param (
    [string]$perf_password,
    [string]$perf_domain
 )

$domainUser = "$perf_domain\pkbadminuser"
$passwordSecureString = (ConvertTo-SecureString -AsPlainText $perf_password -Force)
$domainCredential = New-Object System.Management.Automation.PSCredential ($domainUser, $passwordSecureString)

Start-Sleep -Seconds 20

# get list of domain controllers
$ad_dc = Get-ADDomainController -Credential $domainCredential

Invoke-Command -ComputerName $ad_dc.HostName -Credential $domainCredential -ArgumentList $passwordSecureString -ScriptBlock {
        Add-ADGroupMember -Identity 'Domain Admins' -Members pkbadminuser
        Add-ADGroupMember -Identity 'Enterprise Admins' -Members pkbadminuser
}
