param (
    [string]$perf_domain_password,
    [string]$perf_domain,
    [string]$cloud
 )

$domainName  = "$perf_domain.local"
$netBIOSname = $perf_domain
$mode  = 'WinThreshold'

$scriptsFolder = 'c:\scripts'

try {
    Write-Host "Pass=$perf_domain_password"
    Write-Host "Domain=$perf_domain"
    $PasswordSecureString = (ConvertTo-SecureString -AsPlainText $perf_domain_password -Force)
    Start-Sleep -Seconds 30

    $AdministratorExists = (Get-LocalUser).Name -contains 'Administrator'

    if ($AdministratorExists -eq 'True' -and $cloud.ToLower() -eq 'gcp' ) {
        $AdminUserAccount = Get-LocalUser -Name 'Administrator'
        $AdminUserAccount | Set-LocalUser -Password $PasswordSecureString
        $AdminUserAccount | Enable-LocalUser
    }

    New-LocalUser 'pkbadminuser' -Password $PasswordSecureString -FullName 'Admin User' -PasswordNeverExpires
    Add-LocalGroupMember -Group 'Administrators' -Member 'pkbadminuser'
    Install-WindowsFeature AD-Domain-Services -IncludeAllSubFeature -IncludeManagementTools

    $forestProperties = @{

        DomainName           = $domainName
        DomainNetbiosName    = $netBIOSname
        ForestMode           = $mode
        DomainMode           = $mode
        CreateDnsDelegation  = $false
        InstallDns           = $true
        DatabasePath         = 'C:\Windows\NTDS'
        LogPath              = 'C:\Windows\NTDS'
        SysvolPath           = 'C:\Windows\SYSVOL'
        NoRebootOnCompletion = $true
        SafeModeAdministratorPassword = $PasswordSecureString
        Force                = $true
    }

    Install-ADDSForest @forestProperties

}
catch {
    throw $_.Exception.Message
}
