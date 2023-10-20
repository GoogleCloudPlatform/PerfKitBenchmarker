$domainName  = 'perflab.local'
$netBIOSname = 'perflab'
$mode  = 'WinThreshold'

$scriptsFolder = 'c:\scripts'


try {
    Write-Host "Pass=$args[0]"
    $PasswordSecureString = (ConvertTo-SecureString -AsPlainText $args[0] -Force)
    Start-Sleep -Seconds 30

    New-LocalUser 'adminuser' -Password $PasswordSecureString -FullName 'Admin User' -PasswordNeverExpires
    Add-LocalGroupMember -Group 'Administrators' -Member 'adminuser'


    $AdminUserAccount = Get-LocalUser -Name 'Administrator'
    $AdminUserAccount | Set-LocalUser -Password $PasswordSecureString
    $AdminUserAccount | Enable-LocalUser



    Install-WindowsFeature AD-Domain-Services -IncludeAllSubFeature -IncludeManagementTools



    Import-Module ADDSDeployment

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
