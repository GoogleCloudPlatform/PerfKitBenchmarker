try {
  $ErrorActionPreference = 'stop'
  Write-Host 'Installing cluster components.'
  # Install required Windows features
  Install-WindowsFeature Failover-Clustering -IncludeManagementTools
  Install-WindowsFeature RSAT-AD-PowerShell

  # Open firewall for WSFC
  Set-NetFirewallProfile -Profile Domain, Public, Private -Enabled False

}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
