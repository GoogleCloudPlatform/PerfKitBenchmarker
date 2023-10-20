try {

  Install-WindowsFeature FS-FileServer

  # Open firewall for WSFC
  Set-NetFirewallProfile -Profile Domain, Public, Private -Enabled False

  $local_name = [System.Net.Dns]::GetHostName()
  $joined_computers = (Get-AdComputer -filter "Name -NotLike '$local_name'" | Select-Object Name)

  New-Item 'C:\QWitness' -type directory

  New-SmbShare -Name QWitness -Path 'C:\QWitness' -Description 'SQL File Share Witness' -FullAccess $env:username

  foreach ($comp in $joined_computers) {
    $node = $comp.Name + '$'
    Write-Host $node
    icacls C:\QWitness\ /grant '$($node):(OI)(CI)(M)'
    Grant-SmbShareAccess -Name 'QWitness' -AccountName $node -AccessRight Full -Force
  }
}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
