try {
  icacls C:\QWitness\ /grant 'sql-fci$:(OI)(CI)(M)'
  Grant-SmbShareAccess -Name QWitness -AccountName 'sql-fci$' -AccessRight Full -Force
}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
