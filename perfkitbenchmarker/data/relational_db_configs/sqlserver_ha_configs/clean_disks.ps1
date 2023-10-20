 try {
  $scriptsFolder = 'c:\scripts'
  $diskpartCmdFile =  $scriptsFolder + '\diskpartcmd.txt'

  if (-not (Test-Path $scriptsFolder)) {
    New-Item -Path $scriptsFolder  -ItemType directory
  }

  $physicalDisks = Get-PhysicalDisk | where-object {$_.CanPool -eq $true} | Select-Object -exp DeviceID

  New-Item -Path $diskpartCmdFile -itemtype file -force | OUT-NULL

  foreach($pd in $physicalDisks) {
    Add-Content -path $diskpartCmdFile "select disk $pd"
    Add-Content -path $diskpartCmdFile 'clean'
  }

  diskpart /s $diskpartCmdFile
}
catch {
  Write-Host $_.Exception.Message
  throw $_.Exception.Message
}
