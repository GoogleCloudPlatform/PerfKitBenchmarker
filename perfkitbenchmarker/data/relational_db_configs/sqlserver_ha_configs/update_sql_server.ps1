param (
    [string]$kbNumber
 )

$scriptsFolder = 'c:\scripts'
$sqlsrvUpdateFileName = 'SQLServerUpdate.exe'

function executeCodeBlock($codeBlock) {
  $returnValue = ""
  $maxRetryAttempts = 3
  $retryAttemptWaitTimeInSeconds = 10
  $attempt = 0
  $exceptionMessage = ""

  for ($attempt; $attempt -lt $maxRetryAttempts; $attempt++) {
    $failed = $false
    try {
      switch -Exact ($codeBlock) {
        'Install_Nuget' {
          Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.201 -Force
        }
        'Install_kbupdate' {
          Install-Module kbupdate -Force -Confirm:$false
        }
        'Fetch_kb_url' {
          $returnValue = Get-KbUpdate -Name $kbNumber | Select-Object Link | ForEach-Object {$_.Link}
        }
        'Get_kb_file_name' {
          $components = ($url -split '/')
          $returnValue = $components[$components.length-1]
        }
        'Create_destination_folder' {
          if (-not (Test-Path $scriptsFolder)) {
            New-Item -Path $scriptsFolder  -ItemType directory
          }
        }
        'Download_sql_srv_update' {
          (New-Object System.Net.WebClient).DownloadFile($url, "$scriptsFolder\$sqlsrvUpdateFileName")
        }
        'Install_sql_srv_update' {
          Invoke-Expression "$scriptsFolder\$sqlsrvUpdateFileName /q /IAcceptSQLServerLicenseTerms /Action=Patch /InstanceName=MSSQLSERVER"
        }
      }
    }
    catch {
      $exceptionMessage = $_.Exception.Message
      Write-Host ("{$attempt} Exception occured while executing ($codeBlock): $exceptionMessage")
      Start-Sleep -Seconds $retryAttemptWaitTimeInSeconds
      $failed = $true
    }

    if ($failed -eq $false) {
      break
    }
  }

  if ($attempt -eq $maxRetryAttempts) {
    throw "Exception occured while executing ($codeBlock): $exceptionMessage"
  }

  return $returnValue
}



# install nuget package provider
executeCodeBlock('Install_Nuget')

# install kbupdate module
executeCodeBlock('Install_kbupdate')

# fetch kb url
$url = executeCodeBlock('Fetch_kb_url')

if ([string]::IsNullOrWhitespace($url) -eq $false) {
  # set sql srv update file name
  $sqlsrvUpdateFileName = executeCodeBlock('Get_kb_file_name')

  # create scriptsFolder if not exists
  executeCodeBlock('Create_destination_folder')

  # Download the file
  executeCodeBlock('Download_sql_srv_update')

  # Install update
  executeCodeBlock('Install_sql_srv_update')
}
