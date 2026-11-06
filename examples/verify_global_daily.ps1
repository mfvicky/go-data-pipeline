# Run in PowerShell from the project root after docker compose up --build -d.
# The bundled fixture is synthetic; only this script's newly-created job is deleted.
param(
  [Parameter(Mandatory=$true)][string]$ApiKey,
  [string]$DbUser = 'pipeline',
  [string]$DbName = 'pipeline',
  [switch]$RestartApi,
  [switch]$DeleteAfterCheck
)
$ErrorActionPreference = 'Stop'
$headers = @{'X-API-Key' = $ApiKey}
$body = Get-Content -Raw 'examples/global_daily_sample_job.json'
$base = 'http://localhost:8080/api/v1/pipelines'
$created = Invoke-RestMethod -Method Post -Uri $base -Headers $headers -ContentType 'application/json' -Body $body
$jobId = $created.job_id
if ($jobId -notmatch '^[a-f0-9]{32}$') { throw 'Missing safe job ID in response' }
Write-Host "Created new report job: $jobId"
$status = $null
for ($i = 0; $i -lt 75; $i++) {
  Start-Sleep -Seconds 1
  $status = Invoke-RestMethod -Uri "$base/$jobId/progress" -Headers $headers
  if ($status.state -in @('COMPLETED','FAILED','CANCELLED')) { break }
}
if ($null -eq $status -or $status.state -ne 'COMPLETED') { throw "Job was not completed: $($status.state)" }
$result = Invoke-RestMethod -Uri "$base/$jobId/results" -Headers $headers
if (@($result.daily_report).Count -ne 3) { throw "Expected 3 dates, got $(@($result.daily_report).Count)" }
if (@($result.errors).Count -ne 1) { throw "Expected 1 rejected CSV row, got $(@($result.errors).Count)" }
Write-Host "SUCCESS: 3 report dates; $(@($result.records).Count) records; 1 rejected source row"
$result.daily_report | Format-Table date,cases,deaths,temperature_c,crypto_prices -AutoSize

function Test-ReportFiles([string]$jobID, [bool]$shouldExist) {
  foreach($file in @('global_daily.json','global_daily.csv')) {
    $path = "/app/output/$jobID/$file"
    if($shouldExist) {
      docker compose exec -T api sh -c "test -f '$path'" | Out-Null
    } else {
      docker compose exec -T api sh -c "test ! -e '$path'" | Out-Null
    }
    if($LASTEXITCODE -ne 0) { throw "Export file state did not match expectation: $path" }
  }
}
function Get-DatabaseReportCount([string]$jobID) {
  $rows = docker compose exec -T postgres psql -U $DbUser -d $DbName -tAc "SELECT COUNT(*) FROM pipeline_daily_reports WHERE job_id = '$jobID'"
  if($LASTEXITCODE -ne 0) { throw 'PostgreSQL report verification failed' }
  return [int]($rows.Trim())
}

Test-ReportFiles $jobId $true
if((Get-DatabaseReportCount $jobId) -ne 3) { throw 'Expected 3 database report rows' }
Write-Host 'Verified JSON + CSV exports and 3 PostgreSQL rows.'

if($RestartApi) {
  docker compose restart api | Out-Null
  if($LASTEXITCODE -ne 0) { throw 'Docker API restart failed' }
  $ready = $false
  for($i=0; $i -lt 40; $i++) {
    Start-Sleep -Seconds 1
    try {
      $res = Invoke-WebRequest -Uri 'http://localhost:8080/ready' -TimeoutSec 3
      if([int]$res.StatusCode -eq 200) { $ready=$true;break }
    } catch { }
  }
  if(-not $ready) { throw 'API readiness not restored after restart' }
  $persisted = Invoke-RestMethod -Uri "$base/$jobId/results" -Headers $headers
  if(@($persisted.daily_report).Count -ne 3) { throw 'Report did not survive API restart' }
  Test-ReportFiles $jobId $true
  Write-Host 'Verified result and files survive API restart.'
}

# Assignment API contract: cancelling an already completed job must return 409.
try {
  $unused = Invoke-WebRequest -Method Patch -Uri "$base/$jobId/cancel" -Headers $headers
  throw "Expected PATCH completed-job cancellation to return 409, got $($unused.StatusCode)"
} catch {
  if($null -eq $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 409) { throw }
}
Write-Host 'Verified PATCH /cancel contract (409 for completed job).'

if($DeleteAfterCheck) {
  Invoke-RestMethod -Method Delete -Uri "$base/$jobId" -Headers $headers | Out-Null
  Test-ReportFiles $jobId $false
  if((Get-DatabaseReportCount $jobId) -ne 0) { throw 'Report rows still present after DELETE' }
  try {
    Invoke-WebRequest -Uri "$base/$jobId" -Headers $headers | Out-Null
    throw 'Job still accessible after DELETE'
  } catch {
    if($null -eq $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 404) { throw }
  }
  Write-Host 'Verified DELETE removed job, job-scoped files and GORM report rows.'
}
Write-Host 'Global Daily Report verification completed.'
