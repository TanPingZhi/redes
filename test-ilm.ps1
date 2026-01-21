# test-ilm.ps1
# PowerShell script to test the Ingestion Gateway and ILM lifecycle

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Ingestion Gateway & ILM Test Script  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 1. Set ILM poll interval to 10s for faster testing
Write-Host "`n[Step 1] Setting ILM poll interval to 10s..." -ForegroundColor Yellow
$body = '{"persistent":{"indices.lifecycle.poll_interval":"10s"}}'
$response = Invoke-RestMethod -Uri "http://localhost:9200/_cluster/settings" -Method PUT -ContentType "application/json" -Body $body
Write-Host "ILM Poll Interval set: $($response | ConvertTo-Json -Compress)" -ForegroundColor Green

# 2. Send a test upload
Write-Host "`n[Step 2] Sending test upload..." -ForegroundColor Yellow
$boundary = [System.Guid]::NewGuid().ToString()
$filePath = ".\pom.xml"
$fileBytes = [System.IO.File]::ReadAllBytes($filePath)
$fileEnc = [System.Text.Encoding]::GetEncoding('ISO-8859-1').GetString($fileBytes)

$bodyLines = @(
    "--$boundary",
    "Content-Disposition: form-data; name=`"files`"; filename=`"pom.xml`"",
    "Content-Type: application/octet-stream",
    "",
    $fileEnc,
    "--$boundary",
    "Content-Disposition: form-data; name=`"userName`"",
    "",
    "testUser",
    "--$boundary--"
) -join "`r`n"

$batchId = Invoke-RestMethod -Uri "http://localhost:8080/api/batches/upload" -Method POST -ContentType "multipart/form-data; boundary=$boundary" -Body $bodyLines
Write-Host "Batch ID: $batchId" -ForegroundColor Green

# 3. Query ES for the document
Write-Host "`n[Step 3] Querying Elasticsearch for document..." -ForegroundColor Yellow
Start-Sleep -Seconds 2
$indices = Invoke-RestMethod -Uri "http://localhost:9200/_cat/indices/create-replay-*?v&format=json"
Write-Host "Indices:" -ForegroundColor Cyan
$indices | Format-Table -AutoSize

# 4. Show document status
Write-Host "`n[Step 4] Document status..." -ForegroundColor Yellow
$searchBody = '{"query":{"match_all":{}}}'
$docs = Invoke-RestMethod -Uri "http://localhost:9200/create-replay-*/_search" -Method GET -ContentType "application/json" -Body $searchBody
foreach ($hit in $docs.hits.hits) {
    Write-Host "  ID: $($hit._id) | Status: $($hit._source.status)" -ForegroundColor White
}

# 5. Monitor ILM and wait for deletion
Write-Host "`n[Step 5] Monitoring ILM (waiting for index deletion)..." -ForegroundColor Yellow
Write-Host "  ILM is set to delete after 30s. Checking every 10s..." -ForegroundColor Gray

$maxAttempts = 12  # 2 minutes max
$attempt = 0
$deleted = $false

while ($attempt -lt $maxAttempts -and -not $deleted) {
    Start-Sleep -Seconds 10
    $attempt++
    
    try {
        $indices = Invoke-RestMethod -Uri "http://localhost:9200/_cat/indices/create-replay-*?v&format=json" -ErrorAction Stop
        $count = ($indices | Measure-Object).Count
        Write-Host "  [$attempt/$maxAttempts] Indices remaining: $count" -ForegroundColor White
        
        if ($count -eq 0) {
            $deleted = $true
        }
    } catch {
        Write-Host "  [$attempt/$maxAttempts] No indices found (deleted!)" -ForegroundColor Green
        $deleted = $true
    }
}

if ($deleted) {
    Write-Host "`n========================================" -ForegroundColor Green
    Write-Host "  SUCCESS: Index was deleted by ILM!   " -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
} else {
    Write-Host "`n========================================" -ForegroundColor Red
    Write-Host "  TIMEOUT: Index not deleted yet.      " -ForegroundColor Red
    Write-Host "  Check ILM policy and poll interval.  " -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
}
