# demo-states.ps1
# PowerShell script to demonstrate state transitions visibly

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  State Transition Demo Script         " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Timeline:" -ForegroundColor Yellow
Write-Host "  0s   - Upload starts, document in PENDING" -ForegroundColor Gray
Write-Host "  10s  - MinIO upload completes, status -> READY" -ForegroundColor Gray
Write-Host "  ~30s - Worker picks up, publishes to Kafka, status -> DONE" -ForegroundColor Gray
Write-Host "  5m   - ILM deletes the index" -ForegroundColor Gray
Write-Host ""

# 1. Set ILM poll interval
Write-Host "[Setup] Setting ILM poll interval to 10s..." -ForegroundColor Yellow
try {
    $body = '{"persistent":{"indices.lifecycle.poll_interval":"10s"}}'
    Invoke-RestMethod -Uri "http://localhost:9200/_cluster/settings" -Method PUT -ContentType "application/json" -Body $body | Out-Null
    Write-Host "  Done!" -ForegroundColor Green
} catch {
    Write-Host "  Warning: Could not set ILM poll interval" -ForegroundColor Red
}

# 2. Send test upload (async-ish)
Write-Host ""
Write-Host "[Step 1] Sending upload..." -ForegroundColor Yellow
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
    "demoUser",
    "--$boundary--"
) -join "`r`n"

# Start upload in background
$job = Start-Job -ScriptBlock {
    param($uri, $boundary, $bodyLines)
    Invoke-RestMethod -Uri $uri -Method POST -ContentType "multipart/form-data; boundary=$boundary" -Body $bodyLines
} -ArgumentList "http://localhost:8080/api/batches/upload", $boundary, $bodyLines

Write-Host "  Upload initiated (runs in background with 10s delay before MinIO)" -ForegroundColor Green

# 3. Check for PENDING state
Write-Host ""
Write-Host "[Step 2] Checking for PENDING state (within next 8 seconds)..." -ForegroundColor Yellow
for ($i = 1; $i -le 4; $i++) {
    Start-Sleep -Seconds 2
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:9200/create-replay-*/_search" -Method POST -ContentType "application/json" -Body '{"query":{"match_all":{}}}'
        if ($response.hits.total.value -gt 0) {
            $status = $response.hits.hits[0]._source.status
            Write-Host "  [$($i*2)s] Document found! Status: $status" -ForegroundColor $(if($status -eq "PENDING"){"Cyan"}else{"White"})
        } else {
            Write-Host "  [$($i*2)s] Waiting for document..." -ForegroundColor Gray
        }
    } catch {
        Write-Host "  [$($i*2)s] Index not created yet..." -ForegroundColor Gray
    }
}

# 4. Wait for READY state
Write-Host ""
Write-Host "[Step 3] Waiting for READY state (after MinIO upload completes)..." -ForegroundColor Yellow
for ($i = 1; $i -le 5; $i++) {
    Start-Sleep -Seconds 3
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:9200/create-replay-*/_search" -Method POST -ContentType "application/json" -Body '{"query":{"match_all":{}}}'
        if ($response.hits.total.value -gt 0) {
            $status = $response.hits.hits[0]._source.status
            Write-Host "  [+$($i*3)s] Status: $status" -ForegroundColor $(if($status -eq "READY"){"Green"}elseif($status -eq "PENDING"){"Cyan"}else{"White"})
            if ($status -eq "READY") { break }
        }
    } catch {
        Write-Host "  [+$($i*3)s] Error querying ES" -ForegroundColor Gray
    }
}

# Get the batch ID from the job
$batchId = Receive-Job -Job $job -Wait
Write-Host ""
Write-Host "  Batch ID: $batchId" -ForegroundColor White

# 5. Wait for DONE state (worker processes every 30s)
Write-Host ""
Write-Host "[Step 4] Waiting for DONE state (worker runs every 30s)..." -ForegroundColor Yellow
$maxWait = 45
$startTime = Get-Date
while (((Get-Date) - $startTime).TotalSeconds -lt $maxWait) {
    Start-Sleep -Seconds 5
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:9200/create-replay-*/_search" -Method POST -ContentType "application/json" -Body '{"query":{"match_all":{}}}'
        if ($response.hits.total.value -gt 0) {
            $status = $response.hits.hits[0]._source.status
            $elapsed = [math]::Round(((Get-Date) - $startTime).TotalSeconds)
            Write-Host "  [+${elapsed}s] Status: $status" -ForegroundColor $(if($status -eq "DONE"){"Magenta"}elseif($status -eq "READY"){"Green"}else{"White"})
            if ($status -eq "DONE") { break }
        }
    } catch {
        Write-Host "  Error querying ES" -ForegroundColor Gray
    }
}

# 6. Check Kafka
Write-Host ""
Write-Host "[Step 5] Check Kafka UI for messages!" -ForegroundColor Yellow
Write-Host "  Open: http://localhost:8090" -ForegroundColor Cyan
Write-Host "  Look for topics: topic-alpha, topic-beta" -ForegroundColor Cyan

# 7. Monitor ILM
Write-Host ""
Write-Host "[Step 6] Monitoring for ILM deletion (5 minutes from index creation)..." -ForegroundColor Yellow
Write-Host "  The index will be deleted automatically after 5 minutes." -ForegroundColor Gray
Write-Host "  You can monitor in Kibana: http://localhost:5601 -> Dev Tools" -ForegroundColor Gray
Write-Host "  Run: GET _cat/indices/create-replay-*" -ForegroundColor Gray

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Demo Complete! Check Kafka UI now.   " -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
