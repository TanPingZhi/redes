# demo-mongo.ps1
# PowerShell script to demonstrate state transitions with MongoDB

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  MongoDB Migration Demo Script        " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Timeline:" -ForegroundColor Yellow
Write-Host "  0s   - Upload starts, document in PENDING" -ForegroundColor Gray
Write-Host "  ~2s  - MinIO upload completes, status -> READY" -ForegroundColor Gray
Write-Host "  ~5m  - Worker picks up (cron), status -> DONE" -ForegroundColor Gray
Write-Host "  7d   - MongoDB TTL deletes the document" -ForegroundColor Gray
Write-Host ""

# 1. Verify TTL Index
Write-Host "[Setup] Checking MongoDB TTL Index..." -ForegroundColor Yellow
try {
    $indexes = docker exec mongodb mongosh ingestion --quiet --eval "db.batches.getIndexes()"
    if ($indexes -match "ttl_index") {
        Write-Host "  Verified: TTL index 'ttl_index' exists!" -ForegroundColor Green
    } else {
        Write-Host "  Warning: TTL index not found!" -ForegroundColor Red
        Write-Host $indexes
    }
} catch {
    Write-Host "  Error checking MongoDB: $_" -ForegroundColor Red
}

# 2. Set worker cron to 30s for demo? 
# The application.yml currently has 5 minutes.
# We can't easily change it without rebuild.
# But existing code has 5 minutes. The user asked for "higher" (production) values.
# So we will just wait or tell the user it takes 5 mins.
# actually, for verification, 5 mins is long. 
# But let's just proceed.

# 3. Send test upload
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
    "mongoUser",
    "--$boundary--"
) -join "`r`n"

try {
    $batchId = Invoke-RestMethod -Uri "http://localhost:8080/api/batches/upload" -Method POST -ContentType "multipart/form-data; boundary=$boundary" -Body $bodyLines
    Write-Host "  Batch ID: $batchId" -ForegroundColor White
} catch {
    Write-Host "  Upload failed: $_" -ForegroundColor Red
    exit
}

# 4. Monitor Status
Write-Host ""
Write-Host "[Step 2] Monitoring Status in MongoDB..." -ForegroundColor Yellow
Write-Host "  Querying every 2 seconds..." -ForegroundColor Gray

$startTime = Get-Date
$maxWait = 60 # wait 60 seconds
$done = $false

while (((Get-Date) - $startTime).TotalSeconds -lt $maxWait) {
    Start-Sleep -Seconds 2
    try {
        # Query MongoDB for status
        $statusJson = docker exec mongodb mongosh ingestion --quiet --eval "db.batches.findOne({_id: '$batchId'}, {status: 1, _id: 0})"
        
        # Parse logic is a bit brittle with mongosh output, let's just regex match
        if ($statusJson -match "PENDING") {
            Write-Host "  Status: PENDING" -ForegroundColor Cyan
        } elseif ($statusJson -match "READY") {
            Write-Host "  Status: READY" -ForegroundColor Green
        } elseif ($statusJson -match "DONE") {
            Write-Host "  Status: DONE" -ForegroundColor Magenta
            $done = $true
            break
        } else {
            Write-Host "  Status: Unknown or not found ($statusJson)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "  Error querying MongoDB" -ForegroundColor Gray
    }
}

if (-not $done) {
    Write-Host ""
    Write-Host "Timout waiting for DONE (Worker runs every 5 mins)." -ForegroundColor Yellow
    Write-Host "You can check later in Mango Express: http://localhost:8081" -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "SUCCESS: Flow completed!" -ForegroundColor Green
}

Write-Host ""
Write-Host "[Step 3] Check Mongo Express" -ForegroundColor Yellow
Write-Host "  Open: http://localhost:8081" -ForegroundColor Cyan
Write-Host "  Credentials: admin / admin" -ForegroundColor Cyan
