# Demo Script for Refactored Ingestion Gateway (Compatible)

param([string]$Mode, [switch]$Auto)

$Url = "http://localhost:8080/api/batches/upload"
$UserName = "demo-user"
$FilePaths = @("demo-file-1.txt", "demo-file-2.txt")

# Create dummy files if they doesn't exist
foreach ($path in $FilePaths) {
    if (-not (Test-Path $path)) {
        "Content for $path" | Out-File $path -Encoding utf8
    }
}

function Upload-File {
    param([string[]]$Paths)
    Write-Host "Uploading files: $($Paths -join ', ')..." -ForegroundColor Cyan
    
    try {
        # Construct curl arguments dynamically
        $curlArgs = @("-s", "-F", "userName=$UserName")
        foreach ($p in $Paths) {
            $curlArgs += "-F"
            $curlArgs += "files=@$p"
        }
        $curlArgs += $Url

        # Execute curl
        $response = & curl.exe $curlArgs
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Success! Response: $response" -ForegroundColor Green
            return $response
        } else {
            Write-Error "Upload failed with exit code $LASTEXITCODE"
        }
    } catch {
        Write-Error "Upload failed: $_"
    }
}

function Monitor-Status {
    param($BatchId, $MaxRetries = 30)
    Write-Host "`n[Monitoring] Checking MongoDB status for Batch ID: $BatchId" -ForegroundColor Yellow
    Write-Host "Press Ctrl+C to stop monitoring." -ForegroundColor Gray
    
    $done = $false
    $retries = 0
    while (-not $done -and $retries -lt $MaxRetries) {
        Start-Sleep -Seconds 2
        try {
            # Use docker exec to query mongodb inside the container
            # Ensure your mongodb container name is 'mongodb' as per docker-compose
            $statusJson = docker exec mongodb mongosh ingestion --quiet --eval "db.batches.findOne({_id: '$BatchId'}, {status: 1, _id: 0})"
            
            $timestamp = Get-Date -Format "HH:mm:ss"

            if ($statusJson -match "PENDING") {
                Write-Host "[$timestamp] Status: PENDING" -ForegroundColor Cyan
            } elseif ($statusJson -match "READY") {
                Write-Host "[$timestamp] Status: READY (Waiting for Worker)" -ForegroundColor Green
            } elseif ($statusJson -match "DONE") {
                Write-Host "[$timestamp] Status: DONE (Processed)" -ForegroundColor Magenta
                $done = $true
            } else {
                Write-Host "[$timestamp] Status: $statusJson" -ForegroundColor Gray
            }
        } catch {
            Write-Host "Error querying MongoDB (is the container running?)" -ForegroundColor Red
        }
        $retries++
    }
    if (-not $done) {
        Write-Host "Timeout reached while monitoring status." -ForegroundColor Red
    }
}

Clear-Host
Write-Host "=== Ingestion Gateway Refactoring Demo ===" -ForegroundColor Yellow
Write-Host "Mode: $Mode"
Write-Host "========================================"

if ([string]::IsNullOrWhiteSpace($Mode)) {
    Write-Host "1. Happy Path Demo"
    Write-Host "2. Recovery Demo (Instructions)"
    $Mode = Read-Host "Select an option (1/2)"
}

if ($Mode -eq "1" -or $Mode -eq "HAPPY_PATH") {
    Write-Host "`n[Happy Path] Uploading files..." -ForegroundColor Yellow
    $batchId = Upload-File -Paths $FilePaths
    if ($batchId) {
        Monitor-Status -BatchId $batchId
    }
}
elseif ($Mode -eq "2" -or $Mode -eq "RECOVERY") {
    Write-Host "`n[Recovery Demo] Instructions:" -ForegroundColor Yellow
    if (-not $Auto) {
        Write-Host "1. STOP your Kafka container now (e.g., 'docker stop <kafka-id>')."
        Write-Host "2. Press Enter to proceed with upload."
        Read-Host
    }
    
    Write-Host "Uploading..." -ForegroundColor Yellow
    $batchId = Upload-File -Paths $FilePaths
    
    if ($batchId) {
        Write-Host "`nThe batch should stay in READY state because Kafka is down." -ForegroundColor Gray
        Write-Host "We will start monitoring. While monitoring:" -ForegroundColor Gray
        if (-not $Auto) {
            Write-Host "   - START your Kafka container again."
            Write-Host "   - Wait for the scheduled task (every 30s) to recover it."
        }
        Write-Host "   - Status should change to DONE."
        Monitor-Status -BatchId $batchId -MaxRetries 60
    }
}
else {
    Write-Host "Invalid option: $Mode"
}
