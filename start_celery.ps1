# Celery Worker Startup Script for Windows
# Run this script to start Celery worker and keep it running

Write-Host "========================================" -ForegroundColor Green
Write-Host "CELERY WORKER STARTUP SCRIPT" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Set environment variables
$env:DJANGO_SETTINGS_MODULE = "analytics.settings"

Write-Host "Environment setup complete" -ForegroundColor Yellow
Write-Host "Starting Celery worker..." -ForegroundColor Yellow
Write-Host ""

# Start Celery worker
try {
    Write-Host "Starting Celery worker with configuration:" -ForegroundColor Cyan
    Write-Host "- App: analytics" -ForegroundColor White
    Write-Host "- Log Level: info" -ForegroundColor White
    Write-Host "- Concurrency: 4" -ForegroundColor White
    Write-Host "- Pool: prefork" -ForegroundColor White
    Write-Host "- Broker: memory://" -ForegroundColor White
    Write-Host ""

    # Start the Celery worker
    celery -A analytics worker --loglevel=info --concurrency=4 --pool=prefork
    
} catch {
    Write-Host "Error starting Celery worker: $_" -ForegroundColor Red
    Write-Host "Press any key to exit..." -ForegroundColor Yellow
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

Write-Host ""
Write-Host "Celery worker stopped." -ForegroundColor Red
Write-Host "Press any key to exit..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown") 