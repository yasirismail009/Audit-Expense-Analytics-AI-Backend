Write-Host "Starting Docker Compose with Windows-compatible configuration..." -ForegroundColor Green
Write-Host ""

# Stop any existing containers
Write-Host "Stopping existing containers..." -ForegroundColor Yellow
docker-compose -f docker-compose-windows.yml down

# Remove any existing volumes to start fresh
Write-Host "Removing existing volumes..." -ForegroundColor Yellow
docker volume prune -f

# Start the services
Write-Host "Starting services with docker-compose-windows.yml..." -ForegroundColor Yellow
docker-compose -f docker-compose-windows.yml up -d

Write-Host ""
Write-Host "Docker services are starting up..." -ForegroundColor Green
Write-Host ""
Write-Host "Services:" -ForegroundColor Cyan
Write-Host "- Web: http://localhost:8000" -ForegroundColor White
Write-Host "- Database: localhost:5433" -ForegroundColor White
Write-Host "- Redis: localhost:6380" -ForegroundColor White
Write-Host "- Flower: http://localhost:5555" -ForegroundColor White
Write-Host ""
Write-Host "To view logs: docker-compose -f docker-compose-windows.yml logs -f" -ForegroundColor Gray
Write-Host "To stop: docker-compose -f docker-compose-windows.yml down" -ForegroundColor Gray
Write-Host ""
Write-Host "Press any key to continue..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
