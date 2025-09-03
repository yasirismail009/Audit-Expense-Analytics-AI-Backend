# Windows Docker Mount Issue Fix

## Problem
The original `docker-compose.yml` file uses bind mounts (`- .:/app`) which can cause path resolution issues on Windows with Docker Desktop. The error you encountered:

```
Error response from daemon: error while creating mount source path '/run/desktop/mnt/host/d/Office/analytics': mkdir /run/desktop/mnt/host/d: file exists
```

This happens because Docker Desktop on Windows has trouble resolving Windows paths to Linux container paths.

## Solution
I've created a Windows-compatible version that uses named volumes instead of bind mounts:

### Files Created:
1. **`docker-compose-windows.yml`** - Windows-compatible Docker Compose configuration
2. **`start-docker-windows.bat`** - Windows batch file to start services
3. **`start-docker-windows.ps1`** - PowerShell script to start services

### Key Changes:
- Replaced `- .:/app` with `- app_code:/app`
- Added `app_code` named volume for application code
- Added `logs_volume` for log files
- All other volumes remain the same

## Usage

### Option 1: Use the batch file (Windows)
```cmd
start-docker-windows.bat
```

### Option 2: Use the PowerShell script
```powershell
.\start-docker-windows.ps1
```

### Option 3: Manual commands
```bash
# Stop existing containers
docker-compose -f docker-compose-windows.yml down

# Remove old volumes
docker volume prune -f

# Start with new configuration
docker-compose -f docker-compose-windows.yml up -d
```

## Important Notes

### Development Workflow
Since we're now using named volumes instead of bind mounts, **code changes won't automatically sync** to the running containers. You'll need to:

1. **Rebuild containers** when you make code changes:
   ```bash
   docker-compose -f docker-compose-windows.yml down
   docker-compose -f docker-compose-windows.yml up -d --build
   ```

2. **Or use the original docker-compose.yml** for development (but be aware of the mount issues)

### When to Use Which File:
- **`docker-compose-windows.yml`** - Production or when you need stable containers
- **`docker-compose.yml`** - Development (may have mount issues on Windows)
- **`docker-compose-simple.yml`** - Minimal services for testing

## Troubleshooting

### If you still get mount errors:
1. Ensure Docker Desktop is running
2. Try restarting Docker Desktop
3. Check if your project path contains special characters
4. Consider moving the project to a shorter path (e.g., `C:\analytics`)

### To check container status:
```bash
docker-compose -f docker-compose-windows.yml ps
```

### To view logs:
```bash
docker-compose -f docker-compose-windows.yml logs -f [service_name]
```

## Services Available
- **Web**: http://localhost:8000
- **Database**: localhost:5433
- **Redis**: localhost:6380
- **Flower**: http://localhost:5555

## Reverting to Original
If you want to use the original configuration:
```bash
docker-compose up -d
```

But be aware that you may encounter the same mount issues on Windows.
