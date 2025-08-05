#!/usr/bin/env python3
"""
Simple script to check for uploaded files that haven't been processed.
Run this script to get a quick overview of file processing status.
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta

def run_command(cmd, capture_output=True):
    """Run a command and return result"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def check_docker_status():
    """Check if Docker containers are running"""
    print("🐳 Checking Docker containers...")
    
    success, output, error = run_command("docker ps")
    if not success:
        print("❌ Docker is not running or not accessible")
        return False
    
    lines = output.strip().split('\n')
    if len(lines) <= 1:
        print("⚠️  No Docker containers running")
        return False
    
    print("✅ Docker containers running:")
    for line in lines[1:]:
        if line.strip():
            parts = line.split()
            if len(parts) >= 2:
                container_name = parts[-1]
                status = parts[4] if len(parts) > 4 else "unknown"
                print(f"   {container_name} - {status}")
    
    return True

def check_celery_workers():
    """Check if Celery workers are active"""
    print("\n🌿 Checking Celery workers...")
    
    # Check if celery worker containers are running
    success, output, error = run_command("docker ps --filter name=celery")
    if not success:
        print("❌ Cannot check Celery workers")
        return False
    
    if "celery_worker" in output:
        print("✅ Celery worker container is running")
        
        # Check worker logs for any errors
        success, logs, error = run_command("docker-compose logs --tail=20 celery_worker")
        if success and logs:
            error_lines = [line for line in logs.split('\n') if 'ERROR' in line or 'Exception' in line]
            if error_lines:
                print("⚠️  Recent errors in Celery worker:")
                for line in error_lines[-5:]:  # Show last 5 errors
                    print(f"   {line}")
        return True
    else:
        print("❌ Celery worker container not found")
        return False

def check_redis():
    """Check if Redis is running"""
    print("\n🔴 Checking Redis...")
    
    success, output, error = run_command("docker ps --filter name=redis")
    if success and "redis" in output:
        print("✅ Redis container is running")
        return True
    else:
        print("❌ Redis container not found")
        return False

def check_database():
    """Check if database is accessible"""
    print("\n🗄️  Checking database...")
    
    success, output, error = run_command("docker ps --filter name=db")
    if success and "db" in output:
        print("✅ Database container is running")
        return True
    else:
        print("❌ Database container not found")
        return False

def check_web_service():
    """Check if web service is running"""
    print("\n🌐 Checking web service...")
    
    success, output, error = run_command("docker ps --filter name=web")
    if success and "web" in output:
        print("✅ Web service container is running")
        return True
    else:
        print("❌ Web service container not found")
        return False

def check_logs_for_errors():
    """Check recent logs for errors"""
    print("\n📋 Checking recent logs for errors...")
    
    services = ['web', 'celery_worker', 'celery_beat', 'db', 'redis']
    
    for service in services:
        success, logs, error = run_command(f"docker-compose logs --tail=10 {service}")
        if success and logs:
            error_lines = [line for line in logs.split('\n') 
                          if any(keyword in line.upper() for keyword in ['ERROR', 'EXCEPTION', 'FAILED', 'CRITICAL'])]
            if error_lines:
                print(f"⚠️  Errors in {service}:")
                for line in error_lines[-3:]:  # Show last 3 errors
                    print(f"   {line}")

def suggest_fixes():
    """Suggest common fixes"""
    print("\n🔧 Suggested fixes:")
    print("   1. Start all services: docker-compose up -d")
    print("   2. Restart Celery workers: docker-compose restart celery_worker")
    print("   3. Check logs: docker-compose logs")
    print("   4. Monitor Celery: http://localhost:5555 (Flower)")
    print("   5. Process queued jobs: docker-compose exec web python manage.py process_queued_jobs")

def main():
    """Main function"""
    print("🔍 FILE PROCESSING STATUS CHECK")
    print("=" * 50)
    print(f"Check time: {datetime.now()}")
    
    # Check all services
    docker_ok = check_docker_status()
    if not docker_ok:
        print("\n❌ Docker is not running. Please start Docker first.")
        return
    
    check_web_service()
    check_database()
    check_redis()
    celery_ok = check_celery_workers()
    
    # Check for errors in logs
    check_logs_for_errors()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 SUMMARY")
    print("=" * 50)
    
    if celery_ok:
        print("✅ Celery workers appear to be running")
        print("💡 If files are not processing, try:")
        print("   - Check the web interface for file status")
        print("   - Run: docker-compose exec web python manage.py process_queued_jobs")
    else:
        print("❌ Celery workers are not running properly")
        print("💡 Try restarting Celery:")
        print("   - docker-compose restart celery_worker")
    
    suggest_fixes()
    
    print("\n" + "=" * 50)
    print("✅ Check completed")
    print("=" * 50)

if __name__ == "__main__":
    main() 