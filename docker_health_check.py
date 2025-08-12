#!/usr/bin/env python3
"""
Docker Health Check Script for Analytics System
Monitors all Docker containers and services for health and performance
"""

import subprocess
import json
import time
import sys
from datetime import datetime
import psutil
import docker

class DockerHealthChecker:
    def __init__(self):
        self.client = docker.from_env()
        self.services = [
            'redis', 'web', 'celery_worker', 'celery_beat', 'celery_flower'
        ]
        
    def check_container_status(self):
        """Check the status of all containers"""
        print("🔍 Checking Docker Container Status...")
        print("=" * 60)
        
        try:
            containers = self.client.containers.list(all=True)
            container_info = {}
            
            for container in containers:
                container_info[container.name] = {
                    'status': container.status,
                    'state': container.attrs['State'],
                    'health': container.attrs['State'].get('Health', {}),
                    'created': container.attrs['Created'],
                    'image': container.attrs['Config']['Image']
                }
            
            # Check expected services
            for service in self.services:
                service_name = f"analytics-{service}-1"
                if service_name in container_info:
                    info = container_info[service_name]
                    status_emoji = "✅" if info['status'] == 'running' else "❌"
                    print(f"{status_emoji} {service}: {info['status']}")
                    
                    if info['status'] == 'running':
                        # Check health status
                        health = info['health']
                        if health:
                            health_status = health.get('Status', 'unknown')
                            health_emoji = "🟢" if health_status == 'healthy' else "🟡"
                            print(f"   Health: {health_emoji} {health_status}")
                        
                        # Check uptime
                        created = info['created']
                        if isinstance(created, (int, float)):
                            uptime = time.time() - created
                            hours = int(uptime // 3600)
                            minutes = int((uptime % 3600) // 60)
                            print(f"   Uptime: {hours}h {minutes}m")
                        else:
                            print(f"   Created: {created}")
                else:
                    print(f"❌ {service}: Not found")
            
            return container_info
            
        except Exception as e:
            print(f"❌ Error checking containers: {e}")
            return {}
    
    def check_service_logs(self, service_name, lines=10):
        """Check recent logs for a specific service"""
        try:
            container_name = f"analytics-{service_name}-1"
            container = self.client.containers.get(container_name)
            
            logs = container.logs(tail=lines, timestamps=True).decode('utf-8')
            if logs.strip():
                print(f"\n📋 Recent logs for {service_name}:")
                print("-" * 40)
                print(logs)
            else:
                print(f"\n📋 No recent logs for {service_name}")
                
        except Exception as e:
            print(f"❌ Error getting logs for {service_name}: {e}")
    
    def check_system_resources(self):
        """Check system resource usage"""
        print("\n💻 System Resource Usage:")
        print("=" * 40)
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"CPU Usage: {cpu_percent}%")
        
        # Memory usage
        memory = psutil.virtual_memory()
        print(f"Memory Usage: {memory.percent}% ({memory.used // (1024**3)}GB / {memory.total // (1024**3)}GB)")
        
        # Disk usage
        disk = psutil.disk_usage('/')
        print(f"Disk Usage: {disk.percent}% ({disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB)")
        
        # Docker disk usage
        try:
            result = subprocess.run(['docker', 'system', 'df'], 
                                  capture_output=True, text=True, check=True)
            print(f"\n🐳 Docker Disk Usage:")
            print(result.stdout)
        except Exception as e:
            print(f"❌ Error checking Docker disk usage: {e}")
    
    def check_network_connectivity(self):
        """Check network connectivity between services"""
        print("\n🌐 Network Connectivity Check:")
        print("=" * 40)
        
        # Check if web service is accessible
        try:
            result = subprocess.run(['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 
                                   'http://localhost:8000/'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                status_code = result.stdout.strip()
                if status_code == '200':
                    print("✅ Web service: Accessible (HTTP 200)")
                else:
                    print(f"⚠️  Web service: Accessible (HTTP {status_code})")
            else:
                print("❌ Web service: Not accessible")
        except Exception as e:
            print(f"❌ Web service: Error checking - {e}")
        
        # Check if Flower is accessible
        try:
            result = subprocess.run(['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 
                                   'http://localhost:5555/'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                status_code = result.stdout.strip()
                if status_code == '200':
                    print("✅ Flower (Celery monitoring): Accessible (HTTP 200)")
                else:
                    print(f"⚠️  Flower (Celery monitoring): Accessible (HTTP {status_code})")
            else:
                print("❌ Flower (Celery monitoring): Not accessible")
        except Exception as e:
            print(f"❌ Flower (Celery monitoring): Error checking - {e}")
    
    def check_celery_status(self):
        """Check Celery worker and beat status"""
        print("\n🐝 Celery Status Check:")
        print("=" * 40)
        
        try:
            # Check Celery worker status
            result = subprocess.run(['docker', 'exec', 'analytics-celery_worker-1', 
                                   'celery', '-A', 'analytics', 'inspect', 'active'], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                print("✅ Celery worker: Active and responding")
                # Parse active tasks
                if 'active' in result.stdout:
                    print("   Active tasks found")
                else:
                    print("   No active tasks")
            else:
                print("❌ Celery worker: Not responding")
                
        except Exception as e:
            print(f"❌ Error checking Celery worker: {e}")
        
        try:
            # Check Celery beat status
            result = subprocess.run(['docker', 'exec', 'analytics-celery_beat-1', 
                                   'celery', '-A', 'analytics', 'beat', '--loglevel=info'], 
                                  capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                print("✅ Celery beat: Running")
            else:
                print("❌ Celery beat: Not running")
                
        except Exception as e:
            print(f"❌ Error checking Celery beat: {e}")
    
    def run_comprehensive_check(self):
        """Run all health checks"""
        print(f"🏥 Docker Health Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Check containers
        container_info = self.check_container_status()
        
        # Check system resources
        self.check_system_resources()
        
        # Check network connectivity
        self.check_network_connectivity()
        
        # Check Celery status
        self.check_celery_status()
        
        # Check logs for critical services
        print("\n📋 Critical Service Logs:")
        print("=" * 40)
        
        critical_services = ['celery_worker', 'web', 'redis']
        for service in critical_services:
            self.check_service_logs(service, lines=5)
        
        print("\n" + "=" * 80)
        print("🏥 Health Check Complete!")
        
        return container_info

def main():
    """Main function to run the health check"""
    try:
        checker = DockerHealthChecker()
        checker.run_comprehensive_check()
    except Exception as e:
        print(f"❌ Fatal error in health check: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
