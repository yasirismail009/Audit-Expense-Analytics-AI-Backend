#!/usr/bin/env python

import os
import django
import time
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def monitor_tasks():
    """Real-time Celery task monitoring"""
    
    print("🔍 Real-Time Celery Task Monitor")
    print("=" * 50)
    print("Press Ctrl+C to stop monitoring\n")
    
    try:
        from celery import current_app
        
        inspect = current_app.control.inspect()
        
        while True:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"⏰ {current_time} - Checking task status...")
            
            # Active tasks
            active = inspect.active()
            if active:
                for worker, tasks in active.items():
                    if tasks:
                        print(f"🔄 {worker}: {len(tasks)} active task(s)")
                        for task in tasks:
                            name = task.get('name', 'Unknown')
                            task_id = task.get('id', 'Unknown')[:8] + '...'
                            args = task.get('args', [])
                            print(f"   📋 {name} [{task_id}] - Args: {args}")
                    else:
                        print(f"✅ {worker}: No active tasks")
            else:
                print("😴 No workers responding or no active tasks")
            
            # Reserved tasks
            reserved = inspect.reserved()
            if reserved:
                for worker, tasks in reserved.items():
                    if tasks:
                        print(f"📋 {worker}: {len(tasks)} reserved task(s)")
                        for task in tasks:
                            name = task.get('name', 'Unknown')
                            print(f"   ⏳ {name}")
            
            print("-" * 50)
            time.sleep(3)  # Check every 3 seconds
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Monitor Error: {e}")

def run_completeness_with_monitoring():
    """Run completeness test with task monitoring"""
    
    print("🚀 Running Completeness Test with Monitoring")
    print("=" * 50)
    
    try:
        from core.models import DataFile, Engagement
        from core.tasks import run_gl_completeness_analysis
        
        # Get file info
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        gl_file = DataFile.objects.filter(engagement=engagement, file_type='GL').first()
        
        print(f"📊 File: {gl_file.file_name}")
        print(f"📊 File ID: {gl_file.id}")
        
        print("\n🔄 Queuing completeness analysis task...")
        
        # Queue the task (async)
        task = run_gl_completeness_analysis.delay(str(gl_file.id))
        
        print(f"✅ Task queued with ID: {task.id}")
        print("📊 Monitoring task execution...")
        
        # Monitor task progress
        start_time = time.time()
        while not task.ready():
            elapsed = time.time() - start_time
            print(f"⏳ Task running... ({elapsed:.1f}s elapsed)")
            
            # Check task state
            print(f"   State: {task.state}")
            if hasattr(task, 'info') and task.info:
                print(f"   Info: {task.info}")
            
            time.sleep(2)
            
            # Timeout after 2 minutes
            if elapsed > 120:
                print("⏰ Task timeout - stopping monitor")
                break
        
        if task.ready():
            result = task.result
            elapsed = time.time() - start_time
            
            print(f"\n✅ Task completed in {elapsed:.1f}s")
            print(f"Success: {result.get('success', False) if result else False}")
            
            if result and result.get('success'):
                cr = result.get('completeness_results', {})
                print(f"Status: {cr.get('status', 'Unknown')}")
                print(f"Score: {cr.get('completeness_score', 0):.1f}%")
            elif result:
                print(f"Error: {result.get('error', 'Unknown')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'monitor':
        monitor_tasks()
    elif len(sys.argv) > 1 and sys.argv[1] == 'test':
        run_completeness_with_monitoring()
    else:
        print("Usage:")
        print("  python celery_task_monitor.py monitor   # Monitor all tasks")
        print("  python celery_task_monitor.py test      # Run test with monitoring")
