#!/usr/bin/env python3
"""
Start Parallel Processing System
Automatically listens for jobs and processes them in parallel
"""

import os
import sys
import signal
import time
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
import django
django.setup()

from core.parallel_processor import start_processor, stop_processor, get_processor_stats

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print("\n🛑 Received shutdown signal. Stopping processor...")
    stop_processor()
    sys.exit(0)

def main():
    """Main function to start the parallel processor"""
    
    print("🚀 Starting Parallel Processing System...")
    print(f"📁 Project root: {project_root}")
    print(f"⚙️  Django settings: {os.environ.get('DJANGO_SETTINGS_MODULE')}")
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start the processor with 4 workers
        processor = start_processor(num_workers=4)
        
        print("✅ Parallel processor started successfully!")
        print("📋 Listening for jobs automatically...")
        print("🛑 Press Ctrl+C to stop")
        
        # Main loop - show stats periodically
        while True:
            time.sleep(10)  # Update every 10 seconds
            
            stats = get_processor_stats()
            if stats['is_running']:
                print(f"\n📊 Stats: {stats['jobs_processed']} processed, {stats['jobs_failed']} failed, Uptime: {stats['uptime']}")
            else:
                print("❌ Processor stopped unexpectedly")
                break
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping processor...")
        stop_processor()
    except Exception as e:
        print(f"❌ Error: {e}")
        stop_processor()
        sys.exit(1)

if __name__ == '__main__':
    main() 