#!/usr/bin/env python
"""
Setup script for SAP GL Posting Analysis System
"""

import os
import sys
from pathlib import Path

def setup_environment():
    """Set up the development environment"""
    print("🚀 Setting up SAP GL Posting Analysis System...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        sys.exit(1)
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    
    # Create necessary directories
    directories = [
        'temp_uploads',
        'trained_models',
        'logs',
        'static',
        'media'
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Created directory: {directory}")
    
    # Check if .env file exists
    if not Path('.env').exists():
        print("⚠️  .env file not found. Please copy env.example to .env and configure your settings.")
        print("   cp env.example .env")
    
    print("\n🎉 Environment setup complete!")
    print("\nNext steps:")
    print("1. Copy env.example to .env and configure your settings")
    print("2. Install dependencies: pip install -r requirements.txt")
    print("3. Run migrations: python manage.py migrate")
    print("4. Create superuser: python manage.py createsuperuser")
    print("5. Start the server: python manage.py runserver")
    print("6. Start Celery worker: python start_celery_worker.py")

if __name__ == '__main__':
    setup_environment() 