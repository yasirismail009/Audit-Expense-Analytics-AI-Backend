#!/usr/bin/env python3
"""
Runner script for CSV Upload Flow Completeness Test
"""

import sys
import os
import subprocess
import time

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import requests
        print("✅ requests library available")
    except ImportError:
        print("❌ requests library not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
        print("✅ requests library installed")

def check_server():
    """Check if Django server is running"""
    try:
        import requests
        response = requests.get("http://localhost:8000/api/data-files/", timeout=5)
        if response.status_code == 200:
            print("✅ Django server is running")
            return True
        else:
            print(f"❌ Django server returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to Django server: {e}")
        print("\n💡 To start the server, run:")
        print("   python manage.py runserver")
        print("   or")
        print("   docker-compose up")
        return False

def run_test():
    """Run the completeness test"""
    print("🧪 Running CSV Upload Flow Completeness Test")
    print("=" * 60)
    
    # Check dependencies
    check_dependencies()
    
    # Check server
    if not check_server():
        return False
    
    # Run the test
    try:
        from test_csv_upload_completeness import main
        return main()
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        return False

def main():
    """Main function"""
    print("🚀 CSV Upload Flow Completeness Test Runner")
    print("=" * 60)
    
    success = run_test()
    
    if success:
        print("\n🎉 All tests passed! Your CSV upload flow is complete and robust.")
        return 0
    else:
        print("\n❌ Some tests failed. Please review the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
