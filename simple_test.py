#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def simple_test():
    print('🚀 Simple Test Starting')
    print('=' * 30)
    
    try:
        from core.models import DataFile, CompletenessTestResult
        
        # Check data files
        data_files = DataFile.objects.all()
        print(f"📁 DataFiles count: {data_files.count()}")
        
        for df in data_files[:3]:
            print(f"  - {df.id}: {df.file_name} ({df.status})")
        
        # Check completeness tests
        tests = CompletenessTestResult.objects.all()
        print(f"🧪 CompletenessTestResult count: {tests.count()}")
        
        for test in tests[:3]:
            print(f"  - {test.id}: {test.overall_status} ({test.completeness_score:.1f}%)")
        
        print("✅ Simple test completed successfully!")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    simple_test()