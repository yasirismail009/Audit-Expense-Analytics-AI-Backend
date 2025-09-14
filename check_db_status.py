#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def check_database_status():
    print('🔍 Checking Database Status After Completeness Test')
    print('=' * 60)
    
    try:
        from core.models import DataFile, CompletenessTestResult
        
        # Check DataFile count
        data_file_count = DataFile.objects.count()
        print(f'📁 DataFile count: {data_file_count}')
        
        # Check for the specific data file used in tests
        test_file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
        try:
            test_data_file = DataFile.objects.get(id=test_file_id)
            print(f'✅ Test file found: {test_data_file.file_name}')
            print(f'   Status: {test_data_file.status}')
            print(f'   Records: {test_data_file.total_records}')
        except DataFile.DoesNotExist:
            print(f'❌ Test file with ID {test_file_id} not found.')
            # Show available files
            print('\n📋 Available DataFiles:')
            for f in DataFile.objects.all()[:5]:
                print(f'  {f.id} - {f.file_name} ({f.status})')
        
        # Check CompletenessTestResult count
        completeness_test_count = CompletenessTestResult.objects.count()
        print(f'\n🧪 CompletenessTestResult count: {completeness_test_count}')
        
        if completeness_test_count > 0:
            print('\n📊 CompletenessTestResults:')
            for test in CompletenessTestResult.objects.all().order_by('-test_timestamp'):
                print(f'  ID: {test.id}')
                print(f'  Status: {test.overall_status}')
                print(f'  Score: {test.completeness_score:.1f}%')
                print(f'  File: {test.data_file.file_name if test.data_file else "No file"}')
                print(f'  Timestamp: {test.test_timestamp}')
                print(f'  Has Stats: {"Yes" if test.comprehensive_statistics else "No"}')
                if test.comprehensive_statistics:
                    stats = test.comprehensive_statistics
                    print(f'  Stats Keys: {list(stats.keys())}')
                print('  ---')
        else:
            print('❌ No CompletenessTestResult records found!')
            
        # Check if there are any recent tests
        from django.utils import timezone
        from datetime import timedelta
        recent_tests = CompletenessTestResult.objects.filter(
            test_timestamp__gte=timezone.now() - timedelta(hours=1)
        ).count()
        print(f'\n⏰ Recent tests (last hour): {recent_tests}')
        
    except Exception as e:
        print(f'💥 Error checking database: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    check_database_status()
