#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_celery_queue():
    print('🧪 Testing Celery Queue Functionality')
    print('=' * 50)
    
    try:
        from core.tasks import debug_task, run_gl_completeness_analysis
        
        # Test 1: Simple debug task
        print('📤 Testing debug task...')
        debug_result = debug_task.delay()
        print(f'✅ Debug task queued with ID: {debug_result.id}')
        
        # Wait for debug task
        print('⏳ Waiting for debug task completion...')
        debug_output = debug_result.get(timeout=30)
        print(f'✅ Debug task completed: {debug_output}')
        
        # Test 2: Completeness analysis task
        print('\n📤 Testing completeness analysis task...')
        file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
        completeness_result = run_gl_completeness_analysis.delay(file_id)
        print(f'✅ Completeness task queued with ID: {completeness_result.id}')
        
        # Wait for completeness task (with longer timeout)
        print('⏳ Waiting for completeness task completion...')
        completeness_output = completeness_result.get(timeout=120)
        print(f'✅ Completeness task completed: {completeness_output}')
        
        print('\n🎉 All tests passed! Celery queue is working properly.')
        
    except Exception as e:
        print(f'❌ Test failed: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_celery_queue()
