#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_direct():
    print('🧪 Direct Completeness Test')
    print('=' * 40)
    
    try:
        from core.models import DataFile
        from core.tasks import run_gl_completeness_analysis
        
        # Get first available file
        file = DataFile.objects.first()
        if not file:
            print('❌ No data files found in database')
            return
            
        print(f'📁 Using file: {file.id} - {file.file_name}')
        
        # Run the completeness analysis directly
        print('🔄 Running completeness analysis...')
        result = run_gl_completeness_analysis(file.id)
        
        print('✅ Analysis completed!')
        print(f'📊 Result: {result}')
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_direct()
