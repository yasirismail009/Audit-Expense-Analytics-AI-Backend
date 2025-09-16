#!/usr/bin/env python

import os
import django
import logging

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

# Configure logging to see debug output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

def test_eng008_with_debug():
    """Test ENG-008 with enhanced debugging"""
    
    print("🐳 Enhanced Debug Test for ENG-008")
    print("=" * 60)
    
    try:
        from core.models import DataFile, Engagement
        from core.tasks import run_gl_completeness_analysis
        
        # Find ENG-008
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Engagement: {engagement.engagement_id}")
        
        # Get GL file
        gl_file = DataFile.objects.filter(engagement=engagement, file_type='GL').first()
        print(f"📊 GL File: {gl_file.file_name}")
        print(f"📊 GL File ID: {gl_file.id}")
        
        print("\n🚀 Starting completeness analysis with enhanced debugging...")
        print("=" * 60)
        
        # Run the completeness test with debugging
        result = run_gl_completeness_analysis(str(gl_file.id))
        
        print("\n" + "=" * 60)
        print("✅ DEBUG TEST COMPLETED!")
        print("=" * 60)
        
        if result.get('success'):
            print(f"✅ Success: {result.get('success')}")
            if 'completeness_results' in result:
                cr = result['completeness_results']
                print(f"📊 Status: {cr.get('status')}")
                print(f"📈 Score: {cr.get('completeness_score')}%")
                print(f"🧪 Tests: {cr.get('summary', {}).get('tests_passed')}/{cr.get('summary', {}).get('total_tests')}")
        else:
            print(f"❌ Error: {result.get('error')}")
        
    except Exception as e:
        print(f"❌ Test Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_eng008_with_debug()
