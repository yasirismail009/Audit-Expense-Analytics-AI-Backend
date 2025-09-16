#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_eng008_simple():
    """Simple test for ENG-008 without AI prediction"""
    
    print("🐳 Simple Docker Test for ENG-008")
    print("=" * 50)
    
    try:
        from core.models import DataFile, Engagement, CompletenessTestResult, SAPGLPosting, TrialBalance
        
        # Find ENG-008
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Engagement: {engagement.engagement_id}")
        
        # Get GL file
        gl_file = DataFile.objects.filter(engagement=engagement, file_type='GL').first()
        print(f"📊 GL File: {gl_file.file_name}")
        
        # Check data counts
        gl_count = SAPGLPosting.objects.filter(data_file=gl_file).count()
        tb_count = TrialBalance.objects.filter(data_file__engagement_id=engagement.engagement_id).count()
        
        print(f"📈 GL Records: {gl_count:,}")
        print(f"📈 TB Records: {tb_count:,}")
        
        if gl_count == 0:
            print("❌ No GL data found")
            return
        
        if tb_count == 0:
            print("❌ No TB data found")
            return
        
        # Run completeness test directly (bypassing Celery)
        print("\n🚀 Running completeness test...")
        
        from core.tasks import run_gl_completeness_analysis
        
        # Direct call (not through Celery)
        result = run_gl_completeness_analysis(str(gl_file.id))
        
        print("\n✅ Test completed!")
        print(f"Success: {result.get('success', False)}")
        
        if result.get('success'):
            completeness_results = result.get('completeness_results', {})
            print(f"Status: {completeness_results.get('status')}")
            print(f"Score: {completeness_results.get('completeness_score')}%")
            
            summary = completeness_results.get('summary', {})
            print(f"Tests: {summary.get('tests_passed')}/{summary.get('total_tests')}")
        else:
            print(f"Error: {result.get('error')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_eng008_simple()
