#!/usr/bin/env python

import os
import django
import logging

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

def test_eng008_no_ai():
    """Test ENG-008 without AI prediction - focus on core functionality"""
    
    print("🐳 ENG-008 Core Completeness Test (No AI)")
    print("=" * 55)
    
    try:
        from core.models import DataFile, Engagement, SAPGLPosting, TrialBalance, GLAccount
        
        # Get engagement and file info
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        gl_file = DataFile.objects.filter(engagement=engagement, file_type='GL').first()
        
        print(f"✅ Engagement: {engagement.engagement_id}")
        print(f"📊 GL File: {gl_file.file_name}")
        print(f"📊 File ID: {gl_file.id}")
        
        # Check data availability
        gl_count = SAPGLPosting.objects.filter(data_file=gl_file).count()
        tb_count = TrialBalance.objects.filter(data_file__engagement=engagement).count()
        coa_count = GLAccount.objects.filter(engagement=engagement).count()
        
        print(f"\n📈 Data Summary:")
        print(f"  GL Records: {gl_count:,}")
        print(f"  TB Records: {tb_count:,}")
        print(f"  COA Records: {coa_count:,}")
        
        # Create a direct call version (bypassing AI prediction)
        print(f"\n🚀 Running direct completeness analysis...")
        
        # Import and call the task function directly
        from core.tasks import run_gl_completeness_analysis
        
        # Call the task synchronously (not through Celery queue)
        print("📝 Calling run_gl_completeness_analysis directly...")
        result = run_gl_completeness_analysis(str(gl_file.id))
        
        print(f"\n✅ Analysis completed!")
        print(f"Success: {result.get('success', False)}")
        
        if result.get('success'):
            cr = result.get('completeness_results', {})
            print(f"Status: {cr.get('status', 'Unknown')}")
            print(f"Score: {cr.get('completeness_score', 0):.1f}%")
            
            summary = cr.get('summary', {})
            print(f"Tests Passed: {summary.get('tests_passed', 0)}/{summary.get('total_tests', 7)}")
            
            # Show step results
            print(f"\nStep Results:")
            for i in range(1, 8):
                step_key = f"step{i}_file_completeness" if i == 1 else f"step{i}_{'gl_tb_reconciliation' if i == 2 else 'debit_credit_balance' if i == 3 else 'account_coverage' if i == 4 else 'coa_hierarchy_validation' if i == 5 else 'account_linking' if i == 6 else 'transaction_gaps'}"
                step_data = cr.get(step_key, {})
                passed = step_data.get('passed', False)
                print(f"  Step {i}: {'✅ PASS' if passed else '❌ FAIL'}")
                
        else:
            print(f"❌ Error: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Test Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_eng008_no_ai()
