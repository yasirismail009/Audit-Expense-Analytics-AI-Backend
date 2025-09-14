#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def run_and_save_test():
    file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
    
    print(f'🚀 Running completeness test for file: {file_id}')
    print('=' * 60)
    
    try:
        # Import the task function
        from core.tasks import run_gl_completeness_analysis
        
        # Run the completeness test (this will save results to database)
        result = run_gl_completeness_analysis(file_id)
        
        print('✅ COMPLETENESS TEST COMPLETED AND SAVED!')
        print('=' * 60)
        
        if result and result.get('success', True):
            print(f"📊 Overall Status: {result.get('overall_status', 'Unknown')}")
            print(f"📈 Completeness Score: {result.get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Duration: {result.get('processing_duration', 0):.2f} seconds")
            print(f"🧪 Tests Passed: {result.get('tests_passed', 0)}/{result.get('total_tests', 4)}")
            
            # Show step results
            print("\n📋 Step Results:")
            steps = [
                ("Step 1: GL-TB Reconciliation", result.get('step1_passed', False)),
                ("Step 2: Debit-Credit Balance", result.get('step2_passed', False)),
                ("Step 3: Account Coverage", result.get('step3_passed', False)),
                ("Step 4: Transaction Gap Detection", result.get('step4_passed', False))
            ]
            
            for step_name, passed in steps:
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"  {step_name}: {status}")
            
            # Check if saved to database
            from core.models import CompletenessTestResult, DataFile
            data_file = DataFile.objects.get(id=file_id)
            test_results = CompletenessTestResult.objects.filter(data_file=data_file).order_by('-test_timestamp')
            
            if test_results.exists():
                latest_test = test_results.first()
                print(f"\n💾 Results saved to database:")
                print(f"  Test ID: {latest_test.id}")
                print(f"  Timestamp: {latest_test.test_timestamp}")
                print(f"  Status: {latest_test.overall_status}")
                print(f"  Score: {latest_test.completeness_score:.1f}%")
                
                # Show anomalies count
                critical_issues_count = latest_test.critical_issues_count
                if critical_issues_count > 0:
                    print(f"  Critical Issues: {critical_issues_count} detected")
                
                # Show comprehensive statistics
                stats = latest_test.comprehensive_statistics
                if stats:
                    doc_stats = stats.get('document_statistics', {})
                    user_count = len(stats.get('credit_debit_by_user', []))
                    subtype_count = len(stats.get('credit_debit_by_subtype', []))
                    print(f"  Data Coverage: {user_count} users, {subtype_count} subtypes")
                    print(f"  Documents: {doc_stats.get('total_documents', 0):,} total")
            else:
                print("\n⚠️  No test results found in database (may have failed to save)")
        
        else:
            error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
            print(f"❌ Test Failed: {error_msg}")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_and_save_test()
