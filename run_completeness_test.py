#!/usr/bin/env python

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def run_test():
    print('🚀 Starting AI-Enhanced Completeness Test')
    print('=' * 60)
    
    try:
        # Import after Django setup
        from core.tasks import run_gl_completeness_analysis
        import core.tasks as tasks_module
        
        file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
        
        # Temporarily disable AI prediction to test core functionality
        original_predict = getattr(tasks_module, 'predict_completeness_with_ai', None)
        if original_predict:
            class MockDelay:
                def delay(self, *args, **kwargs):
                    class MockResult:
                        def get(self, timeout=None):
                            return {'success': False, 'error': 'Skipped for direct test'}
                    return MockResult()
            tasks_module.predict_completeness_with_ai = MockDelay()
        
        # Run the completeness test directly (synchronous)
        print("🔄 Running completeness test directly (synchronous mode)...")
        result = run_gl_completeness_analysis(file_id)
        
        # Restore original function
        if original_predict:
            tasks_module.predict_completeness_with_ai = original_predict
        
        print('✅ COMPLETENESS TEST COMPLETED!')
        print('=' * 60)
        
        if result and result.get('success', True):
            print(f"📊 Overall Status: {result.get('overall_status', 'Unknown')}")
            print(f"📈 Completeness Score: {result.get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Duration: {result.get('processing_duration', 0):.2f} seconds")
            print(f"🧪 Tests Passed: {result.get('tests_passed', 0)}/{result.get('total_tests', 4)}")
            
            # Show step results
            print("\n📋 Individual Steps:")
            steps = [
                ("Step 1: GL-TB Reconciliation", result.get('step1_passed', False)),
                ("Step 2: Debit-Credit Balance", result.get('step2_passed', False)),
                ("Step 3: Account Coverage", result.get('step3_passed', False)),
                ("Step 4: Transaction Gap Detection", result.get('step4_passed', False))
            ]
            
            for step_name, passed in steps:
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"  {step_name}: {status}")
            
            # Show critical issues
            critical_issues = result.get('critical_issues_count', 0)
            if critical_issues > 0:
                print(f"\n⚠️  Critical Issues Detected: {critical_issues}")
            
            # Show comprehensive statistics
            stats = result.get('comprehensive_statistics', {})
            if stats:
                print(f"\n📊 Data Statistics:")
                doc_stats = stats.get('document_statistics', {})
                if doc_stats:
                    print(f"  📄 Documents: {doc_stats.get('total_documents', 0):,} total, {doc_stats.get('unique_documents', 0):,} unique")
                    dup_ratio = doc_stats.get('duplicate_document_ratio', 0)
                    print(f"  📄 Duplicate Ratio: {dup_ratio:.2f}%")
                
                user_data = stats.get('credit_debit_by_user', [])
                subtype_data = stats.get('credit_debit_by_subtype', [])
                monthly_data = stats.get('monthly_trends', [])
                
                print(f"  👥 Users: {len(user_data)} unique users")
                print(f"  📁 Account Subtypes: {len(subtype_data)} different subtypes")
                print(f"  📅 Monthly Coverage: {len(monthly_data)} months")
                
                # Show top users by volume
                if user_data:
                    print(f"\n👑 Top 3 Users by Volume:")
                    for i, user in enumerate(user_data[:3], 1):
                        volume = user.get('total_volume', 0)
                        name = user.get('user_name', 'Unknown')
                        print(f"  {i}. {name}: ${volume:,.0f}")
        
        else:
            error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
            print(f"❌ Test Failed: {error_msg}")
        
    except Exception as e:
        print(f"💥 Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_test()
