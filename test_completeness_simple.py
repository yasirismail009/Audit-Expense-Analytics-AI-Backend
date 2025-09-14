#!/usr/bin/env python

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.tasks import run_gl_completeness_analysis

def run_simple_test():
    file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
    print(f"🚀 Starting Completeness Test for file: {file_id}")
    print("=" * 70)
    
    try:
        # Import and patch the AI prediction to skip it
        import core.tasks
        original_predict = getattr(core.tasks, 'predict_completeness_with_ai', None)
        
        # Temporarily disable AI prediction
        def mock_predict(*args, **kwargs):
            class MockTask:
                def delay(self, *args, **kwargs):
                    return self
                def get(self, timeout=None):
                    return {'success': False, 'error': 'AI prediction disabled for testing'}
            return MockTask()
        
        if original_predict:
            core.tasks.predict_completeness_with_ai = mock_predict
        
        # Run the completeness analysis
        result = run_gl_completeness_analysis(file_id)
        
        # Restore original function
        if original_predict:
            core.tasks.predict_completeness_with_ai = original_predict
        
        print("\n" + "=" * 70)
        print("✅ COMPLETENESS TEST COMPLETED!")
        print("=" * 70)
        
        if result.get('success'):
            print(f"📊 Overall Status: {result.get('overall_status', 'Unknown')}")
            print(f"📈 Completeness Score: {result.get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Time: {result.get('processing_duration', 0):.2f} seconds")
            print(f"🧪 Tests Passed: {result.get('tests_passed', 0)}/{result.get('total_tests', 0)}")
            
            # Show step results
            print("\n📋 Step-by-Step Results:")
            steps = [
                ("GL-TB Reconciliation", result.get('step1_passed', False)),
                ("Debit-Credit Balance", result.get('step2_passed', False)), 
                ("Account Coverage", result.get('step3_passed', False)),
                ("Transaction Gap Detection", result.get('step4_passed', False))
            ]
            
            for i, (step_name, passed) in enumerate(steps, 1):
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"  Step {i} ({step_name}): {status}")
            
            # Show anomalies if any
            critical_issues = result.get('critical_issues_count', 0)
            if critical_issues > 0:
                print(f"\n⚠️  Critical Issues Detected: {critical_issues}")
            
            # Show statistics summary
            stats = result.get('comprehensive_statistics', {})
            if stats:
                print(f"\n📊 Data Summary:")
                doc_stats = stats.get('document_statistics', {})
                print(f"  📄 Documents: {doc_stats.get('total_documents', 0)} total, {doc_stats.get('unique_documents', 0)} unique")
                
                users = len(stats.get('credit_debit_by_user', []))
                subtypes = len(stats.get('credit_debit_by_subtype', []))
                print(f"  👥 Coverage: {users} users, {subtypes} account subtypes")
            
        else:
            print(f"❌ Test failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"💥 Error running completeness test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_simple_test()
