#!/usr/bin/env python

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.tasks import run_gl_completeness_analysis

def run_test():
    file_id = 'cd8a5ad7-cc80-4969-a6df-3e919f30147a'
    print(f"🚀 Starting AI-Enhanced Completeness Test for file: {file_id}")
    print("=" * 70)
    
    try:
        # Run the completeness analysis
        result = run_gl_completeness_analysis(file_id)
        
        print("\n" + "=" * 70)
        print("✅ COMPLETENESS TEST COMPLETED!")
        print("=" * 70)
        
        if result.get('success'):
            print(f"📊 Overall Status: {result.get('overall_status', 'Unknown')}")
            print(f"📈 Completeness Score: {result.get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Time: {result.get('processing_duration', 0):.2f} seconds")
            print(f"🧪 Tests Passed: {result.get('tests_passed', 0)}/{result.get('total_tests', 0)}")
            
            if result.get('ai_prediction_used'):
                print(f"🤖 AI Prediction Accuracy: {result.get('ai_accuracy', 'N/A')}")
            
            # Show step results
            print("\n📋 Step-by-Step Results:")
            for i in range(1, 5):
                step_key = f"step{i}_passed"
                step_name = {
                    1: "GL-TB Reconciliation",
                    2: "Debit-Credit Balance", 
                    3: "Account Coverage",
                    4: "Transaction Gap Detection"
                }[i]
                status = "✅ PASS" if result.get(step_key, False) else "❌ FAIL"
                print(f"  Step {i} ({step_name}): {status}")
            
        else:
            print(f"❌ Test failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"💥 Error running completeness test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_test()
