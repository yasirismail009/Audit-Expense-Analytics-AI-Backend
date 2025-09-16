#!/usr/bin/env python

import os
import django
import sys

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_eng008_completeness():
    """Test completeness analysis for engagement Eng-008 in Docker environment"""
    
    print("🐳 Docker Completeness Test for Engagement Eng-008")
    print("=" * 70)
    
    try:
        from core.models import DataFile, Engagement, CompletenessTestResult
        from core.tasks import run_gl_completeness_analysis
        
        # Find ENG-008 engagement
        try:
            engagement = Engagement.objects.get(engagement_id='ENG-008')
            print(f"✅ Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
            print(f"   Client: {engagement.client.client_name}")
            print(f"   Fiscal Year: {engagement.fiscal_year}")
        except Engagement.DoesNotExist:
            print("❌ Engagement ENG-008 not found in database")
            
            # List available engagements
            engagements = Engagement.objects.all()[:10]
            if engagements:
                print("\n📋 Available engagements:")
                for eng in engagements:
                    print(f"   - {eng.engagement_id}: {eng.engagement_name} ({eng.client.client_name})")
            else:
                print("   No engagements found in database")
            return
        
        # Find data files for this engagement
        data_files = DataFile.objects.filter(engagement=engagement)
        print(f"\n📁 Data files for ENG-008: {data_files.count()}")
        
        if not data_files.exists():
            print("❌ No data files found for ENG-008")
            return
        
        # Look for GL file specifically
        gl_files = data_files.filter(file_type='GL')
        if gl_files.exists():
            gl_file = gl_files.first()
            print(f"   📊 GL File: {gl_file.file_name} (ID: {gl_file.id})")
            print(f"   📈 Status: {gl_file.status}")
            print(f"   📋 Records: {gl_file.total_records}")
        else:
            print("❌ No GL file found for ENG-008")
            print("   Available file types:")
            for df in data_files:
                print(f"   - {df.file_type}: {df.file_name}")
            return
        
        # Check for existing completeness tests
        existing_tests = CompletenessTestResult.objects.filter(engagement=engagement)
        print(f"\n🧪 Existing completeness tests: {existing_tests.count()}")
        
        if existing_tests.exists():
            latest_test = existing_tests.order_by('-test_timestamp').first()
            print(f"   Latest: {latest_test.overall_status} ({latest_test.completeness_score:.1f}%)")
            print(f"   Date: {latest_test.test_timestamp}")
        
        # Run the completeness test
        print(f"\n🚀 Running completeness analysis for GL file: {gl_file.id}")
        print("=" * 70)
        
        result = run_gl_completeness_analysis(str(gl_file.id))
        
        print("\n" + "=" * 70)
        print("✅ DOCKER COMPLETENESS TEST COMPLETED!")
        print("=" * 70)
        
        if result.get('success'):
            completeness_results = result.get('completeness_results', {})
            
            print(f"📊 Overall Status: {completeness_results.get('status', 'Unknown')}")
            print(f"📈 Completeness Score: {completeness_results.get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Time: {completeness_results.get('processing_duration', 0):.2f} seconds")
            
            summary = completeness_results.get('summary', {})
            print(f"🧪 Tests Passed: {summary.get('tests_passed', 0)}/{summary.get('total_tests', 7)}")
            print(f"📈 Success Rate: {summary.get('success_rate', 0):.1f}%")
            
            # Show all 7 step results
            print("\n📋 Completeness Test Results:")
            steps_info = [
                ("Step 1: File Completeness", completeness_results.get('step1_file_completeness', {})),
                ("Step 2: GL-TB Reconciliation", completeness_results.get('step2_gl_tb_reconciliation', {})),
                ("Step 3: Debit-Credit Balance", completeness_results.get('step3_debit_credit_balance', {})),
                ("Step 4: Account Coverage", completeness_results.get('step4_account_coverage', {})),
                ("Step 5: COA Hierarchy Validation", completeness_results.get('step5_coa_hierarchy_validation', {})),
                ("Step 6: Account Linking", completeness_results.get('step6_account_linking', {})),
                ("Step 7: Transaction Gap Detection", completeness_results.get('step7_transaction_gaps', {}))
            ]
            
            for step_name, step_data in steps_info:
                passed = step_data.get('passed', False)
                status = "✅ PASS" if passed else "❌ FAIL"
                print(f"  {step_name}: {status}")
                
                if step_data.get('explanation'):
                    print(f"    {step_data['explanation']}")
                print()
            
            # Database verification
            print("💾 Database Verification:")
            test_id = result.get('completeness_test_id')
            if test_id:
                print(f"  ✅ Results saved with ID: {test_id}")
                
                try:
                    saved_test = CompletenessTestResult.objects.get(id=test_id)
                    print(f"  📊 DB Status: {saved_test.overall_status} ({saved_test.completeness_score:.1f}%)")
                    print(f"  📈 DB Tests: {saved_test.tests_passed}/{saved_test.total_tests}")
                    
                    # Verify all steps are saved
                    step_fields = [
                        'step1_file_completeness', 'step2_gl_tb_reconciliation', 
                        'step3_debit_credit_balance', 'step4_account_coverage',
                        'step5_coa_hierarchy_validation', 'step6_account_linking', 
                        'step7_transaction_gaps'
                    ]
                    
                    populated_steps = sum(1 for field in step_fields if getattr(saved_test, field, {}))
                    print(f"  🔍 Step Data: {populated_steps}/7 steps populated")
                    
                    if populated_steps == 7:
                        print("  ✅ All 7 steps successfully saved to database!")
                    else:
                        print(f"  ⚠️  Only {populated_steps} steps saved - possible issue")
                        
                except Exception as e:
                    print(f"  ❌ Error verifying database: {e}")
            else:
                print("  ⚠️ No test ID returned")
                
        else:
            print(f"❌ Test failed: {result.get('error', 'Unknown error')}")
            
        print(f"\n🎯 Docker test completed for ENG-008")
            
    except Exception as e:
        print(f"💥 Error running Docker test: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    test_eng008_completeness()
