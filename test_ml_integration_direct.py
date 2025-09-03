#!/usr/bin/env python3
"""
Direct test of ML model integration during analysis process
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

import django
django.setup()

from core.models import FileProcessingJob, DataFile, SAPGLPosting
from core.sync_analysis import run_duplicate_analysis_sync, run_user_analysis_sync
from datetime import datetime, date

def test_ml_integration_direct():
    """Test ML model integration directly during analysis"""
    print("🔍 Testing ML Model Integration During Analysis (Direct)")
    print("=" * 70)
    
    try:
        # Check if we have any data files
        data_files = DataFile.objects.all()
        if not data_files:
            print("⚠️  No data files found. Creating a test file...")
            
            # Create a test data file
            test_file = DataFile.objects.create(
                file_name="test_ml_integration.csv",
                file_type="CSV",
                fiscal_year=2024,
                audit_start_date=date(2024, 1, 1),
                audit_end_date=date(2024, 12, 31),
                status="PROCESSED"
            )
            print(f"✅ Created test file: {test_file.id}")
        else:
            test_file = data_files[0]
            print(f"✅ Using existing file: {test_file.file_name} (ID: {test_file.id})")
        
        # Check if we have transactions
        transactions = SAPGLPosting.objects.filter(data_file=test_file)
        if not transactions:
            print("⚠️  No transactions found. Creating test transactions...")
            
            # Create some test transactions
            test_transactions = [
                {
                    'document_number': 'TEST001',
                    'posting_date': date(2024, 1, 15),
                    'document_date': date(2024, 1, 15),
                    'gl_account': '1000',
                    'user_name': 'TestUser1',
                    'amount_local_currency': 1000.00,
                    'transaction_type': 'DEBIT'
                },
                {
                    'document_number': 'TEST002',
                    'posting_date': date(2024, 1, 15),
                    'document_date': date(2024, 1, 15),
                    'gl_account': '1000',
                    'user_name': 'TestUser1',
                    'amount_local_currency': 1000.00,
                    'transaction_type': 'DEBIT'
                },
                {
                    'document_number': 'TEST003',
                    'posting_date': date(2024, 1, 16),
                    'document_date': date(2024, 1, 16),
                    'gl_account': '2000',
                    'user_name': 'TestUser2',
                    'amount_local_currency': 5000.00,
                    'transaction_type': 'CREDIT'
                }
            ]
            
            for trans_data in test_transactions:
                SAPGLPosting.objects.create(
                    data_file=test_file,
                    **trans_data
                )
            
            print(f"✅ Created {len(test_transactions)} test transactions")
            transactions = SAPGLPosting.objects.filter(data_file=test_file)
        else:
            print(f"✅ Found {len(transactions)} existing transactions")
        
        # Create a processing job
        job = FileProcessingJob.objects.create(
            data_file=test_file,
            status="IN_PROGRESS",
            started_at=datetime.now()
        )
        print(f"✅ Created processing job: {job.id}")
        
        # Test 1: Duplicate Analysis with ML Integration
        print("\n🔍 Testing Duplicate Analysis ML Integration...")
        try:
            duplicate_result = run_duplicate_analysis_sync(job.id)
            
            if 'error' not in duplicate_result:
                print("   ✅ Duplicate analysis completed successfully")
                print(f"      📊 Analysis ID: {duplicate_result.get('analysis_id')}")
                print(f"      📊 Status: {duplicate_result.get('status')}")
                print(f"      📊 Duration: {duplicate_result.get('processing_duration', 0):.2f}s")
                
                # Check if ML features were generated
                from core.models import DuplicateAnalysisResult
                duplicate_analysis = DuplicateAnalysisResult.objects.get(id=duplicate_result['analysis_id'])
                
                # Check for ML insights in the breakdowns
                if duplicate_analysis.breakdowns and 'ml_insights' in duplicate_analysis.breakdowns:
                    ml_insights = duplicate_analysis.breakdowns['ml_insights']
                    print(f"   🎉 ML Integration Working in Duplicate Analysis!")
                    print(f"      📊 Detection Method: {ml_insights.get('detection_method', 'N/A')}")
                    print(f"      📊 ML Enhanced Duplicates: {ml_insights.get('ml_enhanced_duplicates', 0)}")
                    print(f"      📊 Rule-based Duplicates: {ml_insights.get('rule_based_duplicates', 0)}")
                    duplicate_ml_working = True
                else:
                    print("   ⚠️  No ML insights found in duplicate analysis")
                    duplicate_ml_working = False
                    
            else:
                print(f"   ❌ Duplicate analysis failed: {duplicate_result['error']}")
                duplicate_ml_working = False
                
        except Exception as e:
            print(f"   ❌ Error in duplicate analysis: {e}")
            duplicate_ml_working = False
        
        # Test 2: User Analysis with ML Integration
        print("\n🔍 Testing User Analysis ML Integration...")
        try:
            user_result = run_user_analysis_sync(job.id)
            
            if 'error' not in user_result:
                print("   ✅ User analysis completed successfully")
                print(f"      📊 Analysis ID: {user_result.get('analysis_id')}")
                print(f"      📊 Status: {user_result.get('status')}")
                print(f"      📊 Duration: {user_result.get('processing_duration', 0):.2f}s")
                
                # Check if ML features were generated
                from core.models import UserAnalysisResult
                user_analysis = UserAnalysisResult.objects.get(id=user_result['analysis_id'])
                
                # Check for ML insights in the breakdowns
                if user_analysis.breakdowns and 'ml_insights' in user_analysis.breakdowns:
                    ml_insights = user_analysis.breakdowns['ml_insights']
                    print(f"   🎉 ML Integration Working in User Analysis!")
                    print(f"      📊 Detection Method: {ml_insights.get('detection_method', 'N/A')}")
                    print(f"      📊 ML Detected Anomalies: {len(ml_insights.get('ml_detected_anomalies', []))}")
                    print(f"      📊 Anomaly Severity Breakdown: {ml_insights.get('anomaly_severity_breakdown', {})}")
                    user_ml_working = True
                else:
                    print("   ⚠️  No ML insights found in user analysis")
                    user_ml_working = False
                    
            else:
                print(f"   ❌ User analysis failed: {user_result['error']}")
                user_ml_working = False
                
        except Exception as e:
            print(f"   ❌ Error in user analysis: {e}")
            user_ml_working = False
        
        # Test 3: Check if ML models are actually running
        print("\n🔍 Testing ML Model Execution During Analysis...")
        
        try:
            # Check if ML features are present in both analyses
            if duplicate_ml_working and user_ml_working:
                print("   ✅ ML models are executing during analysis!")
                print("   📊 Evidence:")
                print("      - Duplicate analysis has ML insights")
                print("      - User analysis has ML detected anomalies")
                print("      - ML features are being saved to database")
                ml_execution_working = True
            else:
                print("   ⚠️  ML models may not be executing during analysis")
                ml_execution_working = False
                
        except Exception as e:
            print(f"   ❌ Error checking ML execution: {e}")
            ml_execution_working = False
        
        # Summary
        print("\n" + "=" * 70)
        print("🎯 ML INTEGRATION TEST SUMMARY")
        print("=" * 70)
        
        print(f"✅ Duplicate Analysis ML Integration: {'PASSED' if duplicate_ml_working else 'FAILED'}")
        print(f"✅ User Analysis ML Integration: {'PASSED' if user_ml_working else 'FAILED'}")
        print(f"✅ ML Model Execution During Analysis: {'PASSED' if ml_execution_working else 'FAILED'}")
        
        overall_success = duplicate_ml_working and user_ml_working and ml_execution_working
        
        if overall_success:
            print("\n🎉 ML INTEGRATION WORKING EXCELLENTLY!")
            print("✅ ML models are running during analysis")
            print("✅ ML insights are being generated")
            print("✅ ML features are being saved to database")
            print("✅ End-to-end ML integration is operational")
        else:
            print("\n⚠️  PARTIAL ML INTEGRATION - Some areas need attention")
            print("Please review the failed tests above")
        
        return overall_success
        
    except Exception as e:
        print(f"❌ Error in ML integration test: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("🎯 ML MODEL INTEGRATION DURING ANALYSIS TEST (DIRECT)")
    print("=" * 70)
    
    success = test_ml_integration_direct()
    
    return success

if __name__ == "__main__":
    main()
