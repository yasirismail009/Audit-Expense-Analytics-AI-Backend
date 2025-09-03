#!/usr/bin/env python3
"""
Test script to verify holiday analysis fixes
"""

import os
import sys
import django
from datetime import date, datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob, DataFile, SAPGLPosting, HolidayAnalysisResult
from core.tasks import run_holiday_analysis
from core.holiday_utils import get_holidays, is_holiday
from core.specialized_analysis_models import HolidayAnalysisModel

def test_holiday_utils():
    """Test holiday utilities functionality"""
    print("🔍 Testing Holiday Utilities...")
    
    try:
        # Test holiday retrieval
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        holidays = get_holidays('saudiarabian', start_date, end_date)
        print(f"✅ Retrieved {len(holidays)} Saudi Arabian holidays for 2024")
        
        # Test specific date check
        test_date = date(2024, 9, 23)  # Saudi National Day
        is_holiday_result = is_holiday('saudiarabian', test_date)
        print(f"✅ Holiday check for {test_date}: {is_holiday_result}")
        
        return True
    except Exception as e:
        print(f"❌ Holiday utilities test failed: {e}")
        return False

def test_ml_model():
    """Test ML model functionality"""
    print("🔍 Testing ML Model...")
    
    try:
        model = HolidayAnalysisModel()
        print(f"✅ ML model loaded, trained: {model.is_trained}")
        
        # Test with sample transaction
        from core.models import SAPGLPosting
        sample_transaction = SAPGLPosting(
            posting_date=date(2024, 9, 23),  # Saudi National Day
            amount_local_currency=100000,
            user_name="TEST_USER",
            gl_account="1000"
        )
        
        predictions = model.predict([sample_transaction])
        print(f"✅ ML model predictions: {len(predictions)} results")
        
        return True
    except Exception as e:
        print(f"❌ ML model test failed: {e}")
        return False

def test_holiday_analysis_task():
    """Test holiday analysis task"""
    print("🔍 Testing Holiday Analysis Task...")
    
    try:
        # Create a test data file
        data_file = DataFile.objects.create(
            file_name="test_holiday_analysis.csv",
            fiscal_year=2024,
            audit_start_date=date(2024, 1, 1),
            audit_end_date=date(2024, 12, 31)
        )
        
        # Create test transactions
        test_dates = [
            date(2024, 9, 23),  # Saudi National Day
            date(2024, 12, 25),  # Christmas (not Saudi holiday)
            date(2024, 6, 15),   # Regular day
        ]
        
        for i, test_date in enumerate(test_dates):
            SAPGLPosting.objects.create(
                data_file=data_file,
                posting_date=test_date,
                amount_local_currency=100000 + (i * 50000),
                user_name=f"USER_{i}",
                gl_account=f"100{i}",
                document_number=f"DOC_{i}"
            )
        
        # Create processing job
        job = FileProcessingJob.objects.create(
            data_file=data_file,
            job_type='holiday_analysis',
            status='PENDING'
        )
        
        print(f"✅ Created test data: {data_file.file_name} with {len(test_dates)} transactions")
        
        # Run holiday analysis
        result = run_holiday_analysis(job.id)
        
        if result.get('success'):
            print(f"✅ Holiday analysis completed successfully")
            print(f"   - Holiday postings: {result.get('holiday_postings_count', 0)}")
            print(f"   - Processing duration: {result.get('processing_duration', 0):.2f}s")
            
            # Check database results
            holiday_result = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
            if holiday_result:
                print(f"✅ Results saved to database")
                print(f"   - Analysis ID: {holiday_result.id}")
                print(f"   - Status: {holiday_result.status}")
                print(f"   - ML method: {holiday_result.analysis_summary.get('ml_detection_method', 'unknown')}")
            else:
                print("❌ No results found in database")
                return False
        else:
            print(f"❌ Holiday analysis failed: {result.get('error', 'Unknown error')}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Holiday analysis task test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def cleanup_test_data():
    """Clean up test data"""
    print("🧹 Cleaning up test data...")
    
    try:
        # Remove test data files and related data
        test_files = DataFile.objects.filter(file_name__startswith="test_holiday_analysis")
        for data_file in test_files:
            # Remove related records
            SAPGLPosting.objects.filter(data_file=data_file).delete()
            FileProcessingJob.objects.filter(data_file=data_file).delete()
            HolidayAnalysisResult.objects.filter(data_file=data_file).delete()
            data_file.delete()
        
        print(f"✅ Cleaned up {test_files.count()} test files")
        return True
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 Starting Holiday Analysis Fix Tests")
    print("=" * 50)
    
    tests = [
        ("Holiday Utilities", test_holiday_utils),
        ("ML Model", test_ml_model),
        ("Holiday Analysis Task", test_holiday_analysis_task),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n📋 Running {test_name} Test...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Cleanup
    cleanup_test_data()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Holiday analysis is working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
