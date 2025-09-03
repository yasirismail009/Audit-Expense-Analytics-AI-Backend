#!/usr/bin/env python3
"""
Comprehensive test script for all analysis types and ML integrations
"""

import os
import sys
import django
from datetime import date, datetime, timedelta
import uuid

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    FileProcessingJob, DataFile, SAPGLPosting, 
    GeneralAnalysisResult, DuplicateAnalysisResult, BackdatedAnalysisResult,
    UserAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult,
    HolidayAnalysisResult, OverallAnalysisResult, RiskScoringDocument
)
from core.tasks import (
    run_general_analysis, run_duplicate_analysis, run_backdated_analysis,
    run_user_analysis, run_unusual_days_analysis, run_closing_entries_analysis,
    run_holiday_analysis, run_overall_analysis, run_risk_analysis
)
from core.specialized_analysis_models import (
    AnalysisModelManager, DuplicateAnalysisModel, BackdatedAnalysisModel,
    UserAnalysisModel, UnusualDaysAnalysisModel, ClosingEntriesAnalysisModel,
    HolidayAnalysisModel, RiskAnalysisModel
)
from core.holiday_utils import get_holidays, is_holiday

def create_test_data():
    """Create comprehensive test data for all analysis types"""
    print("🔧 Creating test data...")
    
    # Create test data file
    data_file = DataFile.objects.create(
        file_name="comprehensive_test_data.csv",
        fiscal_year=2024,
        audit_start_date=date(2024, 1, 1),
        audit_end_date=date(2024, 12, 31)
    )
    
    # Create test transactions with various scenarios
    test_transactions = []
    
    # 1. Regular transactions
    for i in range(10):
        test_transactions.append({
            'posting_date': date(2024, 6, 15 + i),
            'document_date': date(2024, 6, 15 + i),
            'amount_local_currency': 50000 + (i * 1000),
            'user_name': f'USER_{i % 3}',
            'gl_account': f'100{i % 5}',
            'document_number': f'DOC_{i:03d}',
            'document_type': 'INVOICE'
        })
    
    # 2. Duplicate transactions
    for i in range(3):
        # Create duplicate
        test_transactions.append({
            'posting_date': date(2024, 6, 20),
            'document_date': date(2024, 6, 20),
            'amount_local_currency': 75000,
            'user_name': 'USER_DUP',
            'gl_account': '1001',
            'document_number': f'DUP_{i}',
            'document_type': 'INVOICE'
        })
    
    # 3. Backdated transactions
    for i in range(3):
        test_transactions.append({
            'posting_date': date(2024, 6, 25),
            'document_date': date(2024, 6, 20 - i),  # Backdated
            'amount_local_currency': 100000 + (i * 5000),
            'user_name': f'USER_BACK_{i}',
            'gl_account': f'200{i}',
            'document_number': f'BACK_{i}',
            'document_type': 'MANUAL'
        })
    
    # 4. Weekend transactions
    for i in range(3):
        test_transactions.append({
            'posting_date': date(2024, 6, 22 + i),  # Weekend dates
            'document_date': date(2024, 6, 22 + i),
            'amount_local_currency': 25000 + (i * 5000),
            'user_name': 'USER_WEEKEND',
            'gl_account': '3001',
            'document_number': f'WEEKEND_{i}',
            'document_type': 'MANUAL'
        })
    
    # 5. Holiday transactions (Saudi National Day)
    test_transactions.append({
        'posting_date': date(2024, 9, 23),  # Saudi National Day
        'document_date': date(2024, 9, 23),
        'amount_local_currency': 150000,
        'user_name': 'USER_HOLIDAY',
        'gl_account': '4001',
        'document_number': 'HOLIDAY_001',
        'document_type': 'MANUAL'
    })
    
    # 6. Closing entries (month-end)
    for i in range(3):
        test_transactions.append({
            'posting_date': date(2024, 6, 30),  # Month-end
            'document_date': date(2024, 6, 30),
            'amount_local_currency': 50000 + (i * 10000),
            'user_name': 'USER_CLOSING',
            'gl_account': '5001',
            'document_number': f'CLOSING_{i}',
            'document_type': 'CLOSING'
        })
    
    # 7. High-volume user
    for i in range(15):
        test_transactions.append({
            'posting_date': date(2024, 6, 10 + i),
            'document_date': date(2024, 6, 10 + i),
            'amount_local_currency': 10000 + (i * 1000),
            'user_name': 'HIGH_VOLUME_USER',
            'gl_account': f'600{i % 3}',
            'document_number': f'HIGH_VOL_{i}',
            'document_type': 'INVOICE'
        })
    
    # Create all transactions
    for i, tx_data in enumerate(test_transactions):
        SAPGLPosting.objects.create(
            data_file=data_file,
            posting_date=tx_data['posting_date'],
            document_date=tx_data['document_date'],
            amount_local_currency=tx_data['amount_local_currency'],
            user_name=tx_data['user_name'],
            gl_account=tx_data['gl_account'],
            document_number=tx_data['document_number'],
            document_type=tx_data['document_type']
        )
    
    print(f"✅ Created {len(test_transactions)} test transactions")
    return data_file

def test_ml_models():
    """Test all ML models"""
    print("\n🔍 Testing ML Models...")
    
    results = {}
    
    try:
        # Test AnalysisModelManager
        manager = AnalysisModelManager()
        print("✅ AnalysisModelManager loaded")
        
        # Test each model type
        model_types = [
            'duplicate', 'backdated', 'user', 'unusual_days', 
            'closing_entries', 'holiday', 'risk'
        ]
        
        for model_type in model_types:
            try:
                model = manager.get_model(model_type)
                print(f"✅ {model_type.capitalize()} model loaded - Trained: {model.is_trained}")
                results[model_type] = True
            except Exception as e:
                print(f"❌ {model_type.capitalize()} model failed: {e}")
                results[model_type] = False
        
        # Test individual models
        models = {
            'DuplicateAnalysisModel': DuplicateAnalysisModel(),
            'BackdatedAnalysisModel': BackdatedAnalysisModel(),
            'UserAnalysisModel': UserAnalysisModel(),
            'UnusualDaysAnalysisModel': UnusualDaysAnalysisModel(),
            'ClosingEntriesAnalysisModel': ClosingEntriesAnalysisModel(),
            'HolidayAnalysisModel': HolidayAnalysisModel(),
            'RiskAnalysisModel': RiskAnalysisModel()
        }
        
        for name, model in models.items():
            try:
                print(f"✅ {name} loaded - Trained: {model.is_trained}")
                results[name] = True
            except Exception as e:
                print(f"❌ {name} failed: {e}")
                results[name] = False
        
        return results
        
    except Exception as e:
        print(f"❌ ML models test failed: {e}")
        return {}

def test_holiday_utilities():
    """Test holiday utilities"""
    print("\n🔍 Testing Holiday Utilities...")
    
    try:
        # Test holiday retrieval
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        holidays = get_holidays('saudiarabian', start_date, end_date)
        print(f"✅ Retrieved {len(holidays)} Saudi Arabian holidays for 2024")
        
        # Test specific holiday check
        test_date = date(2024, 9, 23)  # Saudi National Day
        is_holiday_result = is_holiday('saudiarabian', test_date)
        print(f"✅ Holiday check for {test_date}: {is_holiday_result}")
        
        return True
    except Exception as e:
        print(f"❌ Holiday utilities test failed: {e}")
        return False

def test_analysis_tasks(data_file):
    """Test all analysis tasks"""
    print("\n🔍 Testing Analysis Tasks...")
    
    results = {}
    
    # Create processing job
    job = FileProcessingJob.objects.create(
        data_file=data_file,
        job_type='comprehensive_test',
        status='PENDING'
    )
    
    # Test each analysis task
    tasks = [
        ('General Analysis', run_general_analysis),
        ('Duplicate Analysis', run_duplicate_analysis),
        ('Backdated Analysis', run_backdated_analysis),
        ('User Analysis', run_user_analysis),
        ('Unusual Days Analysis', run_unusual_days_analysis),
        ('Closing Entries Analysis', run_closing_entries_analysis),
        ('Holiday Analysis', run_holiday_analysis),
    ]
    
    for task_name, task_func in tasks:
        try:
            print(f"📋 Running {task_name}...")
            result = task_func(job.id)
            
            if result.get('success'):
                print(f"✅ {task_name} completed successfully")
                results[task_name] = True
            else:
                print(f"❌ {task_name} failed: {result.get('error', 'Unknown error')}")
                results[task_name] = False
                
        except Exception as e:
            print(f"❌ {task_name} crashed: {e}")
            results[task_name] = False
    
    # Test overall and risk analysis
    try:
        print("📋 Running Overall Analysis...")
        overall_result = run_overall_analysis(job.id)
        if overall_result.get('success'):
            print("✅ Overall Analysis completed successfully")
            results['Overall Analysis'] = True
        else:
            print(f"❌ Overall Analysis failed: {overall_result.get('error', 'Unknown error')}")
            results['Overall Analysis'] = False
    except Exception as e:
        print(f"❌ Overall Analysis crashed: {e}")
        results['Overall Analysis'] = False
    
    try:
        print("📋 Running Risk Analysis...")
        risk_result = run_risk_analysis(job.id)
        if risk_result.get('success'):
            print("✅ Risk Analysis completed successfully")
            results['Risk Analysis'] = True
        else:
            print(f"❌ Risk Analysis failed: {risk_result.get('error', 'Unknown error')}")
            results['Risk Analysis'] = False
    except Exception as e:
        print(f"❌ Risk Analysis crashed: {e}")
        results['Risk Analysis'] = False
    
    return results

def check_database_results(data_file):
    """Check if results were saved to database"""
    print("\n🔍 Checking Database Results...")
    
    results = {}
    
    # Check each analysis result table
    analysis_tables = [
        ('General Analysis', GeneralAnalysisResult),
        ('Duplicate Analysis', DuplicateAnalysisResult),
        ('Backdated Analysis', BackdatedAnalysisResult),
        ('User Analysis', UserAnalysisResult),
        ('Unusual Days Analysis', UnusualDaysAnalysisResult),
        ('Closing Entries Analysis', ClosingEntriesAnalysisResult),
        ('Holiday Analysis', HolidayAnalysisResult),
        ('Overall Analysis', OverallAnalysisResult),
        ('Risk Analysis', RiskScoringDocument),
    ]
    
    for analysis_name, model_class in analysis_tables:
        try:
            result = model_class.objects.filter(data_file=data_file).first()
            if result:
                print(f"✅ {analysis_name} results saved to database")
                print(f"   - ID: {result.id}")
                print(f"   - Status: {result.status}")
                results[analysis_name] = True
            else:
                print(f"❌ No {analysis_name} results found in database")
                results[analysis_name] = False
        except Exception as e:
            print(f"❌ Error checking {analysis_name}: {e}")
            results[analysis_name] = False
    
    return results

def cleanup_test_data(data_file):
    """Clean up test data"""
    print("\n🧹 Cleaning up test data...")
    
    try:
        # Remove related records
        SAPGLPosting.objects.filter(data_file=data_file).delete()
        FileProcessingJob.objects.filter(data_file=data_file).delete()
        
        # Remove analysis results
        GeneralAnalysisResult.objects.filter(data_file=data_file).delete()
        DuplicateAnalysisResult.objects.filter(data_file=data_file).delete()
        BackdatedAnalysisResult.objects.filter(data_file=data_file).delete()
        UserAnalysisResult.objects.filter(data_file=data_file).delete()
        UnusualDaysAnalysisResult.objects.filter(data_file=data_file).delete()
        ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).delete()
        HolidayAnalysisResult.objects.filter(data_file=data_file).delete()
        OverallAnalysisResult.objects.filter(data_file=data_file).delete()
        RiskScoringDocument.objects.filter(data_file=data_file).delete()
        
        # Remove data file
        data_file.delete()
        
        print("✅ Test data cleaned up successfully")
        return True
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 Starting Comprehensive Analysis and ML Test Suite")
    print("=" * 60)
    
    data_file = None
    
    try:
        # Create test data
        data_file = create_test_data()
        
        # Run tests
        ml_results = test_ml_models()
        holiday_utils_result = test_holiday_utilities()
        analysis_results = test_analysis_tasks(data_file)
        db_results = check_database_results(data_file)
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 Test Results Summary")
        print("=" * 60)
        
        # ML Models
        print("\n🤖 ML Models:")
        ml_passed = sum(1 for result in ml_results.values() if result)
        ml_total = len(ml_results)
        for name, result in ml_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {name}")
        print(f"  ML Models: {ml_passed}/{ml_total} passed")
        
        # Holiday Utilities
        print(f"\n🎄 Holiday Utilities: {'✅ PASS' if holiday_utils_result else '❌ FAIL'}")
        
        # Analysis Tasks
        print("\n📋 Analysis Tasks:")
        analysis_passed = sum(1 for result in analysis_results.values() if result)
        analysis_total = len(analysis_results)
        for name, result in analysis_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {name}")
        print(f"  Analysis Tasks: {analysis_passed}/{analysis_total} passed")
        
        # Database Results
        print("\n💾 Database Results:")
        db_passed = sum(1 for result in db_results.values() if result)
        db_total = len(db_results)
        for name, result in db_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {name}")
        print(f"  Database Results: {db_passed}/{db_total} passed")
        
        # Overall summary
        total_passed = ml_passed + (1 if holiday_utils_result else 0) + analysis_passed + db_passed
        total_tests = ml_total + 1 + analysis_total + db_total
        
        print(f"\n🎯 Overall Results: {total_passed}/{total_tests} tests passed")
        
        if total_passed == total_tests:
            print("🎉 All tests passed! Analysis and ML systems are working correctly.")
            return 0
        else:
            print("⚠️  Some tests failed. Please check the issues above.")
            return 1
            
    except Exception as e:
        print(f"❌ Test suite crashed: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Cleanup
        if data_file:
            cleanup_test_data(data_file)

if __name__ == "__main__":
    sys.exit(main())
