#!/usr/bin/env python3
"""
Simplified test script for analysis and ML integrations - Docker compatible
"""

import os
import sys
import django
from datetime import date, datetime, timedelta
import uuid

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def test_imports():
    """Test that all required modules can be imported"""
    print("🔍 Testing Module Imports...")
    
    results = {}
    
    try:
        from core.models import (
            FileProcessingJob, DataFile, SAPGLPosting, 
            GeneralAnalysisResult, DuplicateAnalysisResult, BackdatedAnalysisResult,
            UserAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult,
            HolidayAnalysisResult, OverallAnalysisResult, RiskScoringDocument
        )
        print("✅ Core models imported successfully")
        results['Core Models'] = True
    except Exception as e:
        print(f"❌ Core models import failed: {e}")
        results['Core Models'] = False
    
    try:
        from core.tasks import (
            run_general_analysis, run_duplicate_analysis, run_backdated_analysis,
            run_user_analysis, run_unusual_days_analysis, run_closing_entries_analysis,
            run_holiday_analysis, run_overall_analysis, run_risk_analysis
        )
        print("✅ Analysis tasks imported successfully")
        results['Analysis Tasks'] = True
    except Exception as e:
        print(f"❌ Analysis tasks import failed: {e}")
        results['Analysis Tasks'] = False
    
    try:
        from core.specialized_analysis_models import (
            AnalysisModelManager, DuplicateAnalysisModel, BackdatedAnalysisModel,
            UserAnalysisModel, UnusualDaysAnalysisModel, ClosingEntriesAnalysisModel,
            HolidayAnalysisModel, RiskAnalysisModel
        )
        print("✅ ML models imported successfully")
        results['ML Models'] = True
    except Exception as e:
        print(f"❌ ML models import failed: {e}")
        results['ML Models'] = False
    
    try:
        from core.holiday_utils import get_holidays, is_holiday
        print("✅ Holiday utilities imported successfully")
        results['Holiday Utilities'] = True
    except Exception as e:
        print(f"❌ Holiday utilities import failed: {e}")
        results['Holiday Utilities'] = False
    
    return results

def test_ml_models():
    """Test all ML models"""
    print("\n🔍 Testing ML Models...")
    
    results = {}
    
    try:
        from core.specialized_analysis_models import (
            AnalysisModelManager, DuplicateAnalysisModel, BackdatedAnalysisModel,
            UserAnalysisModel, UnusualDaysAnalysisModel, ClosingEntriesAnalysisModel,
            HolidayAnalysisModel, RiskAnalysisModel
        )
        
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
        from core.holiday_utils import get_holidays, is_holiday
        
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

def test_task_functions():
    """Test that task functions can be called (without database)"""
    print("\n🔍 Testing Task Functions...")
    
    results = {}
    
    try:
        from core.tasks import (
            run_general_analysis, run_duplicate_analysis, run_backdated_analysis,
            run_user_analysis, run_unusual_days_analysis, run_closing_entries_analysis,
            run_holiday_analysis, run_overall_analysis, run_risk_analysis
        )
        
        # Test that functions exist and are callable
        tasks = [
            ('General Analysis', run_general_analysis),
            ('Duplicate Analysis', run_duplicate_analysis),
            ('Backdated Analysis', run_backdated_analysis),
            ('User Analysis', run_user_analysis),
            ('Unusual Days Analysis', run_unusual_days_analysis),
            ('Closing Entries Analysis', run_closing_entries_analysis),
            ('Holiday Analysis', run_holiday_analysis),
            ('Overall Analysis', run_overall_analysis),
            ('Risk Analysis', run_risk_analysis),
        ]
        
        for task_name, task_func in tasks:
            try:
                # Just test that the function exists and is callable
                if callable(task_func):
                    print(f"✅ {task_name} function is callable")
                    results[task_name] = True
                else:
                    print(f"❌ {task_name} function is not callable")
                    results[task_name] = False
            except Exception as e:
                print(f"❌ {task_name} function test failed: {e}")
                results[task_name] = False
        
        return results
        
    except Exception as e:
        print(f"❌ Task functions test failed: {e}")
        return {}

def test_ml_prediction():
    """Test ML prediction functionality"""
    print("\n🔍 Testing ML Prediction...")
    
    try:
        from core.specialized_analysis_models import HolidayAnalysisModel
        
        # Create test data
        test_transactions = [
            {
                'posting_date': date(2024, 9, 23),  # Saudi National Day
                'document_date': date(2024, 9, 23),
                'amount_local_currency': 150000,
                'user_name': 'USER_HOLIDAY',
                'gl_account': '4001',
                'document_number': 'HOLIDAY_001',
                'document_type': 'MANUAL'
            },
            {
                'posting_date': date(2024, 6, 15),
                'document_date': date(2024, 6, 15),
                'amount_local_currency': 50000,
                'user_name': 'USER_NORMAL',
                'gl_account': '1001',
                'document_number': 'NORMAL_001',
                'document_type': 'INVOICE'
            }
        ]
        
        # Test HolidayAnalysisModel prediction
        holiday_model = HolidayAnalysisModel()
        predictions = holiday_model.predict(test_transactions)
        
        print(f"✅ Holiday model prediction completed")
        print(f"   - Predictions: {len(predictions)}")
        print(f"   - Model trained: {holiday_model.is_trained}")
        
        return True
        
    except Exception as e:
        print(f"❌ ML prediction test failed: {e}")
        return False

def test_celery_integration():
    """Test Celery integration"""
    print("\n🔍 Testing Celery Integration...")
    
    try:
        from analytics.celery import app
        
        # Test that Celery app is configured
        print(f"✅ Celery app loaded: {app}")
        print(f"   - Broker: {app.conf.broker_url}")
        print(f"   - Result backend: {app.conf.result_backend}")
        
        # Test task registration
        registered_tasks = app.tasks.keys()
        analysis_tasks = [task for task in registered_tasks if 'analysis' in task.lower()]
        print(f"✅ Found {len(analysis_tasks)} analysis-related tasks")
        
        return True
        
    except Exception as e:
        print(f"❌ Celery integration test failed: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 Starting Docker-Compatible Analysis and ML Test Suite")
    print("=" * 60)
    
    try:
        # Run tests
        import_results = test_imports()
        ml_results = test_ml_models()
        holiday_utils_result = test_holiday_utilities()
        task_results = test_task_functions()
        ml_prediction_result = test_ml_prediction()
        celery_result = test_celery_integration()
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 Test Results Summary")
        print("=" * 60)
        
        # Import Tests
        print("\n📦 Module Imports:")
        import_passed = sum(1 for result in import_results.values() if result)
        import_total = len(import_results)
        for name, result in import_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {name}")
        print(f"  Module Imports: {import_passed}/{import_total} passed")
        
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
        
        # Task Functions
        print("\n📋 Task Functions:")
        task_passed = sum(1 for result in task_results.values() if result)
        task_total = len(task_results)
        for name, result in task_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {name}")
        print(f"  Task Functions: {task_passed}/{task_total} passed")
        
        # ML Prediction
        print(f"\n🔮 ML Prediction: {'✅ PASS' if ml_prediction_result else '❌ FAIL'}")
        
        # Celery Integration
        print(f"\n⚡ Celery Integration: {'✅ PASS' if celery_result else '❌ FAIL'}")
        
        # Overall summary
        total_passed = import_passed + ml_passed + (1 if holiday_utils_result else 0) + task_passed + (1 if ml_prediction_result else 0) + (1 if celery_result else 0)
        total_tests = import_total + ml_total + 1 + task_total + 1 + 1
        
        print(f"\n🎯 Overall Results: {total_passed}/{total_tests} tests passed")
        
        if total_passed == total_tests:
            print("🎉 All tests passed! Analysis and ML systems are working correctly in Docker.")
            return 0
        else:
            print("⚠️  Some tests failed. Please check the issues above.")
            return 1
            
    except Exception as e:
        print(f"❌ Test suite crashed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
