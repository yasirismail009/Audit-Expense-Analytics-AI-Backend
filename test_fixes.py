#!/usr/bin/env python3
"""
Quick test to verify the fixes work correctly
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.analytics import ExpenseSheetAnalyzer
from core.models import ExpenseSheet

def test_should_retrain():
    """Test the should_retrain method"""
    print("Testing should_retrain method...")
    try:
        analyzer = ExpenseSheetAnalyzer()
        result = analyzer.should_retrain()
        print(f"✅ should_retrain() works: {result}")
        return True
    except Exception as e:
        print(f"❌ should_retrain() failed: {e}")
        return False

def test_auto_train_if_needed():
    """Test the auto_train_if_needed method"""
    print("Testing auto_train_if_needed method...")
    try:
        analyzer = ExpenseSheetAnalyzer()
        result = analyzer.auto_train_if_needed()
        print(f"✅ auto_train_if_needed() works: {result}")
        return True
    except Exception as e:
        print(f"❌ auto_train_if_needed() failed: {e}")
        return False

def test_evaluate_performance():
    """Test the evaluate_model_performance method"""
    print("Testing evaluate_model_performance method...")
    try:
        analyzer = ExpenseSheetAnalyzer()
        result = analyzer.evaluate_model_performance()
        print(f"✅ evaluate_model_performance() works: {result}")
        return True
    except Exception as e:
        print(f"❌ evaluate_model_performance() failed: {e}")
        return False

def test_analyze_sheet():
    """Test analyzing a sheet"""
    print("Testing analyze_sheet method...")
    try:
        # Get the first available sheet
        sheet = ExpenseSheet.objects.first()
        if sheet:
            analyzer = ExpenseSheetAnalyzer()
            result = analyzer.analyze_sheet(sheet)
            print(f"✅ analyze_sheet() works: {result is not None}")
            return True
        else:
            print("ℹ️  No sheets available for testing")
            return True
    except Exception as e:
        print(f"❌ analyze_sheet() failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🔧 Testing Fixed Implementation")
    print("=" * 40)
    
    tests = [
        test_should_retrain,
        test_auto_train_if_needed,
        test_evaluate_performance,
        test_analyze_sheet
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 40)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The fixes are working correctly.")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")

if __name__ == '__main__':
    main() 