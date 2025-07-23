#!/usr/bin/env python
"""
Script to test duplicate analysis ML model directly
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from core.models import SAPGLPosting, DataFile
from core.ml_models import MLAnomalyDetector
from core.enhanced_duplicate_analysis import EnhancedDuplicateAnalyzer

def test_duplicate_analysis():
    """Test duplicate analysis ML model directly"""
    print("🧪 Testing Duplicate Analysis ML Model...")
    
    try:
        # Get some transactions
        transactions = list(SAPGLPosting.objects.all()[:1000])  # Get first 1000 transactions
        print(f"✅ Retrieved {len(transactions)} transactions for testing")
        
        if not transactions:
            print("❌ No transactions found in database")
            return False
        
        # Test 1: Enhanced Duplicate Analyzer
        print("\n🔍 Test 1: Enhanced Duplicate Analyzer")
        try:
            enhanced_analyzer = EnhancedDuplicateAnalyzer()
            enhanced_result = enhanced_analyzer.analyze_duplicates(transactions)
            print(f"✅ Enhanced analyzer completed")
            print(f"   Duplicates found: {len(enhanced_result.get('duplicate_list', []))}")
            print(f"   Analysis info: {enhanced_result.get('analysis_info', {})}")
        except Exception as e:
            print(f"❌ Enhanced analyzer failed: {e}")
            return False
        
        # Test 2: ML Anomaly Detector
        print("\n🔍 Test 2: ML Anomaly Detector")
        try:
            ml_detector = MLAnomalyDetector()
            print(f"✅ ML detector initialized")
            print(f"   Duplicate model available: {ml_detector.duplicate_model is not None}")
            
            if ml_detector.duplicate_model:
                print(f"   Duplicate model trained: {ml_detector.duplicate_model.is_trained()}")
                
                # Test comprehensive analysis
                file_id = "test_file_123"
                comprehensive_result = ml_detector.duplicate_model.run_comprehensive_analysis(transactions, file_id)
                print(f"✅ Comprehensive analysis completed")
                print(f"   Duplicates found: {len(comprehensive_result.get('duplicate_list', []))}")
                
                # Test if analysis was saved
                has_saved = ml_detector.duplicate_model.has_saved_analysis(file_id)
                print(f"   Analysis saved: {has_saved}")
                
                if has_saved:
                    saved_analysis = ml_detector.duplicate_model.get_saved_analysis(file_id)
                    print(f"   Saved analysis retrieved: {len(saved_analysis.get('duplicate_list', []))} duplicates")
            else:
                print("❌ Duplicate model not available")
                return False
                
        except Exception as e:
            print(f"❌ ML detector failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # Test 3: Direct duplicate analysis
        print("\n🔍 Test 3: Direct Duplicate Analysis")
        try:
            if ml_detector.duplicate_model:
                # Test the enhanced analyzer directly
                direct_result = ml_detector.duplicate_model.run_comprehensive_duplicate_analysis(transactions)
                print(f"✅ Direct analysis completed")
                print(f"   Duplicates found: {len(direct_result.get('duplicate_list', []))}")
                
                # Test model info
                model_info = ml_detector.duplicate_model.get_model_info()
                print(f"   Model info: {model_info}")
            else:
                print("❌ Cannot test direct analysis - duplicate model not available")
                
        except Exception as e:
            print(f"❌ Direct analysis failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        print("\n✅ All duplicate analysis tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error in duplicate analysis test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    test_duplicate_analysis() 